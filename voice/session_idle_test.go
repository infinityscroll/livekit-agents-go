// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"context"
	"errors"
	"testing"
	"time"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/llm"
	"github.com/livekit/agents-go/stream"
	"github.com/livekit/agents-go/vad"
)

func disableVAD() agents.Override[vad.VAD] { return agents.Disable[vad.VAD]() }
func disableTurnDetection() TurnDetectionUpdate {
	return agents.Disable[TurnDetection]()
}

func TestAgentSessionWaitForIdleIsEventDriven(t *testing.T) {
	session, err := NewAgentSession(AgentSessionOptions[struct{}]{
		VADSelection:           disableVAD(),
		DisableUserAwayTimeout: true,
		TurnHandling: &TurnHandlingOptions{
			TurnDetection:        disableTurnDetection(),
			Endpointing:          DefaultEndpointingOptions,
			Interruption:         DefaultInterruptionOptions(),
			PreemptiveGeneration: DefaultPreemptiveGenerationOptions,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	agent := MustAgent(AgentOptions[struct{}]{ID: "idle_agent"})
	if err := session.Start(t.Context(), agent); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close(context.Background()) })

	text := stream.NewChannel[string](1)
	handle, err := session.SayStream(t.Context(), text, SayOptions{})
	if err != nil {
		t.Fatal(err)
	}
	waited := make(chan error, 1)
	go func() {
		_, waitErr := session.WaitForIdle(t.Context())
		waited <- waitErr
	}()
	select {
	case err := <-waited:
		t.Fatalf("WaitForIdle returned while speech was open: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if err := text.Close(); err != nil {
		t.Fatal(err)
	}
	if err := handle.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-waited:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitForIdle was not released by the activity notification")
	}
}

func TestAgentSessionWaitForIdleAndHoldReentrant(t *testing.T) {
	session, err := NewAgentSession(AgentSessionOptions[struct{}]{
		VADSelection:           disableVAD(),
		DisableUserAwayTimeout: true,
		TurnHandling: &TurnHandlingOptions{
			TurnDetection:        disableTurnDetection(),
			Endpointing:          DefaultEndpointingOptions,
			Interruption:         DefaultInterruptionOptions(),
			PreemptiveGeneration: DefaultPreemptiveGenerationOptions,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	agent := MustAgent(AgentOptions[struct{}]{ID: "hold_agent"})
	if err := session.Start(t.Context(), agent); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close(context.Background()) })

	entered, release := make(chan struct{}), make(chan struct{})
	holdDone := make(chan error, 1)
	go func() {
		holdDone <- session.WaitForIdleAndHold(t.Context(), func(holdCtx context.Context, current *Agent[struct{}]) error {
			if current != agent {
				return errors.New("hold received the wrong agent")
			}
			value, nestedErr := WaitForIdleAndHoldValue(holdCtx, session, func(context.Context, *Agent[struct{}]) (string, error) {
				return "nested", nil
			})
			if nestedErr != nil || value != "nested" {
				return errors.New("nested idle hold failed")
			}
			close(entered)
			<-release
			return nil
		})
	}()
	waitClosed(t, entered, "outer idle hold")

	waiter := make(chan error, 1)
	go func() {
		_, waitErr := session.WaitForIdle(t.Context())
		waiter <- waitErr
	}()
	select {
	case err := <-waiter:
		t.Fatalf("separate idle waiter crossed an active hold: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-holdDone; err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-waiter:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("idle waiter was not released after the final nested hold")
	}
}

func TestAgentSessionWaitForIdleAbortsOnClose(t *testing.T) {
	session, err := NewAgentSession(AgentSessionOptions[struct{}]{VADSelection: disableVAD(), DisableUserAwayTimeout: true})
	if err != nil {
		t.Fatal(err)
	}
	agent := MustAgent(AgentOptions[struct{}]{ID: "closing_agent"})
	if err := session.Start(t.Context(), agent); err != nil {
		t.Fatal(err)
	}
	text := stream.NewChannel[string](1)
	if _, err := session.SayStream(t.Context(), text, SayOptions{}); err != nil {
		t.Fatal(err)
	}
	waited := make(chan error, 1)
	go func() {
		_, waitErr := session.WaitForIdle(context.Background())
		waited <- waitErr
	}()
	if err := session.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-waited:
		if !errors.Is(err, ErrSessionClosing) && !errors.Is(err, ErrSessionClosed) {
			t.Fatalf("WaitForIdle close error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitForIdle leaked across session close")
	}
}

func TestAgentSessionWaitForIdleRetargetsAfterFailedHandoff(t *testing.T) {
	session, err := NewAgentSession(AgentSessionOptions[struct{}]{
		VADSelection:           disableVAD(),
		DisableUserAwayTimeout: true,
		TurnHandling: &TurnHandlingOptions{
			TurnDetection:        disableTurnDetection(),
			Endpointing:          DefaultEndpointingOptions,
			Interruption:         DefaultInterruptionOptions(),
			PreemptiveGeneration: DefaultPreemptiveGenerationOptions,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	first := MustAgent(AgentOptions[struct{}]{ID: "handoff_first"})
	if err := session.Start(t.Context(), first); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close(context.Background()) })

	model := newBlockingOpenRealtimeModel()
	override := agents.Use[llm.RealtimeModel](model)
	second := MustAgent(AgentOptions[struct{}]{ID: "handoff_second", Realtime: override})
	updateCtx, cancelUpdate := context.WithCancelCause(t.Context())
	updateDone := make(chan error, 1)
	go func() { updateDone <- session.UpdateAgent(updateCtx, second) }()
	waitClosed(t, model.entered, "handoff realtime activation")

	waitDone := make(chan struct {
		agent *Agent[struct{}]
		err   error
	}, 1)
	go func() {
		agent, waitErr := session.WaitForIdle(t.Context())
		waitDone <- struct {
			agent *Agent[struct{}]
			err   error
		}{agent: agent, err: waitErr}
	}()
	select {
	case result := <-waitDone:
		t.Fatalf("WaitForIdle crossed an in-flight handoff: agent=%v err=%v", result.agent, result.err)
	case <-time.After(20 * time.Millisecond):
	}

	handoffErr := errors.New("cancel test handoff")
	cancelUpdate(handoffErr)
	if err := <-updateDone; !errors.Is(err, handoffErr) {
		t.Fatalf("UpdateAgent error = %v", err)
	}
	select {
	case result := <-waitDone:
		if result.err != nil || result.agent != first {
			t.Fatalf("WaitForIdle retarget result: agent=%v err=%v", result.agent, result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitForIdle did not resume after handoff rollback")
	}
}
