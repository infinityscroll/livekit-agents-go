// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/llm"
	"github.com/infinityscroll/livekit-agents-go/stream"
)

func TestAgentSessionLifecycleSayAndHandoff(t *testing.T) {
	enteredOne, exitedOne, enteredTwo := make(chan struct{}), make(chan struct{}), make(chan struct{})
	agentOne := MustAgent(AgentOptions[struct{}]{
		ID: "agent_one",
		Hooks: AgentHooks[struct{}]{
			OnEnter: func(context.Context, *AgentContext[struct{}]) error { close(enteredOne); return nil },
			OnExit:  func(context.Context, *AgentContext[struct{}]) error { close(exitedOne); return nil },
		},
	})
	agentTwo := MustAgent(AgentOptions[struct{}]{
		ID: "agent_two",
		Hooks: AgentHooks[struct{}]{
			OnEnter: func(context.Context, *AgentContext[struct{}]) error { close(enteredTwo); return nil },
		},
	})
	session, err := NewAgentSession(AgentSessionOptions[struct{}]{DisableUserAwayTimeout: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close(context.Background()) })

	var transcriptMu sync.Mutex
	var transcript strings.Builder
	textOutput := NewManagedTextOutput(TextOutputOptions{
		Capture: func(_ context.Context, value agents.TimedString) error {
			transcriptMu.Lock()
			transcript.WriteString(value.Text)
			transcriptMu.Unlock()
			return nil
		},
	})
	session.Output().SetTranscription(textOutput)

	if err := session.Start(t.Context(), agentOne); err != nil {
		t.Fatal(err)
	}
	waitClosed(t, enteredOne, "first onEnter")
	handle, err := session.Say(t.Context(), "hello", SayOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := handle.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	transcriptMu.Lock()
	gotTranscript := transcript.String()
	transcriptMu.Unlock()
	if gotTranscript != "hello" {
		t.Fatalf("transcript = %q", gotTranscript)
	}

	if err := session.UpdateAgent(t.Context(), agentTwo); err != nil {
		t.Fatal(err)
	}
	waitClosed(t, exitedOne, "first onExit")
	waitClosed(t, enteredTwo, "second onEnter")
	if session.Agent() != agentTwo {
		t.Fatal("new agent was not installed")
	}
	items := session.ChatContext().Items()
	var handoffs, assistant int
	for _, item := range items {
		switch item.(type) {
		case *llm.AgentHandoffItem:
			handoffs++
		case *llm.ChatMessage:
			assistant++
		}
	}
	if handoffs != 2 || assistant != 1 {
		t.Fatalf("history has %d handoffs and %d messages", handoffs, assistant)
	}
	if err := session.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Say(t.Context(), "late", SayOptions{}); !errors.Is(err, ErrSessionNotStarted) && !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("say after close = %v", err)
	}
}

func TestAgentSessionInterruptAndReplyAuthorization(t *testing.T) {
	model := newSessionTestLLM([]sessionLLMResponse{{text: "authorized reply"}})
	session, err := NewAgentSession(AgentSessionOptions[struct{}]{LLM: model, DisableUserAwayTimeout: true})
	if err != nil {
		t.Fatal(err)
	}
	agent := MustAgent(AgentOptions[struct{}]{ID: "assistant"})
	if err := session.Start(t.Context(), agent); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close(context.Background()) })
	if err := session.PauseReplyAuthorization(); err != nil {
		t.Fatal(err)
	}
	handle, err := session.GenerateReply(t.Context(), GenerateReplyOptions{UserInput: "question"})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-handle.InterruptSignal():
		t.Fatal("reply was interrupted")
	case <-time.After(20 * time.Millisecond):
	}
	if handle.Done() {
		t.Fatal("reply completed while authorization was paused")
	}
	if err := session.ResumeReplyAuthorization(); err != nil {
		t.Fatal(err)
	}
	if err := handle.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}

	text := stream.NewChannel[string](1)
	blocking, err := session.SayStream(t.Context(), text, SayOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := session.Interrupt(t.Context(), false); err != nil {
		t.Fatal(err)
	}
	if err := blocking.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !blocking.Interrupted() {
		t.Fatal("blocking speech was not interrupted")
	}
}

func TestAgentSessionCloseEventDispatchedBeforeBusClose(t *testing.T) {
	session, err := NewAgentSession(AgentSessionOptions[struct{}]{DisableUserAwayTimeout: true})
	if err != nil {
		t.Fatal(err)
	}
	subscription, err := session.Subscribe(EventSubscriptionOptions{Capacity: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()

	if err := session.Close(t.Context()); err != nil {
		t.Fatal(err)
	}

	found := false
	for {
		event, recvErr := subscription.Recv(t.Context())
		if recvErr != nil {
			break
		}
		if event.Type() == EventClose {
			found = true
			closeEvent, ok := event.(CloseEvent)
			if !ok {
				t.Fatalf("close event type = %T", event)
			}
			if closeEvent.Reason != CloseReasonUserInitiated || closeEvent.Err != nil {
				t.Fatalf("close event = %#v", closeEvent)
			}
		}
	}
	if !found {
		t.Fatal("close event was not delivered before the event bus closed")
	}
}

func TestAgentSessionCloseReportsCloseEventBarrierTimeout(t *testing.T) {
	const shutdownTimeout = 50 * time.Millisecond
	session, err := NewAgentSession(AgentSessionOptions[struct{}]{
		DisableUserAwayTimeout: true,
		ShutdownTimeout:        shutdownTimeout,
	})
	if err != nil {
		t.Fatal(err)
	}
	subscription, err := session.Subscribe(EventSubscriptionOptions{Capacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()

	// Fill the subscriber before Close. PublishAndWait confirms that the first
	// event has left the global queue and occupies the subscriber's only slot.
	first := DebugMessageEvent{
		EventBase: newEventBase(EventDebugMessage, time.Now()),
		Payload:   map[string]any{"message": "fill subscriber"},
	}
	if err := session.events.PublishAndWait(t.Context(), first); err != nil {
		t.Fatal(err)
	}

	closeErr := session.Close(t.Context())
	if !errors.Is(closeErr, context.DeadlineExceeded) {
		t.Fatalf("close error = %v, want close-event barrier deadline", closeErr)
	}
	if !strings.Contains(closeErr.Error(), "dispatch close event") {
		t.Fatalf("close error = %q, want dispatch context", closeErr)
	}
}

func TestAgentSessionRejectsTypedNilAndConflictingModelConfiguration(t *testing.T) {
	var typedNil *sessionTestLLM
	if _, err := NewAgentSession(AgentSessionOptions[struct{}]{LLM: typedNil}); err == nil || !strings.Contains(err.Error(), "typed nil") {
		t.Fatalf("typed-nil model error = %v", err)
	}
	model := newSessionTestLLM(nil)
	if _, err := NewAgentSession(AgentSessionOptions[struct{}]{LLM: model, LLMModel: "openai/example"}); err == nil || !strings.Contains(err.Error(), "either a concrete model") {
		t.Fatalf("conflicting model error = %v", err)
	}
}

func TestAgentSessionLLMErrorBudgetAllowsConfiguredCount(t *testing.T) {
	model := newSessionTestLLM(nil)
	session, err := NewAgentSession(AgentSessionOptions[struct{}]{LLM: model, DisableUserAwayTimeout: true})
	if err != nil {
		t.Fatal(err)
	}
	subscription, err := session.Subscribe(EventSubscriptionOptions{Capacity: 16})
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	if err := session.Start(t.Context(), MustAgent(AgentOptions[struct{}]{ID: "error_budget"})); err != nil {
		t.Fatal(err)
	}

	for index := 0; index < agents.DefaultSessionConnectOptions.MaxUnrecoverableErrors; index++ {
		model.EmitError(llm.ErrorEvent{Err: errors.New("transiently unrecoverable"), Recoverable: false})
		select {
		case <-session.ctx.Done():
			t.Fatalf("session closed after allowed LLM error %d", index+1)
		default:
		}
	}
	model.EmitError(llm.ErrorEvent{Err: errors.New("budget exceeded"), Recoverable: false})

	foundClose := false
	for !foundClose {
		event, recvErr := subscription.Recv(t.Context())
		if recvErr != nil {
			t.Fatalf("receive close event: %v", recvErr)
		}
		if closeEvent, ok := event.(CloseEvent); ok {
			foundClose = true
			if closeEvent.Reason != CloseReasonError {
				t.Fatalf("close reason = %q", closeEvent.Reason)
			}
		}
	}
}

func TestAgentActivityCloseRunsExitHookOnce(t *testing.T) {
	var exits atomic.Int64
	agent := MustAgent(AgentOptions[struct{}]{ID: "close_once", Hooks: AgentHooks[struct{}]{
		OnExit: func(context.Context, *AgentContext[struct{}]) error { exits.Add(1); return nil },
	}})
	session, err := NewAgentSession(AgentSessionOptions[struct{}]{DisableUserAwayTimeout: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := session.Start(t.Context(), agent); err != nil {
		t.Fatal(err)
	}
	activity, err := session.currentActivity()
	if err != nil {
		t.Fatal(err)
	}
	errs := make(chan error, 2)
	for range 2 {
		go func() { errs <- activity.close(t.Context()) }()
	}
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if exits.Load() != 1 {
		t.Fatalf("onExit calls = %d", exits.Load())
	}
	_ = session.Close(context.Background())
}

func waitClosed(t *testing.T, done <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", label)
	}
}
