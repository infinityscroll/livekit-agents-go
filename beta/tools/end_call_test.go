// SPDX-License-Identifier: Apache-2.0

package tools

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/llm"
	"github.com/infinityscroll/livekit-agents-go/voice"
)

type endCallTestSession struct {
	bus    *voice.EventBus
	state  voice.AgentState
	mu     sync.Mutex
	closes int
	closed chan struct{}
}

func newEndCallTestSession(t *testing.T) *endCallTestSession {
	t.Helper()
	return &endCallTestSession{bus: voice.NewEventBus(t.Context(), voice.EventBusOptions{}), state: voice.AgentStateThinking, closed: make(chan struct{})}
}
func (s *endCallTestSession) Subscribe(options voice.EventSubscriptionOptions) (*voice.EventSubscription, error) {
	return s.bus.Subscribe(options)
}
func (s *endCallTestSession) AgentState() voice.AgentState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}
func (s *endCallTestSession) Close(context.Context, ...voice.CloseOptions) error {
	s.mu.Lock()
	s.closes++
	if s.closes == 1 {
		close(s.closed)
	}
	s.mu.Unlock()
	return nil
}
func (s *endCallTestSession) publish(t *testing.T, event voice.Event) {
	t.Helper()
	if err := s.bus.PublishAndWait(t.Context(), event); err != nil {
		t.Fatal(err)
	}
}

type endCallTestJob struct {
	mu        sync.Mutex
	callbacks []func(context.Context, string) error
	shutdown  []string
	deletes   int
}

func (j *endCallTestJob) AddShutdownCallback(callback func(context.Context, string) error) error {
	j.mu.Lock()
	j.callbacks = append(j.callbacks, callback)
	j.mu.Unlock()
	return nil
}
func (j *endCallTestJob) DeleteRoom(context.Context, string) error {
	j.mu.Lock()
	j.deletes++
	j.mu.Unlock()
	return nil
}
func (j *endCallTestJob) Shutdown(reason string) {
	j.mu.Lock()
	j.shutdown = append(j.shutdown, reason)
	callbacks := append([]func(context.Context, string) error(nil), j.callbacks...)
	j.mu.Unlock()
	for _, callback := range callbacks {
		_ = callback(context.Background(), reason)
	}
}

func endCallExecutable(t *testing.T, set *llm.Toolset) llm.ExecutableTool {
	t.Helper()
	tools := set.Tools()
	if len(tools) != 1 {
		t.Fatalf("tools=%d", len(tools))
	}
	tool, ok := tools[0].(llm.ExecutableTool)
	if !ok {
		t.Fatal("not executable")
	}
	return tool
}

func TestEndCallToolCallbacksGoodbyeAndShutdown(t *testing.T) {
	session, job := newEndCallTestSession(t), &endCallTestJob{}
	var called, completed bool
	set, err := CreateEndCallTool(EndCallToolOptions[struct{}]{
		Job: job, ReplyTimeout: time.Second, ShutdownTimeout: time.Second,
		OnToolCalled: func(_ context.Context, event EndCallToolCalledEvent[struct{}]) error {
			called = event.Context != nil
			return nil
		},
		OnToolCompleted: func(_ context.Context, event EndCallToolCompletedEvent[struct{}]) error {
			completed = event.Output != nil && event.Output.Value == "say goodbye to the user"
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	tool := endCallExecutable(t, set)
	value, err := tool.Execute(t.Context(), json.RawMessage(`{}`), llm.ToolOptions{Context: &llm.RunContext{Session: session}})
	if err != nil || value != "say goodbye to the user" || !called || !completed {
		t.Fatalf("value=%v err=%v called=%v completed=%v", value, err, called, completed)
	}
	select {
	case <-session.closed:
		t.Fatal("session closed before goodbye completed")
	default:
	}
	session.publish(t, voice.NewAgentStateChangedEvent(voice.AgentStateSpeaking, voice.AgentStateListening, time.Now()))
	select {
	case <-session.closed:
	case <-time.After(time.Second):
		t.Fatal("session did not close")
	}
	job.mu.Lock()
	shutdown, deletes := append([]string(nil), job.shutdown...), job.deletes
	job.mu.Unlock()
	if len(shutdown) != 1 || shutdown[0] != string(voice.CloseReasonUserInitiated) || deletes != 1 {
		t.Fatalf("shutdown=%v deletes=%d", shutdown, deletes)
	}
	if err = set.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	_ = session.bus.Close()
}

func TestEndCallToolNullInstructionsClosesImmediatelyAndHonorsFlags(t *testing.T) {
	session := newEndCallTestSession(t)
	set, err := CreateEndCallTool(EndCallToolOptions[struct{}]{EndInstructions: agents.Disable[string](), IgnoreOnEnter: true, ReplyTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	tool := endCallExecutable(t, set)
	if tool.Flags() != llm.ToolFlagIgnoreOnEnter {
		t.Fatalf("flags=%d", tool.Flags())
	}
	value, err := tool.Execute(t.Context(), nil, llm.ToolOptions{Context: &llm.RunContext{Session: session}})
	if err != nil || value != nil {
		t.Fatalf("value=%v err=%v", value, err)
	}
	select {
	case <-session.closed:
	case <-time.After(time.Second):
		t.Fatal("session did not close")
	}
	if err = set.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	_ = session.bus.Close()
}

func TestEndCallToolsetCloseCancelsPendingShutdown(t *testing.T) {
	session := newEndCallTestSession(t)
	set, err := CreateEndCallTool(EndCallToolOptions[struct{}]{ReplyTimeout: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	tool := endCallExecutable(t, set)
	if _, err = tool.Execute(t.Context(), nil, llm.ToolOptions{Context: &llm.RunContext{Session: session}}); err != nil {
		t.Fatal(err)
	}
	closeCtx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err = set.Close(closeCtx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-session.closed:
		t.Fatal("toolset teardown closed session")
	default:
	}
	_ = session.bus.Close()
}

func TestEndCallCallbackPanicReturnsToolError(t *testing.T) {
	session := newEndCallTestSession(t)
	set, err := CreateEndCallTool(EndCallToolOptions[struct{}]{OnToolCalled: func(context.Context, EndCallToolCalledEvent[struct{}]) error { panic("bug") }})
	if err != nil {
		t.Fatal(err)
	}
	_, err = endCallExecutable(t, set).Execute(t.Context(), nil, llm.ToolOptions{Context: &llm.RunContext{Session: session}})
	if err == nil || !errors.Is(err, err) || !stringsContainsTest(err.Error(), "panicked") {
		t.Fatalf("error=%v", err)
	}
	_ = set.Close(t.Context())
	_ = session.bus.Close()
}

func stringsContainsTest(value, part string) bool {
	for index := 0; index+len(part) <= len(value); index++ {
		if value[index:index+len(part)] == part {
			return true
		}
	}
	return false
}
