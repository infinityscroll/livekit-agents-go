// SPDX-License-Identifier: Apache-2.0

package voicetest

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/livekit/agents-go/llm"
)

func TestRunResultOrderingDedupeWaitAndOutput(t *testing.T) {
	result := NewRunResult[string]("hello")
	base := time.Now()
	assistant := llm.NewChatMessage(llm.RoleAssistant, "welcome")
	assistant.CreatedAt = base.Add(time.Second)
	user := llm.NewChatMessage(llm.RoleUser, "hello")
	user.CreatedAt = base
	if !result.Record(assistant) || !result.Record(user) || result.Record(user) {
		t.Fatal("record/dedupe mismatch")
	}
	result.Complete("done", true, nil)
	if _, err := result.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	output, err := result.FinalOutput()
	if err != nil || output != "done" {
		t.Fatalf("output=%q err=%v", output, err)
	}
	events := result.Events()
	if events[0].Item.ItemID() != user.ID || events[1].Item.ItemID() != assistant.ID {
		t.Fatalf("events not chronological: %+v", events)
	}
	assert := result.Require(t)
	assert.NextEvent().IsMessage(MessageAssertOptions{Role: llm.RoleUser})
	assert.NextEvent(EventMessage).IsMessage(MessageAssertOptions{Role: llm.RoleAssistant})
	assert.NoMoreEvents()
}

func TestRunResultFunctionAssertionsRangeAndHandoff(t *testing.T) {
	result := NewRunResult[struct{}]("")
	call := llm.NewFunctionCall("c1", "weather", `{"city":"Paris","days":2}`)
	output := llm.NewFunctionCallOutput("c1", "weather", "sunny", false)
	handoff := &llm.AgentHandoffItem{ID: "h1", OldAgentID: "old", NewAgentID: "new", CreatedAt: time.Now().Add(time.Second)}
	result.Record(call)
	result.Record(output)
	result.RecordHandoff(handoff, "old-agent", &struct{ Name string }{"new"})
	assert := result.Require(t)
	assert.All().ContainsFunctionCall(FunctionCallAssertOptions{Name: "weather", Args: map[string]any{"city": "Paris", "days": float64(2)}})
	falseValue := false
	text := "sunny"
	assert.All().ContainsFunctionCallOutput(FunctionCallOutputAssertOptions{Output: &text, IsError: &falseValue})
	assert.All().ContainsAgentHandoff(AgentHandoffAssertOptions{NewAgentID: "new", NewAgentType: reflect.TypeOf(&struct{ Name string }{})})
}

func TestRunResultAssertionErrorAndDeadline(t *testing.T) {
	result := NewRunResult[struct{}]("")
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if _, err := result.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline, got %v", err)
	}
	defer func() {
		var assertion *AssertionError
		if recovered := recover(); !errors.As(asError(recovered), &assertion) {
			t.Fatalf("expected AssertionError, got %#v", recovered)
		}
	}()
	result.Expect().NextEvent()
}

func asError(value any) error {
	if err, ok := value.(error); ok {
		return err
	}
	return nil
}

func TestMessageJudgeWithFakeLLM(t *testing.T) {
	judge, err := NewFakeLLM(FakeLLMOptions{Responses: []FakeLLMResponse{{
		Input:     "Intent:\nwelcomes the user\n\nMessage:\nwelcome aboard",
		ToolCalls: []FakeToolCall{{Name: "check_intent", Args: map[string]any{"success": true, "reason": "clear welcome"}}},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	defer judge.Close(context.Background())
	result := NewRunResult[struct{}]("")
	result.Record(llm.NewChatMessage(llm.RoleAssistant, "welcome aboard"))
	message := result.Require(t).At(0).IsMessage(MessageAssertOptions{Role: llm.RoleAssistant})
	if err := message.Judge(context.Background(), judge, "welcomes the user"); err != nil {
		t.Fatal(err)
	}
}
