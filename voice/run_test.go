// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"context"
	"errors"
	"testing"

	"github.com/livekit/agents-go/llm"
	voicetest "github.com/livekit/agents-go/voice/testing"
)

func TestAgentSessionRunRecordsTurnAndRejectsNestedRun(t *testing.T) {
	model := newSessionTestLLM([]sessionLLMResponse{{text: "hello there"}})
	session, err := NewAgentSession(AgentSessionOptions[struct{}]{LLM: model, DisableUserAwayTimeout: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := session.Start(t.Context(), MustAgent(AgentOptions[struct{}]{ID: "run_agent"})); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close(context.Background()) })
	if err := session.PauseReplyAuthorization(); err != nil {
		t.Fatal(err)
	}
	result, err := session.Run(t.Context(), RunOptions{UserInput: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Run(t.Context(), RunOptions{UserInput: "nested"}); !errors.Is(err, ErrNestedRun) {
		t.Fatalf("nested run error=%v", err)
	}
	if err := session.ResumeReplyAuthorization(); err != nil {
		t.Fatal(err)
	}
	if _, err := result.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	events := result.Events()
	var user, assistant bool
	for _, event := range events {
		message, ok := event.Item.(*llm.ChatMessage)
		if !ok {
			continue
		}
		user = user || message.Role == llm.RoleUser
		assistant = assistant || message.Role == llm.RoleAssistant
	}
	if !user || !assistant {
		t.Fatalf("run events=%#v", events)
	}
	if _, err := result.FinalOutput(); !errors.Is(err, voicetest.ErrNoFinalOutput) {
		t.Fatalf("FinalOutput error=%v", err)
	}
}

func TestRunStructuredCapturesAgentTaskOutputAndToolEvents(t *testing.T) {
	type answer struct {
		Value string
	}
	var task *AgentTask[answer, struct{}]
	tool := llm.MustTool(llm.FunctionToolOptions[struct {
		Value string `json:"value"`
	}, string]{
		Name: "submit",
		Execute: func(_ context.Context, input struct {
			Value string `json:"value"`
		}, _ llm.ToolOptions) (string, error) {
			if err := task.Complete(answer{Value: input.Value}); err != nil {
				return "", err
			}
			return "submitted", nil
		},
	})
	tools, err := llm.NewToolContext(tool)
	if err != nil {
		t.Fatal(err)
	}
	task, err = NewAgentTask[answer](AgentTaskOptions[struct{}]{AgentOptions: AgentOptions[struct{}]{ID: "output_task", Tools: tools}})
	if err != nil {
		t.Fatal(err)
	}
	call := llm.NewFunctionCall("submit-1", "submit", `{"value":"42"}`)
	model := newSessionTestLLM([]sessionLLMResponse{{calls: []*llm.FunctionCall{call}}, {text: "submitted"}})
	session, err := NewAgentSession(AgentSessionOptions[struct{}]{LLM: model, DisableUserAwayTimeout: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := session.Start(t.Context(), task.Agent); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close(context.Background()) })
	result, err := RunStructured[answer](t.Context(), session, StructuredRunOptions[answer]{RunOptions: RunOptions{UserInput: "answer"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := result.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	output, err := result.FinalOutput()
	if err != nil || output.Value != "42" {
		t.Fatalf("FinalOutput=%#v err=%v", output, err)
	}
	var calls, outputs int
	for _, event := range result.Events() {
		switch event.Type {
		case voicetest.EventFunctionCall:
			calls++
		case voicetest.EventFunctionCallOutput:
			outputs++
		}
	}
	if calls != 1 || outputs != 1 {
		t.Fatalf("tool events calls=%d outputs=%d events=%#v", calls, outputs, result.Events())
	}
}

func TestRunStructuredRetriesOnlyMissingOutput(t *testing.T) {
	model := newSessionTestLLM([]sessionLLMResponse{{text: "one"}, {text: "two"}, {text: "three"}})
	session, err := NewAgentSession(AgentSessionOptions[struct{}]{LLM: model, DisableUserAwayTimeout: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := session.Start(t.Context(), MustAgent(AgentOptions[struct{}]{ID: "plain_agent"})); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close(context.Background()) })
	result, err := RunStructured[string](t.Context(), session, StructuredRunOptions[string]{RunOptions: RunOptions{UserInput: "give output"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := result.Wait(t.Context()); !errors.Is(err, ErrUnexpectedModelBehavior) {
		t.Fatalf("Wait error=%v", err)
	}
	if model.Calls() != 3 {
		t.Fatalf("LLM calls=%d want 3", model.Calls())
	}
}
