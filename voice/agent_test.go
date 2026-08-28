// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"context"
	"testing"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/llm"
)

func TestAgentDefaultsAndDefensiveContexts(t *testing.T) {
	chat := llm.EmptyChatContext()
	_, _ = chat.AddMessage(llm.RoleUser, "before")
	agent, err := NewAgent(AgentOptions[struct{}]{
		Instructions: llm.NewInstructions("help", ""), ChatContext: chat,
	})
	if err != nil {
		t.Fatal(err)
	}
	if agent.ID() != "default_agent" {
		t.Fatalf("id = %q", agent.ID())
	}
	_, _ = chat.AddMessage(llm.RoleUser, "after")
	if agent.ChatContext().Len() != 1 {
		t.Fatal("agent retained caller chat context")
	}
	copy := agent.ChatContext()
	_, _ = copy.AddMessage(llm.RoleUser, "copy mutation")
	if agent.ChatContext().Len() != 1 {
		t.Fatal("agent exposed mutable chat context")
	}
}

func TestAgentModelUpdateTriState(t *testing.T) {
	agent, err := NewAgent(AgentOptions[struct{}]{Instructions: llm.NewInstructions("help", "")})
	if err != nil {
		t.Fatal(err)
	}
	disabled := agents.Disable[llm.LLM]()
	if err := agent.UpdateOptions(context.Background(), AgentUpdateOptions{LLM: &disabled}); err != nil {
		t.Fatal(err)
	}
	if !agent.LLMOverride().IsDisabled() {
		t.Fatal("LLM was not disabled")
	}
	inherited := agents.Override[llm.LLM]{}
	if err := agent.UpdateOptions(context.Background(), AgentUpdateOptions{LLM: &inherited}); err != nil {
		t.Fatal(err)
	}
	if !agent.LLMOverride().IsInherited() {
		t.Fatal("LLM was not restored to inherit")
	}
}

func TestAgentIDValidation(t *testing.T) {
	for _, id := range []string{"BadAgent", "1_agent", "agent-name"} {
		if _, err := NewAgent(AgentOptions[struct{}]{ID: id}); err == nil {
			t.Fatalf("accepted id %q", id)
		}
	}
}

func TestGenerateReplyOptionsValidation(t *testing.T) {
	message := llm.NewChatMessage(llm.RoleUser, "hello")
	if err := (GenerateReplyOptions{UserInput: "hello", UserMessage: message}).Validate(); err == nil {
		t.Fatal("accepted two user inputs")
	}
}
