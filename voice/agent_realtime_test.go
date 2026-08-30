// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"context"
	"strings"
	"testing"

	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/llm"
)

func TestRealtimeAndChatModelSelectionIsMutuallyExclusive(t *testing.T) {
	chat := newSessionTestLLM(nil)
	realtime := newVoiceTestRealtimeModel(llm.RealtimeCapabilities{})
	if _, err := NewAgentSession(AgentSessionOptions[struct{}]{LLM: chat, Realtime: realtime}); err == nil || !strings.Contains(err.Error(), "one LLM selection") {
		t.Fatalf("session model conflict = %v", err)
	}
	if _, err := NewAgent(AgentOptions[struct{}]{
		ID: "conflict", LLM: agents.Use[llm.LLM](chat), Realtime: agents.Use[llm.RealtimeModel](realtime),
	}); err == nil || !strings.Contains(err.Error(), "one LLM selection") {
		t.Fatalf("agent model conflict = %v", err)
	}
}

func TestPerAgentRealtimeOverrideAndTriStateUpdate(t *testing.T) {
	baseRealtime := newVoiceTestRealtimeModel(llm.RealtimeCapabilities{})
	chat := newSessionTestLLM(nil)
	chatOverride := agents.Use[llm.LLM](chat)
	agent := MustAgent(AgentOptions[struct{}]{ID: "override_agent", LLM: chatOverride})
	session, err := NewAgentSession(AgentSessionOptions[struct{}]{Realtime: baseRealtime, DisableUserAwayTimeout: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := session.Start(t.Context(), agent); err != nil {
		t.Fatal(err)
	}
	if baseRealtime.sessionCount() != 0 {
		t.Fatal("inherited realtime model was not shadowed by the agent Chat LLM")
	}
	inheritedChat := agents.Override[llm.LLM]{}
	if err := agent.UpdateOptions(t.Context(), AgentUpdateOptions{LLM: &inheritedChat}); err != nil {
		t.Fatal(err)
	}
	if baseRealtime.sessionCount() != 1 || agent.RealtimeOverride().IsInherited() == false {
		t.Fatal("restoring inheritance did not select the session realtime model")
	}
	disabledRealtime := agents.Disable[llm.RealtimeModel]()
	if err := agent.UpdateOptions(t.Context(), AgentUpdateOptions{Realtime: &disabledRealtime}); err != nil {
		t.Fatal(err)
	}
	active := baseRealtime.latest()
	active.mu.Lock()
	closed := active.closed
	active.mu.Unlock()
	if closed != 1 || !agent.RealtimeOverride().IsDisabled() {
		t.Fatalf("realtime disable closed=%d override=%#v", closed, agent.RealtimeOverride())
	}
	if err := session.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
