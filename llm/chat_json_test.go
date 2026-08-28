// SPDX-License-Identifier: Apache-2.0

package llm

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	agents "github.com/livekit/agents-go"
)

func TestChatContextJSONMatchesAgentsJSSurface(t *testing.T) {
	t.Parallel()
	createdAt := time.UnixMilli(1_734_000_123_456)
	confidence := 0.92
	context := NewChatContext(
		&ChatMessage{
			ID: "item_message", Role: RoleAssistant, Interrupted: true, CreatedAt: createdAt,
			TranscriptConfidence: &confidence,
			Content: []Content{
				TextContent("hello <expression style=\"happy\"/>"),
				InstructionContent{Instructions: NewInstructions("speak", "write")},
				ImageContent{ID: "img_1", Image: "https://example.test/image.png", InferenceDetail: ImageDetailHigh, InferenceWidth: 640},
				AudioContent{Transcript: "heard"},
			},
			Metrics: MetricsReport{ProviderRequestIDs: []string{"req_1"}, LLMNodeTTFT: 250 * time.Millisecond},
			Extra:   map[string]any{"providerKey": "unchanged"},
		},
		&FunctionCall{ID: "item_call", CallID: "call_1", Name: "weather", Arguments: `{"city":"Paris"}`, CreatedAt: createdAt, GroupID: "group_1"},
		&FunctionCallOutput{ID: "item_output", CallID: "call_1", Name: "weather", Output: "sunny", CreatedAt: createdAt},
		&AgentHandoffItem{ID: "item_handoff", NewAgentID: "agent_2", CreatedAt: createdAt},
		&AgentConfigUpdate{ID: "item_config", Instructions: instructionsPointer(NewInstructions("audio", "text")), ToolsAdded: []string{"lookup"}, CreatedAt: createdAt},
	)

	defaultJSON, err := context.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	text := string(defaultJSON)
	for _, absent := range []string{"createdAt", "image_content", "audio_content"} {
		if strings.Contains(text, absent) {
			t.Fatalf("default JSON unexpectedly contains %q: %s", absent, text)
		}
	}
	for _, present := range []string{`"type":"message"`, `"type":"instructions"`, `"args":"{\"city\":\"Paris\"}"`, `"providerRequestIds":["req_1"]`, `"llmNodeTtft":0.25`} {
		if !strings.Contains(text, present) {
			t.Fatalf("default JSON missing %q: %s", present, text)
		}
	}

	fullJSON, err := context.ToJSON(ChatContextJSONOptions{
		ExcludeImage:     agents.Use(false),
		ExcludeAudio:     agents.Use(false),
		ExcludeTimestamp: agents.Use(false),
		StripMarkup:      true,
	})
	if err != nil {
		t.Fatal(err)
	}
	fullText := string(fullJSON)
	for _, present := range []string{`"createdAt":1734000123456`, `"type":"image_content"`, `"type":"audio_content"`, `"transcript":"heard"`} {
		if !strings.Contains(fullText, present) {
			t.Fatalf("full JSON missing %q: %s", present, fullText)
		}
	}
	if strings.Contains(fullText, "<expression") {
		t.Fatalf("strip markup did not apply to assistant text: %s", fullText)
	}
}

func TestChatContextJSONRoundTrip(t *testing.T) {
	t.Parallel()
	createdAt := time.UnixMilli(1_734_000_123_456)
	original := NewChatContext(
		&ChatMessage{ID: "m", Role: RoleUser, Content: []Content{TextContent("hi"), AudioContent{Transcript: "hi"}}, CreatedAt: createdAt},
		&FunctionCall{ID: "c", CallID: "call", Name: "tool", Arguments: `{}`, CreatedAt: createdAt, Extra: map[string]any{"x": "y"}},
	)
	data, err := original.ToJSON(ChatContextJSONOptions{ExcludeAudio: agents.Use(false), ExcludeTimestamp: agents.Use(false)})
	if err != nil {
		t.Fatal(err)
	}
	var decoded ChatContext
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if !original.IsEquivalent(&decoded) {
		t.Fatalf("round trip not equivalent\noriginal=%s\ndecoded=%s", data, mustJSON(t, &decoded))
	}
}

func TestChatContextJSONRejectsUnknownAndDuplicateItems(t *testing.T) {
	t.Parallel()
	var context ChatContext
	if err := json.Unmarshal([]byte(`{"items":[{"id":"x","type":"unknown"}]}`), &context); err == nil {
		t.Fatal("unknown item type accepted")
	}
	if err := json.Unmarshal([]byte(`{"items":[{"id":"x","type":"message","role":"user","content":[],"interrupted":false},{"id":"x","type":"message","role":"user","content":[],"interrupted":false}]}`), &context); err == nil {
		t.Fatal("duplicate item id accepted")
	}
}

func instructionsPointer(value Instructions) *Instructions { return &value }

func mustJSON(t *testing.T, value any) string {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
