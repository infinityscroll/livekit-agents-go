// SPDX-License-Identifier: Apache-2.0

package providerformat

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/infinityscroll/livekit-agents-go/llm"
)

func TestGroupToolCallsParallelAndInvalid(t *testing.T) {
	message := llm.NewChatMessage(llm.RoleAssistant, "checking")
	message.ID = "turn/message"
	callA := llm.NewFunctionCall("call-a", "weather", `{}`)
	callA.ID, callA.GroupID = "independent-a", "turn"
	callB := llm.NewFunctionCall("call-b", "clock", `{}`)
	callB.ID, callB.GroupID = "independent-b", "turn"
	orphan := llm.NewFunctionCall("orphan", "lost", `{}`)
	orphan.ID, orphan.GroupID = "independent-c", "turn"
	outputA := llm.NewFunctionCallOutput("call-a", "weather", "sunny", false)
	outputB := llm.NewFunctionCallOutput("call-b", "clock", "noon", false)
	orphanOutput := llm.NewFunctionCallOutput("missing", "missing", "ignored", false)
	user := llm.NewChatMessage(llm.RoleUser, "thanks")
	user.ID = "user"

	chat := llm.NewChatContext(message, callA, callB, orphan, outputA, outputB, orphanOutput, user)
	groups, err := GroupToolCalls(chat)
	if err != nil {
		t.Fatal(err)
	}
	if len(groups) != 2 {
		t.Fatalf("groups = %d, want 2", len(groups))
	}
	if groups[0].Message == nil || groups[0].Message.ID != message.ID {
		t.Fatalf("assistant group message = %#v", groups[0].Message)
	}
	if got := []string{groups[0].ToolCalls[0].CallID, groups[0].ToolCalls[1].CallID}; !reflect.DeepEqual(got, []string{"call-a", "call-b"}) {
		t.Fatalf("paired calls = %v", got)
	}
	if len(groups[0].ToolOutputs) != 2 || groups[1].Message == nil || groups[1].Message.ID != "user" {
		t.Fatalf("grouping mismatch: %#v", groups)
	}

	// Returned groups must not alias mutable input records.
	message.Content[0] = llm.TextContent("mutated")
	if text, _ := groups[0].Message.RawTextContent(); text != "checking" {
		t.Fatalf("group aliases input: %q", text)
	}
}

func TestConvertMidConversationInstructions(t *testing.T) {
	stamp := time.Unix(123, 0)
	first := llm.NewChatMessage(llm.RoleSystem, "initial")
	first.ID = "first"
	user := llm.NewChatMessage(llm.RoleUser, "hello")
	second := llm.NewChatMessage(llm.RoleDeveloper, "be concise")
	second.ID, second.CreatedAt = "second", stamp

	converted := ConvertMidConversationInstructions(llm.NewChatContext(first, user, second))
	items := converted.Items()
	message, ok := items[2].(*llm.ChatMessage)
	if !ok || message.Role != llm.RoleUser || message.ID != "second" || !message.CreatedAt.Equal(stamp) {
		t.Fatalf("converted message = %#v", items[2])
	}
	text, _ := message.RawTextContent()
	if text != "New instructions received. Apply them carefully: be concise" {
		t.Fatalf("text = %q", text)
	}
}

func TestOpenAIChatAndResponsesFormats(t *testing.T) {
	assistant := &llm.ChatMessage{
		ID:   "turn/message",
		Role: llm.RoleAssistant,
		Content: []llm.Content{
			llm.ImageContent{Image: "data:image/png;base64,aGVsbG8=", InferenceDetail: llm.ImageDetailLow},
			llm.TextContent("caption"),
		},
		Extra: map[string]any{
			"openai":  map[string]any{"phase": "commentary"},
			"xai":     map[string]any{"encrypted": true},
			"ignored": true,
		},
	}
	call := llm.NewFunctionCall("call-1", "lookup", `{"city":"Paris"}`)
	call.GroupID = "turn"
	call.ThoughtSignature = "sig"
	call.Extra = map[string]any{"livekit": map[string]any{"trace": "t"}, "ignored": 1}
	output := llm.NewFunctionCallOutput("call-1", "lookup", "sunny", false)
	chat := llm.NewChatContext(assistant, call, output)

	messages, err := OpenAIChatContext(chat, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 2 || messages[0]["role"] != "assistant" || messages[1]["role"] != "tool" {
		t.Fatalf("messages = %#v", messages)
	}
	calls := messages[0]["tool_calls"].([]map[string]any)
	extra := calls[0]["extra_content"].(map[string]any)
	if _, ok := extra["ignored"]; ok {
		t.Fatalf("unrecognized extra leaked: %#v", extra)
	}
	if google := extra["google"].(map[string]any); google["thoughtSignature"] != "sig" {
		t.Fatalf("thought signature = %#v", google)
	}
	content := messages[0]["content"].([]map[string]any)
	if content[0]["type"] != "image_url" || content[1]["text"] != "caption" {
		t.Fatalf("content = %#v", content)
	}

	responses, err := OpenAIResponsesChatContext(chat, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(responses) != 3 || responses[0]["phase"] != "commentary" || responses[1]["type"] != "function_call" || responses[2]["type"] != "function_call_output" {
		t.Fatalf("responses = %#v", responses)
	}
}

func TestGoogleChatContext(t *testing.T) {
	system := llm.NewChatMessage(llm.RoleSystem, "system")
	user := llm.NewChatMessage(llm.RoleUser, "question")
	assistant := llm.NewChatMessage(llm.RoleAssistant, "answer")
	assistant.ID = "turn/message"
	call := llm.NewFunctionCall("call", "search", `{"q":"go"}`)
	call.GroupID, call.ThoughtSignature = "turn", "thought"
	output := llm.NewFunctionCallOutput("call", "search", "result", true)
	chat := llm.NewChatContext(system, user, assistant, call, output)

	turns, data, err := GoogleChatContext(chat, true)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(data.SystemMessages, []string{"system"}) {
		t.Fatalf("system = %v", data.SystemMessages)
	}
	if len(turns) != 3 || turns[0]["role"] != "user" || turns[1]["role"] != "model" || turns[2]["role"] != "user" {
		t.Fatalf("turns = %#v", turns)
	}
	modelParts := turns[1]["parts"].([]map[string]any)
	function := modelParts[1]
	if function["thoughtSignature"] != "thought" {
		t.Fatalf("function part = %#v", function)
	}
	userParts := turns[2]["parts"].([]map[string]any)
	response := userParts[0]["functionResponse"].(map[string]any)["response"].(map[string]any)
	if response["error"] != "result" {
		t.Fatalf("response = %#v", response)
	}
}

func TestMistralChatContext(t *testing.T) {
	system := llm.NewChatMessage(llm.RoleSystem, "one")
	developer := llm.NewChatMessage(llm.RoleDeveloper, "two")
	user := llm.NewChatMessage(llm.RoleUser, "hello")
	call := llm.NewFunctionCall("call", "tool", `{}`)
	output := llm.NewFunctionCallOutput("call", "tool", "ok", false)

	entries, data, err := MistralChatContext(llm.NewChatContext(system, developer, user, call, output), true)
	if err != nil {
		t.Fatal(err)
	}
	if data.Instructions != "one\ntwo" || len(entries) != 3 {
		t.Fatalf("data=%#v entries=%#v", data, entries)
	}
	if entries[1]["type"] != "function.call" || entries[2]["type"] != "function.result" {
		t.Fatalf("entries = %#v", entries)
	}

	dummy, _, err := MistralChatContext(llm.NewChatContext(system), true)
	if err != nil || len(dummy) != 1 || dummy[0]["content"] != "." {
		t.Fatalf("dummy=%#v err=%v", dummy, err)
	}
}

func TestProviderFormatRejectsUnsupportedImage(t *testing.T) {
	message := &llm.ChatMessage{ID: "image", Role: llm.RoleUser, Content: []llm.Content{
		llm.ImageContent{Image: []byte("not-an-image")},
	}}
	_, err := OpenAIChatContext(llm.NewChatContext(message), true)
	if err == nil || !strings.Contains(err.Error(), "unsupported image MIME") {
		t.Fatalf("error = %v", err)
	}
}

func FuzzGoogleFunctionArguments(f *testing.F) {
	f.Add(`{"value":1}`)
	f.Add(`{}`)
	f.Fuzz(func(t *testing.T, arguments string) {
		call := llm.NewFunctionCall("call", "tool", arguments)
		output := llm.NewFunctionCallOutput("call", "tool", "ok", false)
		chat := llm.NewChatContext(call, output)
		turns, _, err := GoogleChatContext(chat, false)
		if err != nil {
			return
		}
		encoded, err := json.Marshal(turns)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if !json.Valid(encoded) {
			t.Fatalf("invalid JSON: %q", encoded)
		}
	})
}
