// SPDX-License-Identifier: Apache-2.0

package inference

import (
	"encoding/json"
	"testing"

	"github.com/infinityscroll/livekit-agents-go/llm"
)

func TestOpenAIMessageGroupingAndFiltering(t *testing.T) {
	assistant := llm.NewChatMessage(llm.RoleAssistant, "calling tools")
	assistant.ID = "response_1/message"
	callA := llm.NewFunctionCall("call_a", "weather", `{"city":"Paris"}`)
	callA.GroupID = "response_1"
	callA.Extra = map[string]any{"google": map[string]any{"thoughtSignature": "sig"}, "private": "drop"}
	callB := llm.NewFunctionCall("call_b", "time", `{}`)
	callB.GroupID = "response_1"
	outputA := llm.NewFunctionCallOutput("call_a", "weather", "sunny", false)
	outputB := llm.NewFunctionCallOutput("call_b", "time", "noon", false)
	ctx := llm.NewChatContext(assistant, callA, callB, outputA, outputB)
	messages, err := toOpenAIMessages(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 3 {
		t.Fatalf("got %d messages: %#v", len(messages), messages)
	}
	calls := messages[0]["tool_calls"].([]any)
	if len(calls) != 2 {
		t.Fatalf("got %d tool calls", len(calls))
	}
	wired := calls[0].(map[string]any)
	extra := wired["extra_content"].(map[string]any)
	if _, exists := extra["private"]; exists {
		t.Fatal("private extra field leaked")
	}
}

func TestOpenAIImageContent(t *testing.T) {
	message := llm.NewChatMessage(llm.RoleUser, "describe")
	message.Content = append(message.Content, llm.ImageContent{
		Image: []byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n'}, MIMEType: "image/png",
	})
	messages, err := toOpenAIMessages(llm.NewChatContext(message))
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(messages)
	if string(encoded) == "" || len(messages) != 1 {
		t.Fatal("image message was not encoded")
	}
	parts := messages[0]["content"].([]any)
	image := parts[0].(map[string]any)["image_url"].(map[string]any)["url"].(string)
	if len(image) < len("data:image/png;base64,") || image[:len("data:image/png;base64,")] != "data:image/png;base64," {
		t.Fatalf("unexpected data URL %q", image)
	}
}
