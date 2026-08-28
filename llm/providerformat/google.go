// SPDX-License-Identifier: Apache-2.0

package providerformat

import (
	"encoding/json"
	"fmt"

	"github.com/livekit/agents-go/llm"
)

// GoogleFormatData carries system instructions that Gemini accepts separately
// from conversation turns.
type GoogleFormatData struct {
	SystemMessages []string `json:"systemMessages"`
}

// GoogleChatContext converts a ChatContext to Gemini turns and separately
// returned system messages.
func GoogleChatContext(chat *llm.ChatContext, injectDummyUserMessage bool) ([]map[string]any, GoogleFormatData, error) {
	if err := validateChat(chat); err != nil {
		return nil, GoogleFormatData{}, err
	}
	chat = ConvertMidConversationInstructions(chat)
	groups, err := GroupToolCalls(chat)
	if err != nil {
		return nil, GoogleFormatData{}, err
	}
	items := make([]llm.ChatItem, 0, chat.Len())
	for _, group := range groups {
		items = append(items, group.Flatten()...)
	}

	turns := make([]map[string]any, 0, len(items))
	systemMessages := make([]string, 0, 1)
	currentRole := ""
	parts := make([]map[string]any, 0, 2)
	flush := func() {
		if currentRole != "" && len(parts) != 0 {
			turns = append(turns, map[string]any{"role": currentRole, "parts": append([]map[string]any(nil), parts...)})
		}
		parts = parts[:0]
	}

	for _, item := range items {
		if message, ok := item.(*llm.ChatMessage); ok && message.Role == llm.RoleSystem {
			if text, exists := message.RawTextContent(); exists {
				systemMessages = append(systemMessages, text)
			}
			continue
		}

		role := ""
		switch value := item.(type) {
		case *llm.ChatMessage:
			role = "user"
			if value.Role == llm.RoleAssistant {
				role = "model"
			}
		case *llm.FunctionCall:
			role = "model"
		case *llm.FunctionCallOutput:
			role = "user"
		default:
			continue
		}
		if role != currentRole {
			flush()
			currentRole = role
		}

		switch value := item.(type) {
		case *llm.ChatMessage:
			for _, content := range value.Content {
				switch part := content.(type) {
				case llm.TextContent:
					if string(part) != "" {
						parts = append(parts, map[string]any{"text": string(part)})
					}
				case llm.InstructionContent:
					parts = append(parts, map[string]any{"text": part.Instructions.Value()})
				case llm.ImageContent:
					image, imageErr := googleImage(part)
					if imageErr != nil {
						return nil, GoogleFormatData{}, imageErr
					}
					parts = append(parts, image)
				default:
					encoded, encodeErr := json.Marshal(content)
					if encodeErr != nil {
						return nil, GoogleFormatData{}, fmt.Errorf("google provider format: encode content %T: %w", content, encodeErr)
					}
					parts = append(parts, map[string]any{"text": string(encoded)})
				}
			}
		case *llm.FunctionCall:
			arguments := map[string]any{}
			if value.Arguments != "" {
				if decodeErr := json.Unmarshal([]byte(value.Arguments), &arguments); decodeErr != nil {
					return nil, GoogleFormatData{}, fmt.Errorf("google provider format: decode arguments for %q: %w", value.Name, decodeErr)
				}
			}
			part := map[string]any{"functionCall": map[string]any{"id": value.CallID, "name": value.Name, "args": arguments}}
			if value.ThoughtSignature != "" {
				part["thoughtSignature"] = value.ThoughtSignature
			}
			parts = append(parts, part)
		case *llm.FunctionCallOutput:
			response := map[string]any{"output": value.Output}
			if value.IsError {
				response = map[string]any{"error": value.Output}
			}
			parts = append(parts, map[string]any{"functionResponse": map[string]any{
				"id": value.CallID, "name": value.Name, "response": response,
			}})
		}
	}
	flush()
	if injectDummyUserMessage && currentRole != "user" {
		turns = append(turns, map[string]any{"role": "user", "parts": []map[string]any{{"text": "."}}})
	}
	return turns, GoogleFormatData{SystemMessages: systemMessages}, nil
}

// ToGoogleChatContext is a compatibility alias for GoogleChatContext.
func ToGoogleChatContext(chat *llm.ChatContext, injectDummyUserMessage bool) ([]map[string]any, GoogleFormatData, error) {
	return GoogleChatContext(chat, injectDummyUserMessage)
}
