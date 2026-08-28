// SPDX-License-Identifier: Apache-2.0

package providerformat

import "github.com/livekit/agents-go/llm"

// MistralFormatData carries Mistral Conversations API instructions.
type MistralFormatData struct {
	Instructions string `json:"instructions"`
}

// MistralChatContext converts a ChatContext into Mistral Conversations API
// entries and separately returned instructions.
func MistralChatContext(chat *llm.ChatContext, injectDummyUserMessage bool) ([]map[string]any, MistralFormatData, error) {
	if err := validateChat(chat); err != nil {
		return nil, MistralFormatData{}, err
	}
	entries := make([]map[string]any, 0, chat.Len())
	instructions := ""
	for _, item := range chat.Items() {
		switch value := item.(type) {
		case *llm.ChatMessage:
			text := ""
			for _, content := range value.Content {
				switch part := content.(type) {
				case llm.TextContent:
					text = appendText(text, string(part))
				case llm.InstructionContent:
					text = appendText(text, part.Instructions.Value())
				}
			}
			switch value.Role {
			case llm.RoleSystem, llm.RoleDeveloper:
				instructions = appendText(instructions, text)
			case llm.RoleUser:
				entries = append(entries, map[string]any{"type": "message.input", "role": "user", "content": text})
			case llm.RoleAssistant:
				entries = append(entries, map[string]any{"type": "message.output", "role": "assistant", "content": text})
			}
		case *llm.FunctionCall:
			entries = append(entries, map[string]any{
				"type": "function.call", "toolCallId": value.CallID,
				"name": value.Name, "arguments": value.Arguments,
			})
		case *llm.FunctionCallOutput:
			entries = append(entries, map[string]any{
				"type": "function.result", "toolCallId": value.CallID, "result": value.Output,
			})
		}
	}
	if len(entries) == 0 && injectDummyUserMessage {
		entries = append(entries, map[string]any{"type": "message.input", "role": "user", "content": "."})
	}
	return entries, MistralFormatData{Instructions: instructions}, nil
}

// ToMistralChatContext is a compatibility alias for MistralChatContext.
func ToMistralChatContext(chat *llm.ChatContext, injectDummyUserMessage bool) ([]map[string]any, MistralFormatData, error) {
	return MistralChatContext(chat, injectDummyUserMessage)
}
