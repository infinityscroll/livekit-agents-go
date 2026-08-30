// SPDX-License-Identifier: Apache-2.0

package providerformat

import (
	"fmt"

	"github.com/infinityscroll/livekit-agents-go/llm"
)

var openAIExtraKeys = [...]string{"google", "livekit", "xai"}

// OpenAIChatContext converts a ChatContext to OpenAI Chat Completions message
// objects. injectDummyUserMessage is retained for API parity; OpenAI does not
// require a synthetic trailing user turn.
func OpenAIChatContext(chat *llm.ChatContext, injectDummyUserMessage bool) ([]map[string]any, error) {
	if err := validateChat(chat); err != nil {
		return nil, err
	}
	groups, err := GroupToolCalls(chat)
	if err != nil {
		return nil, err
	}
	messages := make([]map[string]any, 0, len(groups)*2)
	for _, group := range groups {
		if group.Empty() {
			continue
		}
		var message map[string]any
		if group.Message != nil {
			message, err = openAIChatItem(group.Message)
			if err != nil {
				return nil, err
			}
		} else {
			message = map[string]any{"role": "assistant"}
		}
		if len(group.ToolCalls) != 0 {
			calls := make([]map[string]any, 0, len(group.ToolCalls))
			for _, call := range group.ToolCalls {
				calls = append(calls, openAIToolCall(call))
			}
			message["tool_calls"] = calls
		}
		messages = append(messages, message)
		for _, output := range group.ToolOutputs {
			converted, convertErr := openAIChatItem(output)
			if convertErr != nil {
				return nil, convertErr
			}
			messages = append(messages, converted)
		}
	}
	return messages, nil
}

// OpenAIResponsesChatContext converts a ChatContext to OpenAI Responses API
// input objects.
func OpenAIResponsesChatContext(chat *llm.ChatContext, injectDummyUserMessage bool) ([]map[string]any, error) {
	if err := validateChat(chat); err != nil {
		return nil, err
	}
	groups, err := GroupToolCalls(chat)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(groups)*2)
	for _, group := range groups {
		if group.Message != nil {
			converted, convertErr := openAIResponsesItem(group.Message)
			if convertErr != nil {
				return nil, convertErr
			}
			items = append(items, converted)
		}
		for _, call := range group.ToolCalls {
			items = append(items, map[string]any{
				"type": "function_call", "call_id": call.CallID,
				"name": call.Name, "arguments": call.Arguments,
			})
		}
		for _, output := range group.ToolOutputs {
			converted, convertErr := openAIResponsesItem(output)
			if convertErr != nil {
				return nil, convertErr
			}
			items = append(items, converted)
		}
	}
	return items, nil
}

// ToOpenAIChatContext is a compatibility alias for OpenAIChatContext.
func ToOpenAIChatContext(chat *llm.ChatContext, injectDummyUserMessage bool) ([]map[string]any, error) {
	return OpenAIChatContext(chat, injectDummyUserMessage)
}

// ToOpenAIResponsesChatContext is a compatibility alias.
func ToOpenAIResponsesChatContext(chat *llm.ChatContext, injectDummyUserMessage bool) ([]map[string]any, error) {
	return OpenAIResponsesChatContext(chat, injectDummyUserMessage)
}

func openAIChatItem(item llm.ChatItem) (map[string]any, error) {
	switch value := item.(type) {
	case *llm.ChatMessage:
		listContent := make([]map[string]any, 0, len(value.Content))
		text := ""
		for _, content := range value.Content {
			switch part := content.(type) {
			case llm.TextContent:
				text = appendText(text, string(part))
			case llm.InstructionContent:
				text = appendText(text, part.Instructions.Value())
			case llm.ImageContent:
				image, err := openAIImage(part, false)
				if err != nil {
					return nil, err
				}
				listContent = append(listContent, image)
			default:
				return nil, fmt.Errorf("OpenAI provider format: unsupported message content %T", content)
			}
		}
		result := map[string]any{"role": string(value.Role)}
		if len(listContent) == 0 {
			result["content"] = text
		} else {
			if text != "" {
				listContent = append(listContent, map[string]any{"type": "text", "text": text})
			}
			result["content"] = listContent
		}
		if extra := filterOpenAIExtra(value.Extra); len(extra) != 0 {
			result["extra_content"] = extra
		}
		return result, nil
	case *llm.FunctionCall:
		return map[string]any{"role": "assistant", "tool_calls": []map[string]any{openAIToolCall(value)}}, nil
	case *llm.FunctionCallOutput:
		return map[string]any{"role": "tool", "tool_call_id": value.CallID, "content": value.Output}, nil
	default:
		return nil, fmt.Errorf("OpenAI provider format: unsupported chat item %T", item)
	}
}

func openAIResponsesItem(item llm.ChatItem) (map[string]any, error) {
	switch value := item.(type) {
	case *llm.ChatMessage:
		listContent := make([]map[string]any, 0, len(value.Content))
		text := ""
		for _, content := range value.Content {
			switch part := content.(type) {
			case llm.TextContent:
				text = appendText(text, string(part))
			case llm.InstructionContent:
				text = appendText(text, part.Instructions.Value())
			case llm.ImageContent:
				image, err := openAIImage(part, true)
				if err != nil {
					return nil, err
				}
				listContent = append(listContent, image)
			default:
				return nil, fmt.Errorf("OpenAI Responses format: unsupported message content %T", content)
			}
		}
		var content any = text
		if len(listContent) != 0 {
			if text != "" {
				listContent = append(listContent, map[string]any{"type": "input_text", "text": text})
			}
			content = listContent
		}
		result := map[string]any{"role": string(value.Role), "content": content}
		if value.Role == llm.RoleAssistant {
			if openai, ok := asStringMap(value.Extra["openai"]); ok {
				if phase, exists := openai["phase"]; exists {
					result["phase"] = phase
				}
			}
		}
		return result, nil
	case *llm.FunctionCall:
		return map[string]any{"type": "function_call", "call_id": value.CallID, "name": value.Name, "arguments": value.Arguments}, nil
	case *llm.FunctionCallOutput:
		return map[string]any{"type": "function_call_output", "call_id": value.CallID, "output": value.Output}, nil
	default:
		return nil, fmt.Errorf("OpenAI Responses format: unsupported chat item %T", item)
	}
}

func openAIToolCall(call *llm.FunctionCall) map[string]any {
	converted := map[string]any{
		"type": "function", "id": call.CallID,
		"function": map[string]any{"name": call.Name, "arguments": call.Arguments},
	}
	extra := filterOpenAIExtra(call.Extra)
	if _, ok := extra["google"]; !ok && call.ThoughtSignature != "" {
		extra["google"] = map[string]any{"thoughtSignature": call.ThoughtSignature}
	}
	if len(extra) != 0 {
		converted["extra_content"] = extra
	}
	return converted
}

func filterOpenAIExtra(extra map[string]any) map[string]any {
	filtered := make(map[string]any, len(openAIExtraKeys))
	for _, key := range openAIExtraKeys {
		if value, ok := extra[key]; ok && value != nil {
			filtered[key] = value
		}
	}
	return filtered
}

func asStringMap(value any) (map[string]any, bool) {
	switch typed := value.(type) {
	case map[string]any:
		return typed, true
	case map[string]string:
		result := make(map[string]any, len(typed))
		for key, value := range typed {
			result[key] = value
		}
		return result, true
	default:
		return nil, false
	}
}

func appendText(existing, next string) string {
	if existing == "" {
		return next
	}
	return existing + "\n" + next
}
