// SPDX-License-Identifier: Apache-2.0

package inference

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/livekit/agents-go/llm"
)

type openAIMessage map[string]any

type chatItemGroup struct {
	message     *llm.ChatMessage
	toolCalls   []*llm.FunctionCall
	toolOutputs []*llm.FunctionCallOutput
}

func toOpenAIMessages(chat *llm.ChatContext) ([]openAIMessage, error) {
	groups := groupChatItems(chat)
	messages := make([]openAIMessage, 0, len(groups)*2)
	for _, group := range groups {
		if group.message == nil && len(group.toolCalls) == 0 && len(group.toolOutputs) == 0 {
			continue
		}
		var message openAIMessage
		if group.message != nil {
			converted, err := openAIChatMessage(group.message)
			if err != nil {
				return nil, err
			}
			message = converted
		} else {
			message = openAIMessage{"role": "assistant"}
		}
		if len(group.toolCalls) != 0 {
			calls := make([]any, 0, len(group.toolCalls))
			for _, call := range group.toolCalls {
				wire := map[string]any{
					"id": call.CallID, "type": "function",
					"function": map[string]any{"name": call.Name, "arguments": call.Arguments},
				}
				extra := filterExtra(call.Extra)
				if len(extra) == 0 && call.ThoughtSignature != "" {
					extra = map[string]any{"google": map[string]any{"thoughtSignature": call.ThoughtSignature}}
				}
				if len(extra) != 0 {
					wire["extra_content"] = extra
				}
				calls = append(calls, wire)
			}
			message["tool_calls"] = calls
		}
		messages = append(messages, message)
		for _, output := range group.toolOutputs {
			messages = append(messages, openAIMessage{
				"role": "tool", "tool_call_id": output.CallID, "content": output.Output,
			})
		}
	}
	return messages, nil
}

func groupChatItems(chat *llm.ChatContext) []*chatItemGroup {
	items := chat.Items()
	groupsByID := make(map[string]*chatItemGroup, len(items))
	ordered := make([]*chatItemGroup, 0, len(items))
	outputs := make([]*llm.FunctionCallOutput, 0)
	addGroup := func(id string) *chatItemGroup {
		if group := groupsByID[id]; group != nil {
			return group
		}
		group := &chatItemGroup{}
		groupsByID[id] = group
		ordered = append(ordered, group)
		return group
	}
	for _, item := range items {
		switch item := item.(type) {
		case *llm.ChatMessage:
			if item.Role == llm.RoleAssistant {
				addGroup(itemGroupID(item.ID)).message = item
			} else {
				addGroup(item.ID).message = item
			}
		case *llm.FunctionCall:
			groupID := item.GroupID
			if groupID == "" {
				groupID = itemGroupID(item.ID)
			}
			group := addGroup(groupID)
			group.toolCalls = append(group.toolCalls, item)
		case *llm.FunctionCallOutput:
			outputs = append(outputs, item)
		}
	}
	callGroups := make(map[string]*chatItemGroup)
	for _, group := range ordered {
		for _, call := range group.toolCalls {
			callGroups[call.CallID] = group
		}
	}
	for _, output := range outputs {
		if group := callGroups[output.CallID]; group != nil {
			group.toolOutputs = append(group.toolOutputs, output)
		}
	}
	for _, group := range ordered {
		if len(group.toolCalls) == 0 && len(group.toolOutputs) == 0 {
			continue
		}
		outputsByID := make(map[string]*llm.FunctionCallOutput, len(group.toolOutputs))
		for _, output := range group.toolOutputs {
			outputsByID[output.CallID] = output
		}
		valid := make(map[string]struct{}, len(group.toolCalls))
		calls := group.toolCalls[:0]
		for _, call := range group.toolCalls {
			if _, ok := outputsByID[call.CallID]; ok {
				valid[call.CallID] = struct{}{}
				calls = append(calls, call)
			}
		}
		group.toolCalls = calls
		group.toolOutputs = filterToolOutputs(group.toolOutputs, valid)
	}
	return ordered
}

func filterToolOutputs(input []*llm.FunctionCallOutput, valid map[string]struct{}) []*llm.FunctionCallOutput {
	output := input[:0]
	for _, item := range input {
		if _, ok := valid[item.CallID]; ok {
			output = append(output, item)
		}
	}
	return output
}

func itemGroupID(id string) string {
	if index := strings.IndexByte(id, '/'); index >= 0 {
		return id[:index]
	}
	return id
}

func openAIChatMessage(message *llm.ChatMessage) (openAIMessage, error) {
	result := openAIMessage{"role": string(message.Role)}
	var text strings.Builder
	parts := make([]any, 0, len(message.Content))
	for _, content := range message.Content {
		switch content := content.(type) {
		case llm.TextContent:
			appendText(&text, string(content))
		case llm.InstructionContent:
			appendText(&text, content.Instructions.Value())
		case llm.ImageContent:
			part, err := openAIImageContent(content)
			if err != nil {
				return nil, err
			}
			parts = append(parts, part)
		case llm.AudioContent:
			return nil, errors.New("OpenAI chat-completions format does not support AudioContent")
		default:
			return nil, fmt.Errorf("unsupported chat content %T", content)
		}
	}
	if len(parts) == 0 {
		result["content"] = text.String()
	} else {
		if text.Len() != 0 {
			parts = append(parts, map[string]any{"type": "text", "text": text.String()})
		}
		result["content"] = parts
	}
	if extra := filterExtra(message.Extra); len(extra) != 0 {
		result["extra_content"] = extra
	}
	return result, nil
}

func appendText(builder *strings.Builder, text string) {
	if builder.Len() != 0 {
		builder.WriteByte('\n')
	}
	builder.WriteString(text)
}

func openAIImageContent(content llm.ImageContent) (map[string]any, error) {
	var imageURL string
	switch image := content.Image.(type) {
	case string:
		if !strings.HasPrefix(image, "http://") && !strings.HasPrefix(image, "https://") && !strings.HasPrefix(image, "data:") {
			return nil, errors.New("image string must be an HTTP(S) or data URL")
		}
		imageURL = image
	case []byte:
		mimeType := content.MIMEType
		if mimeType == "" {
			mimeType = http.DetectContentType(image)
		}
		if !strings.HasPrefix(mimeType, "image/") {
			return nil, fmt.Errorf("image bytes have unsupported content type %q", mimeType)
		}
		imageURL = "data:" + mimeType + ";base64," + base64.StdEncoding.EncodeToString(image)
	case json.RawMessage:
		mimeType := content.MIMEType
		if mimeType == "" {
			mimeType = http.DetectContentType(image)
		}
		imageURL = "data:" + mimeType + ";base64," + base64.StdEncoding.EncodeToString(image)
	default:
		return nil, fmt.Errorf("unsupported image value %T; use URL string or encoded []byte", content.Image)
	}
	detail := content.InferenceDetail
	if detail == "" {
		detail = llm.ImageDetailAuto
	}
	return map[string]any{
		"type":      "image_url",
		"image_url": map[string]any{"url": imageURL, "detail": string(detail)},
	}, nil
}

func filterExtra(extra map[string]any) map[string]any {
	if len(extra) == 0 {
		return nil
	}
	result := make(map[string]any, 3)
	for _, key := range [...]string{"google", "livekit", "xai"} {
		if value, ok := extra[key]; ok && value != nil {
			result[key] = value
		}
	}
	return result
}
