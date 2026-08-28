// SPDX-License-Identifier: Apache-2.0

package providerformat

import (
	"errors"
	"fmt"
	"strings"

	"github.com/livekit/agents-go/llm"
)

// ChatItemGroup is one coherent provider turn. An assistant message may share
// a group with parallel function calls and their corresponding outputs.
type ChatItemGroup struct {
	Message     *llm.ChatMessage
	ToolCalls   []*llm.FunctionCall
	ToolOutputs []*llm.FunctionCallOutput
}

// Empty reports whether the group contains no provider-visible item.
func (g ChatItemGroup) Empty() bool {
	return g.Message == nil && len(g.ToolCalls) == 0 && len(g.ToolOutputs) == 0
}

// Flatten returns the group's provider-visible items in upstream order.
func (g ChatItemGroup) Flatten() []llm.ChatItem {
	items := make([]llm.ChatItem, 0, 1+len(g.ToolCalls)+len(g.ToolOutputs))
	if g.Message != nil {
		items = append(items, g.Message.Clone())
	}
	for _, call := range g.ToolCalls {
		items = append(items, call.Clone())
	}
	for _, output := range g.ToolOutputs {
		items = append(items, cloneFunctionCallOutput(output))
	}
	return items
}

// InstructionConversionOptions controls conversion of system/developer
// messages that appear after the conversation's initial instructions.
type InstructionConversionOptions struct {
	Role     llm.ChatRole
	Template string
}

const defaultInstructionTemplate = "New instructions received. Apply them carefully: {instructions}"

// ConvertMidConversationInstructions converts every system/developer message
// after the first such message into a normal message. This is required by
// providers that accept system instructions only at the beginning.
func ConvertMidConversationInstructions(chat *llm.ChatContext, options ...InstructionConversionOptions) *llm.ChatContext {
	if chat == nil {
		return llm.EmptyChatContext()
	}
	opt := InstructionConversionOptions{Role: llm.RoleUser, Template: defaultInstructionTemplate}
	if len(options) != 0 {
		if options[0].Role != "" {
			opt.Role = options[0].Role
		}
		if options[0].Template != "" {
			opt.Template = options[0].Template
		}
	}

	items := chat.Items()
	converted := make([]llm.ChatItem, 0, len(items))
	firstInstructionsSeen := false
	for _, item := range items {
		message, ok := item.(*llm.ChatMessage)
		if !ok || (message.Role != llm.RoleSystem && message.Role != llm.RoleDeveloper) {
			converted = append(converted, item)
			continue
		}

		text, hasText := message.RawTextContent()
		if firstInstructionsSeen && hasText {
			converted = append(converted, &llm.ChatMessage{
				ID:        message.ID,
				Role:      opt.Role,
				Content:   []llm.Content{llm.TextContent(strings.Replace(opt.Template, "{instructions}", text, 1))},
				Extra:     map[string]any{},
				CreatedAt: message.CreatedAt,
			})
			continue
		}
		firstInstructionsSeen = true
		converted = append(converted, message)
	}
	return llm.NewChatContext(converted...)
}

type orderedGroup struct {
	key   string
	group ChatItemGroup
}

// GroupToolCalls groups assistant messages, parallel function calls, and
// matching outputs using GroupID (or the ID prefix before '/'). Unpaired calls
// and outputs are omitted, matching agents-js provider behavior.
func GroupToolCalls(chat *llm.ChatContext) ([]ChatItemGroup, error) {
	if chat == nil {
		return nil, nil
	}

	groups := make([]orderedGroup, 0, chat.Len())
	indices := make(map[string]int, chat.Len())
	outputs := make([]*llm.FunctionCallOutput, 0)

	addGroup := func(key string) int {
		if index, ok := indices[key]; ok {
			return index
		}
		index := len(groups)
		indices[key] = index
		groups = append(groups, orderedGroup{key: key})
		return index
	}

	for _, item := range chat.Items() {
		switch value := item.(type) {
		case *llm.ChatMessage:
			key := value.ID
			if value.Role == llm.RoleAssistant {
				key = itemGroupPrefix(value.ID)
			}
			index := addGroup(key)
			if groups[index].group.Message != nil {
				return nil, fmt.Errorf("provider format group %q: only one message is allowed", key)
			}
			groups[index].group.Message = value.Clone()
		case *llm.FunctionCall:
			key := value.GroupID
			if key == "" {
				key = itemGroupPrefix(value.ID)
			}
			index := addGroup(key)
			groups[index].group.ToolCalls = append(groups[index].group.ToolCalls, value.Clone())
		case *llm.FunctionCallOutput:
			outputs = append(outputs, cloneFunctionCallOutput(value))
		case *llm.AgentHandoffItem, *llm.AgentConfigUpdate:
			// Preserve insertion ordering but keep session metadata out of the
			// model request, exactly as the JS/Python formatters do.
			addGroup(item.ItemID())
		default:
			return nil, fmt.Errorf("provider format: unsupported chat item %T", item)
		}
	}

	callGroups := make(map[string]int)
	for index := range groups {
		for _, call := range groups[index].group.ToolCalls {
			callGroups[call.CallID] = index
		}
	}
	for _, output := range outputs {
		if index, ok := callGroups[output.CallID]; ok {
			groups[index].group.ToolOutputs = append(groups[index].group.ToolOutputs, output)
		}
	}

	result := make([]ChatItemGroup, 0, len(groups))
	for index := range groups {
		group := &groups[index].group
		valid := make(map[string]struct{}, len(group.ToolOutputs))
		for _, output := range group.ToolOutputs {
			valid[output.CallID] = struct{}{}
		}
		calls := group.ToolCalls[:0]
		for _, call := range group.ToolCalls {
			if _, ok := valid[call.CallID]; ok {
				calls = append(calls, call)
			}
		}
		callIDs := make(map[string]struct{}, len(calls))
		for _, call := range calls {
			callIDs[call.CallID] = struct{}{}
		}
		pairedOutputs := group.ToolOutputs[:0]
		for _, output := range group.ToolOutputs {
			if _, ok := callIDs[output.CallID]; ok {
				pairedOutputs = append(pairedOutputs, output)
			}
		}
		group.ToolCalls = calls
		group.ToolOutputs = pairedOutputs
		result = append(result, *group)
	}
	return result, nil
}

func itemGroupPrefix(id string) string {
	if before, _, ok := strings.Cut(id, "/"); ok {
		return before
	}
	return id
}

func cloneFunctionCallOutput(output *llm.FunctionCallOutput) *llm.FunctionCallOutput {
	if output == nil {
		return nil
	}
	copy := *output
	return &copy
}

func validateChat(chat *llm.ChatContext) error {
	if chat == nil {
		return errors.New("provider format: chat context is nil")
	}
	return nil
}
