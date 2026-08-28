// SPDX-License-Identifier: Apache-2.0

package llm

import (
	"bytes"
	"encoding/json"
	"fmt"
	"image"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type FormatChatHistoryOptions struct {
	IncludeIDs        bool
	IncludeTimestamps bool
}

type ChatContextValidationSeverity string

const (
	ValidationError   ChatContextValidationSeverity = "error"
	ValidationWarning ChatContextValidationSeverity = "warning"
)

type ChatContextValidationCode string

const (
	ValidationDuplicateID             ChatContextValidationCode = "duplicate_id"
	ValidationTimestampOrder          ChatContextValidationCode = "timestamp_order"
	ValidationEmptyMessageContent     ChatContextValidationCode = "empty_message_content"
	ValidationEmptyTextTerm           ChatContextValidationCode = "empty_text_term"
	ValidationMissingImageTerm        ChatContextValidationCode = "missing_image_term"
	ValidationInvalidAudioTerm        ChatContextValidationCode = "invalid_audio_term"
	ValidationInvalidFunctionCall     ChatContextValidationCode = "invalid_function_call"
	ValidationInvalidFunctionCallArgs ChatContextValidationCode = "invalid_function_call_args"
	ValidationInvalidFunctionOutput   ChatContextValidationCode = "invalid_function_call_output"
	ValidationOrphanFunctionOutput    ChatContextValidationCode = "orphan_function_call_output"
)

type ChatContextValidationIssue struct {
	Severity ChatContextValidationSeverity `json:"severity"`
	Code     ChatContextValidationCode     `json:"code"`
	Index    int                           `json:"index"`
	ItemID   string                        `json:"itemId"`
	Message  string                        `json:"message"`
}

type ChatContextValidationResult struct {
	Valid    bool                         `json:"valid"`
	Errors   int                          `json:"errors"`
	Warnings int                          `json:"warnings"`
	Issues   []ChatContextValidationIssue `json:"issues"`
}

// ChatContextDiffOperation identifies an item and the item after which it
// belongs. PreviousItemID is empty for the root of the context.
type ChatContextDiffOperation struct {
	PreviousItemID string `json:"previousItemId,omitempty"`
	ItemID         string `json:"itemId"`
}

type ChatContextDiff struct {
	ToRemove []string                   `json:"toRemove"`
	ToCreate []ChatContextDiffOperation `json:"toCreate"`
	ToUpdate []ChatContextDiffOperation `json:"toUpdate"`
}

// FormatChatHistory renders a stable, human-readable diagnostic view. It is
// intended for logs and deliberately avoids serializing frame payloads.
func FormatChatHistory(chatContext *ChatContext, options FormatChatHistoryOptions) string {
	if chatContext == nil {
		return "Chat history (0 items)"
	}
	items := chatContext.Items()
	if len(items) == 0 {
		return "Chat history (0 items)"
	}
	var output strings.Builder
	fmt.Fprintf(&output, "Chat history (%d items)", len(items))
	for index, item := range items {
		output.WriteString("\n\n")
		output.WriteString(formatChatHistoryItem(item, index, options))
	}
	return output.String()
}

func formatChatHistoryItem(item ChatItem, index int, options FormatChatHistoryOptions) string {
	parts := []string{fmt.Sprintf("[%d]", index)}
	switch value := item.(type) {
	case *ChatMessage:
		parts = append(parts, string(ItemMessage), string(value.Role))
	case *FunctionCall:
		parts = append(parts, string(ItemFunctionCall), value.Name, "call_id="+value.CallID)
	case *FunctionCallOutput:
		name := value.Name
		if name == "" {
			name = "(unnamed)"
		}
		parts = append(parts, string(ItemFunctionCallOutput), name, "call_id="+value.CallID)
		if value.IsError {
			parts = append(parts, "error=true")
		}
	case *AgentHandoffItem:
		parts = append(parts, string(ItemAgentHandoff))
	case *AgentConfigUpdate:
		parts = append(parts, string(ItemAgentConfigUpdate))
	default:
		parts = append(parts, string(item.ItemType()))
	}
	if options.IncludeIDs {
		parts = append(parts, "id="+item.ItemID())
	}
	if options.IncludeTimestamps {
		seconds := float64(item.ItemCreatedAt().UnixNano()) / 1e9
		parts = append(parts, "created_at="+strconv.FormatFloat(seconds, 'f', 3, 64))
	}
	body := formatChatHistoryBody(item)
	header := strings.Join(parts, " ")
	if body == "" {
		return header
	}
	return header + "\n" + indentBlock(body, "  ")
}

func formatChatHistoryBody(item ChatItem) string {
	switch value := item.(type) {
	case *ChatMessage:
		parts := make([]string, 0, len(value.Content))
		for _, content := range value.Content {
			parts = append(parts, formatMessageContent(content))
		}
		result := strings.Join(parts, "\n")
		if strings.TrimSpace(result) == "" {
			return "(empty)"
		}
		return result
	case *FunctionCall:
		return prettyJSONText(value.Arguments)
	case *FunctionCallOutput:
		return prettyJSONText(value.Output)
	case *AgentHandoffItem:
		oldAgent := value.OldAgentID
		if oldAgent == "" {
			oldAgent = "(none)"
		}
		return oldAgent + " -> " + value.NewAgentID
	case *AgentConfigUpdate:
		body := make(map[string]any, 3)
		if value.Instructions != nil {
			body["instructions"] = value.Instructions
		}
		if value.ToolsAdded != nil {
			body["toolsAdded"] = value.ToolsAdded
		}
		if value.ToolsRemoved != nil {
			body["toolsRemoved"] = value.ToolsRemoved
		}
		encoded, _ := json.MarshalIndent(body, "", "  ")
		return string(encoded)
	default:
		return ""
	}
}

func formatMessageContent(content Content) string {
	switch value := content.(type) {
	case TextContent:
		return string(value)
	case InstructionContent:
		return value.Instructions.Value()
	case ImageContent:
		if url, ok := value.Image.(string); ok {
			return "[image url=" + truncateText(url, 120) + "]"
		}
		if source, ok := value.Image.(image.Image); ok {
			bounds := source.Bounds()
			return fmt.Sprintf("[image frame=%dx%d]", bounds.Dx(), bounds.Dy())
		}
		if value.InferenceWidth > 0 && value.InferenceHeight > 0 {
			return fmt.Sprintf("[image frame=%dx%d]", value.InferenceWidth, value.InferenceHeight)
		}
		return "[image]"
	case AudioContent:
		if value.Transcript != "" {
			return "[audio transcript=" + strconv.Quote(truncateText(value.Transcript, 120)) + "]"
		}
		return fmt.Sprintf("[audio frames=%d]", len(value.Frames))
	default:
		return fmt.Sprintf("[%s]", content.contentKind())
	}
}

func prettyJSONText(input string) string {
	var output bytes.Buffer
	if err := json.Indent(&output, []byte(input), "", "  "); err != nil {
		return input
	}
	return output.String()
}

func truncateText(input string, maxRunes int) string {
	if maxRunes <= 0 {
		return ""
	}
	if len(input) <= maxRunes {
		return input
	}
	runes := []rune(input)
	if len(runes) <= maxRunes {
		return input
	}
	if maxRunes <= 3 {
		return string(runes[:maxRunes])
	}
	return string(runes[:maxRunes-3]) + "..."
}

func indentBlock(input, indent string) string {
	return indent + strings.ReplaceAll(input, "\n", "\n"+indent)
}

// ValidateChatContextStructure performs the same realtime structural checks as
// the TypeScript SDK without mutating the supplied history.
func ValidateChatContextStructure(chatContext *ChatContext) ChatContextValidationResult {
	result := ChatContextValidationResult{Valid: true, Issues: make([]ChatContextValidationIssue, 0)}
	if chatContext == nil {
		return result
	}
	items := chatContext.Items()
	ids := make(map[string]struct{}, len(items))
	functionCalls := make(map[string]struct{})
	var previousCreatedAtSet bool
	var previousCreatedAt time.Time

	add := func(issue ChatContextValidationIssue) {
		result.Issues = append(result.Issues, issue)
		if issue.Severity == ValidationError {
			result.Errors++
		} else {
			result.Warnings++
		}
	}
	for index, item := range items {
		itemID := item.ItemID()
		if _, duplicate := ids[itemID]; duplicate {
			add(validationIssue(ValidationError, ValidationDuplicateID, index, itemID,
				fmt.Sprintf("Duplicate item id '%s'", itemID)))
		} else {
			ids[itemID] = struct{}{}
		}
		createdAt := item.ItemCreatedAt()
		if previousCreatedAtSet && createdAt.Before(previousCreatedAt) {
			add(validationIssue(ValidationError, ValidationTimestampOrder, index, itemID,
				fmt.Sprintf("Item createdAt (%s) is older than previous item (%s)", createdAt.Format(timeWireLayout), previousCreatedAt.Format(timeWireLayout))))
		}
		previousCreatedAt, previousCreatedAtSet = createdAt, true

		switch value := item.(type) {
		case *ChatMessage:
			if len(value.Content) == 0 {
				add(validationIssue(ValidationWarning, ValidationEmptyMessageContent, index, itemID, "Message has empty content array"))
			}
			for termIndex, term := range value.Content {
				switch content := term.(type) {
				case TextContent:
					if strings.TrimSpace(string(content)) == "" {
						add(validationIssue(ValidationWarning, ValidationEmptyTextTerm, index, itemID,
							fmt.Sprintf("Message term[%d] is empty text", termIndex)))
					}
				case InstructionContent:
				case ImageContent:
					if content.ID == "" || isNilInterface(content.Image) {
						add(validationIssue(ValidationError, ValidationMissingImageTerm, index, itemID,
							fmt.Sprintf("Message term[%d] has invalid image content", termIndex)))
					}
				case AudioContent:
					if content.Frames == nil {
						add(validationIssue(ValidationError, ValidationInvalidAudioTerm, index, itemID,
							fmt.Sprintf("Message term[%d] has invalid audio content", termIndex)))
					}
				default:
					add(validationIssue(ValidationError, ValidationInvalidAudioTerm, index, itemID,
						fmt.Sprintf("Message term[%d] has invalid content", termIndex)))
				}
			}
		case *FunctionCall:
			if value.Name == "" || value.CallID == "" {
				add(validationIssue(ValidationError, ValidationInvalidFunctionCall, index, itemID, "Function call is missing name or callId"))
			} else {
				functionCalls[value.CallID] = struct{}{}
			}
			if !json.Valid([]byte(value.Arguments)) {
				add(validationIssue(ValidationWarning, ValidationInvalidFunctionCallArgs, index, itemID, "Function call args are not valid JSON"))
			}
		case *FunctionCallOutput:
			if value.CallID == "" {
				add(validationIssue(ValidationError, ValidationInvalidFunctionOutput, index, itemID, "Function call output is missing callId"))
			} else if _, exists := functionCalls[value.CallID]; !exists {
				add(validationIssue(ValidationWarning, ValidationOrphanFunctionOutput, index, itemID,
					fmt.Sprintf("Function call output references unknown callId '%s'", value.CallID)))
			}
		}
	}
	result.Valid = result.Errors == 0
	return result
}

const timeWireLayout = "2006-01-02T15:04:05.000000000Z07:00"

func validationIssue(severity ChatContextValidationSeverity, code ChatContextValidationCode, index int, itemID, message string) ChatContextValidationIssue {
	return ChatContextValidationIssue{Severity: severity, Code: code, Index: index, ItemID: itemID, Message: message}
}

func isNilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

// ComputeChatContextDiff computes a minimal set of ordered remove/create
// operations plus content updates for same-ID messages. Its LCS implementation
// uses linear auxiliary memory, avoiding the quadratic RSS spike of the JS
// implementation for large histories.
func ComputeChatContextDiff(oldContext, newContext *ChatContext) ChatContextDiff {
	var oldItems, newItems []ChatItem
	if oldContext != nil {
		oldItems = oldContext.Items()
	}
	if newContext != nil {
		newItems = newContext.Items()
	}
	oldIDs := make([]string, len(oldItems))
	newIDs := make([]string, len(newItems))
	oldByID := make(map[string]ChatItem, len(oldItems))
	for index, item := range oldItems {
		oldIDs[index] = item.ItemID()
		oldByID[item.ItemID()] = item
	}
	for index, item := range newItems {
		newIDs[index] = item.ItemID()
	}
	lcs := longestCommonSubsequence(oldIDs, newIDs)
	common := make(map[string]struct{}, len(lcs))
	for _, id := range lcs {
		common[id] = struct{}{}
	}
	result := ChatContextDiff{
		ToRemove: make([]string, 0),
		ToCreate: make([]ChatContextDiffOperation, 0),
		ToUpdate: make([]ChatContextDiffOperation, 0),
	}
	for _, item := range oldItems {
		if _, keep := common[item.ItemID()]; !keep {
			result.ToRemove = append(result.ToRemove, item.ItemID())
		}
	}
	previousID := ""
	for _, item := range newItems {
		itemID := item.ItemID()
		if _, keep := common[itemID]; keep {
			oldMessage, oldOK := oldByID[itemID].(*ChatMessage)
			newMessage, newOK := item.(*ChatMessage)
			if oldOK && newOK {
				oldText, _ := oldMessage.RawTextContent()
				newText, _ := newMessage.RawTextContent()
				if oldText != newText {
					result.ToUpdate = append(result.ToUpdate, ChatContextDiffOperation{PreviousItemID: previousID, ItemID: itemID})
				}
			}
			previousID = itemID
			continue
		}
		result.ToCreate = append(result.ToCreate, ChatContextDiffOperation{PreviousItemID: previousID, ItemID: itemID})
		previousID = itemID
	}
	return result
}

func longestCommonSubsequence(left, right []string) []string {
	if len(left) == 0 || len(right) == 0 {
		return nil
	}
	if len(left) == 1 {
		for _, candidate := range right {
			if left[0] == candidate {
				return []string{left[0]}
			}
		}
		return nil
	}
	middle := len(left) / 2
	prefix := lcsPrefixLengths(left[:middle], right)
	suffix := lcsSuffixLengths(left[middle:], right)
	split, best := 0, -1
	for index := 0; index <= len(right); index++ {
		if score := prefix[index] + suffix[index]; score > best {
			split, best = index, score
		}
	}
	prefix, suffix = nil, nil
	first := longestCommonSubsequence(left[:middle], right[:split])
	second := longestCommonSubsequence(left[middle:], right[split:])
	return append(first, second...)
}

func lcsPrefixLengths(left, right []string) []int {
	previous := make([]int, len(right)+1)
	current := make([]int, len(right)+1)
	for _, leftID := range left {
		for index, rightID := range right {
			if leftID == rightID {
				current[index+1] = previous[index] + 1
			} else if previous[index+1] > current[index] {
				current[index+1] = previous[index+1]
			} else {
				current[index+1] = current[index]
			}
		}
		previous, current = current, previous
		clear(current)
	}
	return previous
}

func lcsSuffixLengths(left, right []string) []int {
	next := make([]int, len(right)+1)
	current := make([]int, len(right)+1)
	for leftIndex := len(left) - 1; leftIndex >= 0; leftIndex-- {
		for rightIndex := len(right) - 1; rightIndex >= 0; rightIndex-- {
			if left[leftIndex] == right[rightIndex] {
				current[rightIndex] = next[rightIndex+1] + 1
			} else if next[rightIndex] > current[rightIndex+1] {
				current[rightIndex] = next[rightIndex]
			} else {
				current[rightIndex] = current[rightIndex+1]
			}
		}
		next, current = current, next
		clear(current)
	}
	return next
}
