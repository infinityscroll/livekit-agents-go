// SPDX-License-Identifier: Apache-2.0

package llm

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func utilityMessage(id, text string, role ChatRole, timestamp int64) *ChatMessage {
	message := NewChatMessage(role, text)
	message.ID = id
	message.CreatedAt = time.Unix(timestamp, 0)
	return message
}

func TestComputeChatContextDiff(t *testing.T) {
	item := func(id string) *ChatMessage { return utilityMessage(id, id, RoleUser, 1) }
	tests := []struct {
		name   string
		oldIDs []string
		newIDs []string
		remove []string
		create []ChatContextDiffOperation
	}{
		{"identical", []string{"1", "2"}, []string{"1", "2"}, nil, nil},
		{"empty old", nil, []string{"1", "2"}, nil, []ChatContextDiffOperation{{ItemID: "1"}, {PreviousItemID: "1", ItemID: "2"}}},
		{"empty new", []string{"1", "2"}, nil, []string{"1", "2"}, nil},
		{"middle insert", []string{"1", "3", "4"}, []string{"1", "2", "3", "4"}, nil, []ChatContextDiffOperation{{PreviousItemID: "1", ItemID: "2"}}},
		{"mixed", []string{"1", "2", "3", "4"}, []string{"1", "5", "3", "6"}, []string{"2", "4"}, []ChatContextDiffOperation{{PreviousItemID: "1", ItemID: "5"}, {PreviousItemID: "3", ItemID: "6"}}},
		{"interleaved", []string{"1", "3", "5"}, []string{"1", "2", "3", "4", "5", "6"}, nil, []ChatContextDiffOperation{{PreviousItemID: "1", ItemID: "2"}, {PreviousItemID: "3", ItemID: "4"}, {PreviousItemID: "5", ItemID: "6"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			oldItems := make([]ChatItem, len(test.oldIDs))
			for index, id := range test.oldIDs {
				oldItems[index] = item(id)
			}
			newItems := make([]ChatItem, len(test.newIDs))
			for index, id := range test.newIDs {
				newItems[index] = item(id)
			}
			diff := ComputeChatContextDiff(NewChatContext(oldItems...), NewChatContext(newItems...))
			if fmt.Sprint(diff.ToRemove) != fmt.Sprint(test.remove) {
				t.Fatalf("remove = %v, want %v", diff.ToRemove, test.remove)
			}
			if fmt.Sprint(diff.ToCreate) != fmt.Sprint(test.create) {
				t.Fatalf("create = %v, want %v", diff.ToCreate, test.create)
			}
		})
	}
}

func TestComputeChatContextDiffUpdatesRawMessageText(t *testing.T) {
	oldMessage := utilityMessage("1", `<expression label="happy"/>hello`, RoleAssistant, 1)
	newMessage := utilityMessage("1", `<expression label="sad"/>hello`, RoleAssistant, 1)
	if oldText, _ := oldMessage.TextContent(); oldText != "hello" {
		t.Fatalf("old display text = %q", oldText)
	}
	diff := ComputeChatContextDiff(NewChatContext(oldMessage), NewChatContext(newMessage))
	want := []ChatContextDiffOperation{{ItemID: "1"}}
	if fmt.Sprint(diff.ToUpdate) != fmt.Sprint(want) {
		t.Fatalf("updates = %#v", diff.ToUpdate)
	}
}

func TestValidateChatContextStructure(t *testing.T) {
	call := NewFunctionCall("call_1", "lookup", `{"id":"123"}`)
	call.ID = "call"
	call.CreatedAt = time.Unix(2, 0)
	output := NewFunctionCallOutput("call_1", "lookup", `{"ok":true}`, false)
	output.ID = "output"
	output.CreatedAt = time.Unix(3, 0)
	valid := ValidateChatContextStructure(NewChatContext(
		utilityMessage("user", "hello", RoleUser, 1), call, output,
	))
	if !valid.Valid || valid.Errors != 0 || valid.Warnings != 0 {
		t.Fatalf("valid context rejected: %#v", valid)
	}

	first := utilityMessage("duplicate", "hello", RoleUser, 10)
	second := utilityMessage("duplicate", "   ", RoleAssistant, 5)
	orphan := NewFunctionCallOutput("missing", "lookup", "ok", false)
	orphan.ID = "orphan"
	orphan.CreatedAt = time.Unix(11, 0)
	invalid := ValidateChatContextStructure(NewChatContext(first, second, orphan))
	if invalid.Valid || invalid.Errors < 2 || invalid.Warnings < 2 {
		t.Fatalf("invalid result = %#v", invalid)
	}
	codes := make(map[ChatContextValidationCode]bool)
	for _, issue := range invalid.Issues {
		codes[issue.Code] = true
	}
	for _, code := range []ChatContextValidationCode{ValidationDuplicateID, ValidationTimestampOrder, ValidationEmptyTextTerm, ValidationOrphanFunctionOutput} {
		if !codes[code] {
			t.Fatalf("missing issue %s in %#v", code, invalid.Issues)
		}
	}
}

func TestValidateChatContextTerms(t *testing.T) {
	message := utilityMessage("message", "unused", RoleUser, 1)
	message.Content = []Content{
		ImageContent{},
		AudioContent{Frames: nil},
	}
	result := ValidateChatContextStructure(NewChatContext(message))
	if result.Valid || result.Errors != 2 {
		t.Fatalf("result = %#v", result)
	}
}

func TestFormatChatHistory(t *testing.T) {
	message := utilityMessage("msg", "hello", RoleUser, 1)
	call := NewFunctionCall("call_1", "lookup", `{"id":"123"}`)
	call.ID = "fn"
	call.CreatedAt = time.Unix(2, 0)
	output := FormatChatHistory(NewChatContext(message, call), FormatChatHistoryOptions{IncludeIDs: true, IncludeTimestamps: true})
	for _, fragment := range []string{
		"Chat history (2 items)",
		"[0] message user id=msg created_at=1.000\n  hello",
		"[1] function_call lookup call_id=call_1 id=fn created_at=2.000",
		"  {\n    \"id\": \"123\"\n  }",
	} {
		if !strings.Contains(output, fragment) {
			t.Fatalf("missing %q in:\n%s", fragment, output)
		}
	}
	if got := FormatChatHistory(nil, FormatChatHistoryOptions{}); got != "Chat history (0 items)" {
		t.Fatalf("nil format = %q", got)
	}
}

func TestLongestCommonSubsequenceUsesLinearMemoryShape(t *testing.T) {
	const size = 600
	left := make([]string, size)
	right := make([]string, 0, size)
	for index := range left {
		left[index] = fmt.Sprint(index)
		if index%3 != 1 {
			right = append(right, left[index])
		}
	}
	common := longestCommonSubsequence(left, right)
	if fmt.Sprint(common) != fmt.Sprint(right) {
		t.Fatal("large LCS mismatch")
	}
}

func BenchmarkComputeChatContextDiff(b *testing.B) {
	items := make([]ChatItem, 200)
	for index := range items {
		items[index] = utilityMessage(fmt.Sprint(index), "message", RoleUser, int64(index))
	}
	oldContext := NewChatContext(items...)
	newContext := NewChatContext(items...)
	b.ReportAllocs()
	for b.Loop() {
		_ = ComputeChatContextDiff(oldContext, newContext)
	}
}
