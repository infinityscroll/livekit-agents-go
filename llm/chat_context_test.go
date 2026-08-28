// SPDX-License-Identifier: Apache-2.0

package llm

import (
	"context"
	"slices"
	"testing"
	"time"
)

func TestInstructionsModalityAndConcat(t *testing.T) {
	t.Parallel()
	a := NewInstructions("say A", "write A")
	b := NewInstructions(" and B", " and type B")
	joined := a.Concat(b)
	if got := joined.AsModality(ModalityAudio).Value(); got != "say A and B" {
		t.Fatalf("audio = %q", got)
	}
	if got := joined.AsModality(ModalityText).Value(); got != "write A and type B" {
		t.Fatalf("text = %q", got)
	}
}

func TestChatContextCopyIsIndependent(t *testing.T) {
	t.Parallel()
	ctx := EmptyChatContext()
	message, err := ctx.AddMessage(RoleUser, "hello")
	if err != nil {
		t.Fatal(err)
	}
	copy := ctx.Copy(CopyOptions{})
	message.Content[0] = TextContent("mutated outside context")
	if !copy.Remove(message.ID) {
		t.Fatal("copy did not contain message")
	}
	if ctx.Len() != 1 || copy.Len() != 0 {
		t.Fatalf("lengths = %d, %d", ctx.Len(), copy.Len())
	}
}

func TestChatContextReadonly(t *testing.T) {
	t.Parallel()
	ctx := EmptyChatContext().AsReadonly()
	if _, err := ctx.AddMessage(RoleUser, "no"); err == nil {
		t.Fatal("AddMessage succeeded on readonly context")
	}
}

func TestChatContextInsertAndMergeAreChronological(t *testing.T) {
	base := EmptyChatContext()
	late := NewChatMessage(RoleAssistant, "late")
	late.ID, late.CreatedAt = "late", time.Unix(3, 0)
	early := NewChatMessage(RoleUser, "early")
	early.ID, early.CreatedAt = "early", time.Unix(1, 0)
	if err := base.Insert(late, early); err != nil {
		t.Fatal(err)
	}
	other := EmptyChatContext()
	middle := NewChatMessage(RoleAssistant, "middle")
	middle.ID, middle.CreatedAt = "middle", time.Unix(2, 0)
	duplicate := late.Clone()
	if err := other.Insert(middle, duplicate); err != nil {
		t.Fatal(err)
	}
	if got := base.Merge(other, CopyOptions{}); got != base {
		t.Fatal("Merge did not mutate and return the receiver")
	}
	items := base.Items()
	ids := make([]string, len(items))
	for index := range items {
		ids[index] = items[index].ItemID()
	}
	if !slices.Equal(ids, []string{"early", "middle", "late"}) {
		t.Fatalf("ordered IDs = %v", ids)
	}
}

func TestChatContextCopyFiltersInstructionsEmptyAndInactiveTools(t *testing.T) {
	tools, err := NewToolContext(MustTool(FunctionToolOptions[struct{}, struct{}]{
		Name: "active", Execute: func(_ context.Context, _ struct{}, _ ToolOptions) (struct{}, error) { return struct{}{}, nil },
	}))
	if err != nil {
		t.Fatal(err)
	}
	chat := EmptyChatContext()
	_, _ = chat.AddMessage(RoleSystem, "system")
	_, _ = chat.AddMessage(RoleDeveloper, "developer")
	_, _ = chat.AddMessage(RoleUser, "   ")
	empty := NewChatMessage(RoleUser, "")
	empty.Content = nil
	_ = chat.Insert(empty)
	_ = chat.Insert(NewFunctionCall("1", "active", `{}`), NewFunctionCall("2", "inactive", `{}`))
	filtered := chat.Copy(CopyOptions{ExcludeInstructions: true, ExcludeEmptyMessage: true, ToolContext: tools})
	items := filtered.Items()
	if len(items) != 2 {
		t.Fatalf("filtered items = %#v", items)
	}
	message, ok := items[0].(*ChatMessage)
	if !ok {
		t.Fatalf("first item = %T", items[0])
	}
	text, _ := message.RawTextContent()
	if text != "   " {
		t.Fatalf("blank-but-present message was removed: %q", text)
	}
	call, ok := items[1].(*FunctionCall)
	if !ok || call.Name != "active" {
		t.Fatalf("tool-filtered item = %#v", items[1])
	}
}

func TestChatContextTruncatePreservesSystemAndDropsLeadingToolArtifacts(t *testing.T) {
	chat := EmptyChatContext()
	system := NewChatMessage(RoleSystem, "system")
	system.ID, system.CreatedAt = "system", time.Unix(0, 0)
	old := NewChatMessage(RoleUser, "old")
	old.ID, old.CreatedAt = "old", time.Unix(1, 0)
	call := NewFunctionCall("call", "lookup", `{}`)
	call.ID, call.CreatedAt = "call", time.Unix(2, 0)
	output := NewFunctionCallOutput("call", "lookup", "ok", false)
	output.ID, output.CreatedAt = "output", time.Unix(3, 0)
	latest := NewChatMessage(RoleUser, "latest")
	latest.ID, latest.CreatedAt = "latest", time.Unix(4, 0)
	if err := chat.Insert(system, old, call, output, latest); err != nil {
		t.Fatal(err)
	}
	if got := chat.Truncate(0); got != chat || chat.Len() != 5 {
		t.Fatal("non-positive truncate did not preserve the receiver")
	}
	if got := chat.Truncate(3); got != chat {
		t.Fatal("Truncate did not return the receiver")
	}
	items := chat.Items()
	ids := make([]string, len(items))
	for index := range items {
		ids[index] = items[index].ItemID()
	}
	if !slices.Equal(ids, []string{"system", "latest"}) {
		t.Fatalf("truncated IDs = %v", ids)
	}
}
