// SPDX-License-Identifier: Apache-2.0

package llm

import (
	"fmt"
	"sync"
	"testing"
)

func remoteMessage(id, text string) *ChatMessage {
	message := NewChatMessage(RoleUser, text)
	message.ID = id
	return message
}

func remoteIDs(context *RemoteChatContext) []string {
	items := context.ToChatContext().Items()
	ids := make([]string, len(items))
	for i, item := range items {
		ids[i] = item.ItemID()
	}
	return ids
}

func TestRemoteChatContextOrderingAndDelete(t *testing.T) {
	context := NewRemoteChatContext()
	initial := []struct {
		previous string
		message  *ChatMessage
	}{
		{"", remoteMessage("2", "two")},
		{"2", remoteMessage("4", "four")},
		{"4", remoteMessage("5", "five")},
	}
	for _, insertion := range initial {
		if err := context.Insert(insertion.previous, insertion.message); err != nil {
			t.Fatal(err)
		}
	}
	if err := context.Insert("", remoteMessage("1", "one")); err != nil {
		t.Fatal(err)
	}
	if err := context.Insert("2", remoteMessage("3", "three")); err != nil {
		t.Fatal(err)
	}
	want := []string{"1", "2", "3", "4", "5"}
	if got := remoteIDs(context); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("ordering = %v, want %v", got, want)
	}

	node, ok := context.Get("3")
	if !ok || node.PreviousItemID != "2" || node.NextItemID != "4" {
		t.Fatalf("unexpected node snapshot: %#v, %v", node, ok)
	}
	if err := context.Delete("3"); err != nil {
		t.Fatal(err)
	}
	if got := remoteIDs(context); fmt.Sprint(got) != "[1 2 4 5]" {
		t.Fatalf("ordering after delete = %v", got)
	}
	if context.Len() != 4 {
		t.Fatalf("Len = %d", context.Len())
	}
	if node, ok := context.Get("4"); !ok || node.PreviousItemID != "2" {
		t.Fatalf("neighbor not relinked: %#v, %v", node, ok)
	}
}

func TestRemoteChatContextRejectsInvalidOperations(t *testing.T) {
	context := NewRemoteChatContext()
	message := remoteMessage("1", "one")
	if err := context.Insert("missing", message); err == nil {
		t.Fatal("missing predecessor accepted")
	}
	if context.Len() != 0 {
		t.Fatal("failed insertion changed context")
	}
	if err := context.Insert("", message); err != nil {
		t.Fatal(err)
	}
	if err := context.Insert("", message); err == nil {
		t.Fatal("duplicate accepted")
	}
	if err := context.Delete("missing"); err == nil {
		t.Fatal("missing delete accepted")
	}
}

func TestRemoteChatContextDefensiveCopies(t *testing.T) {
	context := NewRemoteChatContext()
	message := remoteMessage("1", "original")
	if err := context.Insert("", message); err != nil {
		t.Fatal(err)
	}
	message.Content[0] = TextContent("mutated")
	node, _ := context.Get("1")
	node.Item.(*ChatMessage).Content[0] = TextContent("snapshot mutation")
	stored, _ := context.Get("1")
	text, _ := stored.Item.(*ChatMessage).RawTextContent()
	if text != "original" {
		t.Fatalf("stored text = %q", text)
	}
}

func TestRemoteChatContextConcurrentReads(t *testing.T) {
	context := NewRemoteChatContext()
	if err := context.Insert("", remoteMessage("0", "zero")); err != nil {
		t.Fatal(err)
	}
	var wait sync.WaitGroup
	for i := 0; i < 16; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for j := 0; j < 100; j++ {
				_, _ = context.Get("0")
				_ = context.ToChatContext()
			}
		}()
	}
	wait.Wait()
}
