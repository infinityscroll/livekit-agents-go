// SPDX-License-Identifier: Apache-2.0

package voicetest

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/livekit/agents-go/llm"
)

func TestFakeLLMTextUnicodeToolsUsageAndObservation(t *testing.T) {
	fake, err := NewFakeLLM(FakeLLMOptions{Responses: []FakeLLMResponse{{
		Input: "hello", Content: "hi 👋 world", ToolCalls: []FakeToolCall{{Name: "weather", Args: map[string]any{"city": "Paris"}}},
		Usage: &llm.CompletionUsage{CompletionTokens: 4, PromptTokens: 2, TotalTokens: 6},
	}}, ChunkRunes: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer fake.Close(context.Background())
	chat := llm.EmptyChatContext()
	_, _ = chat.AddMessage(llm.RoleUser, "hello")
	output, err := fake.Chat(context.Background(), llm.ChatOptions{ChatContext: chat})
	if err != nil {
		t.Fatal(err)
	}
	collected, err := output.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if collected.Text != "hi 👋 world" || len(collected.ToolCalls) != 1 || collected.ToolCalls[0].Name != "weather" {
		t.Fatalf("unexpected output: %+v", collected)
	}
	if collected.Usage == nil || collected.Usage.TotalTokens != 6 {
		t.Fatalf("missing usage: %+v", collected.Usage)
	}
	call, err := fake.Calls().Recv(context.Background())
	if err != nil || call.Input != "hello" || call.ChatContext.Len() != 1 {
		t.Fatalf("call=%+v err=%v", call, err)
	}
}

func TestFakeLLMInstructionAndFunctionOutputLookup(t *testing.T) {
	fake, err := NewFakeLLM(FakeLLMOptions{Responses: []FakeLLMResponse{
		{Input: "instructions:handoff", Content: "prepared"},
		{Input: "sunny", Content: "take sunglasses"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer fake.Close(context.Background())
	chat := llm.EmptyChatContext()
	_, _ = chat.AddMessage(llm.RoleSystem, "base\ninstructions:handoff")
	_, _ = chat.AddMessage(llm.RoleUser, "ignored")
	stream, _ := fake.Chat(context.Background(), llm.ChatOptions{ChatContext: chat})
	result, err := stream.Collect(context.Background())
	if err != nil || result.Text != "prepared" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	contextWithTool := llm.NewChatContext(llm.NewFunctionCallOutput("call", "weather", "sunny", false))
	stream, _ = fake.Chat(context.Background(), llm.ChatOptions{ChatContext: contextWithTool})
	result, err = stream.Collect(context.Background())
	if err != nil || result.Text != "take sunglasses" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestFakeLLMErrorTimingCancellationAndClose(t *testing.T) {
	boom := errors.New("provider down")
	fake, err := NewFakeLLM(FakeLLMOptions{Responses: []FakeLLMResponse{{Input: "fail", TTFT: time.Millisecond, Duration: 2 * time.Millisecond, Err: boom}, {Input: "slow", TTFT: time.Hour}}})
	if err != nil {
		t.Fatal(err)
	}
	chat := llm.EmptyChatContext()
	_, _ = chat.AddMessage(llm.RoleUser, "fail")
	stream, _ := fake.Chat(context.Background(), llm.ChatOptions{ChatContext: chat})
	if _, err := stream.Recv(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("expected provider error, got %v", err)
	}
	slow := llm.EmptyChatContext()
	_, _ = slow.AddMessage(llm.RoleUser, "slow")
	stream, _ = fake.Chat(context.Background(), llm.ChatOptions{ChatContext: slow})
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF after explicit close, got %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := fake.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := fake.Chat(context.Background(), llm.ChatOptions{ChatContext: chat}); !errors.Is(err, ErrFakeLLMClosed) {
		t.Fatalf("expected closed error, got %v", err)
	}
}

func TestFakeLLMEmptyInputAndDynamicResponse(t *testing.T) {
	fake, err := NewFakeLLM(FakeLLMOptions{})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := fake.Chat(context.Background(), llm.ChatOptions{ChatContext: llm.EmptyChatContext()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(context.Background()); err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("expected missing-input error, got %v", err)
	}
	if err := fake.AddResponses(FakeLLMResponse{Input: "new", Content: "ok"}); err != nil {
		t.Fatal(err)
	}
	chat := llm.EmptyChatContext()
	_, _ = chat.AddMessage(llm.RoleUser, "new")
	stream, _ = fake.Chat(context.Background(), llm.ChatOptions{ChatContext: chat})
	result, err := stream.Collect(context.Background())
	if err != nil || result.Text != "ok" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if !fake.RemoveResponse("new") {
		t.Fatal("response was not removed")
	}
	_ = fake.Close(context.Background())
}
