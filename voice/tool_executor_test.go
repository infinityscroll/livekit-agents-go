// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/livekit/agents-go/llm"
)

type toolArgs struct {
	Value string `json:"value"`
}

func testTool(name string, mode llm.DuplicateMode, flags llm.ToolFlag, execute func(context.Context, toolArgs) (any, error)) llm.ExecutableTool {
	return llm.MustTool(llm.FunctionToolOptions[toolArgs, any]{
		Name: name, OnDuplicate: mode, Flags: flags,
		Execute: func(ctx context.Context, input toolArgs, _ llm.ToolOptions) (any, error) { return execute(ctx, input) },
	})
}

func TestToolExecutorPreservesCallOrder(t *testing.T) {
	tool := testTool("echo", llm.DuplicateAllow, 0, func(_ context.Context, input toolArgs) (any, error) {
		if input.Value == "first" {
			time.Sleep(10 * time.Millisecond)
		}
		return input.Value, nil
	})
	tools, _ := llm.NewToolContext(tool)
	executor, _ := NewToolExecutor(ToolExecutorOptions{MaxConcurrency: 2})
	calls := []*llm.FunctionCall{
		llm.NewFunctionCall("one", "echo", `{"value":"first"}`),
		llm.NewFunctionCall("two", "echo", `{"value":"second"}`),
	}
	results, err := executor.Execute(context.Background(), calls, tools)
	if err != nil {
		t.Fatal(err)
	}
	if results[0].Output.Output != "first" || results[1].Output.Output != "second" {
		t.Fatalf("outputs = %q, %q", results[0].Output.Output, results[1].Output.Output)
	}
}

func TestToolExecutorDuplicateConfirmStripsControlArgument(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	tool := testTool("slow", llm.DuplicateConfirm, 0, func(_ context.Context, input toolArgs) (any, error) {
		if input.Value == "first" {
			close(started)
			<-release
		}
		return input.Value, nil
	})
	tools, _ := llm.NewToolContext(tool)
	executor, _ := NewToolExecutor(ToolExecutorOptions{MaxConcurrency: 2})
	firstDone := make(chan error, 1)
	go func() {
		_, err := executor.Execute(context.Background(), []*llm.FunctionCall{llm.NewFunctionCall("one", "slow", `{"value":"first"}`)}, tools)
		firstDone <- err
	}()
	<-started
	denied, err := executor.Execute(context.Background(), []*llm.FunctionCall{llm.NewFunctionCall("two", "slow", `{"value":"second"}`)}, tools)
	if err != nil {
		t.Fatal(err)
	}
	if denied[0].Output.IsError || denied[0].Err != nil || denied[0].Value == nil {
		t.Fatalf("duplicate confirmation prompt was not returned as a normal tool result: %#v", denied[0])
	}
	confirmed, err := executor.Execute(context.Background(), []*llm.FunctionCall{llm.NewFunctionCall("three", "slow", `{"value":"second","lk_agents_confirm_duplicate":true}`)}, tools)
	if err != nil {
		t.Fatal(err)
	}
	if confirmed[0].Output.IsError || confirmed[0].Output.Output != "second" {
		t.Fatalf("confirmed = %#v", confirmed[0])
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
}

func TestToolExecutorDuplicateTemplateCallbackReceivesPortableCalls(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var argsMu sync.Mutex
	var received llm.DuplicatePromptArgs
	tool := testTool("templated_duplicate", llm.DuplicateReject, 0, func(_ context.Context, input toolArgs) (any, error) {
		if input.Value == "first" {
			close(started)
			<-release
		}
		return input.Value, nil
	})
	tools, _ := llm.NewToolContext(tool)
	executor, err := NewToolExecutor(ToolExecutorOptions{ToolHandling: &AsyncToolOptions{
		DuplicateRejectTemplateFunc: func(args llm.DuplicatePromptArgs) string {
			argsMu.Lock()
			received = args
			argsMu.Unlock()
			return "custom duplicate prompt"
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	firstDone := make(chan error, 1)
	go func() {
		_, executeErr := executor.Execute(t.Context(), []*llm.FunctionCall{
			llm.NewFunctionCall("original-call", tool.Name(), `{"value":"first"}`),
		}, tools)
		firstDone <- executeErr
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("original duplicate-scope call did not start")
	}
	result, err := executor.Execute(t.Context(), []*llm.FunctionCall{
		llm.NewFunctionCall("duplicate-call", tool.Name(), `{"value":"second"}`),
	}, tools)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 || result[0].Err != nil || result[0].Output == nil || result[0].Output.IsError || result[0].Output.Output != "custom duplicate prompt" {
		t.Fatalf("duplicate prompt result = %#v", result)
	}
	argsMu.Lock()
	got := received
	argsMu.Unlock()
	if got.FunctionName != tool.Name() || len(got.FunctionCallsJSON) != 1 || got.FunctionCallsText != got.FunctionCallsJSON[0] {
		t.Fatalf("duplicate template args = %#v", got)
	}
	var wire struct {
		Type   string `json:"type"`
		CallID string `json:"callId"`
		Name   string `json:"name"`
	}
	if err := json.Unmarshal([]byte(got.FunctionCallsJSON[0]), &wire); err != nil {
		t.Fatalf("decode portable function call: %v", err)
	}
	if wire.Type != string(llm.ItemFunctionCall) || wire.CallID != "original-call" || wire.Name != tool.Name() {
		t.Fatalf("portable function call = %#v", wire)
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
}

func TestResolveAsyncToolOptionsPinnedDefaults(t *testing.T) {
	options := resolveAsyncToolOptions(nil)
	checks := map[string]string{
		"update":           options.UpdateTemplate,
		"duplicateReject":  options.DuplicateRejectTemplate,
		"duplicateConfirm": options.DuplicateConfirmTemplate,
		"replyAtTail":      options.ReplyAtTailTemplate,
		"replyMaybe":       options.ReplyMaybeCoveredTemplate,
	}
	for name, value := range checks {
		if strings.TrimSpace(value) == "" {
			t.Fatalf("%s default template is empty", name)
		}
	}
	if options.UpdateTemplate != UpdateTemplate ||
		options.DuplicateRejectTemplate != DuplicateRejectTemplate ||
		options.DuplicateConfirmTemplate != DuplicateConfirmTemplate ||
		options.ReplyAtTailTemplate != ReplyInstructionsAtTail ||
		options.ReplyMaybeCoveredTemplate != ReplyInstructionsMaybeCovered {
		t.Fatalf("resolved defaults diverged: %#v", options)
	}
}

func TestResolveAsyncToolOptionsPreservesExplicitEmptyTemplates(t *testing.T) {
	options := resolveAsyncToolOptions(&AsyncToolOptions{
		UpdateTemplateSet: true, DuplicateRejectTemplateSet: true,
		DuplicateConfirmTemplateSet: true, ReplyAtTailTemplateSet: true,
		ReplyMaybeCoveredTemplateSet: true,
	})
	if options.UpdateTemplate != "" || options.DuplicateRejectTemplate != "" ||
		options.DuplicateConfirmTemplate != "" || options.ReplyAtTailTemplate != "" ||
		options.ReplyMaybeCoveredTemplate != "" {
		t.Fatalf("explicit empty templates were replaced by defaults: %#v", options)
	}
}

func TestToolExecutorCancellation(t *testing.T) {
	started := make(chan struct{})
	tool := testTool("wait", llm.DuplicateAllow, llm.ToolFlagCancellable, func(ctx context.Context, _ toolArgs) (any, error) {
		close(started)
		<-ctx.Done()
		return nil, context.Cause(ctx)
	})
	tools, _ := llm.NewToolContext(tool)
	executor, _ := NewToolExecutor(ToolExecutorOptions{})
	done := make(chan []ToolExecutionResult, 1)
	go func() {
		results, _ := executor.Execute(context.Background(), []*llm.FunctionCall{llm.NewFunctionCall("call", "wait", `{}`)}, tools)
		done <- results
	}()
	<-started
	if err := executor.Cancel("call"); err != nil {
		t.Fatal(err)
	}
	select {
	case results := <-done:
		if !results[0].Output.IsError || !errors.Is(results[0].Err, context.Canceled) {
			t.Fatalf("result = %#v", results[0])
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled tool did not stop")
	}
}

func TestToolExecutorRecoversPanic(t *testing.T) {
	tool := testTool("panic", llm.DuplicateAllow, 0, func(context.Context, toolArgs) (any, error) { panic("boom") })
	tools, _ := llm.NewToolContext(tool)
	executor, _ := NewToolExecutor(ToolExecutorOptions{})
	results, err := executor.Execute(context.Background(), []*llm.FunctionCall{llm.NewFunctionCall("call", "panic", `{}`)}, tools)
	if err != nil {
		t.Fatal(err)
	}
	if !results[0].Output.IsError || results[0].Err == nil {
		t.Fatalf("panic result = %#v", results[0])
	}
}

func TestStripDuplicateConfirmation(t *testing.T) {
	confirmed, stripped, err := stripDuplicateConfirmation(json.RawMessage(`{"x":1,"lk_agents_confirm_duplicate":true}`))
	if err != nil || !confirmed || string(stripped) != `{"x":1}` {
		t.Fatalf("strip = %v, %s, %v", confirmed, stripped, err)
	}
}
