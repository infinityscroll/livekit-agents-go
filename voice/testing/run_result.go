// SPDX-License-Identifier: Apache-2.0

package voicetest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"

	"github.com/livekit/agents-go/llm"
)

type EventType string

const (
	EventMessage            EventType = "message"
	EventFunctionCall       EventType = "function_call"
	EventFunctionCallOutput EventType = "function_call_output"
	EventAgentHandoff       EventType = "agent_handoff"
)

type RunEvent struct {
	Type     EventType
	Item     llm.ChatItem
	OldAgent any
	NewAgent any
}

func (e RunEvent) CreatedAt() int64 {
	if e.Item == nil {
		return 0
	}
	return e.Item.ItemCreatedAt().UnixNano()
}

type RunOutputOptions struct {
	MaxRetries        int
	RetryInstructions string
}

var (
	ErrRunResultNotDone = errors.New("run result is not done")
	ErrNoFinalOutput    = errors.New("run result has no final output")
)

// RunResult records the ordered chat/tool/handoff trace of one test turn. It
// can be populated by AgentSession.Run or directly by deterministic harnesses.
type RunResult[T any] struct {
	mu        sync.RWMutex
	events    []RunEvent
	seen      map[string]struct{}
	userInput string
	output    T
	hasOutput bool
	err       error
	done      chan struct{}
	doneOnce  sync.Once

	assertMu sync.Mutex
	assert   *RunAssert[T]
}

func NewRunResult[T any](userInput string) *RunResult[T] {
	return &RunResult[T]{userInput: userInput, seen: make(map[string]struct{}), done: make(chan struct{})}
}

func cloneChatItem(item llm.ChatItem) llm.ChatItem {
	if item == nil {
		return nil
	}
	items := llm.NewChatContext(item).Items()
	if len(items) == 0 {
		return nil
	}
	return items[0]
}

func eventTypeFor(item llm.ChatItem) (EventType, bool) {
	if item == nil {
		return "", false
	}
	switch item.ItemType() {
	case llm.ItemMessage:
		return EventMessage, true
	case llm.ItemFunctionCall:
		return EventFunctionCall, true
	case llm.ItemFunctionCallOutput:
		return EventFunctionCallOutput, true
	default:
		return "", false
	}
}

func (r *RunResult[T]) Record(item llm.ChatItem) bool {
	kind, ok := eventTypeFor(item)
	if !ok {
		return false
	}
	cloned := cloneChatItem(item)
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.seen[cloned.ItemID()]; exists {
		return false
	}
	r.seen[cloned.ItemID()] = struct{}{}
	r.insertLocked(RunEvent{Type: kind, Item: cloned})
	return true
}

func (r *RunResult[T]) RecordHandoff(item *llm.AgentHandoffItem, oldAgent, newAgent any) bool {
	if item == nil {
		return false
	}
	cloned := cloneChatItem(item)
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.seen[cloned.ItemID()]; exists {
		return false
	}
	r.seen[cloned.ItemID()] = struct{}{}
	r.insertLocked(RunEvent{Type: EventAgentHandoff, Item: cloned, OldAgent: oldAgent, NewAgent: newAgent})
	return true
}

func (r *RunResult[T]) insertLocked(event RunEvent) {
	// Walk from the tail because live events are normally already ordered. Use
	// a strict comparison so equal timestamps retain observation order.
	index := len(r.events)
	for index > 0 && r.events[index-1].CreatedAt() > event.CreatedAt() {
		index--
	}
	r.events = append(r.events, RunEvent{})
	copy(r.events[index+1:], r.events[index:])
	r.events[index] = event
}

func cloneRunEvent(event RunEvent) RunEvent {
	event.Item = cloneChatItem(event.Item)
	return event
}

func (r *RunResult[T]) Events() []RunEvent {
	r.mu.RLock()
	result := make([]RunEvent, len(r.events))
	for index, event := range r.events {
		result[index] = cloneRunEvent(event)
	}
	r.mu.RUnlock()
	return result
}

func (r *RunResult[T]) UserInput() string { return r.userInput }

func (r *RunResult[T]) Done() bool {
	select {
	case <-r.done:
		return true
	default:
		return false
	}
}

func (r *RunResult[T]) Complete(output T, hasOutput bool, err error) {
	r.doneOnce.Do(func() {
		r.mu.Lock()
		r.output, r.hasOutput, r.err = output, hasOutput, err
		r.mu.Unlock()
		close(r.done)
	})
}

func (r *RunResult[T]) Reject(err error) {
	var zero T
	r.Complete(zero, false, err)
}

func (r *RunResult[T]) Wait(ctx context.Context) (*RunResult[T], error) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-r.done:
		r.mu.RLock()
		err := r.err
		r.mu.RUnlock()
		return r, err
	case <-ctx.Done():
		return r, context.Cause(ctx)
	}
}

func (r *RunResult[T]) FinalOutput() (T, error) {
	if !r.Done() {
		var zero T
		return zero, ErrRunResultNotDone
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.err != nil {
		var zero T
		return zero, r.err
	}
	if !r.hasOutput {
		var zero T
		return zero, ErrNoFinalOutput
	}
	return r.output, nil
}

type TestingT interface {
	Helper()
	Fatalf(format string, args ...any)
}

type AssertionError struct{ Message string }

func (e *AssertionError) Error() string { return e.Message }

type RunAssert[T any] struct {
	result *RunResult[T]
	t      TestingT
	index  int
}

func (r *RunResult[T]) Expect() *RunAssert[T] {
	r.assertMu.Lock()
	defer r.assertMu.Unlock()
	if r.assert == nil {
		r.assert = &RunAssert[T]{result: r}
	}
	return r.assert
}

func (r *RunResult[T]) Require(t TestingT) *RunAssert[T] {
	if t != nil {
		t.Helper()
	}
	return &RunAssert[T]{result: r, t: t}
}

func (a *RunAssert[T]) fail(index int, format string, values ...any) {
	message := fmt.Sprintf(format, values...)
	message += "\nContext around failure:\n" + formatEvents(a.result.Events(), index)
	if a.t != nil {
		a.t.Helper()
		a.t.Fatalf("%s", message)
		return
	}
	panic(&AssertionError{Message: message})
}

func normalizeIndex(length, index int) int {
	if index < 0 {
		return length + index
	}
	return index
}

func (a *RunAssert[T]) At(index int) *EventAssert[T] {
	events := a.result.Events()
	normalized := normalizeIndex(len(events), index)
	if normalized < 0 || normalized >= len(events) {
		a.fail(normalized, "event index %d is out of range (total events: %d)", index, len(events))
	}
	return &EventAssert[T]{event: events[normalized], parent: a, index: normalized}
}

func (a *RunAssert[T]) NextEvent(types ...EventType) *EventAssert[T] {
	events := a.result.Events()
	for a.index < len(events) {
		index := a.index
		a.index++
		if len(types) == 0 || slices.Contains(types, events[index].Type) {
			return &EventAssert[T]{event: events[index], parent: a, index: index}
		}
	}
	a.fail(a.index, "expected another event, but none remain")
	return nil
}

func (a *RunAssert[T]) SkipNext(count int) *RunAssert[T] {
	if count < 0 {
		a.fail(a.index, "skip count must not be negative")
	}
	if a.index+count > len(a.result.Events()) {
		a.fail(a.index, "cannot skip %d events; only %d remain", count, len(a.result.Events())-a.index)
	}
	a.index += count
	return a
}

func (a *RunAssert[T]) NoMoreEvents() {
	events := a.result.Events()
	if a.index != len(events) {
		a.fail(a.index, "expected no more events, found %s", events[a.index].Type)
	}
}

func (a *RunAssert[T]) Range(start, end int) *EventRangeAssert[T] {
	events := a.result.Events()
	start = normalizeIndex(len(events), start)
	if end == -1 {
		end = len(events)
	} else {
		end = normalizeIndex(len(events), end)
	}
	if start < 0 || end < start || end > len(events) {
		a.fail(start, "invalid event range [%d:%d] for %d events", start, end, len(events))
	}
	return &EventRangeAssert[T]{events: events[start:end], parent: a, start: start, end: end}
}

func (a *RunAssert[T]) All() *EventRangeAssert[T] { return a.Range(0, -1) }

type MessageAssertOptions struct{ Role llm.ChatRole }
type FunctionCallAssertOptions struct {
	Name string
	Args map[string]any
}
type FunctionCallOutputAssertOptions struct {
	Output  *string
	IsError *bool
}
type AgentHandoffAssertOptions struct {
	NewAgentType reflect.Type
	NewAgentID   string
}

type EventAssert[T any] struct {
	event  RunEvent
	parent *RunAssert[T]
	index  int
}

func (a *EventAssert[T]) Event() RunEvent { return cloneRunEvent(a.event) }

func (a *EventAssert[T]) IsMessage(options MessageAssertOptions) *MessageAssert[T] {
	message, ok := a.event.Item.(*llm.ChatMessage)
	if !ok || a.event.Type != EventMessage {
		a.parent.fail(a.index, "expected message event, got %s", a.event.Type)
	}
	if options.Role != "" && message.Role != options.Role {
		a.parent.fail(a.index, "expected message role %q, got %q", options.Role, message.Role)
	}
	return &MessageAssert[T]{EventAssert: a, Message: message.Clone()}
}

func (a *EventAssert[T]) IsFunctionCall(options FunctionCallAssertOptions) *FunctionCallAssert[T] {
	call, ok := a.event.Item.(*llm.FunctionCall)
	if !ok || a.event.Type != EventFunctionCall {
		a.parent.fail(a.index, "expected function-call event, got %s", a.event.Type)
	}
	if options.Name != "" && call.Name != options.Name {
		a.parent.fail(a.index, "expected function %q, got %q", options.Name, call.Name)
	}
	if options.Args != nil {
		var actual map[string]any
		if err := json.Unmarshal([]byte(call.Arguments), &actual); err != nil {
			a.parent.fail(a.index, "decode arguments for %q: %v", call.Name, err)
		}
		for key, expected := range options.Args {
			value, exists := actual[key]
			if !exists || !reflect.DeepEqual(value, expected) {
				a.parent.fail(a.index, "argument %q: expected %#v, got %#v", key, expected, value)
			}
		}
	}
	return &FunctionCallAssert[T]{EventAssert: a, Call: call.Clone()}
}

func (a *EventAssert[T]) IsFunctionCallOutput(options FunctionCallOutputAssertOptions) *FunctionCallOutputAssert[T] {
	output, ok := a.event.Item.(*llm.FunctionCallOutput)
	if !ok || a.event.Type != EventFunctionCallOutput {
		a.parent.fail(a.index, "expected function-call-output event, got %s", a.event.Type)
	}
	if options.Output != nil && output.Output != *options.Output {
		a.parent.fail(a.index, "expected output %q, got %q", *options.Output, output.Output)
	}
	if options.IsError != nil && output.IsError != *options.IsError {
		a.parent.fail(a.index, "expected is_error=%t, got %t", *options.IsError, output.IsError)
	}
	copy := *output
	return &FunctionCallOutputAssert[T]{EventAssert: a, Output: &copy}
}

func (a *EventAssert[T]) IsAgentHandoff(options AgentHandoffAssertOptions) *AgentHandoffAssert[T] {
	item, ok := a.event.Item.(*llm.AgentHandoffItem)
	if !ok || a.event.Type != EventAgentHandoff {
		a.parent.fail(a.index, "expected agent-handoff event, got %s", a.event.Type)
	}
	if options.NewAgentType != nil {
		actual := reflect.TypeOf(a.event.NewAgent)
		if actual == nil || actual != options.NewAgentType && !actual.AssignableTo(options.NewAgentType) {
			a.parent.fail(a.index, "expected new agent type %v, got %v", options.NewAgentType, actual)
		}
	}
	if options.NewAgentID != "" && item.NewAgentID != options.NewAgentID {
		a.parent.fail(a.index, "expected new agent id %q, got %q", options.NewAgentID, item.NewAgentID)
	}
	copy := *item
	return &AgentHandoffAssert[T]{EventAssert: a, Handoff: &copy, OldAgent: a.event.OldAgent, NewAgent: a.event.NewAgent}
}

type EventRangeAssert[T any] struct {
	events     []RunEvent
	parent     *RunAssert[T]
	start, end int
}

func (a *EventRangeAssert[T]) find(kind EventType, match func(*EventAssert[T]) bool) *EventAssert[T] {
	for index, event := range a.events {
		candidate := &EventAssert[T]{event: event, parent: a.parent, index: a.start + index}
		if event.Type == kind && match(candidate) {
			return candidate
		}
	}
	a.parent.fail(a.start, "no %s event satisfying the criteria in range [%d:%d]", kind, a.start, a.end)
	return nil
}

func (a *EventRangeAssert[T]) ContainsMessage(options MessageAssertOptions) *MessageAssert[T] {
	return a.find(EventMessage, func(candidate *EventAssert[T]) bool {
		message, ok := candidate.event.Item.(*llm.ChatMessage)
		return ok && (options.Role == "" || message.Role == options.Role)
	}).IsMessage(options)
}

func subsetArguments(raw string, expected map[string]any) bool {
	var actual map[string]any
	if json.Unmarshal([]byte(raw), &actual) != nil {
		return false
	}
	for key, wanted := range expected {
		if got, exists := actual[key]; !exists || !reflect.DeepEqual(got, wanted) {
			return false
		}
	}
	return true
}

func (a *EventRangeAssert[T]) ContainsFunctionCall(options FunctionCallAssertOptions) *FunctionCallAssert[T] {
	return a.find(EventFunctionCall, func(candidate *EventAssert[T]) bool {
		call, ok := candidate.event.Item.(*llm.FunctionCall)
		return ok && (options.Name == "" || call.Name == options.Name) && (options.Args == nil || subsetArguments(call.Arguments, options.Args))
	}).IsFunctionCall(options)
}

func (a *EventRangeAssert[T]) ContainsFunctionCallOutput(options FunctionCallOutputAssertOptions) *FunctionCallOutputAssert[T] {
	return a.find(EventFunctionCallOutput, func(candidate *EventAssert[T]) bool {
		output, ok := candidate.event.Item.(*llm.FunctionCallOutput)
		return ok && (options.Output == nil || output.Output == *options.Output) && (options.IsError == nil || output.IsError == *options.IsError)
	}).IsFunctionCallOutput(options)
}

func (a *EventRangeAssert[T]) ContainsAgentHandoff(options AgentHandoffAssertOptions) *AgentHandoffAssert[T] {
	return a.find(EventAgentHandoff, func(candidate *EventAssert[T]) bool {
		item, ok := candidate.event.Item.(*llm.AgentHandoffItem)
		if !ok || options.NewAgentID != "" && item.NewAgentID != options.NewAgentID {
			return false
		}
		if options.NewAgentType == nil {
			return true
		}
		actual := reflect.TypeOf(candidate.event.NewAgent)
		return actual != nil && (actual == options.NewAgentType || actual.AssignableTo(options.NewAgentType))
	}).IsAgentHandoff(options)
}

type MessageAssert[T any] struct {
	*EventAssert[T]
	Message *llm.ChatMessage
}
type FunctionCallAssert[T any] struct {
	*EventAssert[T]
	Call *llm.FunctionCall
}
type FunctionCallOutputAssert[T any] struct {
	*EventAssert[T]
	Output *llm.FunctionCallOutput
}
type AgentHandoffAssert[T any] struct {
	*EventAssert[T]
	Handoff  *llm.AgentHandoffItem
	OldAgent any
	NewAgent any
}

// Judge asks an LLM to make a structured, tool-forced intent judgment.
func (a *MessageAssert[T]) Judge(ctx context.Context, model llm.LLM, intent string) error {
	if model == nil {
		return errors.New("judge LLM is required")
	}
	content, _ := a.Message.TextContent()
	if strings.TrimSpace(content) == "" {
		return errors.New("message is empty")
	}
	if strings.TrimSpace(intent) == "" {
		return errors.New("intent is required")
	}
	type judgment struct {
		Success bool   `json:"success"`
		Reason  string `json:"reason"`
	}
	tool, err := llm.NewTool(llm.FunctionToolOptions[judgment, judgment]{
		Name: "check_intent", Description: "Determine whether the message fulfills the target intent.",
		Parameters: json.RawMessage(`{"type":"object","properties":{"success":{"type":"boolean"},"reason":{"type":"string"}},"required":["success","reason"],"additionalProperties":false}`),
		Execute:    func(_ context.Context, input judgment, _ llm.ToolOptions) (judgment, error) { return input, nil },
	})
	if err != nil {
		return err
	}
	tools, err := llm.NewToolContext(tool)
	if err != nil {
		return err
	}
	chat := llm.EmptyChatContext()
	_, _ = chat.AddMessage(llm.RoleSystem, "You are a strict conversational-agent evaluator. Call check_intent exactly once with the verdict and a concise reason.")
	_, _ = chat.AddMessage(llm.RoleUser, "Intent:\n"+intent+"\n\nMessage:\n"+content)
	output, err := model.Chat(ctx, llm.ChatOptions{ChatContext: chat, ToolContext: tools, ToolChoice: llm.ToolChoice{Kind: llm.ToolChoiceRequired}, Extra: map[string]any{"temperature": 0}})
	if err != nil {
		return err
	}
	defer output.Close()
	collected, err := output.Collect(ctx)
	if err != nil {
		return err
	}
	for _, call := range collected.ToolCalls {
		if call.Name != "check_intent" {
			continue
		}
		var result judgment
		if err := json.Unmarshal([]byte(call.Arguments), &result); err != nil {
			return fmt.Errorf("decode intent judgment: %w", err)
		}
		if !result.Success {
			return &AssertionError{Message: "intent judgment failed: " + result.Reason}
		}
		return nil
	}
	return errors.New("judge LLM did not call check_intent")
}

func formatEvents(events []RunEvent, selected int) string {
	var output strings.Builder
	for index, event := range events {
		marker := "   "
		if index == selected {
			marker = ">>>"
		}
		fmt.Fprintf(&output, "%s[%d] { type: %q", marker, index, event.Type)
		switch item := event.Item.(type) {
		case *llm.ChatMessage:
			text, _ := item.TextContent()
			if len(text) > 50 {
				text = text[:50] + "..."
			}
			fmt.Fprintf(&output, ", role: %q, content: %q, interrupted: %t", item.Role, text, item.Interrupted)
		case *llm.FunctionCall:
			fmt.Fprintf(&output, ", name: %q, args: %s", item.Name, item.Arguments)
		case *llm.FunctionCallOutput:
			text := item.Output
			if len(text) > 50 {
				text = text[:50] + "..."
			}
			fmt.Fprintf(&output, ", output: %q, is_error: %t", text, item.IsError)
		case *llm.AgentHandoffItem:
			fmt.Fprintf(&output, ", old_agent_id: %q, new_agent_id: %q", item.OldAgentID, item.NewAgentID)
		}
		output.WriteString(" }\n")
	}
	return strings.TrimSuffix(output.String(), "\n")
}
