// SPDX-License-Identifier: Apache-2.0

// Package voicetest provides deterministic model doubles and assertion helpers
// for voice-agent tests. The package name avoids colliding with Go's testing
// package when imported without an alias.
package voicetest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/infinityscroll/livekit-agents-go/llm"
	"github.com/infinityscroll/livekit-agents-go/stream"
)

const (
	DefaultFakeLLMChunkRunes      = 3
	DefaultFakeLLMStreamCapacity  = 64
	DefaultFakeLLMObserveCapacity = 128
)

var ErrFakeLLMClosed = errors.New("fake LLM is closed")

type FakeToolCall struct {
	Name string
	Args map[string]any
}

type FakeLLMResponse struct {
	Input     string
	Content   string
	TTFT      time.Duration
	Duration  time.Duration
	ToolCalls []FakeToolCall
	Usage     *llm.CompletionUsage
	Err       error
}

type FakeLLMOptions struct {
	Responses           []FakeLLMResponse
	Label               string
	Provider            string
	Model               string
	ChunkRunes          int
	StreamCapacity      int
	ObservationCapacity int
}

type FakeLLMCall struct {
	Input       string
	ChatContext *llm.ChatContext
	ToolContext *llm.Context
	Options     llm.ChatOptions
}

// FakeLLM selects a scripted response by the latest user input, function
// output, or workflow instruction marker. It is safe to update between calls.
type FakeLLM struct {
	*llm.Base

	mu        sync.RWMutex
	responses map[string]FakeLLMResponse
	streams   map[*FakeLLMStream]struct{}
	closed    bool
	chunk     int
	capacity  int
	event     chan struct{}
	calls     *stream.Channel[FakeLLMCall]
	dropped   atomic.Uint64
	closeOnce sync.Once
	closeErr  error
}

func NewFakeLLM(options FakeLLMOptions) (*FakeLLM, error) {
	label := options.Label
	if label == "" {
		label = "fake-llm"
	}
	provider := options.Provider
	if provider == "" {
		provider = "test"
	}
	model := options.Model
	if model == "" {
		model = "fake"
	}
	chunk := options.ChunkRunes
	if chunk <= 0 {
		chunk = DefaultFakeLLMChunkRunes
	}
	capacity := options.StreamCapacity
	if capacity <= 0 {
		capacity = DefaultFakeLLMStreamCapacity
	}
	observationCapacity := options.ObservationCapacity
	if observationCapacity <= 0 {
		observationCapacity = DefaultFakeLLMObserveCapacity
	}
	value := &FakeLLM{
		Base: llm.NewBase(label, provider, model), responses: make(map[string]FakeLLMResponse, len(options.Responses)),
		streams: make(map[*FakeLLMStream]struct{}), chunk: chunk, capacity: capacity,
		event: make(chan struct{}, 1), calls: stream.NewChannel[FakeLLMCall](observationCapacity),
	}
	if err := value.AddResponses(options.Responses...); err != nil {
		return nil, err
	}
	return value, nil
}

func validateFakeResponse(response FakeLLMResponse) error {
	if response.TTFT < 0 || response.Duration < 0 {
		return errors.New("fake LLM response durations must not be negative")
	}
	for index, call := range response.ToolCalls {
		if call.Name == "" {
			return fmt.Errorf("fake LLM tool call %d has no name", index)
		}
		if _, err := json.Marshal(call.Args); err != nil {
			return fmt.Errorf("encode fake LLM tool call %q: %w", call.Name, err)
		}
	}
	return nil
}

func cloneFakeResponse(response FakeLLMResponse) FakeLLMResponse {
	copy := response
	copy.ToolCalls = make([]FakeToolCall, len(response.ToolCalls))
	for index, call := range response.ToolCalls {
		copy.ToolCalls[index] = FakeToolCall{Name: call.Name, Args: cloneAnyMap(call.Args)}
	}
	if response.Usage != nil {
		usage := *response.Usage
		copy.Usage = &usage
	}
	return copy
}

func cloneAnyMap(value map[string]any) map[string]any {
	if value == nil {
		return nil
	}
	result := make(map[string]any, len(value))
	for key, item := range value {
		result[key] = item
	}
	return result
}

func (f *FakeLLM) AddResponses(responses ...FakeLLMResponse) error {
	for _, response := range responses {
		if err := validateFakeResponse(response); err != nil {
			return err
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return ErrFakeLLMClosed
	}
	for _, response := range responses {
		f.responses[response.Input] = cloneFakeResponse(response)
	}
	return nil
}

func (f *FakeLLM) RemoveResponse(input string) bool {
	f.mu.Lock()
	_, existed := f.responses[input]
	delete(f.responses, input)
	f.mu.Unlock()
	return existed
}

func (f *FakeLLM) Calls() stream.Reader[FakeLLMCall] { return f.calls }
func (f *FakeLLM) DroppedObservations() uint64       { return f.dropped.Load() }

func (f *FakeLLM) lookup(input string) (FakeLLMResponse, bool) {
	f.mu.RLock()
	response, ok := f.responses[input]
	f.mu.RUnlock()
	return cloneFakeResponse(response), ok
}

func (f *FakeLLM) Chat(ctx context.Context, options llm.ChatOptions) (llm.LLMStream, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return nil, ErrFakeLLMClosed
	}
	value := &FakeLLMStream{BaseStream: llm.NewBaseStream(ctx, f.Base, options, f.capacity), fake: f, chunkRunes: f.chunk}
	f.streams[value] = struct{}{}
	f.mu.Unlock()
	input, _ := fakeInput(options.ChatContext)
	call := FakeLLMCall{Input: input, ChatContext: value.ChatContext(), ToolContext: value.ToolContext(), Options: cloneChatOptions(options)}
	if !f.calls.TrySend(call) {
		f.dropped.Add(1)
	}
	go value.run()
	return value, nil
}

func cloneChatOptions(options llm.ChatOptions) llm.ChatOptions {
	copy := options
	if options.ChatContext != nil {
		copy.ChatContext = options.ChatContext.Copy(llm.CopyOptions{})
	}
	copy.Extra = cloneAnyMap(options.Extra)
	return copy
}

func (f *FakeLLM) unregister(value *FakeLLMStream) {
	f.mu.Lock()
	delete(f.streams, value)
	f.mu.Unlock()
	select {
	case f.event <- struct{}{}:
	default:
	}
}

func (f *FakeLLM) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	f.closeOnce.Do(func() {
		f.mu.Lock()
		f.closed = true
		active := make([]*FakeLLMStream, 0, len(f.streams))
		for value := range f.streams {
			active = append(active, value)
		}
		f.mu.Unlock()
		for _, value := range active {
			_ = value.Close()
		}
		for {
			f.mu.RLock()
			remaining := len(f.streams)
			f.mu.RUnlock()
			if remaining == 0 {
				break
			}
			select {
			case <-f.event:
			case <-ctx.Done():
				f.closeErr = context.Cause(ctx)
			}
			if f.closeErr != nil {
				break
			}
		}
		_ = f.calls.Close()
		if err := f.Base.Close(ctx); err != nil && f.closeErr == nil {
			f.closeErr = err
		}
	})
	return f.closeErr
}

type FakeLLMStream struct {
	*llm.BaseStream
	fake       *FakeLLM
	chunkRunes int
}

func (s *FakeLLMStream) run() {
	defer s.fake.unregister(s)
	started := time.Now()
	input, err := fakeInput(s.ChatContext())
	if err != nil {
		s.Finish(err)
		return
	}
	decision, ok := s.fake.lookup(input)
	if !ok {
		s.Finish(nil)
		return
	}
	if err := waitFake(s.Context(), decision.TTFT); err != nil {
		s.Finish(err)
		return
	}
	runes := []rune(decision.Content)
	for start := 0; start < len(runes); start += s.chunkRunes {
		end := min(start+s.chunkRunes, len(runes))
		if err := s.Emit(s.Context(), llm.ChatChunk{ID: "fake", Delta: &llm.ChoiceDelta{Role: llm.RoleAssistant, Content: string(runes[start:end])}}); err != nil {
			s.Finish(err)
			return
		}
	}
	if len(decision.ToolCalls) != 0 {
		calls := make([]*llm.FunctionCall, 0, len(decision.ToolCalls))
		for index, call := range decision.ToolCalls {
			arguments, err := json.Marshal(call.Args)
			if err != nil {
				s.Finish(err)
				return
			}
			calls = append(calls, llm.NewFunctionCall(fmt.Sprintf("fake_call_%d", index), call.Name, string(arguments)))
		}
		if err := s.Emit(s.Context(), llm.ChatChunk{ID: "fake", Delta: &llm.ChoiceDelta{Role: llm.RoleAssistant, ToolCalls: calls}}); err != nil {
			s.Finish(err)
			return
		}
	}
	remaining := decision.Duration - time.Since(started)
	if err := waitFake(s.Context(), remaining); err != nil {
		s.Finish(err)
		return
	}
	if decision.Usage != nil {
		if err := s.Emit(s.Context(), llm.ChatChunk{ID: "fake", Usage: decision.Usage}); err != nil {
			s.Finish(err)
			return
		}
	}
	s.Finish(decision.Err)
}

func waitFake(ctx context.Context, duration time.Duration) error {
	if duration <= 0 {
		return context.Cause(ctx)
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-timer.C:
		return nil
	}
}

func fakeInput(chat *llm.ChatContext) (string, error) {
	if chat == nil || chat.Len() == 0 {
		return "", errors.New("no input text found")
	}
	items := chat.Items()
	for _, item := range items {
		message, ok := item.(*llm.ChatMessage)
		if !ok || message.Role != llm.RoleSystem {
			continue
		}
		text, _ := message.RawTextContent()
		lines := strings.Split(text, "\n")
		if len(lines) > 1 && strings.HasPrefix(lines[len(lines)-1], "instructions:") {
			return lines[len(lines)-1], nil
		}
	}
	switch last := items[len(items)-1].(type) {
	case *llm.ChatMessage:
		if last.Role != llm.RoleUser {
			return "", nil
		}
		text, _ := last.TextContent()
		return text, nil
	case *llm.FunctionCallOutput:
		return last.Output, nil
	default:
		return "", nil
	}
}

var _ llm.LLM = (*FakeLLM)(nil)
var _ llm.LLMStream = (*FakeLLMStream)(nil)
