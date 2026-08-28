// SPDX-License-Identifier: Apache-2.0

package llm

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"time"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/metrics"
	"github.com/livekit/agents-go/stream"
)

type ChoiceDelta struct {
	Role      ChatRole
	Content   string
	ToolCalls []*FunctionCall
	Extra     map[string]any
}

type CompletionUsage struct {
	CompletionTokens    int64
	PromptTokens        int64
	PromptCachedTokens  int64
	CacheCreationTokens int64
	TotalTokens         int64
	ServiceTier         string
}

type ChatChunk struct {
	ID    string
	Delta *ChoiceDelta
	Usage *CompletionUsage
}

func HasResponse(chunk ChatChunk) bool {
	return chunk.Delta != nil && (chunk.Delta.Content != "" || len(chunk.Delta.ToolCalls) != 0)
}

type CollectedResponse struct {
	Text      string
	ToolCalls []*FunctionCall
	Usage     *CompletionUsage
	Extra     map[string]any
}

type ErrorEvent struct {
	Timestamp   time.Time
	Label       string
	Err         error
	Recoverable bool
}

type ChatOptions struct {
	ChatContext       *ChatContext
	ToolContext       *Context
	ConnectOptions    agents.APIConnectOptions
	ParallelToolCalls bool
	ToolChoice        ToolChoice
	Extra             map[string]any
}

type LLM interface {
	Label() string
	Provider() string
	Model() string
	Chat(context.Context, ChatOptions) (LLMStream, error)
	Prewarm(context.Context)
	Close(context.Context) error
	OnMetrics(func(metrics.LLM)) func()
	OnError(func(ErrorEvent)) func()
}

type Base struct {
	label    string
	provider string

	mu      sync.RWMutex
	model   string
	closed  bool
	prewarm func(context.Context) error
	cancel  context.CancelCauseFunc
	wg      sync.WaitGroup

	metrics agents.EventEmitter[metrics.LLM]
	errors  agents.EventEmitter[ErrorEvent]
}

func NewBase(label, provider, model string) *Base {
	return &Base{label: label, provider: provider, model: model}
}
func (b *Base) Label() string { return b.label }
func (b *Base) Provider() string {
	if b.provider == "" {
		return "unknown"
	}
	return b.provider
}
func (b *Base) Model() string {
	b.mu.RLock()
	model := b.model
	b.mu.RUnlock()
	if model == "" {
		return "unknown"
	}
	return model
}
func (b *Base) SetModel(model string)                     { b.mu.Lock(); b.model = model; b.mu.Unlock() }
func (b *Base) SetPrewarm(fn func(context.Context) error) { b.mu.Lock(); b.prewarm = fn; b.mu.Unlock() }
func (b *Base) OnMetrics(fn func(metrics.LLM)) func()     { return b.metrics.Subscribe(fn) }
func (b *Base) OnError(fn func(ErrorEvent)) func()        { return b.errors.Subscribe(fn) }
func (b *Base) EmitMetrics(metric metrics.LLM)            { b.metrics.Emit(metric) }
func (b *Base) EmitError(event ErrorEvent)                { b.errors.Emit(event) }

func (b *Base) Prewarm(parent context.Context) {
	b.mu.Lock()
	if b.closed || b.prewarm == nil || b.cancel != nil {
		b.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancelCause(parent)
	b.cancel = cancel
	fn := b.prewarm
	b.wg.Add(1)
	b.mu.Unlock()
	go func() { defer b.wg.Done(); _ = fn(ctx) }()
}

func (b *Base) Close(ctx context.Context) error {
	b.mu.Lock()
	b.closed = true
	cancel := b.cancel
	b.mu.Unlock()
	if cancel != nil {
		cancel(stream.ErrClosed)
	}
	done := make(chan struct{})
	go func() { b.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

type LLMStream interface {
	stream.Reader[ChatChunk]
	Collect(context.Context) (CollectedResponse, error)
	Close() error
	ChatContext() *ChatContext
	ToolContext() *Context
}

type BaseStream struct {
	ctx       context.Context
	cancel    context.CancelCauseFunc
	output    *stream.Channel[ChatChunk]
	base      *Base
	chat      *ChatContext
	tools     *Context
	started   time.Time
	mu        sync.Mutex
	first     time.Time
	requestID string
	usage     *CompletionUsage
	finished  bool
	closeOnce sync.Once
}

func NewBaseStream(parent context.Context, base *Base, options ChatOptions, capacity int) *BaseStream {
	ctx, cancel := context.WithCancelCause(parent)
	chat := options.ChatContext
	if chat == nil {
		chat = EmptyChatContext()
	}
	return &BaseStream{ctx: ctx, cancel: cancel, output: stream.NewChannel[ChatChunk](capacity), base: base, chat: chat.Copy(CopyOptions{}), tools: options.ToolContext, started: time.Now()}
}
func (s *BaseStream) Context() context.Context                    { return s.ctx }
func (s *BaseStream) ChatContext() *ChatContext                   { return s.chat.Copy(CopyOptions{}) }
func (s *BaseStream) ToolContext() *Context                       { return s.tools }
func (s *BaseStream) Recv(ctx context.Context) (ChatChunk, error) { return s.output.Recv(ctx) }

func (s *BaseStream) Emit(ctx context.Context, chunk ChatChunk) error {
	s.mu.Lock()
	if HasResponse(chunk) && s.first.IsZero() {
		s.first = time.Now()
	}
	if chunk.ID != "" {
		s.requestID = chunk.ID
	}
	if chunk.Usage != nil {
		usage := *chunk.Usage
		s.usage = &usage
	}
	s.mu.Unlock()
	return s.output.Send(ctx, chunk)
}

func (s *BaseStream) Finish(err error) {
	s.mu.Lock()
	if s.finished {
		s.mu.Unlock()
		return
	}
	s.finished = true
	first, requestID, usage := s.first, s.requestID, s.usage
	s.mu.Unlock()
	if err != nil && err != io.EOF {
		_ = s.output.Abort(err)
	} else {
		_ = s.output.Close()
	}
	now := time.Now()
	duration := now.Sub(s.started)
	ttft := time.Duration(-1)
	if !first.IsZero() {
		ttft = first.Sub(s.started)
	}
	metric := metrics.LLM{Label: s.base.Label(), RequestID: requestID, Timestamp: now, Duration: duration, TimeToFirstToken: ttft, Cancelled: context.Cause(s.ctx) != nil, Metadata: metrics.Metadata{ModelProvider: s.base.Provider(), ModelName: s.base.Model()}}
	if usage != nil {
		metric.CompletionTokens = usage.CompletionTokens
		metric.PromptTokens = usage.PromptTokens
		metric.PromptCachedTokens = usage.PromptCachedTokens
		metric.CacheCreationTokens = usage.CacheCreationTokens
		metric.TotalTokens = usage.TotalTokens
		if duration > 0 {
			metric.TokensPerSecond = float64(usage.CompletionTokens) / duration.Seconds()
		}
	}
	s.base.EmitMetrics(metric)
}

func (s *BaseStream) Collect(ctx context.Context) (CollectedResponse, error) {
	result := CollectedResponse{Extra: make(map[string]any)}
	var text strings.Builder
	for {
		chunk, err := s.Recv(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				result.Text = strings.TrimSpace(text.String())
				return result, nil
			}
			return result, err
		}
		if chunk.Delta != nil {
			text.WriteString(chunk.Delta.Content)
			result.ToolCalls = append(result.ToolCalls, chunk.Delta.ToolCalls...)
			for key, value := range chunk.Delta.Extra {
				result.Extra[key] = value
			}
		}
		if chunk.Usage != nil {
			usage := *chunk.Usage
			result.Usage = &usage
		}
	}
}

func (s *BaseStream) Close() error {
	s.closeOnce.Do(func() { s.cancel(stream.ErrClosed); s.Finish(nil) })
	return nil
}

// RunWithRetry centralizes provider retry behavior and error events.
func RunWithRetry(ctx context.Context, base *Base, options agents.APIConnectOptions, operation func(context.Context, int) error) error {
	resolved := options.Resolve()
	var last error
	for attempt := 0; attempt <= resolved.MaxRetries; attempt++ {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		err := operation(ctx, attempt)
		if err == nil {
			return nil
		}
		last = err
		var apiErr *agents.APIError
		if !errors.As(err, &apiErr) || !apiErr.Retryable() || attempt == resolved.MaxRetries {
			base.EmitError(ErrorEvent{Timestamp: time.Now(), Label: base.Label(), Err: err, Recoverable: false})
			break
		}
		base.EmitError(ErrorEvent{Timestamp: time.Now(), Label: base.Label(), Err: err, Recoverable: true})
		delay := resolved.RetryDelay(attempt)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return context.Cause(ctx)
		case <-timer.C:
		}
	}
	if resolved.MaxRetries > 0 {
		return agents.NewAPIConnectionError("failed to generate LLM completion after retries", false, last)
	}
	return last
}
