// SPDX-License-Identifier: Apache-2.0

package llm

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"strings"
	"sync"
	"time"

	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/metrics"
	"github.com/infinityscroll/livekit-agents-go/stream"
)

const (
	defaultFallbackAttemptTimeout = 5 * time.Second
	defaultFallbackRetryInterval  = 500 * time.Millisecond
	defaultFallbackStreamCapacity = 32
)

// AvailabilityChangedEvent reports a provider entering or leaving the
// fallback rotation. Availability is advisory: when every provider is marked
// unavailable, an adapter still tries all of them in order.
type AvailabilityChangedEvent struct {
	LLM       LLM
	Available bool
}

// FallbackOptions configures a FallbackAdapter. Providers are attempted in
// slice order. A zero AttemptTimeout or RetryInterval uses the agents-js
// defaults; MaxRetriesPerLLM defaults to zero.
type FallbackOptions struct {
	LLMs             []LLM
	AttemptTimeout   time.Duration
	MaxRetriesPerLLM int
	RetryInterval    time.Duration
	RetryOnChunkSent bool
	StreamCapacity   int
	// KeepProvidersOpen transfers provider lifetime ownership to the caller.
	// By default Close closes every provider after in-flight recovery ends.
	KeepProvidersOpen bool
}

type fallbackLLMStatus struct {
	available  bool
	recovering bool
}

// FallbackAdapter implements ordered LLM failover with shared health state,
// bounded streams, and at most one recovery probe per provider. It forwards
// child metrics and errors without synthesizing duplicate adapter metrics.
type FallbackAdapter struct {
	base *Base

	llms             []LLM
	attemptTimeout   time.Duration
	maxRetries       int
	retryInterval    time.Duration
	retryOnChunkSent bool
	streamCapacity   int
	closeProviders   bool

	ctx    context.Context
	cancel context.CancelCauseFunc

	mu            sync.RWMutex
	status        []fallbackLLMStatus
	closed        bool
	closeDone     chan struct{}
	closeErr      error
	recoveryWG    sync.WaitGroup
	unsubscribers []func()
	availability  agents.EventEmitter[AvailabilityChangedEvent]
}

func NewFallbackAdapter(options FallbackOptions) (*FallbackAdapter, error) {
	if len(options.LLMs) == 0 {
		return nil, errors.New("at least one LLM instance must be provided")
	}
	if options.MaxRetriesPerLLM < 0 {
		return nil, errors.New("max retries per LLM must not be negative")
	}
	if options.AttemptTimeout < 0 {
		return nil, errors.New("attempt timeout must not be negative")
	}
	if options.RetryInterval < 0 {
		return nil, errors.New("retry interval must not be negative")
	}
	if options.AttemptTimeout == 0 {
		options.AttemptTimeout = defaultFallbackAttemptTimeout
	}
	if options.RetryInterval == 0 {
		options.RetryInterval = defaultFallbackRetryInterval
	}
	if options.StreamCapacity <= 0 {
		options.StreamCapacity = defaultFallbackStreamCapacity
	}

	ctx, cancel := context.WithCancelCause(context.Background())
	a := &FallbackAdapter{
		base:             NewBase("FallbackAdapter", "livekit", "FallbackAdapter"),
		llms:             append([]LLM(nil), options.LLMs...),
		attemptTimeout:   options.AttemptTimeout,
		maxRetries:       options.MaxRetriesPerLLM,
		retryInterval:    options.RetryInterval,
		retryOnChunkSent: options.RetryOnChunkSent,
		streamCapacity:   options.StreamCapacity,
		closeProviders:   !options.KeepProvidersOpen,
		ctx:              ctx,
		cancel:           cancel,
		status:           make([]fallbackLLMStatus, len(options.LLMs)),
		closeDone:        make(chan struct{}),
	}
	for i := range a.status {
		a.status[i].available = true
	}
	for _, provider := range a.llms {
		provider := provider
		a.unsubscribers = append(a.unsubscribers,
			provider.OnMetrics(func(metric metrics.LLM) { a.base.EmitMetrics(metric) }),
			provider.OnError(func(event ErrorEvent) { a.base.EmitError(event) }),
		)
	}
	return a, nil
}

func (a *FallbackAdapter) Label() string    { return a.base.Label() }
func (a *FallbackAdapter) Provider() string { return a.base.Provider() }
func (a *FallbackAdapter) Model() string    { return a.base.Model() }

func (a *FallbackAdapter) Providers() []LLM { return append([]LLM(nil), a.llms...) }

func (a *FallbackAdapter) OnMetrics(fn func(metrics.LLM)) func() { return a.base.OnMetrics(fn) }
func (a *FallbackAdapter) OnError(fn func(ErrorEvent)) func()    { return a.base.OnError(fn) }
func (a *FallbackAdapter) OnAvailabilityChanged(fn func(AvailabilityChangedEvent)) func() {
	return a.availability.Subscribe(fn)
}

func (a *FallbackAdapter) Availability() []bool {
	a.mu.RLock()
	result := make([]bool, len(a.status))
	for i := range a.status {
		result[i] = a.status[i].available
	}
	a.mu.RUnlock()
	return result
}

func (a *FallbackAdapter) Prewarm(ctx context.Context) {
	for _, provider := range a.llms {
		provider.Prewarm(ctx)
	}
}

func (a *FallbackAdapter) Chat(parent context.Context, options ChatOptions) (LLMStream, error) {
	if parent == nil {
		parent = context.Background()
	}
	a.mu.RLock()
	closed := a.closed
	a.mu.RUnlock()
	if closed {
		return nil, stream.ErrClosed
	}
	s := newFallbackLLMStream(parent, a, options)
	go s.run()
	return s, nil
}

func (a *FallbackAdapter) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	a.mu.Lock()
	if !a.closed {
		a.closed = true
		unsubscribers := a.unsubscribers
		a.unsubscribers = nil
		a.cancel(stream.ErrClosed)
		for _, unsubscribe := range unsubscribers {
			unsubscribe()
		}
		go a.finishClose()
	}
	done := a.closeDone
	a.mu.Unlock()
	select {
	case <-done:
		a.mu.RLock()
		err := a.closeErr
		a.mu.RUnlock()
		return err
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (a *FallbackAdapter) finishClose() {
	a.recoveryWG.Wait()
	var result error
	if !a.closeProviders {
		// The providers are caller-owned, but recovery work is adapter-owned and
		// must still finish before Close reports completion.
	} else {
		for _, provider := range a.llms {
			if err := provider.Close(context.Background()); err != nil {
				result = errors.Join(result, err)
			}
		}
	}
	a.mu.Lock()
	a.closeErr = result
	close(a.closeDone)
	a.mu.Unlock()
}

func (a *FallbackAdapter) allUnavailable() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	for i := range a.status {
		if a.status[i].available {
			return false
		}
	}
	return true
}

func (a *FallbackAdapter) isAvailable(index int) bool {
	a.mu.RLock()
	available := a.status[index].available
	a.mu.RUnlock()
	return available
}

func (a *FallbackAdapter) markUnavailable(index int) {
	a.mu.Lock()
	if a.closed || !a.status[index].available {
		a.mu.Unlock()
		return
	}
	a.status[index].available = false
	provider := a.llms[index]
	a.mu.Unlock()
	a.availability.Emit(AvailabilityChangedEvent{LLM: provider, Available: false})
}

func (a *FallbackAdapter) startRecovery(index int, options ChatOptions) {
	a.mu.Lock()
	if a.closed || a.status[index].recovering {
		a.mu.Unlock()
		return
	}
	a.status[index].recovering = true
	a.recoveryWG.Add(1)
	a.mu.Unlock()

	go func() {
		defer a.recoveryWG.Done()
		err := a.probe(a.ctx, index, options)
		a.mu.Lock()
		a.status[index].recovering = false
		if err == nil && !a.closed && !a.status[index].available {
			a.status[index].available = true
			provider := a.llms[index]
			a.mu.Unlock()
			a.availability.Emit(AvailabilityChangedEvent{LLM: provider, Available: true})
			return
		}
		a.mu.Unlock()
	}()
}

func (a *FallbackAdapter) childConnectOptions() agents.APIConnectOptions {
	maxRetries := a.maxRetries
	if maxRetries == 0 {
		// APIConnectOptions reserves zero for its default; negative explicitly
		// disables child retries so fallback owns retry/failover policy.
		maxRetries = -1
	}
	return agents.APIConnectOptions{MaxRetries: maxRetries, RetryInterval: a.retryInterval, Timeout: a.attemptTimeout}
}

func (a *FallbackAdapter) probe(parent context.Context, index int, options ChatOptions) error {
	attemptCtx, cancel := context.WithTimeoutCause(parent, a.attemptTimeout,
		agents.NewAPITimeoutError("LLM fallback recovery timed out", true, context.DeadlineExceeded))
	defer cancel()
	options.ConnectOptions = a.childConnectOptions()
	provider := a.llms[index]
	var errorMu sync.Mutex
	var emittedError error
	unsubscribe := provider.OnError(func(event ErrorEvent) {
		errorMu.Lock()
		emittedError = event.Err
		errorMu.Unlock()
	})
	defer unsubscribe()
	child, err := provider.Chat(attemptCtx, options)
	if err != nil {
		return err
	}
	defer child.Close()
	for {
		_, err = child.Recv(attemptCtx)
		if errors.Is(err, io.EOF) {
			errorMu.Lock()
			err = emittedError
			errorMu.Unlock()
			return err
		}
		if err != nil {
			return err
		}
	}
}

type fallbackLLMStream struct {
	adapter *FallbackAdapter
	ctx     context.Context
	cancel  context.CancelCauseFunc
	stop    func() bool
	output  *stream.Channel[ChatChunk]
	options ChatOptions
	chat    *ChatContext
	tools   *Context

	mu      sync.RWMutex
	current LLMStream
	done    chan struct{}
	once    sync.Once
}

func newFallbackLLMStream(parent context.Context, adapter *FallbackAdapter, options ChatOptions) *fallbackLLMStream {
	ctx, cancel := context.WithCancelCause(parent)
	chat := options.ChatContext
	if chat == nil {
		chat = EmptyChatContext()
	}
	chat = chat.Copy(CopyOptions{})
	options.ChatContext = chat
	options.Extra = maps.Clone(options.Extra)
	s := &fallbackLLMStream{
		adapter: adapter, ctx: ctx, cancel: cancel,
		output: stream.NewChannel[ChatChunk](adapter.streamCapacity), options: options,
		chat: chat, tools: options.ToolContext, done: make(chan struct{}),
	}
	s.stop = context.AfterFunc(adapter.ctx, func() { cancel(context.Cause(adapter.ctx)) })
	return s
}

func (s *fallbackLLMStream) Recv(ctx context.Context) (ChatChunk, error) {
	return s.output.Recv(ctx)
}

func (s *fallbackLLMStream) ChatContext() *ChatContext {
	s.mu.RLock()
	current := s.current
	s.mu.RUnlock()
	if current != nil {
		return current.ChatContext()
	}
	return s.chat.Copy(CopyOptions{})
}

func (s *fallbackLLMStream) ToolContext() *Context { return s.tools }

func (s *fallbackLLMStream) Collect(ctx context.Context) (CollectedResponse, error) {
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

func (s *fallbackLLMStream) Close() error {
	s.once.Do(func() {
		s.cancel(stream.ErrClosed)
		if s.stop != nil {
			s.stop()
		}
		s.mu.RLock()
		current := s.current
		s.mu.RUnlock()
		if current != nil {
			_ = current.Close()
		}
		_ = s.output.Close()
	})
	return nil
}

func (s *fallbackLLMStream) run() {
	defer close(s.done)
	allUnavailable := s.adapter.allUnavailable()
	started := time.Now()
	var lastErr error

	for index, provider := range s.adapter.llms {
		if err := context.Cause(s.ctx); err != nil {
			s.finish(err)
			return
		}
		if !allUnavailable && !s.adapter.isAvailable(index) {
			continue
		}

		sentResponse, err := s.tryProvider(index, provider)
		if err == nil {
			s.finish(nil)
			return
		}
		if cause := context.Cause(s.ctx); cause != nil {
			s.finish(cause)
			return
		}
		lastErr = err
		s.adapter.markUnavailable(index)
		s.adapter.startRecovery(index, s.options)
		if sentResponse && !s.adapter.retryOnChunkSent {
			s.finish(err)
			return
		}
	}

	labels := make([]string, len(s.adapter.llms))
	for i, provider := range s.adapter.llms {
		labels[i] = provider.Label()
	}
	err := agents.NewAPIConnectionError(
		fmt.Sprintf("all LLMs failed (%s) after %.2fs", strings.Join(labels, ", "), time.Since(started).Seconds()),
		false, lastErr,
	)
	s.finish(err)
}

func (s *fallbackLLMStream) tryProvider(index int, provider LLM) (bool, error) {
	timeoutErr := agents.NewAPITimeoutError(
		fmt.Sprintf("LLM %s fallback attempt timed out", provider.Label()), true, context.DeadlineExceeded)
	attemptCtx, cancel := context.WithTimeoutCause(s.ctx, s.adapter.attemptTimeout, timeoutErr)
	defer cancel()

	options := s.options
	options.ConnectOptions = s.adapter.childConnectOptions()
	var errorMu sync.Mutex
	var emittedError error
	unsubscribe := provider.OnError(func(event ErrorEvent) {
		errorMu.Lock()
		emittedError = event.Err
		errorMu.Unlock()
	})
	defer unsubscribe()
	child, err := provider.Chat(attemptCtx, options)
	if err != nil {
		return false, err
	}
	defer child.Close()

	sentResponse := false
	setCurrent := true
	for {
		chunk, recvErr := child.Recv(attemptCtx)
		if recvErr != nil {
			if errors.Is(recvErr, io.EOF) {
				errorMu.Lock()
				recvErr = emittedError
				errorMu.Unlock()
				return sentResponse, recvErr
			}
			return sentResponse, recvErr
		}
		if setCurrent {
			setCurrent = false
			s.mu.Lock()
			s.current = child
			s.mu.Unlock()
		}
		if HasResponse(chunk) {
			sentResponse = true
		}
		if err := s.output.Send(s.ctx, chunk); err != nil {
			return sentResponse, err
		}
	}
}

func (s *fallbackLLMStream) finish(err error) {
	if s.stop != nil {
		s.stop()
	}
	if err == nil || errors.Is(err, io.EOF) {
		_ = s.output.Close()
		return
	}
	_ = s.output.Abort(err)
}
