// SPDX-License-Identifier: Apache-2.0

package tts

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/metrics"
	"github.com/livekit/agents-go/stream"
	"github.com/livekit/agents-go/tokenize"
)

const defaultTTSStreamAdapterCapacity = 32

// StreamAdapterOptions controls the bounded queues used by StreamAdapter.
type StreamAdapterOptions struct {
	StreamCapacity int
}

// StreamAdapter converts a batch TTS into a streaming TTS by incrementally
// tokenizing complete sentences. The wrapped TTS remains caller-owned.
type StreamAdapter struct {
	base      *Base
	wrapped   TTS
	tokenizer *tokenize.SentenceTokenizer
	capacity  int

	ctx    context.Context
	cancel context.CancelCauseFunc

	mu            sync.Mutex
	closed        bool
	streams       map[*StreamAdapterWrapper]struct{}
	unsubscribers []func()
	closeOnce     sync.Once
	closeDone     chan struct{}
}

// NewStreamAdapter constructs a sentence-streaming wrapper. Passing a nil
// tokenizer selects the built-in tokenizer with formatting retained.
func NewStreamAdapter(wrapped TTS, sentenceTokenizer *tokenize.SentenceTokenizer, options ...StreamAdapterOptions) (*StreamAdapter, error) {
	if wrapped == nil {
		return nil, errors.New("TTS stream adapter: wrapped TTS is required")
	}
	if len(options) > 1 {
		return nil, errors.New("TTS stream adapter: at most one options value is accepted")
	}
	var opts StreamAdapterOptions
	if len(options) == 1 {
		opts = options[0]
	}
	if opts.StreamCapacity <= 0 {
		opts.StreamCapacity = defaultTTSStreamAdapterCapacity
	}
	if sentenceTokenizer == nil {
		sentenceTokenizer = tokenize.NewSentenceTokenizer(tokenize.SentenceOptions{RetainFormat: true})
	}
	ctx, cancel := context.WithCancelCause(context.Background())
	a := &StreamAdapter{
		base: NewBase(
			fmt.Sprintf("tts.StreamAdapter<%s>", wrapped.Label()),
			wrapped.Provider(), wrapped.Model(), wrapped.SampleRate(), wrapped.Channels(),
			Capabilities{Streaming: true, AlignedTranscript: true},
		),
		wrapped: wrapped, tokenizer: sentenceTokenizer, capacity: opts.StreamCapacity,
		ctx: ctx, cancel: cancel, streams: make(map[*StreamAdapterWrapper]struct{}),
		closeDone: make(chan struct{}),
	}
	a.unsubscribers = []func(){
		wrapped.OnMetrics(func(metric metrics.TTS) { a.base.EmitMetrics(metric) }),
		wrapped.OnError(func(event ErrorEvent) { a.base.EmitError(event) }),
	}
	return a, nil
}

func (a *StreamAdapter) Label() string                         { return a.base.Label() }
func (a *StreamAdapter) Provider() string                      { return a.wrapped.Provider() }
func (a *StreamAdapter) Model() string                         { return a.wrapped.Model() }
func (a *StreamAdapter) SampleRate() int                       { return a.wrapped.SampleRate() }
func (a *StreamAdapter) Channels() int                         { return a.wrapped.Channels() }
func (a *StreamAdapter) Capabilities() Capabilities            { return a.base.Capabilities() }
func (a *StreamAdapter) WrappedTTS() TTS                       { return a.wrapped }
func (a *StreamAdapter) OnMetrics(fn func(metrics.TTS)) func() { return a.base.OnMetrics(fn) }
func (a *StreamAdapter) OnError(fn func(ErrorEvent)) func()    { return a.base.OnError(fn) }

func (a *StreamAdapter) Synthesize(ctx context.Context, text string, options SynthesizeOptions) (ChunkedStream, error) {
	a.mu.Lock()
	closed := a.closed
	a.mu.Unlock()
	if closed {
		return nil, stream.ErrClosed
	}
	return a.wrapped.Synthesize(ctx, text, options)
}

func (a *StreamAdapter) Stream(parent context.Context, options StreamOptions) (SynthesizeStream, error) {
	if parent == nil {
		parent = context.Background()
	}
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return nil, stream.ErrClosed
	}
	ctx, cancel := context.WithCancelCause(parent)
	wrapper := &StreamAdapterWrapper{
		BaseSynthesizeStream: NewBaseSynthesizeStream(ctx, a.capacity),
		wrapped:              a.wrapped, sentenceStream: a.tokenizer.Stream(), options: options,
		ctx: ctx, cancel: cancel, done: make(chan struct{}),
	}
	wrapper.unregister = func() {
		a.mu.Lock()
		delete(a.streams, wrapper)
		a.mu.Unlock()
	}
	a.streams[wrapper] = struct{}{}
	a.mu.Unlock()
	wrapper.stopAdapter = context.AfterFunc(a.ctx, func() { _ = wrapper.Close() })
	go wrapper.run()
	return wrapper, nil
}

// Close stops active wrapper streams and detaches child listeners. It does not
// close the caller-owned wrapped TTS.
func (a *StreamAdapter) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	a.closeOnce.Do(func() {
		a.mu.Lock()
		a.closed = true
		unsubscribers := a.unsubscribers
		a.unsubscribers = nil
		active := make([]*StreamAdapterWrapper, 0, len(a.streams))
		for value := range a.streams {
			active = append(active, value)
		}
		a.mu.Unlock()
		a.cancel(stream.ErrClosed)
		for _, unsubscribe := range unsubscribers {
			unsubscribe()
		}
		for _, value := range active {
			_ = value.Close()
		}
		go func() {
			for _, value := range active {
				<-value.done
			}
			close(a.closeDone)
		}()
	})
	select {
	case <-a.closeDone:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// StreamAdapterWrapper is the concrete sentence-backed synthesis stream.
type StreamAdapterWrapper struct {
	*BaseSynthesizeStream
	wrapped        TTS
	sentenceStream *tokenize.SentenceStream
	options        StreamOptions
	ctx            context.Context
	cancel         context.CancelCauseFunc

	mu          sync.Mutex
	current     ChunkedStream
	done        chan struct{}
	closeOnce   sync.Once
	stopAdapter func() bool
	unregister  func()
}

func (s *StreamAdapterWrapper) Wait(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (s *StreamAdapterWrapper) Close() error {
	s.closeOnce.Do(func() {
		s.cancel(stream.ErrClosed)
		s.mu.Lock()
		current := s.current
		s.mu.Unlock()
		if current != nil {
			_ = current.Close()
		}
		_ = s.sentenceStream.Abort(stream.ErrClosed)
		_ = s.BaseSynthesizeStream.Close()
	})
	return nil
}

func (s *StreamAdapterWrapper) run() {
	defer close(s.done)
	defer s.unregister()
	defer func() {
		if s.stopAdapter != nil {
			s.stopAdapter()
		}
		_ = s.sentenceStream.Close()
	}()
	inputDone := make(chan error, 1)
	go func() { inputDone <- s.forwardInput() }()

	var cumulative time.Duration
	err := s.synthesizeTokens(&cumulative)
	if !errors.Is(err, io.EOF) {
		s.cancel(err)
		_ = s.sentenceStream.Abort(err)
	} else {
		s.cancel(io.EOF)
	}
	_ = s.BaseSynthesizeStream.EndInput()
	inputErr := <-inputDone
	if errors.Is(err, io.EOF) {
		err = inputErr
	}
	if errors.Is(err, io.EOF) || errors.Is(err, stream.ErrClosed) {
		err = nil
	}
	s.BaseSynthesizeStream.Finish(err)
}

func (s *StreamAdapterWrapper) forwardInput() error {
	for {
		input, err := s.Inputs().Recv(s.ctx)
		if errors.Is(err, io.EOF) {
			return s.sentenceStream.EndInput(s.ctx)
		}
		if err != nil {
			return err
		}
		if input.Flush {
			if err := s.sentenceStream.Flush(s.ctx); err != nil {
				return err
			}
			continue
		}
		if err := s.sentenceStream.PushText(s.ctx, input.Text); err != nil {
			return err
		}
	}
}

func (s *StreamAdapterWrapper) synthesizeTokens(cumulative *time.Duration) error {
	for {
		token, err := s.sentenceStream.Recv(s.ctx)
		if err != nil {
			return err
		}
		if token.Token == "" {
			continue
		}
		child, err := s.wrapped.Synthesize(s.ctx, token.Token, SynthesizeOptions{ConnectOptions: s.options.ConnectOptions})
		if err != nil {
			return err
		}
		s.mu.Lock()
		s.current = child
		s.mu.Unlock()
		first := true
		for {
			audio, recvErr := child.Recv(s.ctx)
			if errors.Is(recvErr, io.EOF) {
				break
			}
			if recvErr != nil {
				_ = child.Close()
				return recvErr
			}
			if first {
				start := *cumulative
				audio.TimedTranscripts = []agents.TimedString{{Text: token.Token, StartTime: &start}}
				first = false
			}
			*cumulative += audio.Frame.Duration()
			if err := s.Emit(s.ctx, audio); err != nil {
				_ = child.Close()
				return err
			}
		}
		_ = child.Close()
		s.mu.Lock()
		if s.current == child {
			s.current = nil
		}
		s.mu.Unlock()
	}
}
