// SPDX-License-Identifier: Apache-2.0

package stt

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/metrics"
	"github.com/livekit/agents-go/stream"
	"github.com/livekit/agents-go/vad"
)

const defaultStreamAdapterCapacity = 32

// StreamAdapterOptions controls the bounded queues used by StreamAdapter.
// Zero values select production defaults.
type StreamAdapterOptions struct {
	StreamCapacity int
}

// StreamAdapter turns a batch STT into a streaming STT by recognizing each
// utterance delimited by a VAD. The wrapped STT and VAD remain caller-owned.
// Child metrics and errors are forwarded without generating duplicate adapter
// metrics.
type StreamAdapter struct {
	base     *Base
	wrapped  STT
	detector vad.VAD
	capacity int

	ctx    context.Context
	cancel context.CancelCauseFunc

	mu            sync.Mutex
	closed        bool
	streams       map[*StreamAdapterWrapper]struct{}
	unsubscribers []func()
	requestWG     sync.WaitGroup
	closeDone     chan struct{}
	closeOnce     sync.Once
}

// NewStreamAdapter constructs a VAD-backed streaming adapter. At most one
// options value is accepted.
func NewStreamAdapter(wrapped STT, detector vad.VAD, options ...StreamAdapterOptions) (*StreamAdapter, error) {
	if wrapped == nil {
		return nil, errors.New("stt stream adapter: wrapped STT is required")
	}
	if detector == nil {
		return nil, errors.New("stt stream adapter: VAD is required")
	}
	if len(options) > 1 {
		return nil, errors.New("stt stream adapter: at most one options value is accepted")
	}
	var opts StreamAdapterOptions
	if len(options) == 1 {
		opts = options[0]
	}
	if opts.StreamCapacity <= 0 {
		opts.StreamCapacity = defaultStreamAdapterCapacity
	}

	caps := wrapped.Capabilities()
	caps.Streaming = true
	caps.InterimResults = false
	caps.AlignedTranscript = AlignedTranscriptNone
	caps.Diarization = false
	ctx, cancel := context.WithCancelCause(context.Background())
	a := &StreamAdapter{
		base: NewBase(
			fmt.Sprintf("stt.StreamAdapter<%s>", wrapped.Label()),
			wrapped.Provider(), wrapped.Model(), caps,
		),
		wrapped: wrapped, detector: detector, capacity: opts.StreamCapacity,
		ctx: ctx, cancel: cancel, streams: make(map[*StreamAdapterWrapper]struct{}),
		closeDone: make(chan struct{}),
	}
	a.unsubscribers = []func(){
		wrapped.OnMetrics(func(metric metrics.STT) { a.base.EmitMetrics(metric) }),
		wrapped.OnError(func(event ErrorEvent) { a.base.EmitError(event) }),
	}
	return a, nil
}

func (a *StreamAdapter) Label() string                         { return a.base.Label() }
func (a *StreamAdapter) Provider() string                      { return a.wrapped.Provider() }
func (a *StreamAdapter) Model() string                         { return a.wrapped.Model() }
func (a *StreamAdapter) Capabilities() Capabilities            { return a.base.Capabilities() }
func (a *StreamAdapter) WrappedSTT() STT                       { return a.wrapped }
func (a *StreamAdapter) OnMetrics(fn func(metrics.STT)) func() { return a.base.OnMetrics(fn) }
func (a *StreamAdapter) OnError(fn func(ErrorEvent)) func()    { return a.base.OnError(fn) }

func (a *StreamAdapter) Recognize(ctx context.Context, frames []agents.AudioFrame, options RecognizeOptions) (SpeechEvent, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return SpeechEvent{}, stream.ErrClosed
	}
	a.requestWG.Add(1)
	a.mu.Unlock()
	defer a.requestWG.Done()
	requestCtx, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(a.ctx, func() { cancel(context.Cause(a.ctx)) })
	defer func() {
		stop()
		cancel(context.Canceled)
	}()
	return a.wrapped.Recognize(requestCtx, frames, options)
}

func (a *StreamAdapter) Stream(parent context.Context, options StreamOptions) (SpeechStream, error) {
	if parent == nil {
		parent = context.Background()
	}
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return nil, stream.ErrClosed
	}
	ctx, cancel := context.WithCancelCause(parent)
	base := NewBaseStream(ctx, a.capacity)
	vadStream, err := a.detector.Stream(ctx)
	if err != nil {
		cancel(err)
		_ = base.Close()
		a.mu.Unlock()
		return nil, err
	}
	wrapper := &StreamAdapterWrapper{
		BaseStream: base, wrapped: a.wrapped, vadStream: vadStream,
		options: options, ctx: ctx, cancel: cancel, done: make(chan struct{}),
	}
	wrapper.unregister = func() {
		a.mu.Lock()
		delete(a.streams, wrapper)
		a.mu.Unlock()
	}
	a.streams[wrapper] = struct{}{}
	a.mu.Unlock()
	wrapper.stopAdapter = context.AfterFunc(a.ctx, func() {
		wrapper.cancel(context.Cause(a.ctx))
		_ = wrapper.Close()
	})
	go wrapper.run()
	return wrapper, nil
}

// Close cancels active adapter streams, detaches forwarded listeners, and
// waits for their goroutines. It intentionally does not close the caller-owned
// wrapped STT or VAD.
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
			a.requestWG.Wait()
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

// StreamAdapterWrapper is the concrete SpeechStream returned by
// StreamAdapter. Wait is useful when deterministic shutdown is required.
type StreamAdapterWrapper struct {
	*BaseStream
	wrapped   STT
	vadStream vad.VADStream
	options   StreamOptions
	ctx       context.Context
	cancel    context.CancelCauseFunc

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
		_ = s.vadStream.Close()
		_ = s.BaseStream.Close()
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
		_ = s.vadStream.Close()
	}()

	inputDone := make(chan error, 1)
	go func() { inputDone <- s.forwardInput() }()
	err := s.recognizeEvents()
	// A provider may terminate before the caller ends input. Closing the input
	// side wakes the forwarder without leaking a goroutine.
	_ = s.BaseStream.EndInput()
	if !errors.Is(err, io.EOF) {
		s.cancel(err)
		_ = s.vadStream.Close()
	} else {
		s.cancel(io.EOF)
	}
	inputErr := <-inputDone
	if errors.Is(err, io.EOF) {
		err = inputErr
	}
	if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) && context.Cause(s.ctx) == stream.ErrClosed {
		err = nil
	}
	s.BaseStream.Finish(err)
}

func (s *StreamAdapterWrapper) forwardInput() error {
	for {
		input, err := s.Inputs().Recv(s.ctx)
		if errors.Is(err, io.EOF) {
			return s.vadStream.EndInput()
		}
		if err != nil {
			return err
		}
		if input.Flush {
			if err := s.vadStream.Flush(s.ctx); err != nil {
				return err
			}
			continue
		}
		if input.Frame != nil {
			if err := s.vadStream.Push(s.ctx, *input.Frame); err != nil {
				return err
			}
		}
	}
}

func (s *StreamAdapterWrapper) recognizeEvents() error {
	for {
		event, err := s.vadStream.Recv(s.ctx)
		if err != nil {
			return err
		}
		switch event.Type {
		case vad.StartOfSpeech:
			if err := s.Emit(s.ctx, SpeechEvent{Type: StartOfSpeech}); err != nil {
				return err
			}
		case vad.EndOfSpeech:
			if err := s.Emit(s.ctx, SpeechEvent{Type: EndOfSpeech}); err != nil {
				return err
			}
			result, err := s.wrapped.Recognize(s.ctx, event.Frames, RecognizeOptions{
				ConnectOptions: s.options.ConnectOptions,
				Language:       s.options.Language,
			})
			if err != nil {
				return err
			}
			if len(result.Alternatives) == 0 || result.Alternatives[0].Text == "" {
				continue
			}
			result.Type = FinalTranscript
			if err := s.Emit(s.ctx, result); err != nil {
				return err
			}
		}
	}
}
