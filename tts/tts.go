// SPDX-License-Identifier: Apache-2.0

package tts

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/metrics"
	"github.com/livekit/agents-go/stream"
)

type SynthesizedAudio struct {
	RequestID        string
	SegmentID        string
	Frame            agents.AudioFrame
	DeltaText        string
	Final            bool
	TimedTranscripts []agents.TimedString
}

type Capabilities struct {
	Streaming         bool
	AlignedTranscript bool
}

type ErrorEvent struct {
	Timestamp   time.Time
	Label       string
	Err         error
	Recoverable bool
}

type SynthesizeOptions struct {
	ConnectOptions agents.APIConnectOptions
}

type StreamOptions struct {
	ConnectOptions agents.APIConnectOptions
}

type TTS interface {
	Label() string
	Provider() string
	Model() string
	SampleRate() int
	Channels() int
	Capabilities() Capabilities
	Synthesize(context.Context, string, SynthesizeOptions) (ChunkedStream, error)
	Stream(context.Context, StreamOptions) (SynthesizeStream, error)
	Close(context.Context) error
	OnMetrics(func(metrics.TTS)) func()
	OnError(func(ErrorEvent)) func()
}

type Base struct {
	label        string
	provider     string
	sampleRate   int
	channels     int
	capabilities Capabilities

	mu         sync.RWMutex
	model      string
	markupKey  string
	expressive atomic.Bool
	metrics    agents.EventEmitter[metrics.TTS]
	errors     agents.EventEmitter[ErrorEvent]
}

func NewBase(label, provider, model string, sampleRate, channels int, capabilities Capabilities) *Base {
	return &Base{
		label: label, provider: provider, model: model,
		sampleRate: sampleRate, channels: channels, capabilities: capabilities,
	}
}

func (b *Base) Label() string              { return b.label }
func (b *Base) Provider() string           { return b.provider }
func (b *Base) SampleRate() int            { return b.sampleRate }
func (b *Base) Channels() int              { return b.channels }
func (b *Base) Capabilities() Capabilities { return b.capabilities }
func (b *Base) Expressive() bool           { return b.expressive.Load() }
func (b *Base) SetExpressive(enabled bool) { b.expressive.Store(enabled) }

func (b *Base) Model() string {
	b.mu.RLock()
	model := b.model
	b.mu.RUnlock()
	if model == "" {
		return "unknown"
	}
	return model
}

func (b *Base) SetModel(model string) {
	b.mu.Lock()
	b.model = model
	b.mu.Unlock()
}

func (b *Base) MarkupProviderKey() string {
	b.mu.RLock()
	key := b.markupKey
	b.mu.RUnlock()
	return key
}

func (b *Base) SetMarkupProviderKey(key string) {
	b.mu.Lock()
	b.markupKey = key
	b.mu.Unlock()
}

func (b *Base) OnMetrics(fn func(metrics.TTS)) func() { return b.metrics.Subscribe(fn) }
func (b *Base) OnError(fn func(ErrorEvent)) func()    { return b.errors.Subscribe(fn) }
func (b *Base) EmitMetrics(metric metrics.TTS)        { b.metrics.Emit(metric) }
func (b *Base) EmitError(event ErrorEvent)            { b.errors.Emit(event) }

type ChunkedStream interface {
	stream.Reader[SynthesizedAudio]
	Close() error
}

type SynthesizeStream interface {
	stream.Reader[SynthesizedAudio]
	PushText(context.Context, string) error
	Flush(context.Context) error
	EndInput() error
	Close() error
}

type StreamInput struct {
	Text  string
	Flush bool
}

type BaseSynthesizeStream struct {
	ctx       context.Context
	cancel    context.CancelCauseFunc
	input     *stream.Channel[StreamInput]
	output    *stream.Channel[SynthesizedAudio]
	endOnce   sync.Once
	closeOnce sync.Once
}

func NewBaseSynthesizeStream(parent context.Context, capacity int) *BaseSynthesizeStream {
	ctx, cancel := context.WithCancelCause(parent)
	return &BaseSynthesizeStream{
		ctx: ctx, cancel: cancel,
		input:  stream.NewChannel[StreamInput](capacity),
		output: stream.NewChannel[SynthesizedAudio](capacity),
	}
}

func (s *BaseSynthesizeStream) Context() context.Context           { return s.ctx }
func (s *BaseSynthesizeStream) Inputs() stream.Reader[StreamInput] { return s.input }
func (s *BaseSynthesizeStream) Recv(ctx context.Context) (SynthesizedAudio, error) {
	return s.output.Recv(ctx)
}

func (s *BaseSynthesizeStream) PushText(ctx context.Context, text string) error {
	if text == "" {
		return nil
	}
	return s.input.Send(ctx, StreamInput{Text: text})
}

func (s *BaseSynthesizeStream) Flush(ctx context.Context) error {
	return s.input.Send(ctx, StreamInput{Flush: true})
}

func (s *BaseSynthesizeStream) EndInput() error {
	s.endOnce.Do(func() { _ = s.input.Close() })
	return nil
}

func (s *BaseSynthesizeStream) Emit(ctx context.Context, audio SynthesizedAudio) error {
	return s.output.Send(ctx, audio)
}

func (s *BaseSynthesizeStream) Finish(err error) {
	if err != nil && err != io.EOF {
		_ = s.output.Abort(err)
	} else {
		_ = s.output.Close()
	}
}

func (s *BaseSynthesizeStream) Close() error {
	s.closeOnce.Do(func() {
		s.cancel(stream.ErrClosed)
		_ = s.input.Abort(stream.ErrClosed)
		_ = s.output.Close()
	})
	return nil
}

type BaseChunkedStream struct {
	ctx       context.Context
	cancel    context.CancelCauseFunc
	output    *stream.Channel[SynthesizedAudio]
	closeOnce sync.Once
}

func NewBaseChunkedStream(parent context.Context, capacity int) *BaseChunkedStream {
	ctx, cancel := context.WithCancelCause(parent)
	return &BaseChunkedStream{ctx: ctx, cancel: cancel, output: stream.NewChannel[SynthesizedAudio](capacity)}
}

func (s *BaseChunkedStream) Context() context.Context { return s.ctx }
func (s *BaseChunkedStream) Recv(ctx context.Context) (SynthesizedAudio, error) {
	return s.output.Recv(ctx)
}
func (s *BaseChunkedStream) Emit(ctx context.Context, audio SynthesizedAudio) error {
	return s.output.Send(ctx, audio)
}
func (s *BaseChunkedStream) Finish(err error) {
	if err != nil && err != io.EOF {
		_ = s.output.Abort(err)
	} else {
		_ = s.output.Close()
	}
}
func (s *BaseChunkedStream) Close() error {
	s.closeOnce.Do(func() {
		s.cancel(stream.ErrClosed)
		_ = s.output.Close()
	})
	return nil
}

// RequestMetrics tracks one synthesis attempt using monotonic time.
type RequestMetrics struct {
	base       *Base
	requestID  string
	segmentID  string
	started    time.Time
	firstByte  time.Time
	characters int64
	audio      time.Duration
	streamed   bool
}

func BeginRequest(base *Base, requestID, segmentID, text string, streamed bool) *RequestMetrics {
	return &RequestMetrics{base: base, requestID: requestID, segmentID: segmentID, started: time.Now(), characters: int64(utf8.RuneCountInString(text)), streamed: streamed}
}

func (r *RequestMetrics) AddAudio(frame agents.AudioFrame) {
	if r.firstByte.IsZero() {
		r.firstByte = time.Now()
	}
	r.audio += frame.Duration()
}

func (r *RequestMetrics) Finish(cancelled bool, inputTokens, outputTokens int64) {
	now := time.Now()
	ttfb := time.Duration(0)
	if !r.firstByte.IsZero() {
		ttfb = r.firstByte.Sub(r.started)
	}
	r.base.EmitMetrics(metrics.TTS{
		Label: r.base.Label(), RequestID: r.requestID, Timestamp: now,
		TimeToFirstByte: ttfb, Duration: now.Sub(r.started), AudioDuration: r.audio,
		Cancelled: cancelled, CharactersCount: r.characters, InputTokens: inputTokens,
		OutputTokens: outputTokens, Streamed: r.streamed, SegmentID: r.segmentID,
		Metadata: metrics.Metadata{ModelProvider: r.base.Provider(), ModelName: r.base.Model()},
	})
}
