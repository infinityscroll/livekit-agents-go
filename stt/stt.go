// SPDX-License-Identifier: Apache-2.0

package stt

import (
	"context"
	"io"
	"sync"
	"time"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/metrics"
	"github.com/livekit/agents-go/stream"
)

type SpeechEventType uint8

const (
	StartOfSpeech SpeechEventType = iota
	InterimTranscript
	FinalTranscript
	EndOfSpeech
	RecognitionUsageEvent
	PreflightTranscript
)

type AlignedTranscript string

const (
	AlignedTranscriptNone  AlignedTranscript = ""
	AlignedTranscriptWord  AlignedTranscript = "word"
	AlignedTranscriptChunk AlignedTranscript = "chunk"
)

type SpeechData struct {
	Language        agents.LanguageCode
	Text            string
	StartTime       time.Duration
	EndTime         time.Duration
	Confidence      float64
	Words           []agents.TimedString
	SpeakerID       string
	SourceLanguages []agents.LanguageCode
	SourceTexts     []string
	TargetLanguages []agents.LanguageCode
	TargetTexts     []string
	Metadata        map[string]any
}

type RecognitionUsage struct {
	AudioDuration time.Duration
	InputTokens   int64
	OutputTokens  int64
}

type SpeechEvent struct {
	Type             SpeechEventType
	Alternatives     []SpeechData
	RequestID        string
	RecognitionUsage *RecognitionUsage
}

type Capabilities struct {
	Streaming         bool
	InterimResults    bool
	AlignedTranscript AlignedTranscript
	Diarization       bool
	Keyterms          bool
	ChatContext       bool
}

type ErrorEvent struct {
	Timestamp   time.Time
	Label       string
	Err         error
	Recoverable bool
}

type RecognizeOptions struct {
	ConnectOptions agents.APIConnectOptions
	Language       agents.LanguageCode
}

type StreamOptions struct {
	ConnectOptions agents.APIConnectOptions
	Language       agents.LanguageCode
}

type STT interface {
	Label() string
	Provider() string
	Model() string
	Capabilities() Capabilities
	Recognize(context.Context, []agents.AudioFrame, RecognizeOptions) (SpeechEvent, error)
	Stream(context.Context, StreamOptions) (SpeechStream, error)
	Close(context.Context) error
	OnMetrics(func(metrics.STT)) func()
	OnError(func(ErrorEvent)) func()
}

// SessionKeytermUpdater is implemented by recognizers that can update the
// framework-managed keyterm overlay without replacing caller-provided model
// options. AgentSession uses this optional interface only when
// Capabilities().Keyterms is true.
//
// Implementations must copy keyterms before returning. An empty slice clears
// keyterms left by a previous session binding.
type SessionKeytermUpdater interface {
	UpdateSessionKeyterms(keyterms []string) error
}

// Base implements provider metadata and typed events for STT implementations.
type Base struct {
	label        string
	provider     string
	model        string
	mu           sync.RWMutex
	capabilities Capabilities
	metrics      agents.EventEmitter[metrics.STT]
	errors       agents.EventEmitter[ErrorEvent]
}

func NewBase(label, provider, model string, capabilities Capabilities) *Base {
	return &Base{label: label, provider: provider, model: model, capabilities: capabilities}
}

func (b *Base) Label() string    { return b.label }
func (b *Base) Provider() string { return b.provider }

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

func (b *Base) Capabilities() Capabilities {
	b.mu.RLock()
	caps := b.capabilities
	b.mu.RUnlock()
	return caps
}

func (b *Base) UpdateCapabilities(update func(*Capabilities)) {
	b.mu.Lock()
	update(&b.capabilities)
	b.mu.Unlock()
}

func (b *Base) OnMetrics(fn func(metrics.STT)) func() { return b.metrics.Subscribe(fn) }
func (b *Base) OnError(fn func(ErrorEvent)) func()    { return b.errors.Subscribe(fn) }
func (b *Base) EmitMetrics(metric metrics.STT)        { b.metrics.Emit(metric) }
func (b *Base) EmitError(event ErrorEvent)            { b.errors.Emit(event) }

// MeasureRecognize provides the common metrics behavior of the TS/Python base
// class while leaving the provider request implementation allocation-free.
func MeasureRecognize(
	ctx context.Context,
	base *Base,
	frames []agents.AudioFrame,
	request func(context.Context) (SpeechEvent, error),
) (SpeechEvent, error) {
	started := time.Now()
	event, err := request(ctx)
	if err != nil {
		base.EmitError(ErrorEvent{Timestamp: time.Now(), Label: base.Label(), Err: err, Recoverable: retryable(err)})
		return event, err
	}
	base.EmitMetrics(metrics.STT{
		Label:         base.Label(),
		RequestID:     event.RequestID,
		Timestamp:     time.Now(),
		Duration:      time.Since(started),
		AudioDuration: agents.CalculateAudioDuration(frames),
		Streamed:      false,
		Metadata:      metrics.Metadata{ModelProvider: base.Provider(), ModelName: base.Model()},
	})
	return event, nil
}

func retryable(err error) bool {
	type retryableError interface{ Retryable() bool }
	if e, ok := err.(retryableError); ok {
		return e.Retryable()
	}
	return false
}

type StreamInput struct {
	Frame *agents.AudioFrame
	Flush bool
}

type SpeechStream interface {
	stream.Reader[SpeechEvent]
	Push(context.Context, agents.AudioFrame) error
	Flush(context.Context) error
	EndInput() error
	Close() error
}

// BaseStream supplies bounded input/output, lifecycle, and cancellation. A
// provider owns the run goroutine and consumes Inputs until io.EOF.
type BaseStream struct {
	ctx       context.Context
	cancel    context.CancelCauseFunc
	input     *stream.Channel[StreamInput]
	output    *stream.Channel[SpeechEvent]
	endOnce   sync.Once
	closeOnce sync.Once
}

func NewBaseStream(parent context.Context, capacity int) *BaseStream {
	ctx, cancel := context.WithCancelCause(parent)
	return &BaseStream{
		ctx: ctx, cancel: cancel,
		input:  stream.NewChannel[StreamInput](capacity),
		output: stream.NewChannel[SpeechEvent](capacity),
	}
}

func (s *BaseStream) Context() context.Context                      { return s.ctx }
func (s *BaseStream) Inputs() stream.Reader[StreamInput]            { return s.input }
func (s *BaseStream) Recv(ctx context.Context) (SpeechEvent, error) { return s.output.Recv(ctx) }

func (s *BaseStream) Push(ctx context.Context, frame agents.AudioFrame) error {
	return s.input.Send(ctx, StreamInput{Frame: &frame})
}

func (s *BaseStream) Flush(ctx context.Context) error {
	return s.input.Send(ctx, StreamInput{Flush: true})
}

func (s *BaseStream) EndInput() error {
	s.endOnce.Do(func() { _ = s.input.Close() })
	return nil
}

func (s *BaseStream) Emit(ctx context.Context, event SpeechEvent) error {
	return s.output.Send(ctx, event)
}

func (s *BaseStream) Finish(err error) {
	if err != nil && err != io.EOF {
		_ = s.output.Abort(err)
	} else {
		_ = s.output.Close()
	}
}

func (s *BaseStream) Close() error {
	s.closeOnce.Do(func() {
		s.cancel(stream.ErrClosed)
		_ = s.input.Abort(stream.ErrClosed)
		_ = s.output.Close()
	})
	return nil
}
