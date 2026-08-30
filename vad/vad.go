// SPDX-License-Identifier: Apache-2.0

package vad

import (
	"context"
	"io"
	"sync"
	"time"

	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/metrics"
	"github.com/infinityscroll/livekit-agents-go/stream"
)

type EventType uint8

const (
	StartOfSpeech EventType = iota
	InferenceDone
	EndOfSpeech
	MetricsCollected
)

type Event struct {
	Type                  EventType
	SamplesIndex          int64
	Timestamp             time.Time
	SpeechDuration        time.Duration
	SilenceDuration       time.Duration
	Frames                []agents.AudioFrame
	Probability           float64
	InferenceDuration     time.Duration
	Speaking              bool
	RawAccumulatedSilence time.Duration
	RawAccumulatedSpeech  time.Duration
}

type Capabilities struct {
	UpdateInterval time.Duration
}

type VAD interface {
	Label() string
	Capabilities() Capabilities
	MinSilenceDuration() (time.Duration, bool)
	Stream(context.Context) (VADStream, error)
	Close(context.Context) error
	OnMetrics(func(metrics.VAD)) func()
}

type Base struct {
	label              string
	capabilities       Capabilities
	minSilenceDuration time.Duration
	hasMinSilence      bool
	metrics            agents.EventEmitter[metrics.VAD]
}

func NewBase(label string, capabilities Capabilities) *Base {
	return &Base{label: label, capabilities: capabilities}
}

func (b *Base) Label() string              { return b.label }
func (b *Base) Capabilities() Capabilities { return b.capabilities }
func (b *Base) MinSilenceDuration() (time.Duration, bool) {
	return b.minSilenceDuration, b.hasMinSilence
}
func (b *Base) SetMinSilenceDuration(value time.Duration) {
	b.minSilenceDuration, b.hasMinSilence = value, true
}
func (b *Base) OnMetrics(fn func(metrics.VAD)) func() { return b.metrics.Subscribe(fn) }
func (b *Base) EmitMetrics(metric metrics.VAD)        { b.metrics.Emit(metric) }

type StreamInput struct {
	Frame *agents.AudioFrame
	Flush bool
}

type VADStream interface {
	stream.Reader[Event]
	Push(context.Context, agents.AudioFrame) error
	Flush(context.Context) error
	EndInput() error
	Close() error
}

type BaseStream struct {
	ctx       context.Context
	cancel    context.CancelCauseFunc
	input     *stream.Channel[StreamInput]
	output    *stream.Channel[Event]
	endOnce   sync.Once
	closeOnce sync.Once
}

func NewBaseStream(parent context.Context, capacity int) *BaseStream {
	ctx, cancel := context.WithCancelCause(parent)
	return &BaseStream{
		ctx: ctx, cancel: cancel,
		input: stream.NewChannel[StreamInput](capacity), output: stream.NewChannel[Event](capacity),
	}
}

func (s *BaseStream) Context() context.Context           { return s.ctx }
func (s *BaseStream) Inputs() stream.Reader[StreamInput] { return s.input }
func (s *BaseStream) Recv(ctx context.Context) (Event, error) {
	return s.output.Recv(ctx)
}
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
func (s *BaseStream) Emit(ctx context.Context, event Event) error {
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
