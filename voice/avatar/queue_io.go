// SPDX-License-Identifier: Apache-2.0

package avatar

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/stream"
	"github.com/infinityscroll/livekit-agents-go/voice"
)

const DefaultQueueAudioCapacity = 64

var ErrQueueAudioOutputClosed = errors.New("avatar queue audio output is closed")

// AudioSegmentEnd marks the ordered boundary after one flushed or interrupted
// audio segment.
type AudioSegmentEnd struct{}

// QueueAudioOutputItem is a type-safe union. Exactly one of Frame or
// SegmentEnd is meaningful; SegmentEnd avoids a pointer allocation per marker.
type QueueAudioOutputItem struct {
	Frame      agents.AudioFrame
	SegmentEnd bool
}

func AudioFrameItem(frame agents.AudioFrame) QueueAudioOutputItem {
	return QueueAudioOutputItem{Frame: frame}
}

func AudioSegmentEndItem() QueueAudioOutputItem {
	return QueueAudioOutputItem{SegmentEnd: true}
}

type QueueAudioOutputClearEvent struct {
	WasCapturing bool
}

type QueueAudioOutputOptions struct {
	SampleRate        int
	Capacity          int
	WaitPlaybackStart bool
}

// AudioReceiver is the provider-worker side of queue I/O. DataStream receivers
// require stronger RTC reader cancellation than server-sdk-go v2.18.1 exposes,
// while QueueAudioOutput implements this interface without background work.
type AudioReceiver interface {
	stream.Reader[QueueAudioOutputItem]
	Start(context.Context) error
	OnClearBuffer(func(QueueAudioOutputClearEvent)) func()
	NotifyPlaybackStarted(...time.Time)
	NotifyPlaybackFinished(time.Duration, bool) error
	Close() error
}

// QueueAudioOutput is a bounded, single-stream AudioOutput for provider WebSocket
// adapters and AvatarRunner implementations. Capture, Flush, and ClearBuffer
// are linearized so a clear can never erase the boundary of the next segment.
type QueueAudioOutput struct {
	managed *voice.ManagedAudioOutput
	queue   *stream.Channel[QueueAudioOutputItem]
	clear   agents.EventEmitter[QueueAudioOutputClearEvent]

	opMu              sync.Mutex
	capturing         bool
	waitPlaybackStart bool
	closed            bool
	closeOnce         sync.Once
}

func NewQueueAudioOutput(options QueueAudioOutputOptions) (*QueueAudioOutput, error) {
	capacity := options.Capacity
	if capacity <= 0 {
		capacity = DefaultQueueAudioCapacity
	}
	output := &QueueAudioOutput{
		queue:             stream.NewChannel[QueueAudioOutputItem](capacity),
		waitPlaybackStart: options.WaitPlaybackStart,
	}
	managed, err := voice.NewManagedAudioOutput(voice.AudioOutputOptions{
		SampleRate:   options.SampleRate,
		Capabilities: voice.AudioOutputCapabilities{Pause: false},
		Capture:      output.capture,
		Flush:        output.flush,
		ClearBuffer:  output.clearBuffer,
	})
	if err != nil {
		return nil, err
	}
	output.managed = managed
	return output, nil
}

// Recv reads from the one shared stream. Multiple concurrent readers split
// items rather than broadcast them and should only be used intentionally.
func (o *QueueAudioOutput) Recv(ctx context.Context) (QueueAudioOutputItem, error) {
	return o.queue.Recv(ctx)
}

func (o *QueueAudioOutput) OnClearBuffer(fn func(QueueAudioOutputClearEvent)) func() {
	return o.clear.Subscribe(fn)
}

func (o *QueueAudioOutput) Closed() bool { return o.queue.Closed() }

// Start satisfies the receiver side of the Python queue-I/O contract. Queue
// output is ready immediately and owns no background goroutine.
func (o *QueueAudioOutput) Start(context.Context) error {
	if o.Closed() {
		return ErrQueueAudioOutputClosed
	}
	return nil
}

func (o *QueueAudioOutput) CaptureFrame(ctx context.Context, frame agents.AudioFrame) error {
	return o.managed.CaptureFrame(ctx, frame)
}

func (o *QueueAudioOutput) capture(ctx context.Context, frame agents.AudioFrame) error {
	o.opMu.Lock()
	defer o.opMu.Unlock()
	if o.closed {
		return ErrQueueAudioOutputClosed
	}
	first := !o.capturing
	if err := o.queue.Send(ctx, AudioFrameItem(frame)); err != nil {
		return err
	}
	o.capturing = true
	if first && !o.waitPlaybackStart {
		o.managed.NotifyPlaybackStarted(time.Now())
	}
	return nil
}

func (o *QueueAudioOutput) Flush(ctx context.Context) error {
	return o.managed.Flush(ctx)
}

func (o *QueueAudioOutput) flush(ctx context.Context) error {
	o.opMu.Lock()
	defer o.opMu.Unlock()
	if o.closed {
		return ErrQueueAudioOutputClosed
	}
	if !o.capturing {
		return nil
	}
	if err := o.queue.Send(ctx, AudioSegmentEndItem()); err != nil {
		return err
	}
	o.capturing = false
	return nil
}

func (o *QueueAudioOutput) ClearBuffer(ctx context.Context) error {
	return o.managed.ClearBuffer(ctx)
}

func (o *QueueAudioOutput) clearBuffer(ctx context.Context) error {
	o.opMu.Lock()
	if o.closed {
		o.opMu.Unlock()
		return ErrQueueAudioOutputClosed
	}
	wasCapturing := o.capturing
	if wasCapturing {
		if err := o.queue.Send(ctx, AudioSegmentEndItem()); err != nil {
			o.opMu.Unlock()
			return err
		}
	}
	o.capturing = false
	o.opMu.Unlock()
	o.clear.Emit(QueueAudioOutputClearEvent{WasCapturing: wasCapturing})
	return nil
}

func (o *QueueAudioOutput) NotifyPlaybackStarted(createdAt ...time.Time) {
	timestamp := time.Now()
	if len(createdAt) != 0 && !createdAt[0].IsZero() {
		timestamp = createdAt[0]
	}
	if o.waitPlaybackStart {
		o.managed.NotifyPlaybackStarted(timestamp)
	}
}

// NotifyPlaybackFinished mirrors the Python/TypeScript transport callback.
func (o *QueueAudioOutput) NotifyPlaybackFinished(playbackPosition time.Duration, interrupted bool) error {
	return o.managed.NotifyPlaybackFinished(voice.PlaybackFinishedEvent{PlaybackPosition: playbackPosition, Interrupted: interrupted})
}

// NotifyPlaybackFinishedEvent additionally preserves synchronized transcript.
func (o *QueueAudioOutput) NotifyPlaybackFinishedEvent(event voice.PlaybackFinishedEvent) error {
	return o.managed.NotifyPlaybackFinished(event)
}

func (o *QueueAudioOutput) WaitForPlayout(ctx context.Context) (voice.PlaybackFinishedEvent, error) {
	return o.managed.WaitForPlayout(ctx)
}

func (o *QueueAudioOutput) Pause(ctx context.Context) error  { return o.managed.Pause(ctx) }
func (o *QueueAudioOutput) Resume(ctx context.Context) error { return o.managed.Resume(ctx) }
func (o *QueueAudioOutput) CanPause() bool                   { return false }
func (o *QueueAudioOutput) SampleRate() int                  { return o.managed.SampleRate() }
func (o *QueueAudioOutput) SetAttached(attached bool)        { o.managed.SetAttached(attached) }
func (o *QueueAudioOutput) OnAttached()                      { o.managed.OnAttached() }
func (o *QueueAudioOutput) OnDetached()                      { o.managed.OnDetached() }
func (o *QueueAudioOutput) OnPlaybackStarted(fn func(voice.PlaybackStartedEvent)) func() {
	return o.managed.OnPlaybackStarted(fn)
}
func (o *QueueAudioOutput) OnPlaybackFinished(fn func(voice.PlaybackFinishedEvent)) func() {
	return o.managed.OnPlaybackFinished(fn)
}
func (o *QueueAudioOutput) PendingPlayoutSegments() uint64 {
	return o.managed.PendingPlayoutSegments()
}
func (o *QueueAudioOutput) CapturedPlayoutSegments() uint64 {
	return o.managed.CapturedPlayoutSegments()
}

// Close gracefully closes the readable stream after already accepted items.
// It is idempotent and unblocks producers currently applying backpressure.
func (o *QueueAudioOutput) Close() error {
	o.closeOnce.Do(func() {
		// Close first so a producer blocked on the bounded queue is released
		// before Close waits for the operation mutex.
		_ = o.queue.Close()
		o.opMu.Lock()
		o.closed = true
		o.capturing = false
		o.opMu.Unlock()
		o.managed.Close()
	})
	return nil
}

// AClose is the Python/TypeScript-compatible spelling.
func (o *QueueAudioOutput) AClose(context.Context) error { return o.Close() }

var _ voice.AudioOutput = (*QueueAudioOutput)(nil)
var _ AudioReceiver = (*QueueAudioOutput)(nil)
var _ stream.Reader[QueueAudioOutputItem] = (*QueueAudioOutput)(nil)
var _ io.Closer = (*QueueAudioOutput)(nil)
