// SPDX-License-Identifier: Apache-2.0

package roomio

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/voice"
)

// SynchronizedAudioOutput is an ownership-neutral proxy used by RoomIO when
// transcript synchronization is enabled. It preserves the complete audio
// contract while giving the session a distinct synchronization chain, matching
// the TypeScript/Python SDK behavior for external outputs.
type SynchronizedAudioOutput struct {
	next     voice.AudioOutput
	attached atomic.Bool
}

func NewSynchronizedAudioOutput(next voice.AudioOutput) *SynchronizedAudioOutput {
	return &SynchronizedAudioOutput{next: next}
}
func (o *SynchronizedAudioOutput) CaptureFrame(ctx context.Context, frame agents.AudioFrame) error {
	return o.next.CaptureFrame(ctx, frame)
}
func (o *SynchronizedAudioOutput) Flush(ctx context.Context) error { return o.next.Flush(ctx) }
func (o *SynchronizedAudioOutput) ClearBuffer(ctx context.Context) error {
	return o.next.ClearBuffer(ctx)
}
func (o *SynchronizedAudioOutput) WaitForPlayout(ctx context.Context) (voice.PlaybackFinishedEvent, error) {
	return o.next.WaitForPlayout(ctx)
}
func (o *SynchronizedAudioOutput) Pause(ctx context.Context) error  { return o.next.Pause(ctx) }
func (o *SynchronizedAudioOutput) Resume(ctx context.Context) error { return o.next.Resume(ctx) }
func (o *SynchronizedAudioOutput) CanPause() bool                   { return o.next.CanPause() }
func (o *SynchronizedAudioOutput) SampleRate() int                  { return o.next.SampleRate() }
func (o *SynchronizedAudioOutput) OnPlaybackStarted(fn func(voice.PlaybackStartedEvent)) func() {
	return o.next.OnPlaybackStarted(fn)
}
func (o *SynchronizedAudioOutput) OnPlaybackFinished(fn func(voice.PlaybackFinishedEvent)) func() {
	return o.next.OnPlaybackFinished(fn)
}
func (o *SynchronizedAudioOutput) PendingPlayoutSegments() uint64 {
	return o.next.PendingPlayoutSegments()
}
func (o *SynchronizedAudioOutput) CapturedPlayoutSegments() uint64 {
	return o.next.CapturedPlayoutSegments()
}
func (o *SynchronizedAudioOutput) SetAttached(attached bool) { o.attached.Store(attached) }
func (o *SynchronizedAudioOutput) OnAttached() {
	o.attached.Store(true)
	o.next.SetAttached(true)
	o.next.OnAttached()
}
func (o *SynchronizedAudioOutput) OnDetached() {
	o.attached.Store(false)
	o.next.SetAttached(false)
	o.next.OnDetached()
}

// SynchronizedTextOutput delays timed transcript chunks until their audio
// offset is reached. It creates no goroutine: playback callbacks only update a
// small state machine, while callers wait with their own context. A bounded
// single-operation gate preserves transcript order and backpressure.
type SynchronizedTextOutput struct {
	next  voice.TextOutput
	audio voice.AudioOutput

	ctx      context.Context
	cancel   context.CancelCauseFunc
	gate     chan struct{}
	closed   atomic.Bool
	attached atomic.Bool

	mu          sync.Mutex
	active      bool
	startedAt   time.Time
	epoch       uint64
	finished    uint64
	changed     chan struct{}
	unsubscribe []func()
	closeOnce   sync.Once
}

func NewSynchronizedTextOutput(parent context.Context, audio voice.AudioOutput, next voice.TextOutput) *SynchronizedTextOutput {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancelCause(parent)
	o := &SynchronizedTextOutput{
		next: next, audio: audio, ctx: ctx, cancel: cancel,
		gate: make(chan struct{}, 1), changed: make(chan struct{}),
	}
	o.gate <- struct{}{}
	if audio != nil {
		o.unsubscribe = append(o.unsubscribe,
			audio.OnPlaybackStarted(o.onStarted),
			audio.OnPlaybackFinished(o.onFinished),
		)
	}
	return o
}

func (o *SynchronizedTextOutput) onStarted(event voice.PlaybackStartedEvent) {
	o.mu.Lock()
	o.active = true
	o.epoch++
	o.startedAt = event.CreatedAt
	if o.startedAt.IsZero() {
		o.startedAt = time.Now()
	}
	o.signalLocked()
	o.mu.Unlock()
}

func (o *SynchronizedTextOutput) onFinished(voice.PlaybackFinishedEvent) {
	o.mu.Lock()
	o.active = false
	o.finished++
	o.signalLocked()
	o.mu.Unlock()
}

func (o *SynchronizedTextOutput) CaptureText(ctx context.Context, text agents.TimedString) error {
	if o.closed.Load() || !o.attached.Load() {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := o.acquire(ctx); err != nil {
		return err
	}
	defer o.release()
	offset := text.StartTime
	if text.StartTimeOffset != nil {
		offset = text.StartTimeOffset
	}
	if offset != nil {
		forward, err := o.waitUntil(ctx, max(*offset, 0))
		if err != nil || !forward {
			return err
		}
	}
	return o.next.CaptureText(ctx, text)
}

func (o *SynchronizedTextOutput) Flush(ctx context.Context) error {
	if o.closed.Load() {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := o.acquire(ctx); err != nil {
		return err
	}
	defer o.release()
	return o.next.Flush(ctx)
}

func (o *SynchronizedTextOutput) waitUntil(ctx context.Context, offset time.Duration) (bool, error) {
	o.mu.Lock()
	initialFinished := o.finished
	var observedEpoch uint64
	for {
		if o.closed.Load() || !o.attached.Load() {
			o.mu.Unlock()
			return false, nil
		}
		if o.active {
			if observedEpoch == 0 {
				observedEpoch = o.epoch
			} else if observedEpoch != o.epoch {
				o.mu.Unlock()
				return false, nil
			}
			remaining := offset - time.Since(o.startedAt)
			if remaining <= 0 {
				o.mu.Unlock()
				return true, nil
			}
			changed := o.changed
			o.mu.Unlock()
			timer := time.NewTimer(remaining)
			select {
			case <-timer.C:
			case <-changed:
				timer.Stop()
			case <-ctx.Done():
				timer.Stop()
				return false, context.Cause(ctx)
			case <-o.ctx.Done():
				timer.Stop()
				return false, nil
			}
			o.mu.Lock()
			continue
		}
		if observedEpoch != 0 || o.finished != initialFinished {
			o.mu.Unlock()
			return false, nil
		}
		changed := o.changed
		o.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return false, context.Cause(ctx)
		case <-o.ctx.Done():
			return false, nil
		}
		o.mu.Lock()
	}
}

func (o *SynchronizedTextOutput) acquire(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-o.ctx.Done():
		return context.Cause(o.ctx)
	case <-o.gate:
		return nil
	}
}
func (o *SynchronizedTextOutput) release() {
	select {
	case o.gate <- struct{}{}:
	default:
	}
}
func (o *SynchronizedTextOutput) signalLocked() {
	close(o.changed)
	o.changed = make(chan struct{})
}
func (o *SynchronizedTextOutput) SetAttached(attached bool) {
	o.attached.Store(attached)
	o.next.SetAttached(attached)
	o.mu.Lock()
	o.signalLocked()
	o.mu.Unlock()
}
func (o *SynchronizedTextOutput) OnAttached() {
	o.attached.Store(true)
	o.next.OnAttached()
}
func (o *SynchronizedTextOutput) OnDetached() {
	o.attached.Store(false)
	o.next.OnDetached()
	o.mu.Lock()
	o.signalLocked()
	o.mu.Unlock()
}
func (o *SynchronizedTextOutput) Close() {
	o.closeOnce.Do(func() {
		o.closed.Store(true)
		o.cancel(ErrRoomIOClosed)
		for _, unsubscribe := range o.unsubscribe {
			unsubscribe()
		}
		o.unsubscribe = nil
		o.mu.Lock()
		o.signalLocked()
		o.mu.Unlock()
	})
}

var _ voice.AudioOutput = (*SynchronizedAudioOutput)(nil)
var _ voice.TextOutput = (*SynchronizedTextOutput)(nil)
