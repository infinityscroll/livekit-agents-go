// SPDX-License-Identifier: Apache-2.0

package recorderio

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/voice"
)

// RecorderAudioInput is an ownership-neutral AudioInput decorator. Close only
// detaches the recorder wrapper; the caller remains responsible for the source.
type RecorderAudioInput struct {
	recorder *RecorderIO
	source   voice.AudioInput
	ctx      context.Context
	cancel   context.CancelCauseFunc
	attached atomic.Bool
	closed   atomic.Bool

	mu      sync.Mutex
	frames  []agents.AudioFrame
	bytes   int
	started time.Time
	padded  bool
	space   chan struct{}
}

func newRecorderAudioInput(recorder *RecorderIO, source voice.AudioInput) *RecorderAudioInput {
	ctx, cancel := context.WithCancelCause(context.Background())
	return &RecorderAudioInput{recorder: recorder, source: source, ctx: ctx, cancel: cancel, space: make(chan struct{})}
}

func (i *RecorderAudioInput) Recv(ctx context.Context) (agents.AudioFrame, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if i.closed.Load() {
		return agents.AudioFrame{}, io.EOF
	}
	recvCtx, recvCancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(i.ctx, func() { recvCancel(context.Cause(i.ctx)) })
	frame, err := i.source.Recv(recvCtx)
	cause := context.Cause(recvCtx)
	stop()
	recvCancel(context.Canceled)
	if err != nil {
		if cause != nil {
			err = cause
		}
		return agents.AudioFrame{}, err
	}
	if !i.recorder.timingOpen() {
		return frame, nil
	}
	if err := validateFrame(frame); err != nil {
		return agents.AudioFrame{}, err
	}
	needed, err := audioFrameBytes(frame)
	if err != nil {
		return agents.AudioFrame{}, err
	}
	if needed > i.recorder.options.maxBufferedBytes {
		return agents.AudioFrame{}, ErrBufferLimit
	}
	copyFrame := cloneFrame(frame)
	for {
		i.mu.Lock()
		if i.closed.Load() || !i.recorder.timingOpen() {
			i.mu.Unlock()
			return frame, nil
		}
		if i.bytes <= i.recorder.options.maxBufferedBytes-needed && len(i.frames) < i.recorder.options.maxBufferedFrames {
			if i.started.IsZero() {
				i.started = i.recorder.options.clock()
			}
			i.frames = append(i.frames, copyFrame)
			i.bytes += needed
			i.mu.Unlock()
			return frame, nil
		}
		space := i.space
		i.mu.Unlock()
		select {
		case <-ctx.Done():
			return agents.AudioFrame{}, context.Cause(ctx)
		case <-space:
		}
	}
}

func (i *RecorderAudioInput) takeBuffer(padSince time.Time) ([]agents.AudioFrame, error) {
	i.mu.Lock()
	frames := i.frames
	i.frames = nil
	i.bytes = 0
	i.signalSpaceLocked()
	started, padded := i.started, i.padded
	if !padSince.IsZero() && !started.IsZero() && started.After(padSince) && !padded && len(frames) != 0 {
		i.padded = true
		padding := started.Sub(padSince)
		first := frames[0]
		_, elements, err := silenceDimensions(padding, first.SampleRate, first.Channels)
		frameBytes := framesBytes(frames)
		if err != nil || elements > (i.recorder.options.maxBufferedBytes-frameBytes)/2 || len(frames) >= i.recorder.options.maxBufferedFrames {
			i.mu.Unlock()
			return frames, errors.Join(ErrInputPaddingSkipped, err)
		}
		silence, err := silenceFrame(padding, first.SampleRate, first.Channels)
		if err != nil {
			i.mu.Unlock()
			return frames, errors.Join(ErrInputPaddingSkipped, err)
		}
		frames = append([]agents.AudioFrame{silence}, frames...)
	}
	i.mu.Unlock()
	return frames, nil
}

func (i *RecorderAudioInput) wake() {
	i.mu.Lock()
	i.signalSpaceLocked()
	i.mu.Unlock()
}

func (i *RecorderAudioInput) signalSpaceLocked() {
	close(i.space)
	i.space = make(chan struct{})
}

func (i *RecorderAudioInput) startedAt() (time.Time, bool) {
	if i == nil {
		return time.Time{}, false
	}
	i.mu.Lock()
	started := i.started
	i.mu.Unlock()
	return started, !started.IsZero()
}

func (i *RecorderAudioInput) SetAttached(attached bool) {
	i.attached.Store(attached)
	i.source.SetAttached(attached)
}
func (i *RecorderAudioInput) OnAttached() {
	i.attached.Store(true)
	i.source.SetAttached(true)
	i.source.OnAttached()
}
func (i *RecorderAudioInput) OnDetached() {
	i.attached.Store(false)
	i.source.SetAttached(false)
	i.source.OnDetached()
}

func (i *RecorderAudioInput) Close() error { return i.close() }
func (i *RecorderAudioInput) close() error {
	if !i.closed.CompareAndSwap(false, true) {
		return nil
	}
	i.cancel(ErrClosed)
	i.wake()
	return nil
}

func validateFrame(frame agents.AudioFrame) error {
	maxInt := int(^uint(0) >> 1)
	if frame.SampleRate <= 0 || frame.Channels <= 0 || frame.SamplesPerChannel < 0 || frame.SamplesPerChannel > maxInt/frame.Channels || len(frame.Data) != frame.SamplesPerChannel*frame.Channels {
		return errors.Join(agents.ErrInvalidAudioFormat, errors.New("recorderio: malformed audio frame"))
	}
	return nil
}

func cloneFrame(frame agents.AudioFrame) agents.AudioFrame {
	frame.Data = append([]int16(nil), frame.Data...)
	return frame
}

func audioFrameBytes(frame agents.AudioFrame) (int, error) {
	if len(frame.Data) > int(^uint(0)>>1)/2 {
		return 0, ErrBufferLimit
	}
	return len(frame.Data) * 2, nil
}

func silenceFrame(duration time.Duration, sampleRate, channels int) (agents.AudioFrame, error) {
	_, elements, err := silenceDimensions(duration, sampleRate, channels)
	if err != nil {
		return agents.AudioFrame{}, err
	}
	return agents.NewAudioFrame(make([]int16, elements), sampleRate, channels)
}

func silenceDimensions(duration time.Duration, sampleRate, channels int) (samplesPerChannel, elements int, err error) {
	if duration < 0 || sampleRate <= 0 || channels <= 0 {
		return 0, 0, agents.ErrInvalidAudioFormat
	}
	seconds := duration / time.Second
	remainder := duration % time.Second
	maxInt64 := int64(^uint64(0) >> 1)
	if int64(seconds) > maxInt64/int64(sampleRate) || remainder != 0 && int64(sampleRate) > maxInt64/int64(remainder) {
		return 0, 0, ErrBufferLimit
	}
	samples := int64(seconds)*int64(sampleRate) + int64(remainder)*int64(sampleRate)/int64(time.Second)
	if samples < 0 || samples > int64(^uint(0)>>1)/int64(channels) {
		return 0, 0, ErrBufferLimit
	}
	return int(samples), int(samples) * channels, nil
}

var _ voice.AudioInput = (*RecorderAudioInput)(nil)
