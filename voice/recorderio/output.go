// SPDX-License-Identifier: Apache-2.0

package recorderio

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/voice"
)

type pauseInterval struct{ start, end time.Time }

type recorderOutputSegment struct {
	frames             []agents.AudioFrame
	bytes              int
	acceptedDownstream bool
	captureFailed      bool
	capturesInFlight   int
	finishRequested    bool
	flushed            bool
	playoutAwaited     bool
	settling           bool
	forceDrop          bool
	recorded           bool

	speechStart       time.Time
	currentPauseStart time.Time
	pauses            []pauseInterval

	done  chan struct{}
	event voice.PlaybackFinishedEvent
	err   error
}

type finishActionKind uint8

const (
	finishNone finishActionKind = iota
	finishSegmentAction
	requestDownstreamFinish
)

type finishAction struct {
	kind    finishActionKind
	segment *recorderOutputSegment
	event   voice.PlaybackFinishedEvent
	drop    bool
}

// PlaybackFinishedNotifier is an optional recovery contract for an output
// that retains a captured segment when CaptureFrame returns an error. Standard
// managed outputs roll that count back; ParticipantAudioOutput is recovered
// through ClearBuffer instead.
type PlaybackFinishedNotifier interface {
	NotifyPlaybackFinished(voice.PlaybackFinishedEvent) error
}

// OpenSegmentAbandoner is an optional recovery seam for an output that counts
// a frame and then rejects it. Managed SDK outputs roll their count back, while
// unusual sinks that retain an open-segment latch can expose this method so a
// retry starts a fresh downstream segment without an observable Flush.
type OpenSegmentAbandoner interface{ AbandonOpenSegment() }

// RecorderAudioOutput is an ownership-neutral AudioOutput decorator. It owns
// only its subscriptions to the wrapped output.
type RecorderAudioOutput struct {
	recorder  *RecorderIO
	next      voice.AudioOutput
	ctx       context.Context
	cancel    context.CancelCauseFunc
	attached  atomic.Bool
	closed    atomic.Bool
	accepting atomic.Bool

	captureGate chan struct{}
	mu          sync.Mutex
	segments    []*recorderOutputSegment
	current     *recorderOutputSegment
	deferred    []voice.PlaybackFinishedEvent

	started              time.Time
	lastSpeechEndTime    time.Time
	currentPauseStart    time.Time
	pauses               []pauseInterval
	bufferedBytes        int
	bufferedFrames       int
	captured, finished   uint64
	last                 voice.PlaybackFinishedEvent
	draining, drainAgain bool

	startedEvents  agents.EventEmitter[voice.PlaybackStartedEvent]
	finishedEvents agents.EventEmitter[voice.PlaybackFinishedEvent]
	unsubscribes   []func()
}

func newRecorderAudioOutput(recorder *RecorderIO, next voice.AudioOutput) *RecorderAudioOutput {
	ctx, cancel := context.WithCancelCause(context.Background())
	output := &RecorderAudioOutput{recorder: recorder, next: next, ctx: ctx, cancel: cancel, captureGate: make(chan struct{}, 1)}
	output.accepting.Store(true)
	output.captureGate <- struct{}{}
	output.unsubscribes = append(output.unsubscribes,
		next.OnPlaybackStarted(func(event voice.PlaybackStartedEvent) { output.startedEvents.Emit(event) }),
		next.OnPlaybackFinished(func(event voice.PlaybackFinishedEvent) { _ = output.NotifyPlaybackFinished(event) }),
	)
	return output
}

func (o *RecorderAudioOutput) CaptureFrame(ctx context.Context, frame agents.AudioFrame) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if o.closed.Load() {
		return ErrClosed
	}
	if !o.accepting.Load() {
		return ErrClosing
	}
	if err := validateFrame(frame); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-o.ctx.Done():
		return context.Cause(o.ctx)
	case <-o.captureGate:
	}
	defer func() { o.captureGate <- struct{}{} }()
	if !o.accepting.Load() {
		return ErrClosing
	}

	recordFrame := o.recorder.timingOpen()
	needed, err := audioFrameBytes(frame)
	if err != nil {
		return err
	}
	captureTime := o.recorder.options.clock()
	downstreamBefore := o.next.CapturedPlayoutSegments()

	o.mu.Lock()
	segment := o.current
	if segment == nil {
		if len(o.segments) >= o.recorder.options.maxBufferedFrames {
			o.mu.Unlock()
			return ErrBufferLimit
		}
		kept := o.pauses[:0]
		for _, pause := range o.pauses {
			if pause.end.After(captureTime) {
				if pause.start.Before(captureTime) {
					pause.start = captureTime
				}
				kept = append(kept, pause)
			}
		}
		o.pauses = kept
		if !o.currentPauseStart.IsZero() && o.currentPauseStart.Before(captureTime) {
			o.currentPauseStart = captureTime
		}
		segment = &recorderOutputSegment{
			speechStart: captureTime, currentPauseStart: o.currentPauseStart,
			pauses: append([]pauseInterval(nil), o.pauses...), done: make(chan struct{}),
		}
		o.segments = append(o.segments, segment)
		o.current = segment
		o.captured++
	}
	segment.capturesInFlight++
	if recordFrame {
		if needed > o.recorder.options.maxBufferedBytes || segment.bytes > o.recorder.options.maxBufferedBytes-needed || o.bufferedBytes > o.recorder.options.maxBufferedBytes-needed || o.bufferedFrames >= o.recorder.options.maxBufferedFrames {
			segment.captureFailed, segment.flushed = true, true
			segment.capturesInFlight--
			if o.current == segment {
				o.current = nil
			}
			o.mu.Unlock()
			o.requestDrain()
			return ErrBufferLimit
		}
		segment.bytes += needed // reserve while the downstream capture is in flight
		o.bufferedBytes += needed
		o.bufferedFrames++
	}
	o.mu.Unlock()

	captureCtx, captureCancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(o.ctx, func() { captureCancel(context.Cause(o.ctx)) })
	err = o.next.CaptureFrame(captureCtx, frame)
	captureCause := context.Cause(captureCtx)
	stop()
	captureCancel(context.Canceled)
	if err != nil && captureCause != nil {
		err = captureCause
	}
	downstreamAfter := o.next.CapturedPlayoutSegments()

	o.mu.Lock()
	if downstreamAfter > downstreamBefore {
		segment.acceptedDownstream = true
	}
	if err == nil {
		if recordFrame {
			segment.frames = append(segment.frames, cloneFrame(frame))
			segment.recorded = true
		}
		if o.started.IsZero() {
			o.started = o.recorder.options.clock()
		}
	} else {
		if recordFrame {
			segment.bytes -= needed
			o.bufferedBytes -= needed
			o.bufferedFrames--
		}
		segment.captureFailed, segment.flushed = true, true
		if o.current == segment {
			o.current = nil
		}
	}
	segment.capturesInFlight--
	o.mu.Unlock()
	o.requestDrain()
	return err
}

func (o *RecorderAudioOutput) Flush(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	o.mu.Lock()
	if o.current != nil {
		o.current.flushed = true
		o.current = nil
	}
	o.mu.Unlock()
	err := o.next.Flush(ctx)
	o.requestDrain()
	return err
}

func (o *RecorderAudioOutput) ClearBuffer(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	err := o.next.ClearBuffer(ctx)
	o.requestDrain()
	return err
}

func (o *RecorderAudioOutput) Pause(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	now := o.recorder.options.clock()
	recording := o.recorder.timingOpen()
	o.mu.Lock()
	if recording && o.currentPauseStart.IsZero() {
		o.currentPauseStart = now
	}
	segment := o.current
	if segment == nil && len(o.segments) != 0 {
		segment = o.segments[len(o.segments)-1]
	}
	if recording && segment != nil && segment.currentPauseStart.IsZero() {
		segment.currentPauseStart = o.currentPauseStart
	}
	o.mu.Unlock()
	return o.next.Pause(ctx)
}

func (o *RecorderAudioOutput) Resume(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	now := o.recorder.options.clock()
	recording := o.recorder.timingOpen()
	overflow := false
	o.mu.Lock()
	if recording && !o.currentPauseStart.IsZero() {
		if len(o.pauses) < o.recorder.options.maxBufferedFrames {
			o.pauses = append(o.pauses, pauseInterval{o.currentPauseStart, now})
		} else {
			overflow = true
		}
		o.currentPauseStart = time.Time{}
	}
	segment := o.current
	if segment == nil && len(o.segments) != 0 {
		segment = o.segments[len(o.segments)-1]
	}
	if recording && segment != nil && !segment.currentPauseStart.IsZero() {
		if len(segment.pauses) < o.recorder.options.maxBufferedFrames {
			segment.pauses = append(segment.pauses, pauseInterval{segment.currentPauseStart, now})
		} else {
			overflow = true
		}
		segment.currentPauseStart = time.Time{}
	}
	o.mu.Unlock()
	if overflow {
		o.recorder.reportError(ErrBufferLimit)
	}
	return o.next.Resume(ctx)
}

func (o *RecorderAudioOutput) CanPause() bool  { return o.next.CanPause() }
func (o *RecorderAudioOutput) SampleRate() int { return o.next.SampleRate() }

func (o *RecorderAudioOutput) WaitForPlayout(ctx context.Context) (voice.PlaybackFinishedEvent, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	o.mu.Lock()
	var target *recorderOutputSegment
	if len(o.segments) != 0 {
		target = o.segments[len(o.segments)-1]
	}
	last := o.last
	o.mu.Unlock()
	downstream, err := o.next.WaitForPlayout(ctx)
	if err != nil {
		return voice.PlaybackFinishedEvent{}, err
	}
	if target == nil {
		if last != (voice.PlaybackFinishedEvent{}) {
			return last, nil
		}
		return downstream, nil
	}
	o.mu.Lock()
	target.playoutAwaited = true
	o.mu.Unlock()
	o.requestDrain()
	select {
	case <-target.done:
		return target.event, target.err
	case <-ctx.Done():
		return voice.PlaybackFinishedEvent{}, context.Cause(ctx)
	}
}

func (o *RecorderAudioOutput) NotifyPlaybackFinished(event voice.PlaybackFinishedEvent) error {
	o.mu.Lock()
	if len(o.segments) == 0 {
		o.mu.Unlock()
		return voice.ErrUnexpectedPlaybackFinished
	}
	if len(o.deferred) >= o.recorder.options.maxBufferedFrames {
		o.mu.Unlock()
		return ErrBufferLimit
	}
	o.deferred = append(o.deferred, event)
	o.mu.Unlock()
	o.requestDrain()
	return nil
}

func (o *RecorderAudioOutput) requestDrain() {
	o.mu.Lock()
	if o.draining {
		o.drainAgain = true
		o.mu.Unlock()
		return
	}
	o.draining = true
	o.mu.Unlock()
	for {
		action := o.nextFinishAction()
		switch action.kind {
		case finishSegmentAction:
			o.executeFinish(action)
			continue
		case requestDownstreamFinish:
			notifier, ok := o.next.(PlaybackFinishedNotifier)
			if ok {
				if err := notifier.NotifyPlaybackFinished(action.event); err != nil {
					o.forceSegment(action.segment, errors.Join(ErrPlaybackNotificationUnavailable, err))
				}
				if abandoner, ok := o.next.(OpenSegmentAbandoner); ok {
					abandoner.AbandonOpenSegment()
				}
				continue
			}
			// ParticipantAudioOutput and transparent output proxies do not expose
			// an explicit finish notifier. ClearBuffer is their public,
			// context-aware interruption path and emits the authoritative finish
			// needed to release both accounting layers.
			clearCtx, clearCancel := withOptionalTimeout(o.ctx, o.recorder.options.closePlayoutFlushTimeout)
			clearErr := o.next.ClearBuffer(clearCtx)
			clearCancel()
			if clearErr != nil {
				o.forceSegment(action.segment, errors.Join(ErrPlaybackNotificationUnavailable, clearErr))
			}
			continue
		}

		o.mu.Lock()
		if o.drainAgain {
			o.drainAgain = false
			o.mu.Unlock()
			continue
		}
		o.draining = false
		o.mu.Unlock()
		return
	}
}

func (o *RecorderAudioOutput) nextFinishAction() finishAction {
	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.segments) == 0 {
		o.deferred = nil
		return finishAction{}
	}
	segment := o.segments[0]
	if segment.capturesInFlight > 0 || segment.settling {
		return finishAction{}
	}
	var action finishAction
	switch {
	case segment.forceDrop:
		if segment.acceptedDownstream && len(o.deferred) != 0 {
			o.deferred = o.deferred[1:]
		}
		action = finishAction{kind: finishSegmentAction, segment: segment, event: voice.PlaybackFinishedEvent{Interrupted: true}, drop: true}
	case !segment.acceptedDownstream:
		if !segment.flushed && !segment.playoutAwaited {
			return finishAction{}
		}
		action = finishAction{kind: finishSegmentAction, segment: segment, event: voice.PlaybackFinishedEvent{Interrupted: true}}
	case len(o.deferred) != 0:
		event := o.deferred[0]
		o.deferred = o.deferred[1:]
		action = finishAction{kind: finishSegmentAction, segment: segment, event: event}
	case segment.flushed && segment.captureFailed && !segment.finishRequested:
		segment.finishRequested = true
		return finishAction{kind: requestDownstreamFinish, segment: segment, event: voice.PlaybackFinishedEvent{Interrupted: true}}
	default:
		return finishAction{}
	}
	segment.settling = true
	if o.current == segment {
		o.current = nil
	}
	return action
}

func (o *RecorderAudioOutput) forceSegment(segment *recorderOutputSegment, err error) {
	o.mu.Lock()
	segment.forceDrop = true
	segment.err = errors.Join(segment.err, err)
	o.mu.Unlock()
	o.recorder.reportError(err)
	// The outer drainer is still active and will observe forceDrop.
}

func (o *RecorderAudioOutput) executeFinish(action finishAction) {
	segment := action.segment
	now := o.recorder.options.clock()
	finishTime := now
	if !segment.currentPauseStart.IsZero() {
		finishTime = segment.currentPauseStart
	}
	if finishTime.Before(segment.speechStart) {
		finishTime = segment.speechStart
	}
	maxPosition := finishTime.Sub(segment.speechStart)
	position := action.event.PlaybackPosition
	if position < 0 {
		position = 0
	}
	if position > maxPosition {
		position = maxPosition
	}
	action.event.PlaybackPosition = position

	var output []agents.AudioFrame
	var finishErr error
	var padSince time.Time
	if segment.recorded && !action.drop {
		o.mu.Lock()
		padSince = o.lastSpeechEndTime
		o.lastSpeechEndTime = now
		o.mu.Unlock()
		output, finishErr = buildPlaybackFrames(segment, action.event.PlaybackPosition, now, o.recorder.options.maxBufferedBytes, o.recorder.options.maxBufferedFrames)
		if finishErr == nil && len(output) != 0 {
			ctx := o.recorder.context()
			if err := o.recorder.commit(ctx, output, padSince); err != nil && !errors.Is(err, ErrClosed) {
				finishErr = err
			}
		}
	}

	o.mu.Lock()
	if len(o.segments) != 0 && o.segments[0] == segment {
		o.segments = o.segments[1:]
	} else {
		for index, candidate := range o.segments {
			if candidate == segment {
				copy(o.segments[index:], o.segments[index+1:])
				o.segments = o.segments[:len(o.segments)-1]
				break
			}
		}
	}
	o.finished++
	o.last = action.event
	segment.event = action.event
	segment.err = errors.Join(segment.err, finishErr)
	o.bufferedBytes -= segment.bytes
	o.bufferedFrames -= len(segment.frames)
	segment.frames = nil
	segment.bytes = 0
	close(segment.done)
	o.mu.Unlock()
	if finishErr != nil {
		o.recorder.reportError(finishErr)
	}
	o.finishedEvents.Emit(action.event)
}

func (o *RecorderAudioOutput) sealOpenSegment() {
	o.mu.Lock()
	if o.current != nil {
		o.current.flushed = true
		o.current = nil
	}
	o.mu.Unlock()
	o.requestDrain()
}

func (o *RecorderAudioOutput) beginClose() {
	o.accepting.Store(false)
	o.cancel(ErrClosing)
}

func (o *RecorderAudioOutput) dropPending() {
	o.mu.Lock()
	for _, segment := range o.segments {
		segment.forceDrop = true
	}
	o.current = nil
	o.mu.Unlock()
	o.requestDrain()
}

// HasPendingData reports whether agent audio is awaiting an authoritative
// playback result and is therefore not yet safe to encode.
func (o *RecorderAudioOutput) HasPendingData() bool {
	if o == nil {
		return false
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, segment := range o.segments {
		if segment.bytes != 0 || len(segment.frames) != 0 {
			return true
		}
	}
	return false
}

func (o *RecorderAudioOutput) commitInputIfIdle(ctx context.Context) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, segment := range o.segments {
		if segment.bytes != 0 || len(segment.frames) != 0 {
			return nil
		}
	}
	return o.recorder.commit(ctx, nil, o.lastSpeechEndTime)
}

func (o *RecorderAudioOutput) lastSpeechEnd() time.Time {
	if o == nil {
		return time.Time{}
	}
	o.mu.Lock()
	value := o.lastSpeechEndTime
	o.mu.Unlock()
	return value
}

func (o *RecorderAudioOutput) startedAt() (time.Time, bool) {
	if o == nil {
		return time.Time{}, false
	}
	o.mu.Lock()
	value := o.started
	o.mu.Unlock()
	return value, !value.IsZero()
}

func (o *RecorderAudioOutput) PendingPlayoutSegments() uint64 {
	o.mu.Lock()
	value := o.captured - o.finished
	o.mu.Unlock()
	return value
}
func (o *RecorderAudioOutput) CapturedPlayoutSegments() uint64 {
	o.mu.Lock()
	value := o.captured
	o.mu.Unlock()
	return value
}
func (o *RecorderAudioOutput) OnPlaybackStarted(fn func(voice.PlaybackStartedEvent)) func() {
	return o.startedEvents.Subscribe(fn)
}
func (o *RecorderAudioOutput) OnPlaybackFinished(fn func(voice.PlaybackFinishedEvent)) func() {
	return o.finishedEvents.Subscribe(fn)
}
func (o *RecorderAudioOutput) SetAttached(attached bool) {
	o.attached.Store(attached)
	o.next.SetAttached(attached)
}
func (o *RecorderAudioOutput) OnAttached() {
	o.attached.Store(true)
	o.next.SetAttached(true)
	o.next.OnAttached()
}
func (o *RecorderAudioOutput) OnDetached() {
	o.attached.Store(false)
	o.next.SetAttached(false)
	o.next.OnDetached()
}

func (o *RecorderAudioOutput) close() {
	if !o.closed.CompareAndSwap(false, true) {
		return
	}
	for _, unsubscribe := range o.unsubscribes {
		unsubscribe()
	}
	o.unsubscribes = nil
}

func (r *RecorderIO) context() context.Context {
	r.mu.Lock()
	ctx := r.ctx
	r.mu.Unlock()
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func buildPlaybackFrames(segment *recorderOutputSegment, playbackPosition time.Duration, now time.Time, maxBytes, maxFrames int) ([]agents.AudioFrame, error) {
	if len(segment.frames) == 0 {
		return nil, nil
	}
	if maxBytes < 1 || maxFrames < 1 {
		return nil, ErrBufferLimit
	}
	finishTime := now
	pauses := append([]pauseInterval(nil), segment.pauses...)
	if !segment.currentPauseStart.IsZero() {
		finishTime = segment.currentPauseStart
		pauses = append(pauses, pauseInterval{segment.currentPauseStart, finishTime})
	}
	trailingSilence := now.Sub(finishTime)
	if trailingSilence < 0 {
		trailingSilence = 0
	}

	playbackStart := finishTime.Add(-playbackPosition)
	var totalPause time.Duration
	for _, pause := range pauses {
		if pause.end.After(pause.start) {
			totalPause += pause.end.Sub(pause.start)
		}
	}
	playbackStart = playbackStart.Add(-totalPause)
	type positionedPause struct{ position, duration time.Duration }
	positioned := make([]positionedPause, 0, len(pauses))
	var accumulated time.Duration
	for _, pause := range pauses {
		duration := pause.end.Sub(pause.start)
		if duration < 0 {
			duration = 0
		}
		position := pause.start.Sub(playbackStart) - accumulated
		if position < 0 {
			position = 0
		}
		if position > playbackPosition {
			position = playbackPosition
		}
		positioned = append(positioned, positionedPause{position, duration})
		accumulated += duration
	}

	first := segment.frames[0]
	if err := validateFrame(first); err != nil {
		return nil, err
	}
	result := make([]agents.AudioFrame, 0, min(len(segment.frames), maxFrames))
	bytesUsed := 0
	appendFrame := func(frame agents.AudioFrame) error {
		if frame.SamplesPerChannel == 0 {
			return nil
		}
		needed, err := audioFrameBytes(frame)
		if err != nil {
			return err
		}
		if needed > maxBytes || bytesUsed > maxBytes-needed || len(result) >= maxFrames {
			return ErrBufferLimit
		}
		result = append(result, frame)
		bytesUsed += needed
		return nil
	}
	appendSilence := func(duration time.Duration) error {
		samples, elements, err := silenceDimensions(duration, first.SampleRate, first.Channels)
		if err != nil {
			return err
		}
		if samples == 0 {
			return nil
		}
		if elements > maxBytes/2 || bytesUsed > maxBytes-elements*2 || len(result) >= maxFrames {
			return ErrBufferLimit
		}
		frame, err := silenceFrame(duration, first.SampleRate, first.Channels)
		if err != nil {
			return err
		}
		return appendFrame(frame)
	}

	var elapsed time.Duration
	pauseIndex := 0
	stop := false
	for _, original := range segment.frames {
		if err := validateFrame(original); err != nil {
			return nil, err
		}
		current := original
		frameDuration := current.Duration()
		if elapsed+frameDuration > playbackPosition {
			left, _, err := splitFrame(current, playbackPosition-elapsed)
			if err != nil {
				return nil, err
			}
			current = left
			stop = true
		}
		for pauseIndex < len(positioned) && positioned[pauseIndex].position <= elapsed {
			if err := appendSilence(positioned[pauseIndex].duration); err != nil {
				return nil, err
			}
			pauseIndex++
		}
		currentDuration := current.Duration()
		for pauseIndex < len(positioned) && positioned[pauseIndex].position < elapsed+currentDuration {
			pause := positioned[pauseIndex]
			left, right, err := splitFrame(current, pause.position-elapsed)
			if err != nil {
				return nil, err
			}
			if err := appendFrame(left); err != nil {
				return nil, err
			}
			elapsed += left.Duration()
			if err := appendSilence(pause.duration); err != nil {
				return nil, err
			}
			current = right
			currentDuration = current.Duration()
			pauseIndex++
		}
		if err := appendFrame(current); err != nil {
			return nil, err
		}
		elapsed += current.Duration()
		if stop {
			break
		}
	}
	for pauseIndex < len(positioned) {
		pause := positioned[pauseIndex]
		if pause.position <= playbackPosition {
			if err := appendSilence(pause.duration); err != nil {
				return nil, err
			}
		}
		pauseIndex++
	}
	if len(result) != 0 && trailingSilence > 0 {
		if err := appendSilence(trailingSilence); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func splitFrame(frame agents.AudioFrame, position time.Duration) (agents.AudioFrame, agents.AudioFrame, error) {
	if err := validateFrame(frame); err != nil {
		return agents.AudioFrame{}, agents.AudioFrame{}, err
	}
	if position <= 0 {
		empty, _ := agents.NewAudioFrame(nil, frame.SampleRate, frame.Channels)
		return empty, frame, nil
	}
	if position >= frame.Duration() {
		empty, _ := agents.NewAudioFrame(nil, frame.SampleRate, frame.Channels)
		return frame, empty, nil
	}
	samples := int(int64(position) * int64(frame.SampleRate) / int64(time.Second))
	if samples < 0 {
		samples = 0
	}
	if samples > frame.SamplesPerChannel {
		samples = frame.SamplesPerChannel
	}
	index := samples * frame.Channels
	left, err := agents.NewAudioFrame(append([]int16(nil), frame.Data[:index]...), frame.SampleRate, frame.Channels)
	if err != nil {
		return agents.AudioFrame{}, agents.AudioFrame{}, err
	}
	right, err := agents.NewAudioFrame(append([]int16(nil), frame.Data[index:]...), frame.SampleRate, frame.Channels)
	return left, right, err
}

var _ voice.AudioOutput = (*RecorderAudioOutput)(nil)
