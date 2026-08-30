// SPDX-License-Identifier: Apache-2.0

package roomio

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/stream"
	"github.com/infinityscroll/livekit-agents-go/voice"
	mediabase "github.com/livekit/media-sdk"
	protolg "github.com/livekit/protocol/logger"
	lksdk "github.com/livekit/server-sdk-go/v2"
	lkmedia "github.com/livekit/server-sdk-go/v2/pkg/media"
)

var (
	ErrParticipantAudioOutputClosed = errors.New("roomio: participant audio output is closed")
	ErrAudioOutputNotStarted        = errors.New("roomio: participant audio output is not started")
)

type audioPlayoutSink interface {
	WriteSample(mediabase.PCM16Sample) error
	WaitForPlayout()
	ClearQueue()
	Close() error
}

// audioPlayoutQueueReporter is implemented by sinks that can expose exact
// queued source duration. server-sdk-go's PCMLocalTrack does not currently
// expose it, so the production adapter falls back to its paced deadline model.
type audioPlayoutQueueReporter interface {
	QueuedDuration() time.Duration
}

type outputCommandKind uint8

const (
	outputCapture outputCommandKind = iota
	outputFlush
	outputClear
)

type outputCommand struct {
	kind       outputCommandKind
	frame      agents.AudioFrame
	generation uint64
	done       chan error
}

// ParticipantAudioOutput publishes a bounded PCM source and provides exact
// segment accounting. A single forwarding actor serializes all media and paces
// it in real time, preventing PCMLocalTrack's internal deque from growing
// without bound even when synthesis produces audio faster than real time.
type ParticipantAudioOutput struct {
	ctx    context.Context
	cancel context.CancelCauseFunc
	room   *lksdk.Room
	bridge *RTCBridge
	opts   RoomOutputOptions
	onErr  func(error)

	commands *stream.Channel[outputCommand]
	attached atomic.Bool
	closed   atomic.Bool

	mu              sync.Mutex
	sink            audioPlayoutSink
	publication     *lksdk.LocalTrackPublication
	started         bool
	startErr        error
	startedChanged  chan struct{}
	connected       bool
	connectedChange chan struct{}
	subscribed      map[string]bool
	subChange       chan struct{}

	generation  uint64
	genChange   chan struct{}
	paused      bool
	pauseClear  bool
	pauseChange chan struct{}

	segmentOpen       bool
	segmentGeneration uint64
	firstFrame        bool
	pushed            time.Duration
	sourcePushed      time.Duration
	sourceDiscarded   time.Duration
	sourceQueued      time.Duration
	playoutDeadline   time.Time
	captured          uint64
	finished          uint64
	lastFinished      voice.PlaybackFinishedEvent
	playoutChange     chan struct{}

	startedEvents  agents.EventEmitter[voice.PlaybackStartedEvent]
	finishedEvents agents.EventEmitter[voice.PlaybackFinishedEvent]

	rtcSub    *RTCSubscription
	done      chan struct{}
	rtcDone   chan struct{}
	startDone chan struct{}
	closeDone chan struct{}
	startOnce sync.Once
	closeOnce sync.Once
}

func NewParticipantAudioOutput(parent context.Context, room *lksdk.Room, bridge *RTCBridge, options RoomOutputOptions, onError func(error)) (*ParticipantAudioOutput, error) {
	if room == nil || room.LocalParticipant == nil {
		return nil, errors.New("roomio: connected room with local participant is required for audio output")
	}
	if bridge == nil {
		return nil, errors.New("roomio: RTC bridge is required for audio output")
	}
	resolved, err := resolveOutputOptions(&options)
	if err != nil {
		return nil, err
	}
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancelCause(parent)
	sub, err := bridge.SubscribeTypes(DefaultParticipantEventQueue,
		RTCEventReconnecting,
		RTCEventReconnected,
		RTCEventDisconnected,
		RTCEventLocalTrackSubscribed,
	)
	if err != nil {
		cancel(err)
		return nil, err
	}
	output := newParticipantAudioOutput(ctx, cancel, resolved, onError)
	output.room = room
	output.bridge = bridge
	output.rtcSub = sub
	output.connected = room.ConnectionState() == lksdk.ConnectionStateConnected
	go output.runRTC()
	return output, nil
}

func newParticipantAudioOutput(ctx context.Context, cancel context.CancelCauseFunc, options RoomOutputOptions, onError func(error)) *ParticipantAudioOutput {
	if ctx == nil {
		ctx = context.Background()
	}
	if cancel == nil {
		ctx, cancel = context.WithCancelCause(ctx)
	}
	o := &ParticipantAudioOutput{
		ctx: ctx, cancel: cancel, opts: options, onErr: onError,
		commands:       stream.NewChannel[outputCommand](options.AudioCapacity),
		startedChanged: make(chan struct{}), connectedChange: make(chan struct{}),
		subscribed: make(map[string]bool), subChange: make(chan struct{}),
		genChange: make(chan struct{}), pauseChange: make(chan struct{}),
		playoutChange: make(chan struct{}), done: make(chan struct{}), rtcDone: make(chan struct{}),
		startDone: make(chan struct{}), closeDone: make(chan struct{}),
	}
	go o.run()
	return o
}

// Start publishes the local track and, by default, waits for a remote
// subscription. The SDK's PublishTrack call has its own ten-second timeout but
// does not accept a context; no helper goroutine is used around it.
func (o *ParticipantAudioOutput) Start(ctx context.Context) error {
	o.startOnce.Do(func() {
		go func() {
			_ = o.start(ctx)
			close(o.startDone)
		}()
	})
	return o.waitStarted(ctx, 0, false)
}

func (o *ParticipantAudioOutput) start(ctx context.Context) error {
	if o.closed.Load() {
		o.completeStart(ErrParticipantAudioOutputClosed)
		return ErrParticipantAudioOutputClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := o.waitConnected(ctx, 0); err != nil {
		o.completeStart(err)
		return err
	}
	trackOptions := make([]lkmedia.PCMLocalTrackOption, 0, 1)
	if o.opts.Encryptor != nil {
		trackOptions = append(trackOptions, lkmedia.WithEncryptor(o.opts.Encryptor))
	}
	track, err := lkmedia.NewPCMLocalTrack(o.opts.AudioSampleRate, o.opts.AudioChannels, protolg.GetLogger(), trackOptions...)
	if err != nil {
		o.completeStart(err)
		return err
	}
	o.mu.Lock()
	if o.closed.Load() {
		o.mu.Unlock()
		_ = track.Close()
		o.completeStart(ErrParticipantAudioOutputClosed)
		return ErrParticipantAudioOutputClosed
	}
	o.sink = track
	o.mu.Unlock()
	publication, err := o.room.LocalParticipant.PublishTrack(track, &o.opts.AudioPublishOptions)
	if err != nil {
		o.rollbackStart(track, nil)
		o.completeStart(err)
		return err
	}
	o.mu.Lock()
	if o.closed.Load() {
		o.mu.Unlock()
		o.rollbackStart(track, publication)
		o.completeStart(ErrParticipantAudioOutputClosed)
		return ErrParticipantAudioOutputClosed
	}
	o.publication = publication
	o.mu.Unlock()
	if boolValue(o.opts.WaitForSubscription, true) {
		if err := o.waitSubscription(ctx, publication.SID()); err != nil {
			o.rollbackStart(track, publication)
			o.completeStart(err)
			return err
		}
	}
	o.completeStart(nil)
	return nil
}

func (o *ParticipantAudioOutput) rollbackStart(track audioPlayoutSink, publication *lksdk.LocalTrackPublication) {
	o.mu.Lock()
	if o.sink == track {
		o.sink = nil
	}
	if o.publication == publication {
		o.publication = nil
	}
	o.mu.Unlock()
	if publication != nil && o.room != nil && o.room.LocalParticipant != nil {
		if err := o.room.LocalParticipant.UnpublishTrack(publication.SID()); err != nil && !errors.Is(err, lksdk.ErrCannotFindTrack) {
			o.report(err)
		}
	}
	if track != nil {
		track.ClearQueue()
		if err := track.Close(); err != nil {
			o.report(err)
		}
	}
}

func (o *ParticipantAudioOutput) completeStart(err error) {
	o.mu.Lock()
	if !o.started {
		o.started = true
		o.startErr = err
		close(o.startedChanged)
	}
	o.mu.Unlock()
}

func (o *ParticipantAudioOutput) waitStarted(ctx context.Context, generation uint64, interruptible bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		o.mu.Lock()
		if interruptible && generation != o.generation {
			o.mu.Unlock()
			return nil
		}
		if o.started {
			err := o.startErr
			o.mu.Unlock()
			return err
		}
		changed, genChanged := o.startedChanged, o.genChange
		o.mu.Unlock()
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-o.ctx.Done():
			return ErrParticipantAudioOutputClosed
		case <-changed:
		case <-genChanged:
		}
	}
}

func (o *ParticipantAudioOutput) CaptureFrame(ctx context.Context, frame agents.AudioFrame) error {
	if o.closed.Load() {
		return ErrParticipantAudioOutputClosed
	}
	o.mu.Lock()
	generation := o.generation
	o.mu.Unlock()
	if err := o.waitStarted(ctx, generation, true); err != nil {
		return err
	}
	o.mu.Lock()
	if generation != o.generation {
		o.mu.Unlock()
		return nil
	}
	o.mu.Unlock()
	converted, err := convertAudioFrame(frame, o.opts.AudioSampleRate, o.opts.AudioChannels)
	if err != nil {
		return err
	}
	command := outputCommand{kind: outputCapture, frame: converted, generation: generation, done: make(chan error, 1)}
	if err := o.commands.Send(ctx, command); err != nil {
		return err
	}
	select {
	case err := <-command.done:
		return err
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-o.ctx.Done():
		return ErrParticipantAudioOutputClosed
	}
}

func (o *ParticipantAudioOutput) Flush(ctx context.Context) error {
	if o.closed.Load() {
		return ErrParticipantAudioOutputClosed
	}
	return o.commands.Send(ctx, outputCommand{kind: outputFlush})
}

func (o *ParticipantAudioOutput) ClearBuffer(ctx context.Context) error {
	if o.closed.Load() {
		return ErrParticipantAudioOutputClosed
	}
	o.mu.Lock()
	o.generation++
	close(o.genChange)
	o.genChange = make(chan struct{})
	if o.sink != nil {
		o.refreshQueuedLocked(time.Now())
		o.sourceDiscarded += o.sourceQueued
		o.sourceQueued = 0
		o.playoutDeadline = time.Time{}
		o.sink.ClearQueue()
	}
	o.mu.Unlock()
	return o.sendControl(ctx, outputClear)
}

func (o *ParticipantAudioOutput) sendControl(ctx context.Context, kind outputCommandKind) error {
	if o.closed.Load() {
		return ErrParticipantAudioOutputClosed
	}
	command := outputCommand{kind: kind, done: make(chan error, 1)}
	if err := o.commands.Send(ctx, command); err != nil {
		return err
	}
	select {
	case err := <-command.done:
		return err
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-o.ctx.Done():
		return ErrParticipantAudioOutputClosed
	}
}

func (o *ParticipantAudioOutput) Pause(ctx context.Context) error {
	if ctx != nil {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		default:
		}
	}
	o.mu.Lock()
	if !o.paused {
		o.paused = true
		o.pauseClear = false
		close(o.pauseChange)
		o.pauseChange = make(chan struct{})
	}
	o.mu.Unlock()
	return nil
}

func (o *ParticipantAudioOutput) Resume(ctx context.Context) error {
	if ctx != nil {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		default:
		}
	}
	o.mu.Lock()
	if o.paused {
		o.paused = false
		close(o.pauseChange)
		o.pauseChange = make(chan struct{})
	}
	o.mu.Unlock()
	return nil
}

func (*ParticipantAudioOutput) CanPause() bool              { return true }
func (o *ParticipantAudioOutput) SampleRate() int           { return o.opts.AudioSampleRate }
func (o *ParticipantAudioOutput) SetAttached(attached bool) { o.attached.Store(attached) }
func (o *ParticipantAudioOutput) OnAttached()               { o.attached.Store(true) }
func (o *ParticipantAudioOutput) OnDetached()               { o.attached.Store(false) }

func (o *ParticipantAudioOutput) OnPlaybackStarted(fn func(voice.PlaybackStartedEvent)) func() {
	return o.startedEvents.Subscribe(fn)
}

func (o *ParticipantAudioOutput) OnPlaybackFinished(fn func(voice.PlaybackFinishedEvent)) func() {
	return o.finishedEvents.Subscribe(fn)
}

func (o *ParticipantAudioOutput) PendingPlayoutSegments() uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.captured - o.finished
}

func (o *ParticipantAudioOutput) CapturedPlayoutSegments() uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.captured
}

func (o *ParticipantAudioOutput) WaitForPlayout(ctx context.Context) (voice.PlaybackFinishedEvent, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	o.mu.Lock()
	target := o.captured
	for o.finished < target {
		changed := o.playoutChange
		o.mu.Unlock()
		select {
		case <-ctx.Done():
			return voice.PlaybackFinishedEvent{}, context.Cause(ctx)
		case <-o.ctx.Done():
			return voice.PlaybackFinishedEvent{}, ErrParticipantAudioOutputClosed
		case <-changed:
		}
		o.mu.Lock()
	}
	result := o.lastFinished
	o.mu.Unlock()
	return result, nil
}

func (o *ParticipantAudioOutput) run() {
	defer close(o.done)
	for {
		command, err := o.commands.Recv(o.ctx)
		if err != nil {
			return
		}
		switch command.kind {
		case outputCapture:
			err = o.handleCapture(command)
		case outputFlush:
			err = o.finishSegment(false)
		case outputClear:
			err = o.finishSegment(true)
		}
		if command.done != nil {
			command.done <- err
		}
	}
}

func (o *ParticipantAudioOutput) handleCapture(command outputCommand) error {
	if !o.attached.Load() {
		return nil
	}
	if err := o.waitConnected(o.ctx, command.generation); err != nil {
		if errors.Is(err, ErrParticipantAudioOutputClosed) {
			return err
		}
		return nil
	}
	o.mu.Lock()
	if command.generation != o.generation {
		o.mu.Unlock()
		return nil
	}
	openedSegment := false
	if !o.segmentOpen {
		o.segmentOpen = true
		o.segmentGeneration = command.generation
		o.captured++
		openedSegment = true
		close(o.playoutChange)
		o.playoutChange = make(chan struct{})
	}
	o.pushed += command.frame.Duration()
	o.mu.Unlock()

	duration := command.frame.Duration()

readyLoop:
	for {
		for {
			o.mu.Lock()
			if command.generation != o.generation {
				o.mu.Unlock()
				return nil
			}
			paused := o.paused
			pauseChanged, genChanged := o.pauseChange, o.genChange
			if paused && !o.pauseClear {
				o.pauseClear = true
				o.refreshQueuedLocked(time.Now())
				o.sourceDiscarded += o.sourceQueued
				o.sourceQueued = 0
				o.playoutDeadline = time.Time{}
				if o.sink != nil {
					o.sink.ClearQueue()
				}
			}
			o.mu.Unlock()
			if !paused {
				break
			}
			select {
			case <-o.ctx.Done():
				return ErrParticipantAudioOutputClosed
			case <-pauseChanged:
			case <-genChanged:
			}
		}

		now := time.Now()
		o.mu.Lock()
		if command.generation != o.generation || o.sink == nil {
			o.mu.Unlock()
			return nil
		}
		o.refreshQueuedLocked(now)
		wait := time.Duration(0)
		if o.sourceQueued > 0 && o.opts.AudioQueue > 0 && o.sourceQueued+duration > o.opts.AudioQueue {
			wait = o.sourceQueued + duration - o.opts.AudioQueue
		}
		genChanged, pauseChanged := o.genChange, o.pauseChange
		o.mu.Unlock()
		if wait <= 0 {
			break readyLoop
		}
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-genChanged:
			timer.Stop()
			return nil
		case <-pauseChanged:
			timer.Stop()
			continue readyLoop
		case <-o.ctx.Done():
			timer.Stop()
			return ErrParticipantAudioOutputClosed
		}
	}

	o.mu.Lock()
	if command.generation != o.generation || o.sink == nil {
		o.mu.Unlock()
		return nil
	}
	sink := o.sink
	o.mu.Unlock()
	if err := sink.WriteSample(mediabase.PCM16Sample(command.frame.Data)); err != nil {
		o.failCapture(command, duration, openedSegment)
		return err
	}

	// Account media only after the SDK accepted the frame. ClearBuffer may race
	// with WriteSample (the SDK call has no context), so discard a stale write
	// from both the queue and public playout accounting.
	o.mu.Lock()
	if command.generation != o.generation || sink != o.sink {
		o.mu.Unlock()
		sink.ClearQueue()
		return nil
	}
	now := time.Now()
	started := !o.firstFrame
	if started {
		o.firstFrame = true
	}
	if o.playoutDeadline.Before(now) {
		o.playoutDeadline = now
	}
	o.playoutDeadline = o.playoutDeadline.Add(duration)
	o.sourcePushed += duration
	o.refreshQueuedLocked(now)
	o.mu.Unlock()
	if started {
		o.startedEvents.Emit(voice.PlaybackStartedEvent{CreatedAt: now})
	}
	return nil
}

func (o *ParticipantAudioOutput) failCapture(command outputCommand, duration time.Duration, openedSegment bool) {
	o.mu.Lock()
	if command.generation != o.generation || !o.segmentOpen || o.segmentGeneration != command.generation {
		o.mu.Unlock()
		return
	}
	o.pushed = max(o.pushed-duration, 0)
	if !openedSegment || o.sourcePushed != 0 {
		o.mu.Unlock()
		return
	}
	// No frame from this segment reached the source. Finish the registered
	// segment as interrupted so WaitForPlayout cannot remain stuck after a
	// failed first submission.
	o.segmentOpen = false
	event := voice.PlaybackFinishedEvent{Interrupted: true}
	o.lastFinished = event
	o.finished++
	o.firstFrame = false
	o.sourcePushed = 0
	o.sourceDiscarded = 0
	o.sourceQueued = 0
	o.playoutDeadline = time.Time{}
	close(o.playoutChange)
	o.playoutChange = make(chan struct{})
	o.mu.Unlock()
	o.finishedEvents.Emit(event)
}

func (o *ParticipantAudioOutput) finishSegment(forceInterrupted bool) error {
	o.mu.Lock()
	if !o.segmentOpen {
		o.mu.Unlock()
		return nil
	}
	o.segmentOpen = false
	generation := o.segmentGeneration
	sink := o.sink
	o.mu.Unlock()
	if sink != nil {
		sink.WaitForPlayout()
	}
	o.mu.Lock()
	o.refreshQueuedLocked(time.Now())
	interrupted := forceInterrupted || generation != o.generation
	played := o.sourcePushed - o.sourceDiscarded
	if interrupted {
		played -= o.sourceQueued
		if sink != nil {
			sink.ClearQueue()
		}
	}
	played = max(played, 0)
	event := voice.PlaybackFinishedEvent{PlaybackPosition: played, Interrupted: interrupted}
	o.lastFinished = event
	o.finished++
	o.firstFrame = false
	o.pushed = 0
	o.sourcePushed = 0
	o.sourceDiscarded = 0
	o.sourceQueued = 0
	o.playoutDeadline = time.Time{}
	close(o.playoutChange)
	o.playoutChange = make(chan struct{})
	o.mu.Unlock()
	o.finishedEvents.Emit(event)
	return nil
}

func (o *ParticipantAudioOutput) refreshQueuedLocked(now time.Time) {
	if reporter, ok := o.sink.(audioPlayoutQueueReporter); ok {
		o.sourceQueued = max(reporter.QueuedDuration(), 0)
		return
	}
	if o.playoutDeadline.IsZero() || !o.playoutDeadline.After(now) {
		o.sourceQueued = 0
		return
	}
	o.sourceQueued = o.playoutDeadline.Sub(now)
}

func (o *ParticipantAudioOutput) waitConnected(ctx context.Context, generation uint64) error {
	for {
		o.mu.Lock()
		if generation != 0 && generation != o.generation {
			o.mu.Unlock()
			return nil
		}
		if o.connected {
			o.mu.Unlock()
			return nil
		}
		changed, genChanged := o.connectedChange, o.genChange
		o.mu.Unlock()
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-o.ctx.Done():
			return ErrParticipantAudioOutputClosed
		case <-changed:
		case <-genChanged:
		}
	}
}

func (o *ParticipantAudioOutput) waitSubscription(ctx context.Context, sid string) error {
	for {
		o.mu.Lock()
		if o.subscribed[sid] {
			o.mu.Unlock()
			return nil
		}
		changed := o.subChange
		o.mu.Unlock()
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-o.ctx.Done():
			return ErrParticipantAudioOutputClosed
		case <-changed:
		}
	}
}

func (o *ParticipantAudioOutput) runRTC() {
	defer close(o.rtcDone)
	if o.rtcSub == nil {
		return
	}
	defer o.rtcSub.Close()
	for {
		event, err := o.rtcSub.Recv(o.ctx)
		if err != nil {
			if context.Cause(o.ctx) == nil && !errors.Is(err, io.EOF) {
				o.report(err)
				if errors.Is(err, ErrRTCEventOverflow) {
					o.beginClose()
				}
			}
			return
		}
		o.mu.Lock()
		switch event.Type {
		case RTCEventReconnecting, RTCEventDisconnected:
			o.connected = false
			close(o.connectedChange)
			o.connectedChange = make(chan struct{})
		case RTCEventReconnected:
			o.connected = true
			close(o.connectedChange)
			o.connectedChange = make(chan struct{})
		case RTCEventLocalTrackSubscribed:
			if event.LocalPublication != nil {
				o.subscribed[event.LocalPublication.SID()] = true
				close(o.subChange)
				o.subChange = make(chan struct{})
			}
		}
		o.mu.Unlock()
	}
}

func (o *ParticipantAudioOutput) Publication() *lksdk.LocalTrackPublication {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.publication
}

func (o *ParticipantAudioOutput) report(err error) {
	if err != nil && o.onErr != nil {
		_ = invokeCallback("OnError", func() { o.onErr(err) })
	}
}

func (o *ParticipantAudioOutput) Close(ctx context.Context) error {
	if o == nil {
		return nil
	}
	o.beginClose()
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-o.closeDone:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (o *ParticipantAudioOutput) beginClose() {
	o.closeOnce.Do(func() {
		o.closed.Store(true)
		o.cancel(ErrParticipantAudioOutputClosed)
		o.startOnce.Do(func() {
			o.completeStart(ErrParticipantAudioOutputClosed)
			close(o.startDone)
		})
		go o.finishClose()
	})
}

func (o *ParticipantAudioOutput) finishClose() {
	<-o.startDone
	o.mu.Lock()
	close(o.genChange)
	sink, publication := o.sink, o.publication
	o.sink = nil
	o.publication = nil
	o.mu.Unlock()
	if sink != nil {
		sink.ClearQueue()
		if err := sink.Close(); err != nil {
			o.report(err)
		}
	}
	if publication != nil && o.room != nil && o.room.LocalParticipant != nil {
		if err := o.room.LocalParticipant.UnpublishTrack(publication.SID()); err != nil && !errors.Is(err, lksdk.ErrCannotFindTrack) {
			o.report(err)
		}
	}
	_ = o.commands.Abort(ErrParticipantAudioOutputClosed)
	if o.rtcSub != nil {
		_ = o.rtcSub.Close()
	}
	<-o.done
	<-o.rtcDone
	close(o.closeDone)
}

var _ voice.AudioOutput = (*ParticipantAudioOutput)(nil)
