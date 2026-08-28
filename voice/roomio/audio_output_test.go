// SPDX-License-Identifier: Apache-2.0

package roomio

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/voice"
	mediabase "github.com/livekit/media-sdk"
)

type fakePlayoutSink struct {
	mu            sync.Mutex
	writes        []mediabase.PCM16Sample
	writeErrors   []error
	writeAttempts int
	queued        time.Duration
	clears        int
	waiting       chan struct{}
	gate          chan struct{}
	waitOnce      sync.Once
	gateOnce      sync.Once
}

func newFakePlayoutSink(block bool) *fakePlayoutSink {
	s := &fakePlayoutSink{waiting: make(chan struct{})}
	if block {
		s.gate = make(chan struct{})
	}
	return s
}

func (s *fakePlayoutSink) WriteSample(sample mediabase.PCM16Sample) error {
	s.mu.Lock()
	s.writeAttempts++
	if len(s.writeErrors) > 0 {
		err := s.writeErrors[0]
		s.writeErrors = s.writeErrors[1:]
		if err != nil {
			s.mu.Unlock()
			return err
		}
	}
	s.writes = append(s.writes, append(mediabase.PCM16Sample(nil), sample...))
	s.queued += time.Duration(len(sample)) * time.Second / 24_000
	s.mu.Unlock()
	return nil
}
func (s *fakePlayoutSink) WaitForPlayout() {
	s.waitOnce.Do(func() { close(s.waiting) })
	if s.gate != nil {
		<-s.gate
	}
	s.mu.Lock()
	s.queued = 0
	s.mu.Unlock()
}
func (s *fakePlayoutSink) ClearQueue() {
	s.mu.Lock()
	s.clears++
	s.queued = 0
	s.mu.Unlock()
	s.release()
}
func (s *fakePlayoutSink) Close() error { s.release(); return nil }
func (s *fakePlayoutSink) release() {
	s.mu.Lock()
	s.queued = 0
	s.mu.Unlock()
	if s.gate != nil {
		s.gateOnce.Do(func() { close(s.gate) })
	}
}
func (s *fakePlayoutSink) QueuedDuration() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.queued
}
func (s *fakePlayoutSink) writeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.writes)
}
func (s *fakePlayoutSink) writeAttemptCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writeAttempts
}
func (s *fakePlayoutSink) clearCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.clears
}
func (s *fakePlayoutSink) failWrites(errs ...error) {
	s.mu.Lock()
	s.writeErrors = append(s.writeErrors, errs...)
	s.mu.Unlock()
}

func testParticipantAudioOutput(t *testing.T, sink audioPlayoutSink, started bool) *ParticipantAudioOutput {
	t.Helper()
	options := DefaultRoomOutputOptions()
	ctx, cancel := context.WithCancelCause(context.Background())
	output := newParticipantAudioOutput(ctx, cancel, options, func(err error) { t.Errorf("output error: %v", err) })
	output.mu.Lock()
	output.sink = sink
	output.connected = true
	output.mu.Unlock()
	output.OnAttached()
	if started {
		output.completeStart(nil)
	}
	// This focused constructor has no RTC actor.
	close(output.rtcDone)
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), time.Second)
		defer closeCancel()
		if err := output.Close(closeCtx); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return output
}

func frameFor(duration time.Duration, value int16) agents.AudioFrame {
	samples := int(duration * 24_000 / time.Second)
	frame, _ := agents.NewAudioFrame(make([]int16, samples), 24_000, 1)
	for index := range frame.Data {
		frame.Data[index] = value
	}
	return frame
}

func waitForAudioOutputState(t *testing.T, predicate func() bool, message string) {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(100 * time.Microsecond)
	defer ticker.Stop()
	for !predicate() {
		select {
		case <-deadline.C:
			t.Fatal(message)
		case <-ticker.C:
		}
	}
}

func TestParticipantAudioOutputFlushWaitsForSourceAndFinishesOnce(t *testing.T) {
	t.Parallel()
	sink := newFakePlayoutSink(true)
	output := testParticipantAudioOutput(t, sink, true)
	started := 0
	finished := make(chan voice.PlaybackFinishedEvent, 2)
	output.OnPlaybackStarted(func(voice.PlaybackStartedEvent) { started++ })
	output.OnPlaybackFinished(func(event voice.PlaybackFinishedEvent) { finished <- event })

	frame := frameFor(20*time.Millisecond, 1)
	if err := output.CaptureFrame(context.Background(), frame); err != nil {
		t.Fatal(err)
	}
	if err := output.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	// A repeated producer flush cannot finish the segment twice.
	if err := output.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-sink.waiting:
	case <-time.After(time.Second):
		t.Fatal("source playout was not awaited")
	}

	result := make(chan voice.PlaybackFinishedEvent, 1)
	go func() {
		event, _ := output.WaitForPlayout(context.Background())
		result <- event
	}()
	select {
	case <-result:
		t.Fatal("WaitForPlayout returned before the source drained")
	case <-time.After(10 * time.Millisecond):
	}
	sink.release()
	select {
	case event := <-result:
		if event.Interrupted || event.PlaybackPosition != frame.Duration() {
			t.Fatalf("playback = %#v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("playout did not finish")
	}
	if started != 1 {
		t.Fatalf("playback started %d times", started)
	}
	select {
	case event := <-finished:
		if event.Interrupted {
			t.Fatalf("finished event = %#v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("missing finished event")
	}
	select {
	case event := <-finished:
		t.Fatalf("duplicate finished event: %#v", event)
	case <-time.After(10 * time.Millisecond):
	}
}

func TestParticipantAudioOutputPausedCaptureCanBeInterrupted(t *testing.T) {
	t.Parallel()
	sink := newFakePlayoutSink(false)
	output := testParticipantAudioOutput(t, sink, true)
	if err := output.Pause(context.Background()); err != nil {
		t.Fatal(err)
	}
	captured := make(chan error, 1)
	go func() { captured <- output.CaptureFrame(context.Background(), frameFor(20*time.Millisecond, 1)) }()
	deadline := time.After(time.Second)
	for output.PendingPlayoutSegments() != 1 {
		select {
		case <-deadline:
			t.Fatal("paused capture did not open a segment")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if err := output.ClearBuffer(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := <-captured; err != nil {
		t.Fatal(err)
	}
	event, err := output.WaitForPlayout(context.Background())
	if err != nil || !event.Interrupted || event.PlaybackPosition != 0 {
		t.Fatalf("interrupted playback = %#v, %v", event, err)
	}
	if sink.writeCount() != 0 {
		t.Fatal("paused interrupted frame reached the source")
	}
}

func TestParticipantAudioOutputDropsCaptureInterruptedBeforeStart(t *testing.T) {
	t.Parallel()
	sink := newFakePlayoutSink(false)
	output := testParticipantAudioOutput(t, sink, false)
	result := make(chan error, 1)
	go func() { result <- output.CaptureFrame(context.Background(), frameFor(20*time.Millisecond, 1)) }()
	time.Sleep(5 * time.Millisecond)
	if err := output.ClearBuffer(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("capture remained blocked after interruption")
	}
	if output.CapturedPlayoutSegments() != 0 || sink.writeCount() != 0 {
		t.Fatalf("captured=%d writes=%d", output.CapturedPlayoutSegments(), sink.writeCount())
	}
}

func TestParticipantAudioOutputWaitsForPreviousSegment(t *testing.T) {
	t.Parallel()
	sink := newFakePlayoutSink(true)
	output := testParticipantAudioOutput(t, sink, true)
	first := frameFor(20*time.Millisecond, 1)
	second := frameFor(40*time.Millisecond, 2)
	if err := output.CaptureFrame(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if err := output.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-sink.waiting
	secondDone := make(chan error, 1)
	go func() { secondDone <- output.CaptureFrame(context.Background(), second) }()
	select {
	case <-secondDone:
		t.Fatal("next segment captured before prior playout")
	case <-time.After(10 * time.Millisecond):
	}
	if sink.writeCount() != 1 {
		t.Fatalf("source writes = %d", sink.writeCount())
	}
	sink.release()
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("next capture did not resume")
	}
	if sink.writeCount() != 2 {
		t.Fatalf("source writes = %d", sink.writeCount())
	}
}

func TestParticipantAudioOutputPlaybackStartedOnceAcrossPauseResume(t *testing.T) {
	t.Parallel()
	sink := newFakePlayoutSink(false)
	output := testParticipantAudioOutput(t, sink, true)
	var started atomic.Int32
	output.OnPlaybackStarted(func(voice.PlaybackStartedEvent) { started.Add(1) })

	frame := frameFor(20*time.Millisecond, 1)
	for range 3 {
		if err := output.CaptureFrame(context.Background(), frame); err != nil {
			t.Fatal(err)
		}
	}
	if got := started.Load(); got != 1 {
		t.Fatalf("playback started %d times before pause", got)
	}
	if err := output.Pause(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := output.Resume(context.Background()); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := output.CaptureFrame(context.Background(), frame); err != nil {
			t.Fatal(err)
		}
	}

	if got := started.Load(); got != 1 {
		t.Fatalf("playback started %d times across pause/resume", got)
	}
	if err := output.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	event, err := output.WaitForPlayout(context.Background())
	if err != nil || event.Interrupted || event.PlaybackPosition != 6*frame.Duration() {
		t.Fatalf("playback = %#v, %v", event, err)
	}
}

func TestParticipantAudioOutputPauseAgainDuringResumeGate(t *testing.T) {
	t.Parallel()
	sink := newFakePlayoutSink(false)
	output := testParticipantAudioOutput(t, sink, true)
	if err := output.Pause(context.Background()); err != nil {
		t.Fatal(err)
	}

	captured := make(chan error, 1)
	go func() {
		captured <- output.CaptureFrame(context.Background(), frameFor(20*time.Millisecond, 1))
	}()
	waitForAudioOutputState(t, func() bool {
		return output.PendingPlayoutSegments() == 1
	}, "capture did not reach the initial pause gate")

	// Apply the two public state transitions under one lock so the forwarding
	// actor deterministically observes a resume wake-up with playback paused
	// again. This is the race that the inner pause-state recheck protects.
	output.mu.Lock()
	output.paused = false
	close(output.pauseChange)
	output.pauseChange = make(chan struct{})
	output.paused = true
	close(output.pauseChange)
	output.pauseChange = make(chan struct{})
	output.mu.Unlock()

	select {
	case err := <-captured:
		t.Fatalf("capture escaped the second pause gate: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if sink.writeCount() != 0 {
		t.Fatal("frame reached the sink while playback was paused again")
	}
	if err := output.Resume(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-captured:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("capture did not resume after the final resume")
	}
	if sink.writeCount() != 1 {
		t.Fatalf("source writes = %d", sink.writeCount())
	}
}

func TestParticipantAudioOutputExcludesDiscardedQueuedAudioAfterResume(t *testing.T) {
	t.Parallel()
	sink := newFakePlayoutSink(false)
	output := testParticipantAudioOutput(t, sink, true)
	queued := frameFor(5*time.Second, 1)
	resumed := frameFor(200*time.Millisecond, 2)
	if err := output.CaptureFrame(context.Background(), queued); err != nil {
		t.Fatal(err)
	}
	if err := output.Pause(context.Background()); err != nil {
		t.Fatal(err)
	}

	held := make(chan error, 1)
	go func() { held <- output.CaptureFrame(context.Background(), resumed) }()
	waitForAudioOutputState(t, func() bool { return sink.clearCount() == 1 }, "pause did not discard queued audio")
	if err := output.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := output.Resume(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-held:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("paused capture did not resume")
	}
	event, err := output.WaitForPlayout(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if event.Interrupted {
		t.Fatalf("playback = %#v", event)
	}
	if event.PlaybackPosition < resumed.Duration() {
		t.Fatalf("resumed frame was not accounted: %#v", event)
	}
	if event.PlaybackPosition >= time.Second {
		t.Fatalf("discarded queued frame was reported as played: %#v", event)
	}
	if sink.writeCount() != 2 {
		t.Fatalf("source writes = %d", sink.writeCount())
	}
}

func TestParticipantAudioOutputFinishesWhenPausedAfterForwardingDrain(t *testing.T) {
	t.Parallel()
	sink := newFakePlayoutSink(false)
	output := testParticipantAudioOutput(t, sink, true)
	frame := frameFor(20*time.Millisecond, 1)
	if err := output.CaptureFrame(context.Background(), frame); err != nil {
		t.Fatal(err)
	}
	if err := output.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := output.Pause(context.Background()); err != nil {
		t.Fatal(err)
	}
	event, err := output.WaitForPlayout(context.Background())
	if err != nil || event.Interrupted || event.PlaybackPosition != frame.Duration() {
		t.Fatalf("playback = %#v, %v", event, err)
	}
}

func TestParticipantAudioOutputDropsNextSegmentInterruptedDuringPriorPlayout(t *testing.T) {
	t.Parallel()
	sink := newFakePlayoutSink(true)
	output := testParticipantAudioOutput(t, sink, true)
	var finished atomic.Int32
	output.OnPlaybackFinished(func(voice.PlaybackFinishedEvent) { finished.Add(1) })
	first := frameFor(20*time.Millisecond, 1)
	if err := output.CaptureFrame(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if err := output.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-sink.waiting:
	case <-time.After(time.Second):
		t.Fatal("prior segment did not enter playout")
	}

	next := make(chan error, 1)
	go func() {
		next <- output.CaptureFrame(context.Background(), frameFor(40*time.Millisecond, 2))
	}()
	select {
	case err := <-next:
		t.Fatalf("next capture escaped prior playout: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	if err := output.ClearBuffer(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-next:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("interrupted next capture remained blocked")
	}
	event, err := output.WaitForPlayout(context.Background())
	if err != nil || !event.Interrupted || event.PlaybackPosition >= first.Duration() {
		t.Fatalf("playback = %#v, %v", event, err)
	}
	if sink.writeCount() != 1 || output.CapturedPlayoutSegments() != 1 {
		t.Fatalf("writes=%d captured=%d", sink.writeCount(), output.CapturedPlayoutSegments())
	}
	if got := finished.Load(); got != 1 {
		t.Fatalf("playback finished %d times", got)
	}
}

func TestParticipantAudioOutputProducerFlushAfterClearFinishesOnce(t *testing.T) {
	t.Parallel()
	sink := newFakePlayoutSink(false)
	output := testParticipantAudioOutput(t, sink, true)
	var finished atomic.Int32
	output.OnPlaybackFinished(func(voice.PlaybackFinishedEvent) { finished.Add(1) })
	first := frameFor(20*time.Millisecond, 1)
	if err := output.CaptureFrame(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if err := output.ClearBuffer(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := output.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	event, err := output.WaitForPlayout(context.Background())
	if err != nil || !event.Interrupted || event.PlaybackPosition >= first.Duration() {
		t.Fatalf("first playback = %#v, %v", event, err)
	}
	if got := finished.Load(); got != 1 {
		t.Fatalf("first segment finished %d times", got)
	}

	second := frameFor(40*time.Millisecond, 2)
	if err := output.CaptureFrame(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	if err := output.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	event, err = output.WaitForPlayout(context.Background())
	if err != nil || event.Interrupted || event.PlaybackPosition != second.Duration() {
		t.Fatalf("second playback = %#v, %v", event, err)
	}
	if got := finished.Load(); got != 2 {
		t.Fatalf("two segments finished %d times", got)
	}
}

func TestParticipantAudioOutputFirstWriteFailureCompletesSegment(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("write failed")
	sink := newFakePlayoutSink(false)
	sink.failWrites(sentinel)
	output := testParticipantAudioOutput(t, sink, true)
	var started, finished atomic.Int32
	output.OnPlaybackStarted(func(voice.PlaybackStartedEvent) { started.Add(1) })
	output.OnPlaybackFinished(func(voice.PlaybackFinishedEvent) { finished.Add(1) })

	if err := output.CaptureFrame(context.Background(), frameFor(20*time.Millisecond, 1)); !errors.Is(err, sentinel) {
		t.Fatalf("CaptureFrame error = %v", err)
	}
	event, err := output.WaitForPlayout(context.Background())
	if err != nil || !event.Interrupted || event.PlaybackPosition != 0 {
		t.Fatalf("failed playback = %#v, %v", event, err)
	}
	if err := output.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := started.Load(); got != 0 {
		t.Fatalf("failed write emitted playbackStarted %d times", got)
	}
	if got := finished.Load(); got != 1 {
		t.Fatalf("failed segment finished %d times", got)
	}
	if sink.writeAttemptCount() != 1 || sink.writeCount() != 0 {
		t.Fatalf("write attempts=%d accepted=%d", sink.writeAttemptCount(), sink.writeCount())
	}

	next := frameFor(40*time.Millisecond, 2)
	if err := output.CaptureFrame(context.Background(), next); err != nil {
		t.Fatal(err)
	}
	if err := output.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	event, err = output.WaitForPlayout(context.Background())
	if err != nil || event.Interrupted || event.PlaybackPosition != next.Duration() {
		t.Fatalf("recovered playback = %#v, %v", event, err)
	}
	if got := started.Load(); got != 1 {
		t.Fatalf("recovered playback started %d times", got)
	}
	if got := finished.Load(); got != 2 {
		t.Fatalf("recovered segments finished %d times", got)
	}
}

func TestParticipantAudioOutputCloseBeforeStart(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancelCause(context.Background())
	output := newParticipantAudioOutput(ctx, cancel, DefaultRoomOutputOptions(), func(err error) {
		t.Errorf("output error: %v", err)
	})
	close(output.rtcDone)

	closeCtx, closeCancel := context.WithTimeout(context.Background(), time.Second)
	defer closeCancel()
	if err := output.Close(closeCtx); err != nil {
		t.Fatal(err)
	}
	if err := output.Start(context.Background()); !errors.Is(err, ErrParticipantAudioOutputClosed) {
		t.Fatalf("Start after Close = %v", err)
	}
	if err := output.Close(closeCtx); err != nil {
		t.Fatalf("second Close = %v", err)
	}
}

func TestParticipantAudioOutputConcurrentStartClose(t *testing.T) {
	t.Parallel()
	for iteration := range 16 {
		ctx, cancel := context.WithCancelCause(context.Background())
		output := newParticipantAudioOutput(ctx, cancel, DefaultRoomOutputOptions(), func(err error) {
			t.Errorf("iteration %d output error: %v", iteration, err)
		})
		// Leave connected false: Start cannot touch RTC or publish a real track,
		// regardless of which side wins the race.
		close(output.rtcDone)
		release := make(chan struct{})
		started := make(chan error, 1)
		closed := make(chan error, 1)
		go func() {
			<-release
			started <- output.Start(context.Background())
		}()
		go func() {
			<-release
			closeCtx, closeCancel := context.WithTimeout(context.Background(), time.Second)
			defer closeCancel()
			closed <- output.Close(closeCtx)
		}()
		close(release)

		select {
		case err := <-started:
			if !errors.Is(err, ErrParticipantAudioOutputClosed) {
				t.Fatalf("iteration %d Start = %v", iteration, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("iteration %d Start remained blocked", iteration)
		}
		select {
		case err := <-closed:
			if err != nil {
				t.Fatalf("iteration %d Close = %v", iteration, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("iteration %d Close remained blocked", iteration)
		}
	}
}
