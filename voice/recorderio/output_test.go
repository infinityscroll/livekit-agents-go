// SPDX-License-Identifier: Apache-2.0

package recorderio

import (
	"context"
	"encoding/binary"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/voice"
)

func TestBuildPlaybackFramesTruncatesAndInsertsPauseSilence(t *testing.T) {
	start := time.Unix(1_700_000_000, 0)
	frame := audioFrame(100*time.Millisecond, 1_000, 1, 7)
	segment := &recorderOutputSegment{
		frames: []agents.AudioFrame{frame}, recorded: true, speechStart: start,
		pauses: []pauseInterval{{start.Add(40 * time.Millisecond), start.Add(60 * time.Millisecond)}},
	}
	frames, err := buildPlaybackFrames(segment, 100*time.Millisecond, start.Add(120*time.Millisecond), 1<<20, 1<<10)
	if err != nil {
		t.Fatal(err)
	}
	merged, err := agents.MergeFrames(frames)
	if err != nil {
		t.Fatal(err)
	}
	if merged.SamplesPerChannel != 120 {
		t.Fatalf("samples = %d, want 120", merged.SamplesPerChannel)
	}
	for index, value := range merged.Data {
		want := int16(7)
		if index >= 40 && index < 60 {
			want = 0
		}
		if value != want {
			t.Fatalf("sample %d = %d, want %d", index, value, want)
		}
	}

	segment.pauses = nil
	frames, err = buildPlaybackFrames(segment, 35*time.Millisecond, start.Add(35*time.Millisecond), 1<<20, 1<<10)
	if err != nil {
		t.Fatal(err)
	}
	merged, err = agents.MergeFrames(frames)
	if err != nil {
		t.Fatal(err)
	}
	if merged.SamplesPerChannel != 35 {
		t.Fatalf("truncated samples = %d", merged.SamplesPerChannel)
	}
}

func TestFinishWhilePausedRetainsTrailingSilence(t *testing.T) {
	start := time.Unix(1_700_000_000, 0)
	segment := &recorderOutputSegment{
		frames: []agents.AudioFrame{audioFrame(100*time.Millisecond, 1_000, 1, 9)}, recorded: true,
		speechStart: start, currentPauseStart: start.Add(50 * time.Millisecond),
	}
	frames, err := buildPlaybackFrames(segment, 50*time.Millisecond, start.Add(100*time.Millisecond), 1<<20, 1<<10)
	if err != nil {
		t.Fatal(err)
	}
	merged, err := agents.MergeFrames(frames)
	if err != nil {
		t.Fatal(err)
	}
	if merged.SamplesPerChannel != 100 {
		t.Fatalf("samples = %d", merged.SamplesPerChannel)
	}
	for index, value := range merged.Data {
		if index < 50 && value != 9 {
			t.Fatalf("played sample %d = %d", index, value)
		}
		if index >= 50 && value != 0 {
			t.Fatalf("trailing silence %d = %d", index, value)
		}
	}
}

func TestPauseBeforeSegmentIsDiscarded(t *testing.T) {
	fixture := newRecorderFixture(t, newTestAudioOutput(1_000), func(options *RecorderOptions) {
		options.SampleRate = 1_000
	})
	if err := fixture.output.Pause(t.Context()); err != nil {
		t.Fatal(err)
	}
	fixture.clock.Advance(500 * time.Millisecond)
	if err := fixture.output.Resume(t.Context()); err != nil {
		t.Fatal(err)
	}
	fixture.clock.Advance(500 * time.Millisecond)
	if err := fixture.output.CaptureFrame(t.Context(), audioFrame(20*time.Millisecond, 1_000, 1, 4)); err != nil {
		t.Fatal(err)
	}
	fixture.clock.Advance(20 * time.Millisecond)
	if err := fixture.sink.NotifyPlaybackFinished(voice.PlaybackFinishedEvent{PlaybackPosition: 20 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	if err := fixture.recorder.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if pcm := fixture.encoder.bytes(); len(pcm) != 20*4 {
		t.Fatalf("pause before segment produced %d samples, want 20", len(pcm)/4)
	}
}

func TestPauseOverlappingSegmentIsClippedToCaptureTime(t *testing.T) {
	fixture := newRecorderFixture(t, newTestAudioOutput(1_000), func(options *RecorderOptions) {
		options.SampleRate = 1_000
	})
	if err := fixture.output.Pause(t.Context()); err != nil {
		t.Fatal(err)
	}
	fixture.clock.Advance(500 * time.Millisecond)
	if err := fixture.output.CaptureFrame(t.Context(), audioFrame(20*time.Millisecond, 1_000, 1, 4)); err != nil {
		t.Fatal(err)
	}
	fixture.clock.Advance(200 * time.Millisecond)
	if err := fixture.output.Resume(t.Context()); err != nil {
		t.Fatal(err)
	}
	fixture.clock.Advance(20 * time.Millisecond)
	if err := fixture.sink.NotifyPlaybackFinished(voice.PlaybackFinishedEvent{PlaybackPosition: 20 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	if err := fixture.recorder.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	pcm := fixture.encoder.bytes()
	if len(pcm) != 220*4 {
		t.Fatalf("overlapping pause produced %d samples, want 220", len(pcm)/4)
	}
	for sample := 0; sample < 220; sample++ {
		right := int16(binary.LittleEndian.Uint16(pcm[sample*4+2:]))
		want := int16(0)
		if sample >= 200 {
			want = 4
		}
		if right != want {
			t.Fatalf("right sample %d = %d, want %d", sample, right, want)
		}
	}
}

func TestBuildPlaybackFramesRejectsHugeSilenceBeforeAllocation(t *testing.T) {
	start := time.Unix(1_700_000_000, 0)
	pauseStart := start.Add(time.Millisecond)
	segment := &recorderOutputSegment{
		frames:            []agents.AudioFrame{audioFrame(time.Millisecond, 1_000, 1, 9)},
		recorded:          true,
		speechStart:       start,
		currentPauseStart: pauseStart,
	}
	_, err := buildPlaybackFrames(segment, time.Millisecond, pauseStart.Add(time.Duration(1<<63-1)), 4, 2)
	if !errors.Is(err, ErrBufferLimit) {
		t.Fatalf("huge trailing silence = %v", err)
	}
}

func TestDroppedSegmentSettlesWhenWaitedWithoutFlush(t *testing.T) {
	sink := newTestAudioOutput(48_000)
	sink.hook = func(*testAudioOutput, context.Context, agents.AudioFrame) error { return nil }
	fixture := newRecorderFixture(t, sink, nil)
	if err := fixture.output.CaptureFrame(t.Context(), audioFrame(20*time.Millisecond, 48_000, 1, 1)); err != nil {
		t.Fatal(err)
	}
	event, err := fixture.output.WaitForPlayout(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !event.Interrupted || event.PlaybackPosition != 0 {
		t.Fatalf("dropped event = %#v", event)
	}
	if fixture.output.PendingPlayoutSegments() != 0 {
		t.Fatalf("pending = %d", fixture.output.PendingPlayoutSegments())
	}
	if err := fixture.recorder.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestPlaybackPositionIsClampedToElapsedWallTime(t *testing.T) {
	fixture := newRecorderFixture(t, newTestAudioOutput(48_000), nil)
	if err := fixture.output.CaptureFrame(t.Context(), audioFrame(100*time.Millisecond, 48_000, 1, 5)); err != nil {
		t.Fatal(err)
	}
	fixture.clock.Advance(30 * time.Millisecond)
	if err := fixture.sink.NotifyPlaybackFinished(voice.PlaybackFinishedEvent{PlaybackPosition: time.Second, Interrupted: true}); err != nil {
		t.Fatal(err)
	}
	event, err := fixture.output.WaitForPlayout(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if event.PlaybackPosition != 30*time.Millisecond {
		t.Fatalf("clamped position = %v", event.PlaybackPosition)
	}
	if err := fixture.recorder.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestOutputBufferLimitRejectsBeforeDownstreamAndSettles(t *testing.T) {
	frame := audioFrame(20*time.Millisecond, 1_000, 1, 1)
	fixture := newRecorderFixture(t, newTestAudioOutput(1_000), func(options *RecorderOptions) {
		options.SampleRate = 1_000
		options.MaxBufferedBytes = len(frame.Data) // half the required PCM bytes
		options.MaxQueuedBytes = len(frame.Data) * 2
	})
	if err := fixture.output.CaptureFrame(t.Context(), frame); !errors.Is(err, ErrBufferLimit) {
		t.Fatalf("CaptureFrame = %v", err)
	}
	if fixture.sink.CapturedPlayoutSegments() != 0 || fixture.output.PendingPlayoutSegments() != 0 {
		t.Fatalf("downstream=%d recorder=%d", fixture.sink.CapturedPlayoutSegments(), fixture.output.PendingPlayoutSegments())
	}
	if err := fixture.recorder.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestOutputFrameCountBoundsZeroLengthFramesAndAllowsRetry(t *testing.T) {
	fixture := newRecorderFixture(t, newTestAudioOutput(1_000), func(options *RecorderOptions) {
		options.SampleRate = 1_000
		options.MaxBufferedFrames = 1
		options.MaxQueuedFrames = 2
	})
	frame, err := agents.NewAudioFrame(nil, 1_000, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.output.CaptureFrame(t.Context(), frame); err != nil {
		t.Fatal(err)
	}
	if err := fixture.output.CaptureFrame(t.Context(), frame); !errors.Is(err, ErrBufferLimit) {
		t.Fatalf("second zero-length frame = %v", err)
	}
	if fixture.output.PendingPlayoutSegments() != 0 || fixture.sink.PendingPlayoutSegments() != 0 {
		t.Fatalf("failed segment remained pending: recorder=%d sink=%d", fixture.output.PendingPlayoutSegments(), fixture.sink.PendingPlayoutSegments())
	}
	if err := fixture.output.CaptureFrame(t.Context(), frame); err != nil {
		t.Fatalf("retry = %v", err)
	}
	if err := fixture.sink.NotifyPlaybackFinished(voice.PlaybackFinishedEvent{Interrupted: true}); err != nil {
		t.Fatal(err)
	}
	if err := fixture.recorder.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestPlaybackFinishQueueIsBoundedWhileCaptureIsParked(t *testing.T) {
	sink := newTestAudioOutput(1_000)
	parked := make(chan struct{})
	release := make(chan struct{})
	sink.hook = func(output *testAudioOutput, ctx context.Context, _ agents.AudioFrame) error {
		output.accept()
		close(parked)
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-release:
			return nil
		}
	}
	fixture := newRecorderFixture(t, sink, func(options *RecorderOptions) {
		options.SampleRate = 1_000
		options.MaxBufferedFrames = 1
		options.MaxQueuedFrames = 2
	})
	captured := make(chan error, 1)
	go func() {
		captured <- fixture.output.CaptureFrame(context.Background(), audioFrame(time.Millisecond, 1_000, 1, 1))
	}()
	<-parked
	if err := sink.NotifyPlaybackFinished(voice.PlaybackFinishedEvent{PlaybackPosition: time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	if err := fixture.output.NotifyPlaybackFinished(voice.PlaybackFinishedEvent{}); !errors.Is(err, ErrBufferLimit) {
		t.Fatalf("surplus deferred finish = %v", err)
	}
	close(release)
	if err := <-captured; err != nil {
		t.Fatal(err)
	}
	if err := fixture.recorder.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestRejectedCaptureCanBeRetriedWithoutFlush(t *testing.T) {
	sentinel := errors.New("capture rejected")
	sink := newTestAudioOutput(48_000)
	var captures atomic.Int32
	sink.hook = func(output *testAudioOutput, _ context.Context, _ agents.AudioFrame) error {
		if captures.Add(1) == 1 {
			return sentinel
		}
		output.accept()
		return nil
	}
	fixture := newRecorderFixture(t, sink, nil)
	if err := fixture.output.CaptureFrame(t.Context(), audioFrame(20*time.Millisecond, 48_000, 1, 1)); !errors.Is(err, sentinel) {
		t.Fatalf("first capture = %v", err)
	}
	if fixture.output.PendingPlayoutSegments() != 0 {
		t.Fatalf("rejected pending = %d", fixture.output.PendingPlayoutSegments())
	}
	if err := fixture.output.CaptureFrame(t.Context(), audioFrame(20*time.Millisecond, 48_000, 1, 2)); err != nil {
		t.Fatal(err)
	}
	fixture.clock.Advance(20 * time.Millisecond)
	marker := "retried"
	want := voice.PlaybackFinishedEvent{PlaybackPosition: 20 * time.Millisecond, SynchronizedTranscript: &marker}
	if err := sink.NotifyPlaybackFinished(want); err != nil {
		t.Fatal(err)
	}
	got, err := fixture.output.WaitForPlayout(t.Context())
	if err != nil || got.PlaybackPosition != want.PlaybackPosition || got.SynchronizedTranscript == nil || *got.SynchronizedTranscript != marker {
		t.Fatalf("retried wait = %#v, %v", got, err)
	}
	if err := fixture.recorder.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestCountedRejectedCaptureSettlesBothOutputsAndRetries(t *testing.T) {
	sentinel := errors.New("counted rejection")
	sink := newTestAudioOutput(48_000)
	var captures atomic.Int32
	sink.hook = func(output *testAudioOutput, _ context.Context, _ agents.AudioFrame) error {
		output.accept()
		if captures.Add(1) == 1 {
			return sentinel
		}
		return nil
	}
	fixture := newRecorderFixture(t, sink, nil)
	if err := fixture.output.CaptureFrame(t.Context(), audioFrame(20*time.Millisecond, 48_000, 1, 1)); !errors.Is(err, sentinel) {
		t.Fatalf("first capture = %v", err)
	}
	if fixture.output.PendingPlayoutSegments() != 0 || sink.PendingPlayoutSegments() != 0 {
		t.Fatalf("pending recorder=%d sink=%d", fixture.output.PendingPlayoutSegments(), sink.PendingPlayoutSegments())
	}
	if err := fixture.output.CaptureFrame(t.Context(), audioFrame(20*time.Millisecond, 48_000, 1, 2)); err != nil {
		t.Fatal(err)
	}
	fixture.clock.Advance(20 * time.Millisecond)
	if err := sink.NotifyPlaybackFinished(voice.PlaybackFinishedEvent{PlaybackPosition: 20 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.output.WaitForPlayout(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := fixture.recorder.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

type notificationOpaqueOutput struct{ voice.AudioOutput }

func TestCountedRejectedCaptureFallsBackToClearBuffer(t *testing.T) {
	sentinel := errors.New("counted rejection")
	sink := newTestAudioOutput(48_000)
	var captures atomic.Int32
	sink.hook = func(output *testAudioOutput, _ context.Context, _ agents.AudioFrame) error {
		output.accept()
		if captures.Add(1) == 2 {
			return sentinel
		}
		return nil
	}
	sink.clearHook = func(output *testAudioOutput) error {
		return output.NotifyPlaybackFinished(voice.PlaybackFinishedEvent{Interrupted: true})
	}
	fixture := newRecorderFixture(t, notificationOpaqueOutput{AudioOutput: sink}, nil)
	if err := fixture.output.CaptureFrame(t.Context(), audioFrame(20*time.Millisecond, 48_000, 1, 1)); err != nil {
		t.Fatal(err)
	}
	if err := fixture.output.CaptureFrame(t.Context(), audioFrame(20*time.Millisecond, 48_000, 1, 2)); !errors.Is(err, sentinel) {
		t.Fatalf("second capture = %v", err)
	}
	if fixture.output.PendingPlayoutSegments() != 0 || sink.PendingPlayoutSegments() != 0 {
		t.Fatalf("pending recorder=%d sink=%d", fixture.output.PendingPlayoutSegments(), sink.PendingPlayoutSegments())
	}
	if err := fixture.output.CaptureFrame(t.Context(), audioFrame(20*time.Millisecond, 48_000, 1, 3)); err != nil {
		t.Fatalf("retry = %v", err)
	}
	fixture.clock.Advance(20 * time.Millisecond)
	if err := sink.NotifyPlaybackFinished(voice.PlaybackFinishedEvent{PlaybackPosition: 20 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	if err := fixture.recorder.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestFinishDuringParkedCaptureIsDeferredUntilFrameRecorded(t *testing.T) {
	sink := newTestAudioOutput(48_000)
	parked := make(chan struct{})
	release := make(chan struct{})
	sink.hook = func(output *testAudioOutput, ctx context.Context, _ agents.AudioFrame) error {
		output.accept()
		close(parked)
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-release:
			return nil
		}
	}
	fixture := newRecorderFixture(t, sink, nil)
	captureDone := make(chan error, 1)
	go func() {
		captureDone <- fixture.output.CaptureFrame(context.Background(), audioFrame(40*time.Millisecond, 48_000, 1, 8))
	}()
	<-parked
	fixture.clock.Advance(30 * time.Millisecond)
	if err := sink.NotifyPlaybackFinished(voice.PlaybackFinishedEvent{PlaybackPosition: 20 * time.Millisecond, Interrupted: true}); err != nil {
		t.Fatal(err)
	}
	waitDone := make(chan error, 1)
	go func() {
		event, err := fixture.output.WaitForPlayout(context.Background())
		if err == nil && event.PlaybackPosition != 20*time.Millisecond {
			err = errors.New("wrong playback position")
		}
		waitDone <- err
	}()
	select {
	case err := <-waitDone:
		t.Fatalf("wait returned while capture parked: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	if err := <-captureDone; err != nil {
		t.Fatal(err)
	}
	if err := <-waitDone; err != nil {
		t.Fatal(err)
	}
	if err := fixture.recorder.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestManagedOutputEarlyFinishThenRemainingFrameDoesNotStall(t *testing.T) {
	var downstream *voice.ManagedAudioOutput
	var captures atomic.Int32
	var err error
	downstream, err = voice.NewManagedAudioOutput(voice.AudioOutputOptions{
		SampleRate: 48_000,
		Capture: func(context.Context, agents.AudioFrame) error {
			if captures.Add(1) == 1 {
				return downstream.NotifyPlaybackFinished(voice.PlaybackFinishedEvent{Interrupted: true})
			}
			return nil
		},
		ClearBuffer: func(context.Context) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	recorder, err := NewRecorderIO(RecorderOptions{
		Clock:          newManualClock().Now,
		WriteInterval:  time.Hour,
		EncoderFactory: func(context.Context, string, int) (PCMEncoder, error) { return &memoryEncoder{}, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := recorder.RecordInput(newQueuedAudioInput()); err != nil {
		t.Fatal(err)
	}
	output, err := recorder.RecordOutput(downstream)
	if err != nil {
		t.Fatal(err)
	}
	if err := recorder.Start(t.Context(), filepath.Join(t.TempDir(), "managed.ogg")); err != nil {
		t.Fatal(err)
	}
	if err := output.CaptureFrame(t.Context(), audioFrame(20*time.Millisecond, 48_000, 1, 1)); err != nil {
		t.Fatal(err)
	}
	if err := output.CaptureFrame(t.Context(), audioFrame(20*time.Millisecond, 48_000, 1, 2)); err != nil {
		t.Fatal(err)
	}
	if err := output.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	event, err := output.WaitForPlayout(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !event.Interrupted {
		t.Fatalf("remaining dropped segment = %#v", event)
	}
	if err := recorder.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestParkedThenDroppedFrameSettlesAfterWait(t *testing.T) {
	sink := newTestAudioOutput(48_000)
	parked := make(chan struct{})
	release := make(chan struct{})
	sink.hook = func(_ *testAudioOutput, ctx context.Context, _ agents.AudioFrame) error {
		close(parked)
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-release:
			return nil
		}
	}
	fixture := newRecorderFixture(t, sink, nil)
	captureDone := make(chan error, 1)
	go func() {
		captureDone <- fixture.output.CaptureFrame(context.Background(), audioFrame(20*time.Millisecond, 48_000, 1, 8))
	}()
	<-parked
	waitDone := make(chan voice.PlaybackFinishedEvent, 1)
	go func() {
		event, _ := fixture.output.WaitForPlayout(context.Background())
		waitDone <- event
	}()
	select {
	case <-waitDone:
		t.Fatal("wait returned before parked capture completed")
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	if err := <-captureDone; err != nil {
		t.Fatal(err)
	}
	if event := <-waitDone; !event.Interrupted || event.PlaybackPosition != 0 {
		t.Fatalf("drop event = %#v", event)
	}
	if err := fixture.recorder.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestCloseCancelsParkedDownstreamCaptureWithoutLeak(t *testing.T) {
	sink := newTestAudioOutput(48_000)
	parked := make(chan struct{})
	sink.hook = func(_ *testAudioOutput, ctx context.Context, _ agents.AudioFrame) error {
		close(parked)
		<-ctx.Done()
		return context.Cause(ctx)
	}
	fixture := newRecorderFixture(t, sink, nil)
	captured := make(chan error, 1)
	go func() {
		captured <- fixture.output.CaptureFrame(context.Background(), audioFrame(20*time.Millisecond, 48_000, 1, 9))
	}()
	<-parked
	if err := fixture.recorder.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-captured:
		if !errors.Is(err, ErrClosing) {
			t.Fatalf("parked capture = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("parked downstream capture leaked across Close")
	}
	if fixture.output.PendingPlayoutSegments() != 0 {
		t.Fatalf("pending after close = %d", fixture.output.PendingPlayoutSegments())
	}
}

func TestOlderFinishIsNotMisattributedToOverlappingDroppedSegment(t *testing.T) {
	sink := newTestAudioOutput(48_000)
	var captures atomic.Int32
	sink.hook = func(output *testAudioOutput, _ context.Context, _ agents.AudioFrame) error {
		if captures.Add(1) == 1 {
			output.accept()
			return nil
		}
		return output.NotifyPlaybackFinished(voice.PlaybackFinishedEvent{PlaybackPosition: 10 * time.Millisecond})
	}
	fixture := newRecorderFixture(t, sink, nil)
	if err := fixture.output.CaptureFrame(t.Context(), audioFrame(10*time.Millisecond, 48_000, 1, 11)); err != nil {
		t.Fatal(err)
	}
	if err := fixture.output.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := fixture.output.CaptureFrame(t.Context(), audioFrame(10*time.Millisecond, 48_000, 1, 22)); err != nil {
		t.Fatal(err)
	}
	event, err := fixture.output.WaitForPlayout(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !event.Interrupted || event.PlaybackPosition != 0 {
		t.Fatalf("second dropped event = %#v", event)
	}
	if fixture.output.PendingPlayoutSegments() != 0 {
		t.Fatalf("pending = %d", fixture.output.PendingPlayoutSegments())
	}
	if err := fixture.recorder.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestInterruptedWhilePausedThenSubsequentSpeech(t *testing.T) {
	sink := newTestAudioOutput(48_000)
	fixture := newRecorderFixture(t, sink, nil)
	if err := fixture.output.CaptureFrame(t.Context(), audioFrame(20*time.Millisecond, 48_000, 1, 1111)); err != nil {
		t.Fatal(err)
	}
	fixture.clock.Advance(10 * time.Millisecond)
	if err := fixture.output.Pause(t.Context()); err != nil {
		t.Fatal(err)
	}
	fixture.clock.Advance(10 * time.Millisecond)
	if err := sink.NotifyPlaybackFinished(voice.PlaybackFinishedEvent{PlaybackPosition: 10 * time.Millisecond, Interrupted: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.output.WaitForPlayout(t.Context()); err != nil {
		t.Fatal(err)
	}
	// The second turn begins while the output is still paused. Its pause clock
	// must be clipped to this new segment rather than inherited from turn one.
	if err := fixture.output.CaptureFrame(t.Context(), audioFrame(20*time.Millisecond, 48_000, 1, 2222)); err != nil {
		t.Fatal(err)
	}
	fixture.clock.Advance(10 * time.Millisecond)
	if err := fixture.output.Resume(t.Context()); err != nil {
		t.Fatal(err)
	}
	fixture.clock.Advance(20 * time.Millisecond)
	if err := sink.NotifyPlaybackFinished(voice.PlaybackFinishedEvent{PlaybackPosition: 20 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.output.WaitForPlayout(t.Context()); err != nil {
		t.Fatal(err)
	}
	if fixture.output.PendingPlayoutSegments() != 0 {
		t.Fatalf("pending after follow-on turn = %d", fixture.output.PendingPlayoutSegments())
	}
	if err := fixture.recorder.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	pcm := fixture.encoder.bytes()
	foundFollowOn := false
	for offset := 2; offset+1 < len(pcm); offset += 4 {
		if int16(binary.LittleEndian.Uint16(pcm[offset:])) == 2222 {
			foundFollowOn = true
			break
		}
	}
	if !foundFollowOn {
		t.Fatal("follow-on speech was lost after paused interruption")
	}
}
