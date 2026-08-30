// SPDX-License-Identifier: Apache-2.0

package avatar

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/voice"
)

func avatarTestFrame() agents.AudioFrame {
	frame, _ := agents.NewAudioFrame([]int16{1, -2, 3, -4}, 16000, 1)
	return frame
}

func TestQueueAudioOutputSegmentsClearAndPlayout(t *testing.T) {
	output, err := NewQueueAudioOutput(QueueAudioOutputOptions{SampleRate: 16000, Capacity: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer output.Close()
	started := make(chan voice.PlaybackStartedEvent, 1)
	output.OnPlaybackStarted(func(event voice.PlaybackStartedEvent) { started <- event })
	clearEvents := make(chan QueueAudioOutputClearEvent, 1)
	output.OnClearBuffer(func(event QueueAudioOutputClearEvent) { clearEvents <- event })

	frame := avatarTestFrame()
	if err := output.CaptureFrame(t.Context(), frame); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("missing eager playback-started event")
	}
	if err := output.ClearBuffer(t.Context()); err != nil {
		t.Fatal(err)
	}
	if event := <-clearEvents; !event.WasCapturing {
		t.Fatalf("clear event = %#v", event)
	}
	item, err := output.Recv(t.Context())
	if err != nil || item.SegmentEnd || len(item.Frame.Data) != len(frame.Data) {
		t.Fatalf("frame item = %#v, %v", item, err)
	}
	item, err = output.Recv(t.Context())
	if err != nil || !item.SegmentEnd {
		t.Fatalf("segment item = %#v, %v", item, err)
	}
	finished := voice.PlaybackFinishedEvent{PlaybackPosition: frame.Duration(), Interrupted: true}
	if err := output.NotifyPlaybackFinishedEvent(finished); err != nil {
		t.Fatal(err)
	}
	if event, err := output.WaitForPlayout(t.Context()); err != nil || event != finished {
		t.Fatalf("WaitForPlayout = %#v, %v", event, err)
	}

	if err := output.CaptureFrame(t.Context(), frame); err != nil {
		t.Fatal(err)
	}
	if err := output.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := output.Recv(t.Context()); err != nil {
		t.Fatal(err)
	}
	if end, err := output.Recv(t.Context()); err != nil || !end.SegmentEnd {
		t.Fatalf("next boundary = %#v, %v", end, err)
	}
}

func TestQueueAudioOutputBoundedBackpressureAndCloseUnblocks(t *testing.T) {
	output, err := NewQueueAudioOutput(QueueAudioOutputOptions{Capacity: 1, WaitPlaybackStart: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := output.CaptureFrame(t.Context(), avatarTestFrame()); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- output.CaptureFrame(context.Background(), avatarTestFrame()) }()
	select {
	case err := <-done:
		t.Fatalf("second capture did not apply backpressure: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- output.Close() }()
	select {
	case err := <-done:
		if !errors.Is(err, io.EOF) {
			t.Fatalf("capture error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not unblock capture")
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close deadlocked")
	}
	if err := output.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestQueueAudioOutputCaptureHonorsCancellation(t *testing.T) {
	output, err := NewQueueAudioOutput(QueueAudioOutputOptions{Capacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer output.Close()
	if err := output.CaptureFrame(t.Context(), avatarTestFrame()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	if err := output.CaptureFrame(ctx, avatarTestFrame()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("CaptureFrame error = %v", err)
	}
}

func TestQueueAudioOutputWaitPlaybackStart(t *testing.T) {
	output, err := NewQueueAudioOutput(QueueAudioOutputOptions{Capacity: 2, WaitPlaybackStart: true})
	if err != nil {
		t.Fatal(err)
	}
	defer output.Close()
	started := make(chan struct{}, 1)
	output.OnPlaybackStarted(func(voice.PlaybackStartedEvent) { started <- struct{}{} })
	if err := output.CaptureFrame(t.Context(), avatarTestFrame()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
		t.Fatal("playback started eagerly")
	default:
	}
	output.NotifyPlaybackStarted(time.Time{})
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("missing explicit playback start")
	}
}
