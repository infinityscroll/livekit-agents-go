// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"context"
	"errors"
	"testing"
	"time"

	agents "github.com/infinityscroll/livekit-agents-go"
)

func TestManagedAudioOutputSegmentAccounting(t *testing.T) {
	output, err := NewManagedAudioOutput(AudioOutputOptions{SampleRate: 16000, ClearBuffer: func(context.Context) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	frame, _ := agents.NewAudioFrame(make([]int16, 160), 16000, 1)
	if err := output.CaptureFrame(context.Background(), frame); err != nil {
		t.Fatal(err)
	}
	if err := output.CaptureFrame(context.Background(), frame); err != nil {
		t.Fatal(err)
	}
	if got := output.CapturedPlayoutSegments(); got != 1 {
		t.Fatalf("segments = %d", got)
	}
	if err := output.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := output.CaptureFrame(context.Background(), frame); err != nil {
		t.Fatal(err)
	}
	if got := output.CapturedPlayoutSegments(); got != 2 {
		t.Fatalf("segments after flush = %d", got)
	}
	done := make(chan struct{})
	go func() {
		_, err := output.WaitForPlayout(context.Background())
		if err != nil {
			t.Error(err)
		}
		close(done)
	}()
	if err := output.NotifyPlaybackFinished(PlaybackFinishedEvent{PlaybackPosition: 10 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
		t.Fatal("wait returned before target segment")
	case <-time.After(10 * time.Millisecond):
	}
	if err := output.NotifyPlaybackFinished(PlaybackFinishedEvent{PlaybackPosition: 10 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("wait did not return")
	}
	if err := output.NotifyPlaybackFinished(PlaybackFinishedEvent{}); !errors.Is(err, ErrUnexpectedPlaybackFinished) {
		t.Fatalf("extra finish = %v", err)
	}
}

func TestManagedAudioOutputPauseRequiresWholeChain(t *testing.T) {
	tail, _ := NewManagedAudioOutput(AudioOutputOptions{SampleRate: 16000})
	head, _ := NewManagedAudioOutput(AudioOutputOptions{SampleRate: 16000, Capabilities: AudioOutputCapabilities{Pause: true}, Next: tail})
	if head.CanPause() {
		t.Fatal("chain should not support pause")
	}
	if err := head.Pause(context.Background()); err == nil {
		t.Fatal("expected pause error")
	}
}

func TestManagedAudioOutputFailedFirstFrameDoesNotLeakSegment(t *testing.T) {
	want := errors.New("capture failed")
	output, err := NewManagedAudioOutput(AudioOutputOptions{
		SampleRate: 16000,
		Capture:    func(context.Context, agents.AudioFrame) error { return want },
	})
	if err != nil {
		t.Fatal(err)
	}
	frame, _ := agents.NewAudioFrame(make([]int16, 160), 16000, 1)
	if got := output.CaptureFrame(context.Background(), frame); !errors.Is(got, want) {
		t.Fatalf("capture error = %v", got)
	}
	if got := output.PendingPlayoutSegments(); got != 0 {
		t.Fatalf("pending segments = %d", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := output.WaitForPlayout(ctx); err != nil {
		t.Fatalf("wait after failed frame: %v", err)
	}
}

func TestAgentInputAttachState(t *testing.T) {
	input := NewBaseAudioInput(context.Background(), 1, nil)
	controller := NewAgentInput(nil, nil)
	controller.SetAudioEnabled(false)
	controller.SetAudio(input)
	if input.Attached() {
		t.Fatal("disabled input was attached")
	}
	controller.SetAudioEnabled(true)
	if !input.Attached() {
		t.Fatal("enabled input was not attached")
	}
	controller.SetAudio(nil)
	if input.Attached() {
		t.Fatal("replaced input remained attached")
	}
	_ = input.Close()
}
