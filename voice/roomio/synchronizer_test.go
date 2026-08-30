// SPDX-License-Identifier: Apache-2.0

package roomio

import (
	"context"
	"sync"
	"testing"
	"time"

	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/voice"
)

type recordingTextOutput struct {
	mu       sync.Mutex
	texts    []agents.TimedString
	flushes  int
	attached bool
}

func (o *recordingTextOutput) CaptureText(_ context.Context, text agents.TimedString) error {
	o.mu.Lock()
	o.texts = append(o.texts, text)
	o.mu.Unlock()
	return nil
}
func (o *recordingTextOutput) Flush(context.Context) error {
	o.mu.Lock()
	o.flushes++
	o.mu.Unlock()
	return nil
}
func (o *recordingTextOutput) SetAttached(attached bool) {
	o.mu.Lock()
	o.attached = attached
	o.mu.Unlock()
}
func (o *recordingTextOutput) OnAttached() { o.SetAttached(true) }
func (o *recordingTextOutput) OnDetached() { o.SetAttached(false) }
func (o *recordingTextOutput) count() int  { o.mu.Lock(); defer o.mu.Unlock(); return len(o.texts) }

func TestSynchronizedTextOutputWaitsForTimedAudio(t *testing.T) {
	t.Parallel()
	audio, err := voice.NewManagedAudioOutput(voice.AudioOutputOptions{SampleRate: 24_000})
	if err != nil {
		t.Fatal(err)
	}
	next := &recordingTextOutput{}
	output := NewSynchronizedTextOutput(context.Background(), audio, next)
	output.SetAttached(true)
	output.OnAttached()
	t.Cleanup(output.Close)

	offset := 20 * time.Millisecond
	done := make(chan error, 1)
	go func() {
		done <- output.CaptureText(context.Background(), agents.TimedString{Text: "hello", StartTime: &offset})
	}()
	time.Sleep(5 * time.Millisecond)
	if next.count() != 0 {
		t.Fatal("timed text was forwarded before playback")
	}
	audio.NotifyPlaybackStarted(time.Now())
	select {
	case <-done:
		t.Fatal("timed text was forwarded before its offset")
	case <-time.After(10 * time.Millisecond):
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed text was not forwarded")
	}
	if next.count() != 1 {
		t.Fatalf("text count = %d", next.count())
	}
}

func TestSynchronizedTextOutputDropsInterruptedFutureText(t *testing.T) {
	t.Parallel()
	audio, _ := voice.NewManagedAudioOutput(voice.AudioOutputOptions{SampleRate: 24_000})
	next := &recordingTextOutput{}
	output := NewSynchronizedTextOutput(context.Background(), audio, next)
	output.OnAttached()
	t.Cleanup(output.Close)
	audio.NotifyPlaybackStarted(time.Now())
	offset := time.Second
	done := make(chan error, 1)
	go func() {
		done <- output.CaptureText(context.Background(), agents.TimedString{Text: "future", StartTime: &offset})
	}()
	time.Sleep(5 * time.Millisecond)
	// Seed one captured segment so ManagedAudioOutput accepts the completion.
	frame, _ := agents.NewAudioFrame(make([]int16, 1), 24_000, 1)
	_ = audio.CaptureFrame(context.Background(), frame)
	_ = audio.NotifyPlaybackFinished(voice.PlaybackFinishedEvent{Interrupted: true})
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("interrupted timed text remained blocked")
	}
	if next.count() != 0 {
		t.Fatal("interrupted future text was forwarded")
	}
}
