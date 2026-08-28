// SPDX-License-Identifier: Apache-2.0

package transcription

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/tokenize"
	livekit "github.com/livekit/protocol/livekit"
)

func TestTextAudioSynchronizerPacesAndFinalizes(t *testing.T) {
	t.Parallel()
	synchronizer, err := NewTextAudioSynchronizer(context.Background(), TextSyncOptions{
		Language: "en-US", Speed: 1_000_000, NewSentenceDelay: time.Nanosecond,
		SentenceTokenizer: tokenize.NewSentenceTokenizer(tokenize.SentenceOptions{
			MinSentenceLength: 1, StreamContextLength: 1,
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	events := make(chan *livekit.TranscriptionSegment, 8)
	unsubscribe := synchronizer.OnTextUpdated(func(segment *livekit.TranscriptionSegment) {
		copy := protoSegmentCopy(segment)
		events <- copy
	})
	defer unsubscribe()
	frame, err := agents.NewAudioFrame(make([]int16, 2400), 24_000, 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := synchronizer.PushAudio(ctx, frame); err != nil {
		t.Fatal(err)
	}
	if err := synchronizer.PushText(ctx, "Hello world."); err != nil {
		t.Fatal(err)
	}
	if err := synchronizer.MarkAudioSegmentEnd(ctx); err != nil {
		t.Fatal(err)
	}
	if err := synchronizer.MarkTextSegmentEnd(ctx); err != nil {
		t.Fatal(err)
	}
	if err := synchronizer.SegmentPlayoutStarted(); err != nil {
		t.Fatal(err)
	}
	var final *livekit.TranscriptionSegment
	for final == nil {
		select {
		case event := <-events:
			if event.Id == "" || event.Language != "en-US" {
				t.Fatalf("event = %#v", event)
			}
			if event.Final {
				final = event
			}
		case <-ctx.Done():
			t.Fatal("timed out waiting for final transcript")
		}
	}
	if final.Text != "Hello world." {
		t.Fatalf("final text = %q", final.Text)
	}
	if err := synchronizer.SegmentPlayoutFinished(); err != nil {
		t.Fatal(err)
	}
	if err := synchronizer.Close(ctx, false); err != nil {
		t.Fatal(err)
	}
	if got := synchronizer.PlayedText(); got != " Hello world." {
		t.Fatalf("played text = %q", got)
	}
}

func TestTextAudioSynchronizerInterruptBeforePlayoutSuppressesText(t *testing.T) {
	t.Parallel()
	synchronizer, err := NewTextAudioSynchronizer(context.Background(), DefaultTextSyncOptions())
	if err != nil {
		t.Fatal(err)
	}
	var count int
	var mu sync.Mutex
	synchronizer.OnTextUpdated(func(*livekit.TranscriptionSegment) {
		mu.Lock()
		count++
		mu.Unlock()
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := synchronizer.PushText(ctx, "This should not be emitted."); err != nil {
		t.Fatal(err)
	}
	if err := synchronizer.MarkTextSegmentEnd(ctx); err != nil {
		t.Fatal(err)
	}
	if err := synchronizer.MarkAudioSegmentEnd(ctx); err != nil {
		t.Fatal(err)
	}
	if err := synchronizer.Close(ctx, true); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if count != 0 {
		t.Fatalf("emitted %d transcript events after interruption", count)
	}
	if err := synchronizer.PushText(ctx, "late"); !errors.Is(err, ErrClosed) {
		t.Fatalf("late PushText error = %v", err)
	}
}

func TestTextAudioSynchronizerBoundsUnpairedSegments(t *testing.T) {
	t.Parallel()
	options := DefaultTextSyncOptions()
	options.SegmentQueueCapacity = 1
	synchronizer, err := NewTextAudioSynchronizer(context.Background(), options)
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 2; index++ {
		if err := synchronizer.PushText(context.Background(), "queued"); err != nil {
			t.Fatal(err)
		}
		if err := synchronizer.MarkTextSegmentEnd(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := synchronizer.PushText(ctx, "must block"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("third unpaired segment error = %v", err)
	}
	closeCtx, closeCancel := context.WithTimeout(context.Background(), time.Second)
	defer closeCancel()
	if err := synchronizer.Close(closeCtx, true); err != nil {
		t.Fatal(err)
	}
}

func TestTextAudioSynchronizerValidatesOptions(t *testing.T) {
	t.Parallel()
	if _, err := NewTextAudioSynchronizer(context.Background(), TextSyncOptions{Speed: -1}); err == nil {
		t.Fatal("negative speed accepted")
	}
	if _, err := NewTextAudioSynchronizer(context.Background(), TextSyncOptions{NewSentenceDelay: -1}); err == nil {
		t.Fatal("negative sentence delay accepted")
	}
}

func protoSegmentCopy(input *livekit.TranscriptionSegment) *livekit.TranscriptionSegment {
	if input == nil {
		return nil
	}
	return &livekit.TranscriptionSegment{
		Id: input.Id, Text: input.Text, StartTime: input.StartTime,
		EndTime: input.EndTime, Final: input.Final, Language: input.Language,
	}
}
