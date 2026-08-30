// SPDX-License-Identifier: Apache-2.0

package tts

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/infinityscroll/livekit-agents-go/metrics"
	"github.com/infinityscroll/livekit-agents-go/tokenize"
)

func TestStreamAdapterSynthesizesSentencesInOrder(t *testing.T) {
	wrapped := newAdapterTestTTS("batch", "provider", "model", 24000, Capabilities{})
	adapter, err := NewStreamAdapter(wrapped, tokenize.NewSentenceTokenizer(tokenize.SentenceOptions{MinSentenceLength: 1}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = adapter.Close(context.Background()) })
	value, err := adapter.Stream(context.Background(), StreamOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := value.PushText(context.Background(), "First. Second."); err != nil {
		t.Fatal(err)
	}
	_ = value.EndInput()
	audio, terminal := collectAudio(t, value)
	if terminal != nil {
		t.Fatal(terminal)
	}
	if len(audio) != 2 || len(audio[0].TimedTranscripts) != 1 || len(audio[1].TimedTranscripts) != 1 {
		t.Fatalf("audio = %+v", audio)
	}
	if audio[0].TimedTranscripts[0].Text != "First." || audio[1].TimedTranscripts[0].Text != "Second." {
		t.Fatalf("transcripts = %+v / %+v", audio[0].TimedTranscripts, audio[1].TimedTranscripts)
	}
	firstStart := audio[0].TimedTranscripts[0].StartTime
	secondStart := audio[1].TimedTranscripts[0].StartTime
	if firstStart == nil || *firstStart != 0 || secondStart == nil || *secondStart != audio[0].Frame.Duration() {
		t.Fatalf("starts = %v / %v", firstStart, secondStart)
	}
}

func TestStreamAdapterForwardsAndDetachesEvents(t *testing.T) {
	wrapped := newAdapterTestTTS("batch", "provider", "model", 24000, Capabilities{})
	adapter, err := NewStreamAdapter(wrapped, nil)
	if err != nil {
		t.Fatal(err)
	}
	var metricsCount atomic.Int64
	adapter.OnMetrics(func(metrics.TTS) { metricsCount.Add(1) })
	wrapped.EmitMetrics(metrics.TTS{Label: "before"})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := adapter.Close(ctx); err != nil {
		t.Fatal(err)
	}
	wrapped.EmitMetrics(metrics.TTS{Label: "after"})
	if metricsCount.Load() != 1 {
		t.Fatalf("metrics = %d", metricsCount.Load())
	}
}
