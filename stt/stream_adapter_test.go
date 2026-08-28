// SPDX-License-Identifier: Apache-2.0

package stt

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/metrics"
	"github.com/livekit/agents-go/vad"
)

type adapterTestVAD struct {
	*vad.Base
	closeCalls atomic.Int64
}

func newAdapterTestVAD() *adapterTestVAD {
	return &adapterTestVAD{Base: vad.NewBase("test-vad", vad.Capabilities{})}
}

func (v *adapterTestVAD) Stream(ctx context.Context) (vad.VADStream, error) {
	value := &adapterTestVADStream{BaseStream: vad.NewBaseStream(ctx, 8)}
	go value.run()
	return value, nil
}

func (v *adapterTestVAD) Close(context.Context) error { v.closeCalls.Add(1); return nil }

type adapterTestVADStream struct{ *vad.BaseStream }

func (s *adapterTestVADStream) run() {
	var frames []agents.AudioFrame
	started := false
	for {
		input, err := s.Inputs().Recv(s.Context())
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			s.Finish(err)
			return
		}
		if input.Frame != nil {
			frames = append(frames, *input.Frame)
			if !started {
				started = true
				_ = s.Emit(s.Context(), vad.Event{Type: vad.StartOfSpeech})
			}
		}
	}
	if started {
		_ = s.Emit(s.Context(), vad.Event{Type: vad.EndOfSpeech, Frames: frames})
	}
	s.Finish(nil)
}

func TestStreamAdapterOrdersVADEventsAndTranscript(t *testing.T) {
	wrapped := newAdapterTestSTT("batch", "provider", "model", Capabilities{Keyterms: true})
	wrapped.recognizeText = "recognized"
	detector := newAdapterTestVAD()
	adapter, err := NewStreamAdapter(wrapped, detector)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = adapter.Close(context.Background()) })
	if adapter.Provider() != "provider" || adapter.Model() != "model" {
		t.Fatal("wrapped attribution was not preserved")
	}
	caps := adapter.Capabilities()
	if !caps.Streaming || caps.InterimResults || caps.Diarization || !caps.Keyterms {
		t.Fatalf("capabilities = %+v", caps)
	}
	value, err := adapter.Stream(context.Background(), StreamOptions{Language: "en-US"})
	if err != nil {
		t.Fatal(err)
	}
	frame, _ := agents.NewAudioFrame(make([]int16, 160), 16000, 1)
	if err := value.Push(context.Background(), frame); err != nil {
		t.Fatal(err)
	}
	_ = value.EndInput()
	events, err := collectSpeech(t, value)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 || events[0].Type != StartOfSpeech || events[1].Type != EndOfSpeech ||
		events[2].Type != FinalTranscript || events[2].Alternatives[0].Text != "recognized" {
		t.Fatalf("events = %+v", events)
	}
}

func TestStreamAdapterDetachesForwarders(t *testing.T) {
	wrapped := newAdapterTestSTT("batch", "provider", "model", Capabilities{})
	adapter, err := NewStreamAdapter(wrapped, newAdapterTestVAD())
	if err != nil {
		t.Fatal(err)
	}
	var received atomic.Int64
	adapter.OnMetrics(func(metrics.STT) { received.Add(1) })
	wrapped.EmitMetrics(metrics.STT{Label: "before"})
	if err := adapter.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	wrapped.EmitMetrics(metrics.STT{Label: "after"})
	if received.Load() != 1 {
		t.Fatalf("received %d forwarded metrics", received.Load())
	}
}

func TestStreamAdapterRecognizeFailureTerminatesStream(t *testing.T) {
	wrapped := newAdapterTestSTT("batch", "provider", "model", Capabilities{})
	wrapped.recognizeErr = errors.New("recognize failed")
	adapter, err := NewStreamAdapter(wrapped, newAdapterTestVAD())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = adapter.Close(context.Background()) })
	value, _ := adapter.Stream(context.Background(), StreamOptions{})
	frame, _ := agents.NewAudioFrame(make([]int16, 160), 16000, 1)
	_ = value.Push(context.Background(), frame)
	_ = value.EndInput()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for {
		_, err = value.Recv(ctx)
		if err != nil {
			break
		}
	}
	if err == nil || errors.Is(err, io.EOF) {
		t.Fatalf("terminal error = %v", err)
	}
}
