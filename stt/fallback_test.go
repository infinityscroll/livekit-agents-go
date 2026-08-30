// SPDX-License-Identifier: Apache-2.0

package stt

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/metrics"
)

type adapterTestSTT struct {
	*Base
	mu               sync.Mutex
	recognizeErr     error
	recognizeText    string
	streamErr        error
	streamText       string
	streamAfterInput bool
	recognizeCalls   atomic.Int64
	streamCalls      atomic.Int64
	closeCalls       atomic.Int64
}

func newAdapterTestSTT(label, provider, model string, caps Capabilities) *adapterTestSTT {
	return &adapterTestSTT{Base: NewBase(label, provider, model, caps), streamAfterInput: true}
}

func (s *adapterTestSTT) Recognize(_ context.Context, _ []agents.AudioFrame, _ RecognizeOptions) (SpeechEvent, error) {
	s.recognizeCalls.Add(1)
	s.mu.Lock()
	err, text := s.recognizeErr, s.recognizeText
	s.mu.Unlock()
	if err != nil {
		return SpeechEvent{}, err
	}
	event := SpeechEvent{Type: FinalTranscript, Alternatives: []SpeechData{{Text: text}}}
	s.EmitMetrics(metrics.STT{Label: s.Label(), RequestID: "test"})
	return event, nil
}

func (s *adapterTestSTT) Stream(ctx context.Context, _ StreamOptions) (SpeechStream, error) {
	s.streamCalls.Add(1)
	s.mu.Lock()
	err, text, afterInput := s.streamErr, s.streamText, s.streamAfterInput
	s.mu.Unlock()
	value := &adapterTestSpeechStream{BaseStream: NewBaseStream(ctx, 8), terminal: err, text: text, afterInput: afterInput}
	go value.run()
	return value, nil
}

func (s *adapterTestSTT) Close(context.Context) error { s.closeCalls.Add(1); return nil }

type adapterTestSpeechStream struct {
	*BaseStream
	terminal   error
	text       string
	afterInput bool
}

func (s *adapterTestSpeechStream) run() {
	if s.terminal != nil && !s.afterInput {
		s.Finish(s.terminal)
		return
	}
	for {
		_, err := s.Inputs().Recv(s.Context())
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			s.Finish(err)
			return
		}
	}
	if s.terminal != nil {
		s.Finish(s.terminal)
		return
	}
	if s.text != "" {
		_ = s.Emit(s.Context(), SpeechEvent{Type: FinalTranscript, Alternatives: []SpeechData{{Text: s.text}}})
	}
	s.Finish(nil)
}

func collectSpeech(t *testing.T, value SpeechStream) ([]SpeechEvent, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var result []SpeechEvent
	for {
		event, err := value.Recv(ctx)
		if errors.Is(err, io.EOF) {
			return result, nil
		}
		if err != nil {
			return result, err
		}
		result = append(result, event)
	}
}

func TestFallbackValidationDefaultsAndCapabilities(t *testing.T) {
	if _, err := NewFallbackAdapter(FallbackOptions{}); err == nil {
		t.Fatal("accepted an empty provider list")
	}
	nonStreaming := newAdapterTestSTT("batch", "p", "m", Capabilities{})
	if _, err := NewFallbackAdapter(FallbackOptions{STTs: []STT{nonStreaming}}); err == nil {
		t.Fatal("accepted a non-streaming STT without VAD")
	}
	a := newAdapterTestSTT("a", "pa", "ma", Capabilities{
		Streaming: true, InterimResults: true, AlignedTranscript: AlignedTranscriptWord,
		Diarization: true, Keyterms: true,
	})
	b := newAdapterTestSTT("b", "pb", "mb", Capabilities{
		Streaming: true, AlignedTranscript: AlignedTranscriptChunk, ChatContext: true,
	})
	adapter, err := NewFallbackAdapter(FallbackOptions{STTInstances: []STT{a, b}, KeepProvidersOpen: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = adapter.Close(context.Background()) })
	if adapter.AttemptTimeout() != 10*time.Second || adapter.MaxRetriesPerSTT() != 1 || adapter.RetryInterval() != 5*time.Second {
		t.Fatalf("unexpected defaults: %v %d %v", adapter.AttemptTimeout(), adapter.MaxRetriesPerSTT(), adapter.RetryInterval())
	}
	caps := adapter.Capabilities()
	if !caps.Streaming || caps.InterimResults || caps.Diarization || caps.AlignedTranscript != AlignedTranscriptWord || !caps.Keyterms || !caps.ChatContext {
		t.Fatalf("unexpected capabilities: %+v", caps)
	}
	if adapter.Provider() != "livekit" || adapter.Model() != "FallbackAdapter" {
		t.Fatalf("unexpected initial attribution: %s/%s", adapter.Provider(), adapter.Model())
	}
}

func TestFallbackRecognizeFailoverAttributionMetricsAndAvailability(t *testing.T) {
	primary := newAdapterTestSTT("primary", "one", "model-one", Capabilities{Streaming: true})
	primary.recognizeErr = agents.NewAPIConnectionError("down", true, nil)
	secondary := newAdapterTestSTT("secondary", "two", "model-two", Capabilities{Streaming: true})
	secondary.recognizeText = "hello"
	adapter, err := NewFallbackAdapter(FallbackOptions{
		STTs: []STT{primary, secondary}, MaxRetriesPerSTT: -1,
		RetryInterval: time.Hour, KeepProvidersOpen: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = adapter.Close(context.Background()) })
	var changes []AvailabilityChangedEvent
	var metricCount atomic.Int64
	adapter.OnAvailabilityChanged(func(event AvailabilityChangedEvent) { changes = append(changes, event) })
	adapter.OnMetrics(func(metrics.STT) { metricCount.Add(1) })
	event, err := adapter.Recognize(context.Background(), nil, RecognizeOptions{})
	if err != nil || event.Alternatives[0].Text != "hello" {
		t.Fatalf("recognize = %+v, %v", event, err)
	}
	if adapter.Provider() != "two" || adapter.Model() != "model-two" {
		t.Fatalf("attribution = %s/%s", adapter.Provider(), adapter.Model())
	}
	if metricCount.Load() != 1 {
		t.Fatalf("forwarded metrics = %d", metricCount.Load())
	}
	if len(changes) == 0 || changes[0].STT != primary || changes[0].Available {
		t.Fatalf("availability changes = %+v", changes)
	}
}

func TestFallbackStreamingEOFBeforeFailover(t *testing.T) {
	primary := newAdapterTestSTT("primary", "one", "m1", Capabilities{Streaming: true})
	primary.streamErr = errors.New("stream down")
	secondary := newAdapterTestSTT("secondary", "two", "m2", Capabilities{Streaming: true})
	secondary.streamText = "fallback"
	adapter, err := NewFallbackAdapter(FallbackOptions{
		STTs: []STT{primary, secondary}, MaxRetriesPerSTT: -1,
		RetryInterval: time.Hour, KeepProvidersOpen: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = adapter.Close(context.Background()) })
	value, err := adapter.Stream(context.Background(), StreamOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := value.EndInput(); err != nil {
		t.Fatal(err)
	}
	events, err := collectSpeech(t, value)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Alternatives[0].Text != "fallback" {
		t.Fatalf("events = %+v", events)
	}
	if primary.streamCalls.Load() < 1 || secondary.streamCalls.Load() != 1 {
		t.Fatalf("stream calls primary=%d secondary=%d", primary.streamCalls.Load(), secondary.streamCalls.Load())
	}
}

func TestFallbackRecognizeRecoveryDoesNotChangeAttribution(t *testing.T) {
	primary := newAdapterTestSTT("primary", "one", "m1", Capabilities{Streaming: true})
	primary.recognizeErr = errors.New("down")
	secondary := newAdapterTestSTT("secondary", "two", "m2", Capabilities{Streaming: true})
	secondary.recognizeText = "fallback"
	adapter, err := NewFallbackAdapter(FallbackOptions{
		STTs: []STT{primary, secondary}, MaxRetriesPerSTT: -1, KeepProvidersOpen: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = adapter.Close(context.Background()) })
	if _, err := adapter.Recognize(context.Background(), nil, RecognizeOptions{}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for adapter.Status()[0].Recovering && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	primary.mu.Lock()
	primary.recognizeErr = nil
	primary.recognizeText = "recovered"
	primary.mu.Unlock()
	recovered := make(chan struct{}, 1)
	adapter.OnAvailabilityChanged(func(event AvailabilityChangedEvent) {
		if event.STT == primary && event.Available {
			recovered <- struct{}{}
		}
	})
	if _, err := adapter.Recognize(context.Background(), nil, RecognizeOptions{}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-recovered:
	case <-time.After(time.Second):
		t.Fatal("primary did not recover")
	}
	if adapter.Provider() != "two" || adapter.Model() != "m2" {
		t.Fatalf("recovery changed attribution to %s/%s", adapter.Provider(), adapter.Model())
	}
	if event, err := adapter.Recognize(context.Background(), nil, RecognizeOptions{}); err != nil || event.Alternatives[0].Text != "recovered" {
		t.Fatalf("recovered primary = %+v, %v", event, err)
	}
}

func TestFallbackCloseIsConcurrentAndOwnsProviders(t *testing.T) {
	provider := newAdapterTestSTT("only", "p", "m", Capabilities{Streaming: true})
	adapter, err := NewFallbackAdapter(FallbackOptions{STTs: []STT{provider}})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := adapter.Close(context.Background()); err != nil {
				t.Errorf("Close: %v", err)
			}
		}()
	}
	wg.Wait()
	if provider.closeCalls.Load() != 1 {
		t.Fatalf("provider closed %d times", provider.closeCalls.Load())
	}
	var forwarded atomic.Int64
	adapter.OnMetrics(func(metrics.STT) { forwarded.Add(1) })
	provider.EmitMetrics(metrics.STT{Label: "orphan"})
	if forwarded.Load() != 0 {
		t.Fatal("metrics listener remained attached after Close")
	}
}

func BenchmarkFallbackRecognizePrimary(b *testing.B) {
	provider := newAdapterTestSTT("primary", "p", "m", Capabilities{Streaming: true})
	provider.recognizeText = "ok"
	adapter, err := NewFallbackAdapter(FallbackOptions{STTs: []STT{provider}, KeepProvidersOpen: true})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = adapter.Close(context.Background()) })
	b.ReportAllocs()
	for b.Loop() {
		if _, err := adapter.Recognize(context.Background(), nil, RecognizeOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}
