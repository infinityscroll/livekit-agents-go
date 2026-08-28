// SPDX-License-Identifier: Apache-2.0

package inference

import (
	"context"
	"errors"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	agents "github.com/livekit/agents-go"
	vadpkg "github.com/livekit/agents-go/vad"
)

type scriptedVADPredictor struct {
	mu          sync.Mutex
	probability []float64
	position    int
	fallback    float64
	window      int
	resets      atomic.Int32
	closes      atomic.Int32
}

func (p *scriptedVADPredictor) WindowSamples() int {
	if p.window > 0 {
		return p.window
	}
	return defaultVADWindowSamples
}

func (p *scriptedVADPredictor) Predict(ctx context.Context, window []int16) (float64, error) {
	select {
	case <-ctx.Done():
		return 0, context.Cause(ctx)
	default:
	}
	if len(window) != p.WindowSamples() {
		return 0, errors.New("unexpected VAD window length")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.position < len(p.probability) {
		value := p.probability[p.position]
		p.position++
		return value, nil
	}
	return p.fallback, nil
}

func (p *scriptedVADPredictor) Reset() error { p.resets.Add(1); return nil }
func (p *scriptedVADPredictor) Close() error { p.closes.Add(1); return nil }

func collectVADEvents(t *testing.T, stream *InferenceVADStream) []vadpkg.Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var events []vadpkg.Event
	for {
		event, err := stream.Recv(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, event)
	}
	if err := stream.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	return events
}

func TestInferenceVADSpeechFSMAndBuffers(t *testing.T) {
	predictor := &scriptedVADPredictor{probability: []float64{.8, .9, .1, .1}}
	var factoryCalls atomic.Int32
	detector, err := NewVAD(VADOptions{
		MinimumSpeechDuration:  64 * time.Millisecond,
		MinimumSilenceDuration: 64 * time.Millisecond,
		PrefixPaddingDuration:  32 * time.Millisecond,
		MaximumBufferedSpeech:  time.Second,
		PredictorFactory: func(context.Context) (VADPredictor, error) {
			factoryCalls.Add(1)
			return predictor, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	streamValue, err := detector.Stream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	stream := streamValue.(*InferenceVADStream)
	frame, _ := agents.NewAudioFrame(make([]int16, 4*defaultVADWindowSamples), 16_000, 1)
	if err := stream.Push(context.Background(), frame); err != nil {
		t.Fatal(err)
	}
	if err := stream.EndInput(); err != nil {
		t.Fatal(err)
	}
	events := collectVADEvents(t, stream)
	wantTypes := []vadpkg.EventType{
		vadpkg.InferenceDone, vadpkg.InferenceDone, vadpkg.StartOfSpeech,
		vadpkg.InferenceDone, vadpkg.InferenceDone, vadpkg.EndOfSpeech,
	}
	if len(events) != len(wantTypes) {
		t.Fatalf("VAD events = %#v", events)
	}
	for i, want := range wantTypes {
		if events[i].Type != want {
			t.Fatalf("event %d type = %v, want %v", i, events[i].Type, want)
		}
	}
	start, end := events[2], events[5]
	if start.SamplesIndex != 1_024 || start.SpeechDuration != 64*time.Millisecond || !start.Speaking || len(start.Frames) != 1 || len(start.Frames[0].Data) != 1_024 {
		t.Fatalf("start event = %+v", start)
	}
	if end.SamplesIndex != 2_048 || end.SpeechDuration != 64*time.Millisecond || end.SilenceDuration != 64*time.Millisecond || end.Speaking || len(end.Frames) != 1 || len(end.Frames[0].Data) != 2_048 {
		t.Fatalf("end event = %+v", end)
	}
	if factoryCalls.Load() != 1 || predictor.closes.Load() != 1 {
		t.Fatalf("predictor lifecycle: factory=%d closes=%d", factoryCalls.Load(), predictor.closes.Load())
	}
	if err := detector.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestInferenceVADFlushHardResetsState(t *testing.T) {
	predictor := &scriptedVADPredictor{fallback: .9}
	detector, err := NewVAD(VADOptions{
		MinimumSpeechDuration:  64 * time.Millisecond,
		MinimumSilenceDuration: 64 * time.Millisecond,
		PredictorFactory:       func(context.Context) (VADPredictor, error) { return predictor, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	streamValue, _ := detector.Stream(context.Background())
	stream := streamValue.(*InferenceVADStream)
	one, _ := agents.NewAudioFrame(make([]int16, 512), 16_000, 1)
	two, _ := agents.NewAudioFrame(make([]int16, 1_024), 16_000, 1)
	if err := stream.Push(context.Background(), one); err != nil {
		t.Fatal(err)
	}
	if err := stream.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := stream.Push(context.Background(), two); err != nil {
		t.Fatal(err)
	}
	_ = stream.EndInput()
	events := collectVADEvents(t, stream)
	var starts []vadpkg.Event
	for _, event := range events {
		if event.Type == vadpkg.StartOfSpeech {
			starts = append(starts, event)
		}
	}
	if len(starts) != 1 || starts[0].SamplesIndex != 1_024 {
		t.Fatalf("post-flush starts = %+v", starts)
	}
	if predictor.resets.Load() != 1 {
		t.Fatalf("predictor resets = %d", predictor.resets.Load())
	}
	_ = detector.Close(context.Background())
}

func TestInferenceVADUnavailableIsLazyNoOp(t *testing.T) {
	var calls atomic.Int32
	detector, err := NewVAD(VADOptions{PredictorFactory: func(context.Context) (VADPredictor, error) {
		calls.Add(1)
		return nil, ErrLocalInferenceUnavailable
	}})
	if err != nil {
		t.Fatal(err)
	}
	streamValue, _ := detector.Stream(context.Background())
	stream := streamValue.(*InferenceVADStream)
	frame, _ := agents.NewAudioFrame(make([]int16, 512), 16_000, 1)
	if err := stream.Push(context.Background(), frame); err != nil {
		t.Fatal(err)
	}
	if err := stream.Push(context.Background(), frame); err != nil {
		t.Fatal(err)
	}
	_ = stream.EndInput()
	events := collectVADEvents(t, stream)
	if calls.Load() != 1 {
		t.Fatalf("unavailable factory calls = %d", calls.Load())
	}
	if len(events) != 2 || events[0].Type != vadpkg.InferenceDone || events[1].Type != vadpkg.InferenceDone || events[0].Probability != 0 || events[1].Probability != 0 {
		t.Fatalf("no-op events = %+v", events)
	}
	_ = detector.Close(context.Background())
}

func TestInferenceVADResamplesAndDownmixes(t *testing.T) {
	predictor := &scriptedVADPredictor{fallback: .9}
	detector, err := NewVAD(VADOptions{
		MinimumSpeechDuration: 32 * time.Millisecond,
		PredictorFactory:      func(context.Context) (VADPredictor, error) { return predictor, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	streamValue, _ := detector.Stream(context.Background())
	stream := streamValue.(*InferenceVADStream)
	data := make([]int16, 1_536*2)
	for i := 0; i < len(data); i += 2 {
		data[i], data[i+1] = 1_000, -500
	}
	frame, _ := agents.NewAudioFrame(data, 48_000, 2)
	if err := stream.Push(context.Background(), frame); err != nil {
		t.Fatal(err)
	}
	_ = stream.EndInput()
	events := collectVADEvents(t, stream)
	if len(events) != 2 || events[0].Type != vadpkg.InferenceDone || events[1].Type != vadpkg.StartOfSpeech {
		t.Fatalf("resampled VAD events = %+v", events)
	}
	window := events[0].Frames[0]
	if window.SampleRate != 48_000 || window.Channels != 1 || len(window.Data) != 1_536 || window.Data[0] != 250 {
		t.Fatalf("resampled input window = %+v first=%d", window, window.Data[0])
	}
	_ = detector.Close(context.Background())
}

func FuzzLinearMonoResampler(f *testing.F) {
	f.Add(uint8(5), []byte{0, 1, 2, 3, 4, 5})
	f.Add(uint8(0), []byte{})
	rates := [...]int{8_000, 16_000, 22_050, 24_000, 44_100, 48_000}
	f.Fuzz(func(t *testing.T, rateIndex uint8, raw []byte) {
		if len(raw) > 8_192 {
			raw = raw[:8_192]
		}
		samples := make([]int16, len(raw)/2)
		for i := range samples {
			samples[i] = int16(uint16(raw[i*2]) | uint16(raw[i*2+1])<<8)
		}
		rate := rates[int(rateIndex)%len(rates)]
		resampler := newLinearMonoResampler(rate, InferenceVADSampleRate)
		var output []int16
		midpoint := len(samples) / 2
		output = append(output, resampler.push(samples[:midpoint], false)...)
		output = append(output, resampler.push(samples[midpoint:], false)...)
		output = append(output, resampler.push(nil, true)...)
		upper := int(math.Ceil(float64(len(samples))*InferenceVADSampleRate/float64(rate))) + 2
		if len(output) > upper {
			t.Fatalf("resampler output %d exceeds bound %d", len(output), upper)
		}
	})
}

func BenchmarkLinearMonoResampler48kTo16k(b *testing.B) {
	input := make([]int16, 4_800)
	b.ReportAllocs()
	b.SetBytes(int64(len(input) * 2))
	for b.Loop() {
		resampler := newLinearMonoResampler(48_000, 16_000)
		_ = resampler.push(input, true)
	}
}
