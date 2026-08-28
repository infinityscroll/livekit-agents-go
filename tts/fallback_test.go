// SPDX-License-Identifier: Apache-2.0

package tts

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	agents "github.com/livekit/agents-go"
)

type adapterTestTTS struct {
	*Base
	mu               sync.Mutex
	chunkFrames      int
	chunkErr         error
	streamErr        error
	streamFailAfter  int
	streamAfterInput bool
	synthesizeCalls  atomic.Int64
	streamCalls      atomic.Int64
	closeCalls       atomic.Int64
}

func newAdapterTestTTS(label, provider, model string, sampleRate int, caps Capabilities) *adapterTestTTS {
	return &adapterTestTTS{
		Base:        NewBase(label, provider, model, sampleRate, 1, caps),
		chunkFrames: 1, streamAfterInput: true,
	}
}

func (t *adapterTestTTS) Synthesize(ctx context.Context, _ string, _ SynthesizeOptions) (ChunkedStream, error) {
	t.synthesizeCalls.Add(1)
	t.mu.Lock()
	frames, terminal := t.chunkFrames, t.chunkErr
	t.mu.Unlock()
	value := &adapterTestChunkedStream{
		BaseChunkedStream: NewBaseChunkedStream(ctx, 8), sampleRate: t.SampleRate(), frames: frames, terminal: terminal,
	}
	go value.run()
	return value, nil
}

func (t *adapterTestTTS) Stream(ctx context.Context, _ StreamOptions) (SynthesizeStream, error) {
	t.streamCalls.Add(1)
	t.mu.Lock()
	terminal, failAfter, afterInput := t.streamErr, t.streamFailAfter, t.streamAfterInput
	t.mu.Unlock()
	value := &adapterTestSynthesizeStream{
		BaseSynthesizeStream: NewBaseSynthesizeStream(ctx, 8), sampleRate: t.SampleRate(),
		terminal: terminal, failAfter: failAfter, afterInput: afterInput,
	}
	go value.run()
	return value, nil
}

func (t *adapterTestTTS) Close(context.Context) error { t.closeCalls.Add(1); return nil }

type adapterTestChunkedStream struct {
	*BaseChunkedStream
	sampleRate int
	frames     int
	terminal   error
}

func (s *adapterTestChunkedStream) run() {
	for i := 0; i < s.frames; i++ {
		frame, _ := agents.NewAudioFrame(make([]int16, 160), s.sampleRate, 1)
		if err := s.Emit(s.Context(), SynthesizedAudio{RequestID: "r", SegmentID: "s", Frame: frame, Final: i == s.frames-1}); err != nil {
			s.Finish(err)
			return
		}
	}
	s.Finish(s.terminal)
}

type adapterTestSynthesizeStream struct {
	*BaseSynthesizeStream
	sampleRate int
	terminal   error
	failAfter  int
	afterInput bool
}

func (s *adapterTestSynthesizeStream) run() {
	if s.terminal != nil && !s.afterInput {
		s.Finish(s.terminal)
		return
	}
	emitted := 0
	for {
		input, err := s.Inputs().Recv(s.Context())
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			s.Finish(err)
			return
		}
		if input.Flush || input.Text == "" {
			continue
		}
		frame, _ := agents.NewAudioFrame(make([]int16, 160), s.sampleRate, 1)
		if err := s.Emit(s.Context(), SynthesizedAudio{RequestID: "r", SegmentID: "s", Frame: frame}); err != nil {
			s.Finish(err)
			return
		}
		emitted++
		if s.terminal != nil && s.failAfter > 0 && emitted >= s.failAfter {
			s.Finish(s.terminal)
			return
		}
	}
	s.Finish(s.terminal)
}

type audioReader interface {
	Recv(context.Context) (SynthesizedAudio, error)
}

func collectAudio(t *testing.T, reader audioReader) ([]SynthesizedAudio, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var result []SynthesizedAudio
	for {
		value, err := reader.Recv(ctx)
		if errors.Is(err, io.EOF) {
			return result, nil
		}
		if err != nil {
			return result, err
		}
		result = append(result, value)
	}
}

func TestFallbackValidationDefaultsCapabilitiesAndAttribution(t *testing.T) {
	if _, err := NewFallbackAdapter(FallbackOptions{}); err == nil {
		t.Fatal("accepted an empty provider list")
	}
	a := newAdapterTestTTS("a", "pa", "ma", 22050, Capabilities{Streaming: true, AlignedTranscript: true})
	b := newAdapterTestTTS("b", "pb", "mb", 24000, Capabilities{})
	adapter, err := NewFallbackAdapter(FallbackOptions{TTSInstances: []TTS{a, b}, KeepProvidersOpen: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = adapter.Close(context.Background()) })
	if adapter.MaxRetriesPerTTS() != 2 || adapter.RecoveryDelay() != time.Second || adapter.RetryOnChunkSent() {
		t.Fatalf("unexpected defaults: %d %v %v", adapter.MaxRetriesPerTTS(), adapter.RecoveryDelay(), adapter.RetryOnChunkSent())
	}
	if adapter.SampleRate() != 24000 || adapter.Channels() != 1 {
		t.Fatalf("format = %d/%d", adapter.SampleRate(), adapter.Channels())
	}
	caps := adapter.Capabilities()
	if !caps.Streaming || caps.AlignedTranscript {
		t.Fatalf("capabilities = %+v", caps)
	}
	if adapter.Provider() != "livekit" || adapter.Model() != "FallbackAdapter" {
		t.Fatalf("initial attribution = %s/%s", adapter.Provider(), adapter.Model())
	}
}

func TestFallbackChunkedFailoverAndResampling(t *testing.T) {
	primary := newAdapterTestTTS("primary", "one", "m1", 22050, Capabilities{Streaming: true})
	primary.chunkFrames = 0
	primary.chunkErr = errors.New("down")
	secondary := newAdapterTestTTS("secondary", "two", "m2", 24000, Capabilities{Streaming: true})
	adapter, err := NewFallbackAdapter(FallbackOptions{
		TTSs: []TTS{primary, secondary}, MaxRetriesPerTTS: -1,
		RecoveryDelay: time.Hour, KeepProvidersOpen: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = adapter.Close(context.Background()) })
	var changes []AvailabilityChangedEvent
	adapter.OnAvailabilityChanged(func(event AvailabilityChangedEvent) { changes = append(changes, event) })
	value, err := adapter.Synthesize(context.Background(), "hello", SynthesizeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	audio, err := collectAudio(t, value)
	if err != nil {
		t.Fatal(err)
	}
	if len(audio) == 0 || audio[0].Frame.SampleRate != 24000 {
		t.Fatalf("audio = %+v", audio)
	}
	if len(changes) == 0 || changes[0].TTS != primary || changes[0].Available {
		t.Fatalf("availability changes = %+v", changes)
	}
	if adapter.Provider() != "two" || adapter.Model() != "m2" {
		t.Fatalf("attribution = %s/%s", adapter.Provider(), adapter.Model())
	}
}

func TestFallbackChunkedDoesNotDuplicateAfterVisibleAudio(t *testing.T) {
	primary := newAdapterTestTTS("primary", "one", "m1", 24000, Capabilities{Streaming: true})
	primary.chunkErr = errors.New("failed after audio")
	secondary := newAdapterTestTTS("secondary", "two", "m2", 24000, Capabilities{Streaming: true})
	adapter, err := NewFallbackAdapter(FallbackOptions{
		TTSs: []TTS{primary, secondary}, MaxRetriesPerTTS: -1,
		RecoveryDelay: time.Hour, KeepProvidersOpen: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = adapter.Close(context.Background()) })
	value, _ := adapter.Synthesize(context.Background(), "hello", SynthesizeOptions{})
	audio, terminal := collectAudio(t, value)
	if terminal == nil || len(audio) != 1 {
		t.Fatalf("audio=%d terminal=%v", len(audio), terminal)
	}
	if secondary.synthesizeCalls.Load() != 0 {
		t.Fatalf("secondary was called %d times", secondary.synthesizeCalls.Load())
	}
}

func TestFallbackChunkedRetryOnChunkSentIsExplicit(t *testing.T) {
	primary := newAdapterTestTTS("primary", "one", "m1", 24000, Capabilities{Streaming: true})
	primary.chunkErr = errors.New("failed after audio")
	secondary := newAdapterTestTTS("secondary", "two", "m2", 24000, Capabilities{Streaming: true})
	adapter, err := NewFallbackAdapter(FallbackOptions{
		TTSs: []TTS{primary, secondary}, MaxRetriesPerTTS: -1,
		RetryOnChunkSent: true, RecoveryDelay: time.Hour, KeepProvidersOpen: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = adapter.Close(context.Background()) })
	value, _ := adapter.Synthesize(context.Background(), "hello", SynthesizeOptions{})
	audio, terminal := collectAudio(t, value)
	if terminal != nil || len(audio) != 2 {
		t.Fatalf("audio=%d terminal=%v", len(audio), terminal)
	}
}

func TestFallbackRecoveryRestoresProviderWithoutChangingAttribution(t *testing.T) {
	primary := newAdapterTestTTS("primary", "one", "m1", 24000, Capabilities{Streaming: true})
	primary.chunkFrames = 0
	primary.chunkErr = errors.New("down")
	secondary := newAdapterTestTTS("secondary", "two", "m2", 24000, Capabilities{Streaming: true})
	adapter, err := NewFallbackAdapter(FallbackOptions{
		TTSs: []TTS{primary, secondary}, MaxRetriesPerTTS: -1,
		RecoveryDelay: 5 * time.Millisecond, KeepProvidersOpen: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = adapter.Close(context.Background()) })
	recovered := make(chan struct{}, 1)
	adapter.OnAvailabilityChanged(func(event AvailabilityChangedEvent) {
		if event.TTS == primary && event.Available {
			select {
			case recovered <- struct{}{}:
			default:
			}
		}
	})
	value, _ := adapter.Synthesize(context.Background(), "hello", SynthesizeOptions{})
	if _, err := collectAudio(t, value); err != nil {
		t.Fatal(err)
	}
	primary.mu.Lock()
	primary.chunkErr = nil
	primary.chunkFrames = 1
	primary.mu.Unlock()
	select {
	case <-recovered:
	case <-time.After(time.Second):
		t.Fatal("primary did not recover")
	}
	if adapter.Provider() != "two" || adapter.Model() != "m2" {
		t.Fatalf("recovery probe changed attribution to %s/%s", adapter.Provider(), adapter.Model())
	}
}

func TestFallbackStreamingFailoverReplaysInputAndEOF(t *testing.T) {
	primary := newAdapterTestTTS("primary", "one", "m1", 22050, Capabilities{Streaming: true})
	primary.streamErr = errors.New("immediate failure")
	primary.streamAfterInput = false
	secondary := newAdapterTestTTS("secondary", "two", "m2", 24000, Capabilities{Streaming: true})
	adapter, err := NewFallbackAdapter(FallbackOptions{
		TTSs: []TTS{primary, secondary}, MaxRetriesPerTTS: -1,
		RecoveryDelay: time.Hour, KeepProvidersOpen: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = adapter.Close(context.Background()) })
	value, _ := adapter.Stream(context.Background(), StreamOptions{})
	if err := value.PushText(context.Background(), "hello"); err != nil {
		t.Fatal(err)
	}
	_ = value.EndInput()
	audio, terminal := collectAudio(t, value)
	if terminal != nil || len(audio) != 1 || audio[0].Frame.SampleRate != 24000 {
		t.Fatalf("audio=%+v terminal=%v", audio, terminal)
	}
	if secondary.streamCalls.Load() != 1 {
		t.Fatalf("secondary streams = %d", secondary.streamCalls.Load())
	}
}

func TestFallbackStreamingDoesNotRetryAfterVisibleAudio(t *testing.T) {
	primary := newAdapterTestTTS("primary", "one", "m1", 24000, Capabilities{Streaming: true})
	primary.streamErr = errors.New("failed after audio")
	primary.streamFailAfter = 1
	primary.chunkErr = primary.streamErr
	secondary := newAdapterTestTTS("secondary", "two", "m2", 24000, Capabilities{Streaming: true})
	adapter, err := NewFallbackAdapter(FallbackOptions{
		TTSs: []TTS{primary, secondary}, MaxRetriesPerTTS: -1,
		RecoveryDelay: time.Hour, KeepProvidersOpen: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = adapter.Close(context.Background()) })
	value, _ := adapter.Stream(context.Background(), StreamOptions{})
	_ = value.PushText(context.Background(), "hello")
	_ = value.EndInput()
	audio, terminal := collectAudio(t, value)
	if terminal == nil || len(audio) != 1 {
		t.Fatalf("audio=%d terminal=%v", len(audio), terminal)
	}
	if secondary.streamCalls.Load() != 0 {
		t.Fatalf("secondary was called %d times", secondary.streamCalls.Load())
	}
}

func TestFallbackCloseConcurrentOwnsProviders(t *testing.T) {
	provider := newAdapterTestTTS("only", "p", "m", 24000, Capabilities{Streaming: true})
	adapter, err := NewFallbackAdapter(FallbackOptions{TTSs: []TTS{provider}})
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
}

func FuzzPCM16Resampler(f *testing.F) {
	f.Add(uint16(160), uint16(16000), uint16(24000), byte(1))
	f.Add(uint16(1), uint16(22050), uint16(24000), byte(2))
	f.Fuzz(func(t *testing.T, samples uint16, inRateRaw, outRateRaw uint16, channelRaw byte) {
		if samples > 4096 {
			samples %= 4096
		}
		inRate := 8000 + int(inRateRaw%40001)
		outRate := 8000 + int(outRateRaw%40001)
		channels := 1 + int(channelRaw%2)
		if inRate == outRate {
			outRate++
		}
		r := newPCM16Resampler(inRate, outRate, channels)
		frame, _ := agents.NewAudioFrame(make([]int16, int(samples)*channels), inRate, channels)
		if _, _, err := r.push(frame); err != nil {
			t.Fatal(err)
		}
		flushed, ok := r.flush()
		if ok && (flushed.SampleRate != outRate || flushed.Channels != channels) {
			t.Fatalf("format = %d/%d", flushed.SampleRate, flushed.Channels)
		}
	})
}

func BenchmarkPCM16Resampler48kTo24k(b *testing.B) {
	frame, _ := agents.NewAudioFrame(make([]int16, 480), 48000, 1)
	b.ReportAllocs()
	for b.Loop() {
		r := newPCM16Resampler(48000, 24000, 1)
		if _, _, err := r.push(frame); err != nil {
			b.Fatal(err)
		}
		_, _ = r.flush()
	}
}
