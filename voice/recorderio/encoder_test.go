// SPDX-License-Identifier: Apache-2.0

package recorderio

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
	"time"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/voice"
)

func TestMonoConverterResamplesAndDownmixes(t *testing.T) {
	converter := newMonoConverter(48_000, 10_000)
	frame := audioFrame(100*time.Millisecond, 24_000, 2, 1_000, 3_000)
	output, err := converter.push([]agents.AudioFrame{frame}, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(output) != 4_800 {
		t.Fatalf("resampled samples = %d, want 4800", len(output))
	}
	for index, value := range output {
		if got := floatToPCM16(value); got != 2_000 {
			t.Fatalf("sample %d = %d", index, got)
		}
	}
}

func TestMonoConverterContinuesAfterFractionalRateFlush(t *testing.T) {
	converter := newMonoConverter(48_000, 10_000)
	first, err := converter.push([]agents.AudioFrame{audioFrame(10*time.Millisecond, 44_100, 1, 1_000)}, true)
	if err != nil {
		t.Fatal(err)
	}
	second, err := converter.push([]agents.AudioFrame{audioFrame(10*time.Millisecond, 44_100, 1, 2_000)}, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 480 || len(second) != 480 {
		t.Fatalf("segment lengths = %d, %d; want 480, 480", len(first), len(second))
	}
	if got := floatToPCM16(second[0]); got != 2_000 {
		t.Fatalf("second segment first sample = %d", got)
	}
}

func TestInterleavePrependsSilenceToAlignChunkEnds(t *testing.T) {
	pcm, err := interleavePCM16([]float32{1.0 / 32768}, []float32{2.0 / 32768, 3.0 / 32768})
	if err != nil {
		t.Fatal(err)
	}
	want := []int16{0, 2, 1, 3}
	for index, value := range want {
		if got := int16(binary.LittleEndian.Uint16(pcm[index*2:])); got != value {
			t.Fatalf("PCM[%d] = %d, want %d", index, got, value)
		}
	}
	if floatToPCM16(-0.5/32768) != 0 || floatToPCM16(-1.5/32768) != -1 {
		t.Fatal("PCM conversion does not match JavaScript Math.round negative ties")
	}
}

func TestInputBufferAppliesBackpressureUntilCommit(t *testing.T) {
	frame := audioFrame(10*time.Millisecond, 1_000, 1, 1)
	fixture := newRecorderFixture(t, newTestAudioOutput(1_000), func(options *RecorderOptions) {
		options.SampleRate = 1_000
		options.MaxBufferedBytes = len(frame.Data) * 2
		options.MaxQueuedBytes = len(frame.Data) * 4
	})
	if err := fixture.source.frames.Send(t.Context(), frame); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.input.Recv(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := fixture.source.frames.Send(t.Context(), frame); err != nil {
		t.Fatal(err)
	}
	second := make(chan error, 1)
	go func() {
		_, err := fixture.input.Recv(context.Background())
		second <- err
	}()
	select {
	case err := <-second:
		t.Fatalf("second input bypassed backpressure: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	if err := fixture.output.CaptureFrame(t.Context(), frame); err != nil {
		t.Fatal(err)
	}
	fixture.clock.Advance(10 * time.Millisecond)
	if err := fixture.sink.NotifyPlaybackFinished(voice.PlaybackFinishedEvent{PlaybackPosition: 10 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-second:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("input remained blocked after encoder commit drained its buffer")
	}
	if err := fixture.recorder.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestInputFrameCountBoundsZeroLengthFrames(t *testing.T) {
	fixture := newRecorderFixture(t, newTestAudioOutput(1_000), func(options *RecorderOptions) {
		options.SampleRate = 1_000
		options.MaxBufferedFrames = 1
		options.MaxQueuedFrames = 2
	})
	frame, err := agents.NewAudioFrame(nil, 1_000, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.source.frames.Send(t.Context(), frame); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.input.Recv(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := fixture.source.frames.Send(t.Context(), frame); err != nil {
		t.Fatal(err)
	}
	received := make(chan error, 1)
	go func() {
		_, err := fixture.input.Recv(context.Background())
		received <- err
	}()
	select {
	case err := <-received:
		t.Fatalf("second zero-length frame bypassed frame backpressure: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	frames, err := fixture.input.takeBuffer(time.Time{})
	if err != nil || len(frames) != 1 {
		t.Fatalf("takeBuffer = %d frames, %v", len(frames), err)
	}
	select {
	case err := <-received:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("frame-count backpressure did not release")
	}
	if err := fixture.recorder.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestInputPaddingRejectsHugeSilenceBeforeAllocation(t *testing.T) {
	recorder, err := NewRecorderIO(RecorderOptions{
		MaxBufferedBytes: 4, MaxQueuedBytes: 8,
		MaxBufferedFrames: 2, MaxQueuedFrames: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	input := newRecorderAudioInput(recorder, newQueuedAudioInput())
	start := time.Unix(0, 0)
	input.mu.Lock()
	input.frames = []agents.AudioFrame{audioFrame(time.Millisecond, 1_000, 1, 1)}
	input.bytes = 2
	input.started = start.Add(time.Duration(1<<63 - 1))
	input.mu.Unlock()
	frames, err := input.takeBuffer(start)
	if !errors.Is(err, ErrInputPaddingSkipped) || len(frames) != 1 {
		t.Fatalf("huge input padding = %d frames, %v", len(frames), err)
	}
	_ = input.close()
}

func TestFFmpegEncoderProducesOggOpusWhenAvailable(t *testing.T) {
	if agents.ResolveFFmpegPath() == "" {
		t.Skip("ffmpeg is not installed")
	}
	clock := newManualClock()
	recorder, err := NewRecorderIO(RecorderOptions{Clock: clock.Now, WriteInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	input := newQueuedAudioInput()
	if _, err := recorder.RecordInput(input); err != nil {
		t.Fatal(err)
	}
	sink := newTestAudioOutput(48_000)
	output, err := recorder.RecordOutput(sink)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "recording.ogg")
	if err := recorder.Start(t.Context(), path); err != nil {
		t.Fatal(err)
	}
	if err := output.CaptureFrame(t.Context(), audioFrame(20*time.Millisecond, 48_000, 1, 500)); err != nil {
		t.Fatal(err)
	}
	clock.Advance(20 * time.Millisecond)
	if err := sink.NotifyPlaybackFinished(voice.PlaybackFinishedEvent{PlaybackPosition: 20 * time.Millisecond}); err != nil {
		t.Fatal(err)
	}
	if err := recorder.Close(t.Context()); err != nil {
		// Some minimal FFmpeg installations omit libopus. The deterministic
		// encoder and argument tests still run on those hosts.
		t.Skipf("ffmpeg lacks usable Ogg/Opus support: %v", err)
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) < 4 || !bytes.Equal(encoded[:4], []byte("OggS")) {
		t.Fatalf("output is not an Ogg stream: %x", encoded[:min(8, len(encoded))])
	}
}

func TestDefaultEncoderUsesRootResolverAndOggOpusArguments(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX helper script")
	}
	directory := t.TempDir()
	argumentsPath := filepath.Join(directory, "arguments")
	scriptPath := filepath.Join(directory, "fake-ffmpeg")
	script := "#!/bin/sh\n" +
		"printf '%s\\n' \"$@\" > \"$RECORDER_ARGS\"\n" +
		"last=''\nfor arg in \"$@\"; do last=\"$arg\"; done\n" +
		"cat >/dev/null\nprintf 'OggS' > \"$last\"\n"
	if err := os.WriteFile(scriptPath, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv(agents.FFmpegPathEnvironment, scriptPath)
	t.Setenv("RECORDER_ARGS", argumentsPath)
	outputPath := filepath.Join(directory, "result.ogg")
	encoder, err := newFFmpegEncoder(t.Context(), outputPath, 48_000)
	if err != nil {
		t.Fatal(err)
	}
	if err := encoder.WritePCM(t.Context(), make([]byte, 1_920)); err != nil {
		t.Fatal(err)
	}
	if err := encoder.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	arguments, err := os.ReadFile(argumentsPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"s16le", "48000", "2", "libopus", "ogg", outputPath} {
		if !bytes.Contains(arguments, []byte(required+"\n")) {
			t.Fatalf("missing FFmpeg argument %q in:\n%s", required, arguments)
		}
	}
	encoded, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != "OggS" {
		t.Fatalf("fake output = %q", encoded)
	}
}

func FuzzSplitFramePreservesSamples(f *testing.F) {
	f.Add(uint16(48_000), uint8(2), uint16(960), int64(10*time.Millisecond))
	f.Add(uint16(16_000), uint8(1), uint16(0), int64(-1))
	f.Fuzz(func(t *testing.T, rawRate uint16, rawChannels uint8, rawSamples uint16, positionNS int64) {
		rate := int(rawRate%64_000) + 1
		channels := int(rawChannels%4) + 1
		samples := int(rawSamples % 4_096)
		data := make([]int16, samples*channels)
		for index := range data {
			data[index] = int16(index)
		}
		frame, err := agents.NewAudioFrame(data, rate, channels)
		if err != nil {
			t.Fatal(err)
		}
		left, right, err := splitFrame(frame, time.Duration(positionNS))
		if err != nil {
			t.Fatal(err)
		}
		if left.SamplesPerChannel+right.SamplesPerChannel != samples {
			t.Fatalf("split count = %d+%d, want %d", left.SamplesPerChannel, right.SamplesPerChannel, samples)
		}
		joined := append(append([]int16(nil), left.Data...), right.Data...)
		if !slices.Equal(joined, data) {
			t.Fatal("split changed sample order or values")
		}
	})
}

func FuzzMonoConverter(f *testing.F) {
	f.Add(uint16(24_000), uint8(2), uint16(480), int16(100))
	f.Fuzz(func(t *testing.T, rawRate uint16, rawChannels uint8, rawSamples uint16, sample int16) {
		rate := int(rawRate%64_000) + 1
		channels := int(rawChannels%4) + 1
		samples := int(rawSamples % 2_048)
		frame := audioFrame(time.Duration(samples)*time.Second/time.Duration(rate), rate, channels, sample)
		converter := newMonoConverter(48_000, 1<<20)
		output, err := converter.push([]agents.AudioFrame{frame}, true)
		if errors.Is(err, ErrBufferLimit) {
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		for _, value := range output {
			if value < -1 || value > 1 {
				t.Fatalf("out-of-range sample %v", value)
			}
		}
	})
}

func BenchmarkRecorderConstruction(b *testing.B) {
	for b.Loop() {
		recorder, err := NewRecorderIO(RecorderOptions{})
		if err != nil || recorder == nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStereoMix48k100ms(b *testing.B) {
	left := make([]float32, 4_800)
	right := make([]float32, 4_800)
	b.ReportAllocs()
	for b.Loop() {
		pcm, err := interleavePCM16(left, right)
		if err != nil || len(pcm) != 19_200 {
			b.Fatal(err)
		}
	}
}

func BenchmarkMonoDownmix48k100ms(b *testing.B) {
	frame := audioFrame(100*time.Millisecond, 48_000, 1, 1_000)
	b.ReportAllocs()
	for b.Loop() {
		output := downmixFloat(frame)
		if len(output) != 4_800 {
			b.Fatal(len(output))
		}
	}
}
