// SPDX-License-Identifier: Apache-2.0

package agents

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/infinityscroll/livekit-agents-go/stream"
)

func writeTestWAV(t *testing.T, sampleRate, channels int, samples []int16) string {
	t.Helper()
	var body bytes.Buffer
	for _, sample := range samples {
		if err := binary.Write(&body, binary.LittleEndian, sample); err != nil {
			t.Fatal(err)
		}
	}
	var wav bytes.Buffer
	wav.WriteString("RIFF")
	_ = binary.Write(&wav, binary.LittleEndian, uint32(36+body.Len()))
	wav.WriteString("WAVEfmt ")
	_ = binary.Write(&wav, binary.LittleEndian, uint32(16))
	_ = binary.Write(&wav, binary.LittleEndian, uint16(1))
	_ = binary.Write(&wav, binary.LittleEndian, uint16(channels))
	_ = binary.Write(&wav, binary.LittleEndian, uint32(sampleRate))
	_ = binary.Write(&wav, binary.LittleEndian, uint32(sampleRate*channels*2))
	_ = binary.Write(&wav, binary.LittleEndian, uint16(channels*2))
	_ = binary.Write(&wav, binary.LittleEndian, uint16(16))
	wav.WriteString("data")
	_ = binary.Write(&wav, binary.LittleEndian, uint32(body.Len()))
	wav.Write(body.Bytes())
	path := filepath.Join(t.TempDir(), "audio.wav")
	if err := os.WriteFile(path, wav.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestAudioFramesFromFileNativeWAV(t *testing.T) {
	t.Parallel()
	samples := make([]int16, 4_800)
	for i := range samples {
		samples[i] = int16(i - 2_400)
	}
	path := writeTestWAV(t, 48_000, 1, samples)
	reader, err := AudioFramesFromFile(t.Context(), path, AudioDecodeOptions{
		SampleRate: 48_000, Channels: 1, FrameDuration: 20 * time.Millisecond,
		FFmpegPath: filepath.Join(t.TempDir(), "must-not-run-ffmpeg"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	var got []int16
	frames := 0
	for {
		frame, err := reader.Recv(t.Context())
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		frames++
		if frame.SampleRate != 48_000 || frame.Channels != 1 || frame.SamplesPerChannel != 960 {
			t.Fatalf("unexpected frame format: %#v", frame)
		}
		got = append(got, frame.Data...)
	}
	if frames != 5 || !bytes.Equal(int16LittleEndian(got), int16LittleEndian(samples)) {
		t.Fatalf("decoded frames=%d samples=%d", frames, len(got))
	}
	if err := reader.Wait(t.Context()); err != nil {
		t.Fatal(err)
	}
	if duration := CalculateAudioDurationSeconds(AudioFrame{SampleRate: 48_000, SamplesPerChannel: 960}); duration != 0.02 {
		t.Fatalf("duration = %v", duration)
	}
}

func TestLoopAudioFramesFromFileIsBoundedAndCancelable(t *testing.T) {
	t.Parallel()
	samples := make([]int16, 960)
	for i := range samples {
		samples[i] = int16(i)
	}
	path := writeTestWAV(t, 48_000, 1, samples)
	reader, err := LoopAudioFramesFromFile(t.Context(), path, AudioDecodeOptions{FrameDuration: 20 * time.Millisecond, StreamCapacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		frame, err := reader.Recv(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if frame.Data[10] != samples[10] {
			t.Fatalf("iteration %d data mismatch", i)
		}
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := reader.Wait(ctx); !errors.Is(err, stream.ErrClosed) {
		t.Fatalf("Wait after Close = %v", err)
	}
}

func TestLoopAudioFramesFromEmptyFileTerminates(t *testing.T) {
	t.Parallel()
	path := writeTestWAV(t, 48_000, 1, nil)
	reader, err := LoopAudioFramesFromFile(t.Context(), path, AudioDecodeOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if _, err := reader.Recv(t.Context()); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("empty loop error = %v", err)
	}
}

func TestAudioFramesFromFileValidationAndFFmpegStartError(t *testing.T) {
	t.Parallel()
	if _, err := AudioFramesFromFile(t.Context(), "unused", AudioDecodeOptions{Format: "wav;bad"}); err == nil {
		t.Fatal("invalid format hint accepted")
	}
	path := writeTestWAV(t, 44_100, 1, []int16{1, 2, 3, 4})
	_, err := AudioFramesFromFile(t.Context(), path, AudioDecodeOptions{SampleRate: 48_000, FFmpegPath: filepath.Join(t.TempDir(), "missing")})
	var decodeErr *AudioDecodeError
	if !errors.As(err, &decodeErr) {
		t.Fatalf("error = %T %v, want AudioDecodeError", err, err)
	}
}

func int16LittleEndian(samples []int16) []byte {
	result := make([]byte, len(samples)*2)
	for i, sample := range samples {
		binary.LittleEndian.PutUint16(result[i*2:], uint16(sample))
	}
	return result
}
