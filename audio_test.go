// SPDX-License-Identifier: Apache-2.0

package agents

import (
	"encoding/binary"
	"errors"
	"testing"
	"time"
)

func pcmBytes(values ...int16) []byte {
	b := make([]byte, len(values)*2)
	for i, value := range values {
		binary.LittleEndian.PutUint16(b[i*2:], uint16(value))
	}
	return b
}

func TestAudioByteStreamPacketizesAcrossWrites(t *testing.T) {
	t.Parallel()
	stream, err := NewAudioByteStream(100, 1, 4)
	if err != nil {
		t.Fatal(err)
	}
	if frames := stream.Write(pcmBytes(1, 2, 3)); len(frames) != 0 {
		t.Fatalf("first write produced %d frames", len(frames))
	}
	frames := stream.Write(pcmBytes(4, 5, 6, 7, 8, 9))
	if len(frames) != 2 {
		t.Fatalf("second write produced %d frames, want 2", len(frames))
	}
	if got := frames[0].Data; len(got) != 4 || got[0] != 1 || got[3] != 4 {
		t.Fatalf("first frame = %v", got)
	}
	flushed, err := stream.Flush()
	if err != nil {
		t.Fatal(err)
	}
	if len(flushed) != 1 || len(flushed[0].Data) != 1 || flushed[0].Data[0] != 9 {
		t.Fatalf("flushed = %#v", flushed)
	}
}

func TestAudioByteStreamRejectsPartialSample(t *testing.T) {
	t.Parallel()
	stream, _ := NewAudioByteStream(48_000, 2, 480)
	stream.Write([]byte{1, 2, 3})
	_, err := stream.Flush()
	if !errors.Is(err, ErrIncompletePCMFrame) {
		t.Fatalf("Flush() error = %v", err)
	}
}

func TestAudioDurationAndMerge(t *testing.T) {
	t.Parallel()
	a, _ := NewAudioFrame(make([]int16, 960), 48_000, 1)
	b, _ := NewAudioFrame(make([]int16, 960), 48_000, 1)
	if got := CalculateAudioDuration([]AudioFrame{a, b}); got != 40*time.Millisecond {
		t.Fatalf("duration = %v", got)
	}
	merged, err := MergeFrames([]AudioFrame{a, b})
	if err != nil {
		t.Fatal(err)
	}
	if merged.SamplesPerChannel != 1920 {
		t.Fatalf("samples = %d", merged.SamplesPerChannel)
	}
}

func BenchmarkAudioByteStream20ms(b *testing.B) {
	bytes := pcmBytes(make([]int16, 960)...)
	b.ReportAllocs()
	for b.Loop() {
		stream, _ := NewAudioByteStream(48_000, 1, 960)
		frames := stream.Write(bytes)
		if len(frames) != 1 {
			b.Fatal(len(frames))
		}
	}
}
