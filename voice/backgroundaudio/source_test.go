// SPDX-License-Identifier: Apache-2.0

package backgroundaudio

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	agents "github.com/livekit/agents-go"
)

func TestBuiltinClipsAreExactPinnedAssets(t *testing.T) {
	t.Parallel()
	expected := map[BuiltinAudioClip]string{
		BuiltinHoldMusic:       "cfb2a77542ac01011f41593a5fe9b919ee898343bcc66d72499b77b2640def28",
		BuiltinOfficeAmbience:  "967a445f505e202d3ec03968cff71846284c3b70d4353b3096ce9121d34224c3",
		BuiltinKeyboardTyping:  "56258677554c75019be52f8a16e8494ee5bccfb74c01f490b5a1befd48cbbeae",
		BuiltinKeyboardTyping2: "1b8244f26ae315561aefdbb09387bf16388c3380d74c2e1f0008be0998169954",
	}
	for clip, want := range expected {
		clip, want := clip, want
		t.Run(string(clip), func(t *testing.T) {
			file, err := OpenBuiltinAudio(clip)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			hash := sha256.New()
			if _, err := io.Copy(hash, file); err != nil {
				t.Fatal(err)
			}
			if got := hex.EncodeToString(hash.Sum(nil)); got != want {
				t.Fatalf("asset digest = %s, want %s", got, want)
			}
		})
	}
	if IsBuiltinAudioClip("city-ambience.ogg") {
		t.Fatal("agents-js 1.7.1 does not include city ambience")
	}
}

func TestExtractBuiltinAudioCleanup(t *testing.T) {
	t.Parallel()
	path, cleanup, err := ExtractBuiltinAudio(context.Background(), BuiltinKeyboardTyping2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("extracted file: %v", err)
	}
	directory := filepathDir(path)
	if err := cleanup(); err != nil {
		t.Fatal(err)
	}
	if err := cleanup(); err != nil {
		t.Fatalf("idempotent cleanup: %v", err)
	}
	if _, err := os.Stat(directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temporary directory remains: %v", err)
	}
}

func TestBuiltinStoreDefaultExtractionCachesAndCleans(t *testing.T) {
	t.Parallel()
	store := &builtinStore{}
	first, err := store.resolve(context.Background(), BuiltinKeyboardTyping2)
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.resolve(context.Background(), BuiltinKeyboardTyping2)
	if err != nil || first != second {
		t.Fatalf("cached path = %q, %v; want %q", second, err, first)
	}
	resource, err := BuiltinResources().Open("resources/keyboard-typing2.ogg")
	if err != nil {
		t.Fatal(err)
	}
	if err := resource.Close(); err != nil {
		t.Fatal(err)
	}
	path, cleanup, err := GetBuiltinAudioPath(context.Background(), BuiltinKeyboardTyping2)
	if err != nil {
		t.Fatal(err)
	}
	if err := cleanup(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("compatibility extraction remains: %v", err)
	}
	directory := filepathDir(first)
	if err := store.close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("store directory remains: %v", err)
	}
	if _, err := store.resolve(context.Background(), BuiltinKeyboardTyping2); !errors.Is(err, ErrClosed) {
		t.Fatalf("resolve after close = %v", err)
	}
}

func filepathDir(path string) string {
	for index := len(path) - 1; index >= 0; index-- {
		if os.IsPathSeparator(path[index]) {
			return path[:index]
		}
	}
	return "."
}

func TestOneShotAndFactorySources(t *testing.T) {
	t.Parallel()
	reader := &sliceReader{frames: []agents.AudioFrame{testFrame(48_000, 1, 10, 1)}}
	source := Stream(reader)
	options := sourceOpenOptions{blockDuration: 10 * time.Millisecond, bufferDuration: 40 * time.Millisecond}
	first, err := source.open(context.Background(), options)
	if err != nil || first.reader != reader {
		t.Fatalf("first open = %#v, %v", first.reader, err)
	}
	if _, err := source.open(context.Background(), options); !errors.Is(err, ErrSourceConsumed) {
		t.Fatalf("second open error = %v", err)
	}
	created := 0
	factory := StreamFactorySource(func(context.Context) (FrameReader, error) {
		created++
		return &sliceReader{}, nil
	})
	if _, err := factory.open(context.Background(), options); err != nil {
		t.Fatal(err)
	}
	if _, err := factory.open(context.Background(), options); err != nil {
		t.Fatal(err)
	}
	if created != 2 {
		t.Fatalf("factory created %d streams", created)
	}
	if _, err := factory.open(context.Background(), sourceOpenOptions{loop: true}); err != nil {
		t.Fatalf("factory loop flag must be ignored like agents-js: %v", err)
	}
}

func TestConfigDefaultsAndValidation(t *testing.T) {
	t.Parallel()
	source := Stream(&sliceReader{})
	resolved, err := Config(source).resolved()
	if err != nil {
		t.Fatal(err)
	}
	if resolved.volume != 1 || resolved.probability != 1 {
		t.Fatalf("defaults = volume %v probability %v", resolved.volume, resolved.probability)
	}
	zero := 0.0
	resolved, err = (AudioConfig{Source: source, Volume: &zero, Probability: &zero}).resolved()
	if err != nil || resolved.volume != 0 || resolved.probability != 0 {
		t.Fatalf("explicit zeros = %#v, %v", resolved, err)
	}
	negative := -1.0
	if _, err := (AudioConfig{Source: source, Volume: &negative}).resolved(); err == nil {
		t.Fatal("negative volume accepted")
	}
}

func TestFrameConverterResamplesDownmixesAndPackets(t *testing.T) {
	t.Parallel()
	converter := newFrameConverter(480, .5)
	input := make([]int16, 240*2)
	for index := 0; index < len(input); index += 2 {
		input[index], input[index+1] = 1_000, 3_000
	}
	frame, _ := agents.NewAudioFrame(input, 24_000, 2)
	frames, err := converter.push(frame)
	if err != nil {
		t.Fatal(err)
	}
	frames = append(frames, converter.flush()...)
	if len(frames) != 1 || frames[0].SampleRate != 48_000 || frames[0].Channels != 1 || len(frames[0].Data) != 480 {
		t.Fatalf("converted frames = %#v", frames)
	}
	for index, sample := range frames[0].Data {
		if sample != 1_000 {
			t.Fatalf("sample %d = %d, want 1000", index, sample)
		}
	}
}

func TestApplyVolumeMatchesJavaScriptRoundingAndClipping(t *testing.T) {
	t.Parallel()
	source := []int16{-3, -1, 1, 3, 20_000, -20_000}
	destination := make([]int16, len(source))
	applyVolume(destination, source, .5)
	want := []int16{-1, 0, 1, 2, 10_000, -10_000}
	if !reflect.DeepEqual(destination, want) {
		t.Fatalf("volume result = %v, want %v", destination, want)
	}
	applyVolume(destination, source, 2)
	want = []int16{-6, -2, 2, 6, 32_767, -32_768}
	if !reflect.DeepEqual(destination, want) {
		t.Fatalf("clipped result = %v, want %v", destination, want)
	}
}

func FuzzFrameConverter(f *testing.F) {
	f.Add(24_000, 2, 240, int16(1_000))
	f.Add(48_000, 1, 480, int16(-1_000))
	f.Fuzz(func(t *testing.T, rate, channels, samples int, value int16) {
		if rate < 8_000 || rate > 192_000 || channels < 1 || channels > 8 || samples < 1 || samples > 4_800 {
			t.Skip()
		}
		data := make([]int16, channels*samples)
		for index := range data {
			data[index] = value
		}
		frame, err := agents.NewAudioFrame(data, rate, channels)
		if err != nil {
			t.Fatal(err)
		}
		converter := newFrameConverter(480, 1)
		frames, err := converter.push(frame)
		if err != nil {
			t.Fatal(err)
		}
		frames = append(frames, converter.flush()...)
		for _, output := range frames {
			if output.SampleRate != MixerSampleRate || output.Channels != 1 || len(output.Data) > 480 {
				t.Fatalf("invalid output: %#v", output)
			}
		}
	})
}

func BenchmarkFrameConverter48kMono(b *testing.B) {
	frame := testFrame(48_000, 1, 10, 1_000)
	b.ReportAllocs()
	for b.Loop() {
		converter := newFrameConverter(480, 1)
		_, _ = converter.push(frame)
	}
}

type sliceReader struct {
	frames []agents.AudioFrame
	err    error
	mu     sync.Mutex
}

func (r *sliceReader) Recv(context.Context) (agents.AudioFrame, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.frames) == 0 {
		if r.err != nil {
			return agents.AudioFrame{}, r.err
		}
		return agents.AudioFrame{}, io.EOF
	}
	frame := r.frames[0]
	r.frames = r.frames[1:]
	return frame, nil
}

func testFrame(rate, channels, milliseconds int, value int16) agents.AudioFrame {
	samples := rate * milliseconds / 1_000
	data := make([]int16, samples*channels)
	for index := range data {
		data[index] = value
	}
	frame, _ := agents.NewAudioFrame(data, rate, channels)
	return frame
}
