// SPDX-License-Identifier: Apache-2.0

package roomio

import (
	"errors"
	"testing"

	agents "github.com/infinityscroll/livekit-agents-go"
)

func TestConvertAudioFrameChannelsAndRate(t *testing.T) {
	t.Parallel()
	stereo, err := agents.NewAudioFrame([]int16{100, 300, -100, 100}, 24_000, 2)
	if err != nil {
		t.Fatal(err)
	}
	mono, err := convertAudioFrame(stereo, 24_000, 1)
	if err != nil {
		t.Fatal(err)
	}
	if mono.Channels != 1 || mono.SamplesPerChannel != 2 || mono.Data[0] != 200 || mono.Data[1] != 0 {
		t.Fatalf("downmix = %#v", mono)
	}

	resampled, err := convertAudioFrame(mono, 48_000, 2)
	if err != nil {
		t.Fatal(err)
	}
	if resampled.SampleRate != 48_000 || resampled.Channels != 2 || resampled.SamplesPerChannel < 3 {
		t.Fatalf("resampled format = %#v", resampled)
	}
	for index := 0; index < len(resampled.Data); index += 2 {
		if resampled.Data[index] != resampled.Data[index+1] {
			t.Fatalf("stereo channels differ at %d", index)
		}
	}
}

func TestConvertAudioFrameOwnsSamples(t *testing.T) {
	t.Parallel()
	data := []int16{1, 2, 3}
	frame, _ := agents.NewAudioFrame(data, 24_000, 1)
	converted, err := convertAudioFrame(frame, 24_000, 1)
	if err != nil {
		t.Fatal(err)
	}
	data[0] = 9
	if converted.Data[0] != 1 {
		t.Fatal("converted frame aliases producer memory")
	}
}

func TestConvertAudioFrameRejectsInconsistentSampleCount(t *testing.T) {
	t.Parallel()
	_, err := convertAudioFrame(agents.AudioFrame{
		Data: []int16{1, 2}, SampleRate: 24_000, Channels: 1, SamplesPerChannel: 3,
	}, 24_000, 1)
	if !errors.Is(err, agents.ErrInvalidAudioFormat) {
		t.Fatalf("error = %v", err)
	}
}
