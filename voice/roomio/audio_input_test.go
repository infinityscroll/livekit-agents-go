// SPDX-License-Identifier: Apache-2.0

package roomio

import (
	"context"
	"testing"

	agents "github.com/infinityscroll/livekit-agents-go"
	mediabase "github.com/livekit/media-sdk"
	lksdk "github.com/livekit/server-sdk-go/v2"
)

func TestParticipantAudioInputWriterAttachmentGenerationAndOwnership(t *testing.T) {
	t.Parallel()
	input := &ParticipantAudioInput{
		opts:   RoomInputOptions{AudioSampleRate: 24_000, AudioChannels: 1},
		frames: newRealtimeQueue[agents.AudioFrame](1), participant: &lksdk.RemoteParticipant{}, generation: 7,
	}
	writer := remotePCMWriter{input: input, generation: 7}
	samples := mediabase.PCM16Sample{1, 2}
	if err := writer.WriteSample(samples); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := input.Recv(ctx); err == nil {
		t.Fatal("detached input unexpectedly admitted a frame")
	}

	input.OnAttached()
	_ = writer.WriteSample(samples)
	samples[0] = 9
	frame, err := input.Recv(context.Background())
	if err != nil || frame.Data[0] != 1 {
		t.Fatalf("owned frame = %#v, %v", frame, err)
	}

	input.generation++
	_ = writer.WriteSample(mediabase.PCM16Sample{3})
	ctx, cancel = context.WithCancel(context.Background())
	cancel()
	if _, err := input.Recv(ctx); err == nil {
		t.Fatal("stale decoder generation admitted a frame")
	}
}

func TestParticipantAudioInputDropsOldestAtCapacity(t *testing.T) {
	t.Parallel()
	input := &ParticipantAudioInput{
		opts:   RoomInputOptions{AudioSampleRate: 24_000, AudioChannels: 1},
		frames: newRealtimeQueue[agents.AudioFrame](1), participant: &lksdk.RemoteParticipant{}, generation: 1,
	}
	input.OnAttached()
	writer := remotePCMWriter{input: input, generation: 1}
	_ = writer.WriteSample(mediabase.PCM16Sample{1})
	_ = writer.WriteSample(mediabase.PCM16Sample{2})
	frame, err := input.Recv(context.Background())
	if err != nil || frame.Data[0] != 2 || input.DroppedFrames() != 1 {
		t.Fatalf("frame=%#v dropped=%d err=%v", frame, input.DroppedFrames(), err)
	}
}
