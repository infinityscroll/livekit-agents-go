// SPDX-License-Identifier: Apache-2.0

//go:build cgo

package backgroundaudio

import (
	"context"
	"errors"
	"testing"

	agents "github.com/infinityscroll/livekit-agents-go"
	lksdk "github.com/livekit/server-sdk-go/v2"
)

func TestLiveKitPublisherValidationWithoutNetwork(t *testing.T) {
	t.Parallel()
	if _, err := NewLiveKitPublisher(nil, LiveKitPublisherOptions{}); err == nil {
		t.Fatal("nil room accepted")
	}
	room := lksdk.NewRoom(nil)
	publisher, err := NewLiveKitPublisher(room, LiveKitPublisherOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := publisher.Publish(context.Background(), PublishRequest{}); err == nil {
		t.Fatal("invalid publish request accepted")
	}
	if _, _, err := publisher.Publish(context.Background(), PublishRequest{Name: BackgroundAudioTrackName, SampleRate: MixerSampleRate, Channels: MixerChannels}); err == nil {
		t.Fatal("unconnected room unexpectedly published a track")
	}
	if publication, ok, err := publisher.CurrentPublication(context.Background(), BackgroundAudioTrackName); err != nil || ok || publication.SID != "" {
		t.Fatalf("current publication = %#v, %v, %v", publication, ok, err)
	}
	if err := publisher.Unpublish(context.Background(), ""); err != nil {
		t.Fatalf("empty unpublish: %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := publisher.CurrentPublication(canceled, BackgroundAudioTrackName); !errors.Is(err, context.Canceled) {
		t.Fatalf("current publication cancellation = %v", err)
	}
	if err := publisher.Unpublish(canceled, "sid"); !errors.Is(err, context.Canceled) {
		t.Fatalf("unpublish cancellation = %v", err)
	}
}

func TestLiveKitFrameSinkValidatesBeforeNativeTrack(t *testing.T) {
	t.Parallel()
	sink := &liveKitFrameSink{}
	if _, ok := any(sink).(immediateFrameSink); !ok {
		t.Fatal("LiveKit sink lost allocation-free capture marker")
	}
	if err := sink.CaptureFrame(context.Background(), agents.AudioFrame{}); !errors.Is(err, agents.ErrInvalidAudioFormat) {
		t.Fatalf("invalid frame error = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sink.CaptureFrame(canceled, testFrame(48_000, 1, 10, 1)); !errors.Is(err, context.Canceled) {
		t.Fatalf("capture cancellation = %v", err)
	}
	if err := sink.Close(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("close cancellation = %v", err)
	}
}

func TestStartRoomRejectsNilRoom(t *testing.T) {
	t.Parallel()
	player, err := NewBackgroundAudioPlayer(BackgroundAudioPlayerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := player.StartRoom(context.Background(), nil, nil, LiveKitPublisherOptions{}); err == nil {
		t.Fatal("nil room accepted")
	}
}
