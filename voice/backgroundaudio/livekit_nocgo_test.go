// SPDX-License-Identifier: Apache-2.0

//go:build !cgo

package backgroundaudio

import (
	"context"
	"errors"
	"testing"
)

func TestNoCGOLiveKitAdapterFailsExplicitly(t *testing.T) {
	if _, err := NewLiveKitPublisher(nil, LiveKitPublisherOptions{}); !errors.Is(err, ErrLiveKitMediaUnavailable) {
		t.Fatalf("constructor error = %v", err)
	}
	player, _ := NewBackgroundAudioPlayer(BackgroundAudioPlayerOptions{})
	if err := player.StartRoom(context.Background(), nil, nil, LiveKitPublisherOptions{}); !errors.Is(err, ErrLiveKitMediaUnavailable) {
		t.Fatalf("StartRoom error = %v", err)
	}
}
