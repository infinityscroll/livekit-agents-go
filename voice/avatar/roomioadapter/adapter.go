// SPDX-License-Identifier: Apache-2.0

// Package roomioadapter connects avatar readiness waits to a roomio RTCBridge.
// It is separate so queue/WebSocket avatar plugins do not pay roomio's media
// codec import and startup cost.
package roomioadapter

import (
	"context"
	"errors"

	"github.com/livekit/agents-go/voice/avatar"
	"github.com/livekit/agents-go/voice/roomio"
	lksdk "github.com/livekit/server-sdk-go/v2"
)

// ReadyWaiter returns a lossless, cancellation-aware participant/track waiter.
func ReadyWaiter(bridge *roomio.RTCBridge) avatar.ReadyWaiter {
	return func(ctx context.Context, room *lksdk.Room, identity string, kind lksdk.TrackKind) error {
		if bridge == nil {
			return errors.New("avatar roomio readiness bridge is nil")
		}
		subscription, err := bridge.SubscribeTypes(8, roomio.RTCEventParticipantConnected, roomio.RTCEventTrackPublished)
		if err != nil {
			return err
		}
		defer subscription.Close()
		for {
			if destinationReady(room, identity, kind) {
				return nil
			}
			if _, err := subscription.Recv(ctx); err != nil {
				return err
			}
		}
	}
}

func destinationReady(room *lksdk.Room, identity string, kind lksdk.TrackKind) bool {
	if room == nil {
		return false
	}
	participant := room.GetParticipantByIdentity(identity)
	if participant == nil {
		return false
	}
	if kind == "" {
		return true
	}
	for _, publication := range participant.TrackPublications() {
		if publication.Kind() == kind {
			return true
		}
	}
	return false
}
