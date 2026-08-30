// SPDX-License-Identifier: Apache-2.0

package avatar

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/infinityscroll/livekit-agents-go/llm"
	"github.com/infinityscroll/livekit-agents-go/metrics"
	"github.com/infinityscroll/livekit-agents-go/voice"
	lksdk "github.com/livekit/server-sdk-go/v2"
)

func newAvatarTestAgentSession(t *testing.T) *voice.AgentSession[struct{}] {
	t.Helper()
	session, err := voice.NewAgentSession(voice.AgentSessionOptions[struct{}]{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close(context.Background()) })
	return session
}

func TestAvatarSessionJoinAndPlaybackMetrics(t *testing.T) {
	waited := make(chan struct{}, 1)
	base := NewAvatarSession[struct{}](AvatarSessionOptions{
		AvatarIdentity: "avatar", Provider: "provider",
		ReadyWaiter: func(context.Context, *lksdk.Room, string, lksdk.TrackKind) error {
			waited <- struct{}{}
			return nil
		},
	})
	metricsEvents := make(chan metrics.Avatar, 2)
	base.OnMetrics(func(metric metrics.Avatar) { metricsEvents <- metric })
	if err := base.Start(t.Context(), newAvatarTestAgentSession(t), lksdk.NewRoom(lksdk.NewRoomCallback())); err != nil {
		t.Fatal(err)
	}
	if err := base.WaitForJoin(t.Context()); err != nil {
		t.Fatal(err)
	}
	<-waited
	joinMetric := <-metricsEvents
	if joinMetric.Metadata.ModelProvider != "provider" || joinMetric.SessionStarted.IsZero() || joinMetric.AvatarJoined.Before(joinMetric.SessionStarted) {
		t.Fatalf("join metric = %#v", joinMetric)
	}
	message := llm.NewChatMessage(llm.RoleAssistant, "hello")
	message.Metrics.PlaybackLatency = 25 * time.Millisecond
	base.handleAgentEvent(voice.ConversationItemAddedEvent{Item: message})
	playbackMetric := <-metricsEvents
	if playbackMetric.PlaybackLatency != 25*time.Millisecond || playbackMetric.Metadata.ModelProvider != "provider" {
		t.Fatalf("playback metric = %#v", playbackMetric)
	}
	if err := base.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestAvatarSessionWaitTimeoutRollbackAndReuse(t *testing.T) {
	base := NewAvatarSession[struct{}](AvatarSessionOptions{
		AvatarIdentity: "avatar",
		ReadyWaiter: func(ctx context.Context, _ *lksdk.Room, _ string, _ lksdk.TrackKind) error {
			<-ctx.Done()
			return context.Cause(ctx)
		},
	})
	room := lksdk.NewRoom(lksdk.NewRoomCallback())
	if err := base.Start(t.Context(), newAvatarTestAgentSession(t), room); err != nil {
		t.Fatal(err)
	}
	timeout := 5 * time.Millisecond
	if err := base.WaitForJoin(t.Context(), WaitForJoinOptions{Timeout: &timeout}); !errors.Is(err, ErrAvatarJoinTimeout) {
		t.Fatalf("WaitForJoin error = %v", err)
	}
	if err := base.RollbackStart(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := base.Start(t.Context(), newAvatarTestAgentSession(t), room); err != nil {
		t.Fatalf("Start after rollback = %v", err)
	}
	if err := base.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestAvatarSessionUnknownIdentityAndConcurrentClose(t *testing.T) {
	base := NewAvatarSession[struct{}](AvatarSessionOptions{})
	if base.AvatarIdentity() != "unknown" || base.Provider() != "unknown" {
		t.Fatalf("defaults = %q %q", base.AvatarIdentity(), base.Provider())
	}
	if err := base.Start(t.Context(), newAvatarTestAgentSession(t), lksdk.NewRoom(lksdk.NewRoomCallback())); err != nil {
		t.Fatal(err)
	}
	if err := base.WaitForJoin(t.Context()); err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 2)
	go func() { results <- base.Close(context.Background()) }()
	go func() { results <- base.Close(context.Background()) }()
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
}
