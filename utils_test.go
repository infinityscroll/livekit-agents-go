// SPDX-License-Identifier: Apache-2.0

package agents

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/infinityscroll/livekit-agents-go/rtcbridge"
	lksdk "github.com/livekit/server-sdk-go/v2"
)

func TestDedentPinnedSemantics(t *testing.T) {
	tests := map[string]string{
		"\n  Hello,\n    world!\n": "Hello,\n  world!",
		"\n\talpha\n\t\tbeta\n\t":  "alpha\n\tbeta",
		"  one\n    two   ":        "one\n  two",
		"    one\n  \n    two":     "one\n\ntwo",
		"\n\n":                     "",
		"é\n  text":                "é\n  text",
	}
	for input, want := range tests {
		if got := Dedent(input); got != want {
			t.Fatalf("Dedent(%q)=%q want=%q", input, got, want)
		}
	}
}

func TestIsCloudAndHosted(t *testing.T) {
	for rawURL, want := range map[string]bool{
		"wss://project.livekit.cloud":      true,
		"https://edge.livekit.run/path":    true,
		"https://livekit.cloud":            false,
		"https://notlivekit.cloud.example": false,
		"project.livekit.cloud":            false,
		"%":                                false,
	} {
		if got := IsCloud(rawURL); got != want {
			t.Fatalf("IsCloud(%q)=%v want=%v", rawURL, got, want)
		}
	}
	previous, present := os.LookupEnv("LIVEKIT_REMOTE_EOT_URL")
	t.Cleanup(func() {
		if present {
			_ = os.Setenv("LIVEKIT_REMOTE_EOT_URL", previous)
		} else {
			_ = os.Unsetenv("LIVEKIT_REMOTE_EOT_URL")
		}
	})
	_ = os.Unsetenv("LIVEKIT_REMOTE_EOT_URL")
	if IsHosted() {
		t.Fatal("IsHosted true for absent variable")
	}
	_ = os.Setenv("LIVEKIT_REMOTE_EOT_URL", "")
	if !IsHosted() {
		t.Fatal("IsHosted false for explicitly empty variable")
	}
}

func TestRoomWaitersRejectCancellationAndDisconnectedRoom(t *testing.T) {
	room, bridge := rtcbridge.NewRoom(nil, rtcbridge.RTCBridgeOptions{})
	defer bridge.Close()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := WaitForParticipantAttribute(canceled, room, bridge, "p", "key", "value"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled attribute wait error=%v", err)
	}
	if err := WaitForParticipantAttribute(t.Context(), room, bridge, "p", "key", "value"); !errors.Is(err, ErrRoomNotConnected) {
		t.Fatalf("disconnected attribute wait error=%v", err)
	}
	if _, err := WaitForTrackPublication(t.Context(), room, bridge, WaitForTrackPublicationOptions{Kind: lksdk.TrackKindAudio}); !errors.Is(err, ErrRoomNotConnected) {
		t.Fatalf("disconnected track wait error=%v", err)
	}
}
