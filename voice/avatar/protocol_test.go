// SPDX-License-Identifier: Apache-2.0

package avatar

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/infinityscroll/livekit-agents-go/voice"
)

func TestParsePlaybackFinishedPayloadNeverThrowsAndNormalizesKeys(t *testing.T) {
	tests := []struct {
		name        string
		payload     string
		position    time.Duration
		interrupted bool
		transcript  *string
	}{
		{name: "snake", payload: `{"playback_position":2.25,"interrupted":true,"synchronized_transcript":"heard"}`, position: 2250 * time.Millisecond, interrupted: true, transcript: stringPointer("heard")},
		{name: "camel", payload: `{"playbackPosition":1.5,"interrupted":false,"synchronizedTranscript":"fallback"}`, position: 1500 * time.Millisecond, transcript: stringPointer("fallback")},
		{name: "snake wins", payload: `{"playback_position":1,"playbackPosition":9,"synchronized_transcript":"a","synchronizedTranscript":"b"}`, position: time.Second, transcript: stringPointer("a")},
		{name: "null snake falls back", payload: `{"playback_position":null,"playbackPosition":3}`, position: 3 * time.Second},
		{name: "wrong types", payload: `{"playback_position":"2","interrupted":"false","synchronized_transcript":7}`},
		{name: "negative", payload: `{"playback_position":-1}`},
		{name: "malformed", payload: `{`},
		{name: "array", payload: `[]`},
		{name: "null", payload: `null`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			event := ParsePlaybackFinishedPayload(test.payload)
			if event.PlaybackPosition != test.position || event.Interrupted != test.interrupted || !sameOptionalString(event.SynchronizedTranscript, test.transcript) {
				t.Fatalf("event = %#v", event)
			}
		})
	}
}

func TestMarshalPlaybackFinishedPayloadUsesCanonicalSnakeCase(t *testing.T) {
	transcript := "heard"
	payload, err := MarshalPlaybackFinishedPayload(voice.PlaybackFinishedEvent{
		PlaybackPosition: 1250 * time.Millisecond, Interrupted: true,
		SynchronizedTranscript: &transcript,
	})
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	if err := json.Unmarshal(payload, &raw); err != nil {
		t.Fatal(err)
	}
	if raw["playback_position"] != 1.25 || raw["interrupted"] != true || raw["synchronized_transcript"] != "heard" {
		t.Fatalf("payload = %s", payload)
	}
	if _, ok := raw["playbackPosition"]; ok {
		t.Fatalf("camelCase leaked into %s", payload)
	}
}

func stringPointer(value string) *string { return &value }
func sameOptionalString(left, right *string) bool {
	return left == nil && right == nil || left != nil && right != nil && *left == *right
}
