// SPDX-License-Identifier: Apache-2.0

// Package avatar provides bounded voice I/O and lifecycle primitives for
// LiveKit avatar workers.
package avatar

import (
	"encoding/json"
	"math"
	"time"

	"github.com/infinityscroll/livekit-agents-go/voice"
)

const (
	RPCClearBuffer      = "lk.clear_buffer"
	RPCPlaybackFinished = "lk.playback_finished"
	RPCPlaybackStarted  = "lk.playback_started"
	AudioStreamTopic    = "lk.audio_stream"
)

// ParsePlaybackFinishedPayload is deliberately total: malformed input yields
// a safe zero event so an RPC handler can always release WaitForPlayout. The
// protocol-canonical snake_case fields are preferred, with camelCase accepted
// for JavaScript producers.
func ParsePlaybackFinishedPayload(payload string) voice.PlaybackFinishedEvent {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(payload), &raw); err != nil || raw == nil {
		return voice.PlaybackFinishedEvent{}
	}
	position := decodeFiniteSeconds(firstRaw(raw, "playback_position", "playbackPosition"))
	interrupted := decodeBool(raw["interrupted"])
	transcript, hasTranscript := decodeString(firstRaw(raw, "synchronized_transcript", "synchronizedTranscript"))
	var synchronized *string
	if hasTranscript {
		synchronized = &transcript
	}
	return voice.PlaybackFinishedEvent{
		PlaybackPosition: position, Interrupted: interrupted,
		SynchronizedTranscript: synchronized,
	}
}

// MarshalPlaybackFinishedPayload emits the Python AvatarRunner's canonical
// snake_case protocol with playback_position represented in seconds.
func MarshalPlaybackFinishedPayload(event voice.PlaybackFinishedEvent) ([]byte, error) {
	type wire struct {
		PlaybackPosition       float64 `json:"playback_position"`
		Interrupted            bool    `json:"interrupted"`
		SynchronizedTranscript *string `json:"synchronized_transcript,omitempty"`
	}
	return json.Marshal(wire{
		PlaybackPosition: float64(event.PlaybackPosition) / float64(time.Second),
		Interrupted:      event.Interrupted, SynchronizedTranscript: event.SynchronizedTranscript,
	})
}

func firstRaw(values map[string]json.RawMessage, keys ...string) json.RawMessage {
	for _, key := range keys {
		if value, ok := values[key]; ok && string(value) != "null" {
			return value
		}
	}
	return nil
}

func decodeFiniteSeconds(raw json.RawMessage) time.Duration {
	if len(raw) == 0 {
		return 0
	}
	var value float64
	if json.Unmarshal(raw, &value) != nil || math.IsNaN(value) || math.IsInf(value, 0) || value <= 0 {
		return 0
	}
	maxSeconds := float64(time.Duration(math.MaxInt64)) / float64(time.Second)
	if value >= maxSeconds {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(value * float64(time.Second))
}

func decodeBool(raw json.RawMessage) bool {
	var value bool
	return len(raw) != 0 && json.Unmarshal(raw, &value) == nil && value
}

func decodeString(raw json.RawMessage) (string, bool) {
	var value string
	if len(raw) == 0 || json.Unmarshal(raw, &value) != nil {
		return "", false
	}
	return value, true
}
