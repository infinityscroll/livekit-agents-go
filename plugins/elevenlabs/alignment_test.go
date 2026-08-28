// SPDX-License-Identifier: Apache-2.0

package elevenlabs

import (
	"encoding/base64"
	"errors"
	"testing"
	"time"
)

func TestDecodeTTSProviderEventCasingFamilies(t *testing.T) {
	pcm := []byte{1, 0, 2, 0}
	encoded := base64.StdEncoding.EncodeToString(pcm)
	tests := []string{
		`{"context_id":"ctx","audio":"` + encoded + `","is_final":true,"normalized_alignment":{"chars":["H","i"],"char_start_times_ms":[10,20],"char_durations_ms":[10,10]}}`,
		`{"contextId":"ctx","audio":"` + encoded + `","isFinal":true,"normalizedAlignment":{"chars":["H","i"],"charStartTimesMs":[10,20],"charDurationsMs":[10,10]}}`,
		`{"contextId":"ctx","isFinal":true,"alignment":{"chars":["H","i"],"charsStartTimesMs":[10,20],"charsDurationsMs":[10,10]}}`,
	}
	for _, fixture := range tests {
		event, err := decodeTTSProviderEvent([]byte(fixture))
		if err != nil {
			t.Fatalf("decode %s: %v", fixture, err)
		}
		if event.ContextID != "ctx" || !event.Final {
			t.Fatalf("unexpected event: %+v", event)
		}
		if len(event.Audio) != 0 && string(event.Audio) != string(pcm) {
			t.Fatalf("audio mismatch: %v", event.Audio)
		}
	}
}

func TestAlignmentProducesUnicodeTimedWordsAndNormalizesSilence(t *testing.T) {
	state := alignmentState{}
	words, err := state.add(&providerAlignment{
		Chars:      []string{"Hi", " ", "世", "界", " "},
		StartTimes: []int64{100, 120, 130, 150, 170},
		Durations:  []int64{20, 10, 20, 20, 10},
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(words) != 1 || words[0].Text != "Hi " {
		t.Fatalf("unexpected complete words: %+v", words)
	}
	if words[0].StartTime == nil || *words[0].StartTime != 0 {
		t.Fatalf("leading silence was not normalized: %+v", words[0])
	}
	remaining, err := state.add(nil, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(remaining) != 1 || remaining[0].Text != "世界 " {
		t.Fatalf("unexpected flushed words: %+v", remaining)
	}
	if remaining[0].EndTime == nil || *remaining[0].EndTime != 80*time.Millisecond {
		t.Fatalf("unexpected normalized end: %+v", remaining[0])
	}
}

func TestAlignmentRejectsMalformedAndStrictBase64(t *testing.T) {
	_, err := decodeTTSProviderEvent([]byte(`{"context_id":"x","audio":"%%%"}`))
	var protocol *ProtocolError
	if !errors.As(err, &protocol) {
		t.Fatalf("expected protocol error for base64, got %T: %v", err, err)
	}
	_, err = decodeTTSProviderEvent([]byte(`{"context_id":"x","alignment":{"chars":["a"],"char_start_times_ms":[],"char_durations_ms":[1]}}`))
	if !errors.As(err, &protocol) {
		t.Fatalf("expected protocol error for arrays, got %T: %v", err, err)
	}
}
