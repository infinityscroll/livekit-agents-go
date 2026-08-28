// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestEventTimestampWireFormat(t *testing.T) {
	event := NewUserStateChangedEvent(UserStateListening, UserStateSpeaking, time.UnixMilli(1_700_000_000_123))
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	wire := string(data)
	for _, fragment := range []string{`"type":"user_state_changed"`, `"createdAt":1700000000123`, `"oldState":"listening"`} {
		if !strings.Contains(wire, fragment) {
			t.Fatalf("missing %s in %s", fragment, wire)
		}
	}
}

func TestDurationEventUsesMilliseconds(t *testing.T) {
	event := EOTPredictionEvent{EventBase: newEventBase(EventEOTPrediction, time.UnixMilli(5)), Probability: .8, Threshold: .7, InferenceDuration: 1250 * time.Microsecond, Delay: 2 * time.Millisecond}
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(data); !strings.Contains(got, `"inferenceDurationMs":1.25`) || !strings.Contains(got, `"delayMs":2`) {
		t.Fatalf("wire = %s", got)
	}
}
