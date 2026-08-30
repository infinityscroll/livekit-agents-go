// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"testing"
	"time"

	agents "github.com/infinityscroll/livekit-agents-go"
)

func TestTurnHandlingDefaults(t *testing.T) {
	legacy := DefaultTurnHandlingOptions(false)
	streaming := DefaultTurnHandlingOptions(true)
	if legacy.Endpointing.MinDelay != 500*time.Millisecond || streaming.Endpointing.MinDelay != 300*time.Millisecond {
		t.Fatalf("endpointing defaults: legacy=%s streaming=%s", legacy.Endpointing.MinDelay, streaming.Endpointing.MinDelay)
	}
	if !legacy.Interruption.Enabled || legacy.Interruption.BackchannelBoundary.Start != time.Second || legacy.PreemptiveGeneration.PreemptiveTTS {
		t.Fatalf("turn defaults = %+v", legacy)
	}
}

func TestTurnDetectionTriState(t *testing.T) {
	omitted := TurnHandlingOptions{}
	disabled := TurnHandlingOptions{TurnDetection: agents.Disable[TurnDetection]()}
	mode := TurnHandlingOptions{TurnDetection: UseTurnDetectionMode(TurnDetectionManual)}
	_, modeSet := mode.TurnDetection.Value()
	if !omitted.TurnDetection.IsInherited() || !disabled.TurnDetection.IsDisabled() || !modeSet {
		t.Fatal("turn detection tri-state collapsed")
	}
}

func TestUserTurnAccumulatorOnlyResetsExplicitly(t *testing.T) {
	maxWords := 3
	start := time.Unix(0, 0)
	var accumulator UserTurnAccumulator
	accumulator.Add(start, "one two", 2)
	accumulator.Add(start.Add(time.Second), "three", 1)
	if !accumulator.Exceeded(start.Add(time.Second), UserTurnLimitOptions{MaxWords: &maxWords}) {
		t.Fatal("word limit not exceeded")
	}
	accumulator.Reset()
	if accumulator.Exceeded(start.Add(2*time.Second), UserTurnLimitOptions{MaxWords: &maxWords}) {
		t.Fatal("reset did not clear limit")
	}
}
