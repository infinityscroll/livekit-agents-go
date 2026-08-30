// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/infinityscroll/livekit-agents-go/inference"
)

func TestFreezeReportEventSeversMutablePayload(t *testing.T) {
	created := time.Unix(1_700_000_000, 123_000_000)
	payload := map[string]any{"nested": map[string]any{"value": "before"}}
	event := DebugMessageEvent{EventBase: newEventBase(EventDebugMessage, created), Payload: payload}
	frozen, err := FreezeReportEvent(event)
	if err != nil {
		t.Fatal(err)
	}
	if frozen.Type() != EventDebugMessage || !frozen.Time().Equal(created) || frozen.EncodedSize() == 0 {
		t.Fatalf("invalid frozen metadata: type=%q time=%v size=%d", frozen.Type(), frozen.Time(), frozen.EncodedSize())
	}
	payload["nested"].(map[string]any)["value"] = "after"
	report := CreateSessionReport(SessionReportOptions{Events: []Event{frozen}})
	wire, err := SessionReportToJSON(report)
	if err != nil {
		t.Fatal(err)
	}
	events := wire["events"].([]any)
	nested := events[0].(map[string]any)["payload"].(map[string]any)["nested"].(map[string]any)
	if nested["value"] != "before" {
		t.Fatalf("frozen payload mutated: %#v", nested)
	}
	encoded, err := json.Marshal(frozen)
	if err != nil || len(encoded) != frozen.EncodedSize() {
		t.Fatalf("MarshalJSON size=%d want=%d err=%v", len(encoded), frozen.EncodedSize(), err)
	}
}

func TestAgentSessionReportOptionsUseResolvedDefaults(t *testing.T) {
	session, err := NewAgentSession(AgentSessionOptions[struct{}]{DisableUserAwayTimeout: true})
	if err != nil {
		t.Fatal(err)
	}
	recording := RecordingOptions{Transcript: true, Redaction: true}
	options := session.ReportOptions(recording)
	if options.MaxToolSteps != DefaultMaxToolSteps || options.UserAwayTimeout != nil || options.Recording != recording {
		t.Fatalf("unexpected report options: %#v", options)
	}
}

func TestAgentSessionReportOptionsSeverRuntimeInterruptionState(t *testing.T) {
	session, err := NewAgentSession(AgentSessionOptions[struct{}]{})
	if err != nil {
		t.Fatal(err)
	}
	timeout := 7 * time.Second
	boundary := BackchannelBoundary{Start: 2 * time.Second, End: 3 * time.Second}
	session.opts.turn.Interruption.Detector = &inference.AdaptiveInterruptionDetector{}
	session.opts.turn.Interruption.FalseInterruptionTimeout = &timeout
	session.opts.turn.Interruption.BackchannelBoundary = &boundary

	options := session.ReportOptions(DefaultRecordingOptions())
	if options.Interruption.Detector != nil {
		t.Fatal("report retained runtime interruption detector")
	}
	if options.Interruption.FalseInterruptionTimeout == &timeout || options.Interruption.BackchannelBoundary == &boundary {
		t.Fatal("report retained mutable interruption option pointers")
	}
	*options.Interruption.FalseInterruptionTimeout = time.Second
	options.Interruption.BackchannelBoundary.Start = time.Second
	if *session.opts.turn.Interruption.FalseInterruptionTimeout != timeout || session.opts.turn.Interruption.BackchannelBoundary.Start != boundary.Start {
		t.Fatal("mutating report options changed live session options")
	}
}
