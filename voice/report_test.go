// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/llm"
	"github.com/livekit/agents-go/metrics"
)

func TestSessionReportWireCompatibility(t *testing.T) {
	t.Parallel()
	createdAt := time.UnixMilli(1_734_000_123_456)
	userAway := 15 * time.Second
	recordingStarted := createdAt.Add(-4 * time.Second)
	chat := llm.NewChatContext(
		&llm.ChatMessage{ID: "m", Role: llm.RoleUser, Content: []llm.Content{llm.TextContent("hello")}, CreatedAt: createdAt, Extra: map[string]any{"providerKey": "keep"}},
		&llm.FunctionCall{ID: "c", CallID: "call", Name: "tool", Arguments: `{}`, CreatedAt: createdAt},
	)
	report := CreateSessionReport(SessionReportOptions{
		JobID: "job", RoomID: "RM_1", Room: "support", ChatHistory: chat,
		Options: ReportSessionOptions{
			Interruption: DefaultInterruptionOptions(), Endpointing: DefaultEndpointingOptions,
			MaxToolSteps: 3, UserAwayTimeout: &userAway,
			PreemptiveGeneration: DefaultPreemptiveGenerationOptions,
			Recording:            DefaultRecordingOptions(),
		},
		Events: []Event{
			NewUserStateChangedEvent(UserStateListening, UserStateSpeaking, createdAt),
			MetricsCollectedEvent{EventBase: newEventBase(EventMetricsCollected, createdAt), Metrics: metrics.LLM{}},
			NewErrorEvent(errors.New("provider unavailable"), reportTestModel{}, createdAt),
			NewCloseEvent(CloseReasonError, errors.New("closed"), createdAt),
		},
		EnableRecording: true, Timestamp: createdAt, AudioRecordingPath: "/tmp/audio.ogg",
		AudioRecordingStartedAt: &recordingStarted,
		ModelUsage: []metrics.ModelUsage{
			metrics.LLMUsage{Provider: "openai", Model: "gpt", InputTokens: 3, SessionDuration: 1500 * time.Millisecond},
			metrics.TTSUsage{Provider: "elevenlabs", Model: "flash", AudioDuration: 2 * time.Second},
		},
	})
	data, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, required := range []string{
		`"job_id":"job"`, `"room_id":"RM_1"`, `"enable_user_data_training":true`,
		`"audio_recording_started_at":1734000119456`, `"timestamp":1734000123456`,
		`"created_at":1734000123.456`, `"arguments":"{}"`, `"providerKey":"keep"`,
		`"session_duration":1.5`, `"audio_duration":2`, `"error":"provider unavailable"`,
		`"source":{"model":"model","provider":"provider"}`,
	} {
		if !strings.Contains(text, required) {
			t.Fatalf("report missing %q: %s", required, text)
		}
	}
	for _, absent := range []string{`metrics_collected`, `session_usage_updated`, `speech_handle`, `speech_input`, `session_duration_ms`, `audio_duration_ms`} {
		if strings.Contains(text, absent) {
			t.Fatalf("report unexpectedly contains %q: %s", absent, text)
		}
	}
	if report.Duration != 4*time.Second {
		t.Fatalf("duration = %s", report.Duration)
	}
}

func TestToSnakeCaseDeepPreservesExtraKeysAndRenamesArgs(t *testing.T) {
	t.Parallel()
	converted, err := ToSnakeCaseDeep(map[string]any{
		"oldState": "idle",
		"args":     `{}`,
		"extra": map[string]any{
			"providerKey": map[string]any{"nestedKey": true},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(converted)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	for _, expected := range []string{`"old_state"`, `"arguments"`, `"providerKey"`, `"nestedKey"`} {
		if !strings.Contains(text, expected) {
			t.Fatalf("converted JSON missing %s: %s", expected, text)
		}
	}
	if strings.Contains(text, "provider_key") || strings.Contains(text, "nested_key") {
		t.Fatalf("extra keys were rewritten: %s", text)
	}
}

func TestResolveRecordingOptionsSparseAllOn(t *testing.T) {
	t.Parallel()
	no := false
	resolved := ResolveRecordingOptions(RecordingOptionsUpdate{Logs: &no})
	if !resolved.Audio || resolved.Logs || !resolved.Traces || !resolved.Transcript || resolved.Redaction {
		t.Fatalf("unexpected recording options: %#v", resolved)
	}
	if !resolved.Enabled() || DisabledRecordingOptions().Enabled() {
		t.Fatal("recording Enabled result is wrong")
	}
}

type reportTestModel struct{}

func (reportTestModel) Model() string    { return "model" }
func (reportTestModel) Provider() string { return "provider" }

func TestReportSessionOptionsFromAgentUsesResolvedDefaults(t *testing.T) {
	t.Parallel()
	options := ReportSessionOptionsFromAgent(AgentSessionOptions[struct{}]{}, DefaultRecordingOptions())
	if options.MaxToolSteps != DefaultMaxToolSteps || options.UserAwayTimeout == nil || *options.UserAwayTimeout != DefaultUserAwayTimeout {
		t.Fatalf("unexpected defaults: %#v", options)
	}
	disabled := ReportSessionOptionsFromAgent(AgentSessionOptions[struct{}]{DisableUserAwayTimeout: true}, DisabledRecordingOptions())
	if disabled.UserAwayTimeout != nil {
		t.Fatalf("disabled user-away timeout = %v", *disabled.UserAwayTimeout)
	}
	if agents.Version == "" {
		t.Fatal("SDK version is empty")
	}
}
