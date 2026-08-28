// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"
	"unicode"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/llm"
	"github.com/livekit/agents-go/metrics"
)

// RecordingOptions is the fully resolved, granular recording policy shared by
// reports and telemetry. Use DefaultRecordingOptions or DisabledRecordingOptions
// rather than relying on the zero value when constructing a policy directly.
type RecordingOptions struct {
	Audio      bool `json:"audio"`
	Traces     bool `json:"traces"`
	Logs       bool `json:"logs"`
	Transcript bool `json:"transcript"`
	Redaction  bool `json:"redaction"`
}

func DefaultRecordingOptions() RecordingOptions {
	return RecordingOptions{Audio: true, Traces: true, Logs: true, Transcript: true}
}

func DisabledRecordingOptions() RecordingOptions { return RecordingOptions{} }

// RecordingOptionsUpdate mirrors the sparse TypeScript/Python recording
// object. Omitted fields inherit the all-on policy.
type RecordingOptionsUpdate struct {
	Audio      *bool
	Traces     *bool
	Logs       *bool
	Transcript *bool
	Redaction  *bool
}

func ResolveRecordingOptions(update RecordingOptionsUpdate) RecordingOptions {
	result := DefaultRecordingOptions()
	if update.Audio != nil {
		result.Audio = *update.Audio
	}
	if update.Traces != nil {
		result.Traces = *update.Traces
	}
	if update.Logs != nil {
		result.Logs = *update.Logs
	}
	if update.Transcript != nil {
		result.Transcript = *update.Transcript
	}
	if update.Redaction != nil {
		result.Redaction = *update.Redaction
	}
	return result
}

func (o RecordingOptions) Enabled() bool {
	return o.Audio || o.Traces || o.Logs || o.Transcript
}

// ReportSessionOptions is the subset of AgentSession options carried in the
// cross-SDK session report wire contract.
type ReportSessionOptions struct {
	Interruption         InterruptionOptions
	Endpointing          EndpointingOptions
	MaxToolSteps         int
	UserAwayTimeout      *time.Duration
	PreemptiveGeneration PreemptiveGenerationOptions
	Recording            RecordingOptions
}

// ReportSessionOptionsFromAgent converts public session options without
// starting models, allocating RTC resources, or changing ownership.
func ReportSessionOptionsFromAgent[UserData any](options AgentSessionOptions[UserData], recording RecordingOptions) ReportSessionOptions {
	turn := DefaultTurnHandlingOptions(false)
	if options.TurnHandling != nil {
		turn = *cloneTurnHandling(options.TurnHandling)
	}
	maxToolSteps := options.MaxToolSteps
	if maxToolSteps == 0 {
		maxToolSteps = DefaultMaxToolSteps
	}
	var userAway *time.Duration
	if !options.DisableUserAwayTimeout {
		value := optionsValue(options.UserAwayTimeout, DefaultUserAwayTimeout)
		userAway = &value
	}
	return ReportSessionOptions{
		Interruption: turn.Interruption, Endpointing: turn.Endpointing,
		MaxToolSteps: maxToolSteps, UserAwayTimeout: userAway,
		PreemptiveGeneration: turn.PreemptiveGeneration, Recording: recording,
	}
}

type SessionReport struct {
	JobID                   string
	RoomID                  string
	Room                    string
	Options                 ReportSessionOptions
	Events                  []Event
	ChatHistory             *llm.ChatContext
	EnableRecording         bool
	StartedAt               time.Time
	Timestamp               time.Time
	AudioRecordingPath      string
	AudioRecordingStartedAt *time.Time
	Duration                time.Duration
	ModelUsage              []metrics.ModelUsage
}

type SessionReportOptions struct {
	JobID                   string
	RoomID                  string
	Room                    string
	Options                 ReportSessionOptions
	Events                  []Event
	ChatHistory             *llm.ChatContext
	EnableRecording         bool
	StartedAt               time.Time
	Timestamp               time.Time
	AudioRecordingPath      string
	AudioRecordingStartedAt *time.Time
	ModelUsage              []metrics.ModelUsage
}

func CreateSessionReport(options SessionReportOptions) *SessionReport {
	now := options.Timestamp
	if now.IsZero() {
		now = time.Now()
	}
	startedAt := options.StartedAt
	if startedAt.IsZero() {
		startedAt = now
	}
	chat := options.ChatHistory
	if chat == nil {
		chat = llm.EmptyChatContext()
	} else {
		chat = chat.Copy(llm.CopyOptions{})
	}
	report := &SessionReport{
		JobID: options.JobID, RoomID: options.RoomID, Room: options.Room,
		Options: options.Options, Events: append([]Event(nil), options.Events...), ChatHistory: chat,
		EnableRecording: options.EnableRecording, StartedAt: startedAt, Timestamp: now,
		AudioRecordingPath: options.AudioRecordingPath, ModelUsage: cloneModelUsage(options.ModelUsage),
	}
	if options.AudioRecordingStartedAt != nil {
		started := *options.AudioRecordingStartedAt
		report.AudioRecordingStartedAt = &started
		report.Duration = max(0, now.Sub(started))
	}
	return report
}

func cloneModelUsage(input []metrics.ModelUsage) []metrics.ModelUsage {
	if input == nil {
		return nil
	}
	result := make([]metrics.ModelUsage, 0, len(input))
	for _, usage := range input {
		switch value := usage.(type) {
		case *metrics.LLMUsage:
			if value != nil {
				result = append(result, *value)
			}
		case *metrics.TTSUsage:
			if value != nil {
				result = append(result, *value)
			}
		case *metrics.STTUsage:
			if value != nil {
				result = append(result, *value)
			}
		case *metrics.RequestUsage:
			if value != nil {
				result = append(result, *value)
			}
		case nil:
		default:
			result = append(result, value)
		}
	}
	return result
}

func (r *SessionReport) MarshalJSON() ([]byte, error) {
	wire, err := SessionReportToJSON(r)
	if err != nil {
		return nil, err
	}
	return json.Marshal(wire)
}

// SessionReportToJSON emits the pinned agents-js/Python-compatible report
// shape used by LiveKit Cloud observability.
func SessionReportToJSON(report *SessionReport) (map[string]any, error) {
	if report == nil {
		return nil, errors.New("session report is nil")
	}
	events := make([]any, 0, len(report.Events))
	for index, event := range report.Events {
		if event == nil {
			return nil, fmt.Errorf("session report event %d is nil", index)
		}
		if event.Type() == EventMetricsCollected || event.Type() == EventSessionUsageUpdated {
			continue
		}
		wire, err := eventToReportJSON(event)
		if err != nil {
			return nil, fmt.Errorf("serialize session report event %d (%s): %w", index, event.Type(), err)
		}
		events = append(events, wire)
	}
	chatHistory, err := chatHistoryToReportJSON(report.ChatHistory)
	if err != nil {
		return nil, fmt.Errorf("serialize session report chat history: %w", err)
	}
	usage := any(nil)
	if report.ModelUsage != nil {
		items := make([]any, 0, len(report.ModelUsage))
		for index, modelUsage := range report.ModelUsage {
			if modelUsage == nil || isNilInterface(modelUsage) {
				return nil, fmt.Errorf("serialize session report usage %d: value is nil", index)
			}
			items = append(items, modelUsageToReportJSON(modelUsage))
		}
		usage = items
	}
	var recordingStarted any
	if report.AudioRecordingStartedAt != nil {
		recordingStarted = report.AudioRecordingStartedAt.UnixMilli()
	}
	var audioPath any
	if report.AudioRecordingPath != "" {
		audioPath = report.AudioRecordingPath
	}
	return map[string]any{
		"job_id":                     report.JobID,
		"room_id":                    report.RoomID,
		"room":                       report.Room,
		"events":                     events,
		"audio_recording_path":       audioPath,
		"audio_recording_started_at": recordingStarted,
		"options": map[string]any{
			"allow_interruptions":              report.Options.Interruption.Enabled,
			"discard_audio_if_uninterruptible": report.Options.Interruption.DiscardAudioIfUninterruptible,
			"min_interruption_duration":        durationSecondsReport(report.Options.Interruption.MinDuration),
			"min_interruption_words":           report.Options.Interruption.MinWords,
			"min_endpointing_delay":            durationSecondsReport(report.Options.Endpointing.MinDelay),
			"max_endpointing_delay":            durationSecondsReport(report.Options.Endpointing.MaxDelay),
			"max_tool_steps":                   report.Options.MaxToolSteps,
			"user_away_timeout":                optionalDurationSeconds(report.Options.UserAwayTimeout),
			"preemptive_generation": map[string]any{
				"enabled":             report.Options.PreemptiveGeneration.Enabled,
				"preemptive_tts":      report.Options.PreemptiveGeneration.PreemptiveTTS,
				"max_speech_duration": durationSecondsReport(report.Options.PreemptiveGeneration.MaxSpeechDuration),
				"max_retries":         report.Options.PreemptiveGeneration.MaxRetries,
			},
			"recording_options": map[string]any{
				"audio": report.Options.Recording.Audio, "traces": report.Options.Recording.Traces,
				"logs": report.Options.Recording.Logs, "transcript": report.Options.Recording.Transcript,
				"redaction": report.Options.Recording.Redaction,
			},
		},
		"chat_history":              chatHistory,
		"enable_user_data_training": report.EnableRecording,
		"timestamp":                 unixMillisecondsReport(report.Timestamp),
		"usage":                     usage,
		"sdk_version":               agents.Version,
	}, nil
}

func eventToReportJSON(event Event) (map[string]any, error) {
	if frozen, ok := event.(*FrozenReportEvent); ok {
		return frozen.reportJSON()
	}
	data, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}
	var native map[string]any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&native); err != nil {
		return nil, err
	}
	switch value := event.(type) {
	case ErrorEvent:
		native["error"] = serializeReportError(value.Err)
		native["source"] = serializeReportSource(value.Source)
	case *ErrorEvent:
		if value != nil {
			native["error"] = serializeReportError(value.Err)
			native["source"] = serializeReportSource(value.Source)
		}
	case CloseEvent:
		native["error"] = serializeReportError(value.Err)
	case *CloseEvent:
		if value != nil {
			native["error"] = serializeReportError(value.Err)
		}
	case OverlappingSpeechEvent:
		native = overlappingSpeechReportJSON(value)
	case *OverlappingSpeechEvent:
		if value != nil {
			native = overlappingSpeechReportJSON(*value)
		}
	case UserTurnExceededEvent:
		native["duration"] = durationMillisecondsReport(value.Duration)
	case *UserTurnExceededEvent:
		if value != nil {
			native["duration"] = durationMillisecondsReport(value.Duration)
		}
	}
	converted, err := toSnakeCaseDeep(native, false)
	if err != nil {
		return nil, err
	}
	result, ok := converted.(map[string]any)
	if !ok {
		return nil, errors.New("event serialization did not produce an object")
	}
	return result, nil
}

func overlappingSpeechReportJSON(event OverlappingSpeechEvent) map[string]any {
	result := map[string]any{
		"type":                  event.Type(),
		"createdAt":             event.EventBase.CreatedAt,
		"detectedAt":            unixMillisecondsReport(event.DetectedAt),
		"isInterruption":        event.Interruption,
		"totalDurationInS":      durationSecondsReport(event.TotalDuration),
		"predictionDurationInS": durationSecondsReport(event.PredictionDuration),
		"detectionDelayInS":     durationSecondsReport(event.DetectionDelay),
		"probability":           event.Probability,
		"numRequests":           event.NumRequests,
	}
	if event.AgentEnded != nil {
		result["agentEnded"] = *event.AgentEnded
	}
	if event.OverlapStartedAt != nil {
		result["overlapStartedAt"] = unixMillisecondsReport(*event.OverlapStartedAt)
	}
	if len(event.Probabilities) != 0 {
		result["probabilities"] = append([]float64(nil), event.Probabilities...)
	}
	return result
}

type modelProvider interface {
	Model() string
	Provider() string
}

func serializeReportSource(source any) any {
	if source == nil || isNilInterface(source) {
		return nil
	}
	if model, ok := source.(modelProvider); ok {
		return map[string]any{"model": safeModelValue(model.Model()), "provider": safeModelValue(model.Provider())}
	}
	return fmt.Sprint(source)
}

func safeModelValue(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func serializeReportError(err error) any {
	if err == nil || isNilInterface(err) {
		return nil
	}
	return err.Error()
}

func isNilInterface(value any) bool {
	if value == nil {
		return true
	}
	ref := reflect.ValueOf(value)
	switch ref.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return ref.IsNil()
	default:
		return false
	}
}

func chatHistoryToReportJSON(context *llm.ChatContext) (any, error) {
	if context == nil {
		context = llm.EmptyChatContext()
	}
	data, err := context.ToJSON(llm.ChatContextJSONOptions{ExcludeTimestamp: agents.Use(false)})
	if err != nil {
		return nil, err
	}
	var native map[string]any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&native); err != nil {
		return nil, err
	}
	if items, ok := native["items"].([]any); ok {
		for _, raw := range items {
			item, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			milliseconds, ok := jsonNumberFloat(item["createdAt"])
			if ok {
				item["createdAt"] = milliseconds / 1000
			}
		}
	}
	return toSnakeCaseDeep(native, false)
}

func jsonNumberFloat(value any) (float64, bool) {
	switch number := value.(type) {
	case json.Number:
		parsed, err := number.Float64()
		return parsed, err == nil
	case float64:
		return number, true
	case int64:
		return float64(number), true
	case int:
		return float64(number), true
	default:
		return 0, false
	}
}

func modelUsageToReportJSON(usage metrics.ModelUsage) map[string]any {
	filtered := metrics.FilterZeroValues(usage)
	result := make(map[string]any, len(filtered))
	for key, value := range filtered {
		switch key {
		case "sessionDurationMs":
			if milliseconds, ok := jsonNumberFloat(value); ok {
				result["session_duration"] = milliseconds / 1000
			}
		case "audioDurationMs":
			if milliseconds, ok := jsonNumberFloat(value); ok {
				result["audio_duration"] = milliseconds / 1000
			}
		default:
			result[pythonFieldName(key)] = value
		}
	}
	return result
}

// ToSnakeCaseDeep recursively converts JSON-visible camelCase fields to the
// Python wire names. Provider-owned keys inside an "extra" object are kept
// verbatim. The special chat-call field "args" becomes "arguments".
func ToSnakeCaseDeep(value any) (any, error) { return toSnakeCaseDeep(value, false) }

func toSnakeCaseDeep(value any, preserveKeys bool) (any, error) {
	if value == nil {
		return nil, nil
	}
	switch current := value.(type) {
	case map[string]any:
		result := make(map[string]any, len(current))
		for key, child := range current {
			converted, err := toSnakeCaseDeep(child, preserveKeys || key == "extra")
			if err != nil {
				return nil, err
			}
			outputKey := key
			if !preserveKeys {
				outputKey = pythonFieldName(key)
			}
			result[outputKey] = converted
		}
		return result, nil
	case []any:
		result := make([]any, len(current))
		for index, child := range current {
			converted, err := toSnakeCaseDeep(child, preserveKeys)
			if err != nil {
				return nil, err
			}
			result[index] = converted
		}
		return result, nil
	case json.RawMessage:
		return normalizeJSONValue(current, preserveKeys)
	case json.Number, string, bool, float64, float32, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return current, nil
	default:
		return normalizeJSONValue(current, preserveKeys)
	}
}

func normalizeJSONValue(value any, preserveKeys bool) (any, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var normalized any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&normalized); err != nil {
		return nil, err
	}
	return toSnakeCaseDeep(normalized, preserveKeys)
}

func pythonFieldName(key string) string {
	if key == "args" {
		return "arguments"
	}
	var builder strings.Builder
	builder.Grow(len(key) + 4)
	for index, r := range key {
		if unicode.IsUpper(r) {
			if index > 0 {
				builder.WriteByte('_')
			}
			builder.WriteRune(unicode.ToLower(r))
			continue
		}
		builder.WriteRune(r)
	}
	return builder.String()
}

func durationSecondsReport(value time.Duration) float64 {
	return float64(value) / float64(time.Second)
}

func durationMillisecondsReport(value time.Duration) float64 {
	return float64(value) / float64(time.Millisecond)
}

func optionalDurationSeconds(value *time.Duration) any {
	if value == nil {
		return nil
	}
	return durationSecondsReport(*value)
}

func unixMillisecondsReport(value time.Time) int64 {
	if value.IsZero() {
		return 0
	}
	return value.UnixMilli()
}
