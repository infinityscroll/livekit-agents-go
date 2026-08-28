// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"encoding/json"
	"time"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/llm"
	"github.com/livekit/agents-go/metrics"
)

type EventType string

const (
	EventUserInputTranscribed     EventType = "user_input_transcribed"
	EventUserTranscriptionTimeout EventType = "user_transcription_timeout"
	EventAgentStateChanged        EventType = "agent_state_changed"
	EventUserStateChanged         EventType = "user_state_changed"
	EventConversationItemAdded    EventType = "conversation_item_added"
	EventFunctionToolsExecuted    EventType = "function_tools_executed"
	EventMetricsCollected         EventType = "metrics_collected"
	EventSessionUsageUpdated      EventType = "session_usage_updated"
	EventDebugMessage             EventType = "debug_message"
	EventSpeechCreated            EventType = "speech_created"
	EventAgentFalseInterruption   EventType = "agent_false_interruption"
	EventOverlappingSpeech        EventType = "overlapping_speech"
	EventEOTPrediction            EventType = "eot_prediction"
	EventError                    EventType = "error"
	EventClose                    EventType = "close"
)

type UserState string

const (
	UserStateSpeaking  UserState = "speaking"
	UserStateListening UserState = "listening"
	UserStateAway      UserState = "away"
)

type AgentState string

const (
	AgentStateInitializing AgentState = "initializing"
	AgentStateIdle         AgentState = "idle"
	AgentStateListening    AgentState = "listening"
	AgentStateThinking     AgentState = "thinking"
	AgentStateSpeaking     AgentState = "speaking"
)

type CloseReason string

const (
	CloseReasonError                   CloseReason = "error"
	CloseReasonJobShutdown             CloseReason = "job_shutdown"
	CloseReasonParticipantDisconnected CloseReason = "participant_disconnected"
	CloseReasonUserInitiated           CloseReason = "user_initiated"
)

type SpeechSource string

const (
	SpeechSourceSay           SpeechSource = "say"
	SpeechSourceGenerateReply SpeechSource = "generate_reply"
	SpeechSourceToolResponse  SpeechSource = "tool_response"
)

// EventTimestamp preserves the epoch-millisecond wire contract while exposing
// a time.Time conversion to Go callers.
type EventTimestamp time.Time

func NewEventTimestamp(value time.Time) EventTimestamp { return EventTimestamp(value) }
func (t EventTimestamp) Time() time.Time               { return time.Time(t) }
func (t EventTimestamp) MarshalJSON() ([]byte, error)  { return json.Marshal(time.Time(t).UnixMilli()) }
func (t *EventTimestamp) UnmarshalJSON(data []byte) error {
	var milliseconds int64
	if err := json.Unmarshal(data, &milliseconds); err != nil {
		return err
	}
	*t = EventTimestamp(time.UnixMilli(milliseconds))
	return nil
}

type Event interface {
	Type() EventType
	Time() time.Time
}

type EventBase struct {
	Kind      EventType      `json:"type"`
	CreatedAt EventTimestamp `json:"createdAt"`
}

func newEventBase(kind EventType, createdAt time.Time) EventBase {
	if createdAt.IsZero() {
		createdAt = time.Now()
	}
	return EventBase{Kind: kind, CreatedAt: NewEventTimestamp(createdAt)}
}
func (e EventBase) Type() EventType { return e.Kind }
func (e EventBase) Time() time.Time { return e.CreatedAt.Time() }

type UserStateChangedEvent struct {
	EventBase
	OldState UserState `json:"oldState"`
	NewState UserState `json:"newState"`
}

func NewUserStateChangedEvent(oldState, newState UserState, createdAt time.Time) UserStateChangedEvent {
	return UserStateChangedEvent{EventBase: newEventBase(EventUserStateChanged, createdAt), OldState: oldState, NewState: newState}
}

type AgentStateChangedEvent struct {
	EventBase
	OldState AgentState `json:"oldState"`
	NewState AgentState `json:"newState"`
}

func NewAgentStateChangedEvent(oldState, newState AgentState, createdAt time.Time) AgentStateChangedEvent {
	return AgentStateChangedEvent{EventBase: newEventBase(EventAgentStateChanged, createdAt), OldState: oldState, NewState: newState}
}

type UserInputTranscribedEvent struct {
	EventBase
	Transcript string               `json:"transcript"`
	Final      bool                 `json:"isFinal"`
	ItemID     *string              `json:"itemId"`
	SpeakerID  *string              `json:"speakerId"`
	Language   *agents.LanguageCode `json:"language"`
}

type UserTranscriptionTimeoutEvent struct {
	EventBase
	SpeechDuration     time.Duration `json:"-"`
	VADSpeechStartedAt time.Time     `json:"-"`
}

func (e UserTranscriptionTimeoutEvent) MarshalJSON() ([]byte, error) {
	type wire struct {
		EventBase
		SpeechDuration     float64 `json:"speechDuration"`
		VADSpeechStartedAt int64   `json:"vadSpeechStartedAt"`
	}
	return json.Marshal(wire{EventBase: e.EventBase, SpeechDuration: float64(e.SpeechDuration) / float64(time.Millisecond), VADSpeechStartedAt: e.VADSpeechStartedAt.UnixMilli()})
}

type MetricsCollectedEvent struct {
	EventBase
	Metrics metrics.Metric `json:"metrics"`
}

type AgentSessionUsage struct {
	ModelUsage []metrics.ModelUsage `json:"modelUsage"`
}

type SessionUsageUpdatedEvent struct {
	EventBase
	Usage AgentSessionUsage `json:"usage"`
}

type ConversationItemAddedEvent struct {
	EventBase
	Item llm.ChatItem `json:"item"`
}

type FunctionToolsExecutedEvent struct {
	EventBase
	FunctionCalls       []*llm.FunctionCall       `json:"functionCalls"`
	FunctionCallOutputs []*llm.FunctionCallOutput `json:"functionCallOutputs"`
}

func (e FunctionToolsExecutedEvent) Pairs() [][2]llm.ChatItem {
	length := min(len(e.FunctionCalls), len(e.FunctionCallOutputs))
	result := make([][2]llm.ChatItem, length)
	for index := 0; index < length; index++ {
		result[index] = [2]llm.ChatItem{e.FunctionCalls[index], e.FunctionCallOutputs[index]}
	}
	return result
}

type SpeechCreatedEvent struct {
	EventBase
	UserInitiated bool          `json:"userInitiated"`
	Source        SpeechSource  `json:"source"`
	SpeechHandle  *SpeechHandle `json:"-"`
}

type EOTPredictionEvent struct {
	EventBase
	Probability       float64       `json:"probability"`
	Threshold         float64       `json:"threshold"`
	InferenceDuration time.Duration `json:"-"`
	Delay             time.Duration `json:"-"`
}

func (e EOTPredictionEvent) MarshalJSON() ([]byte, error) {
	type wire struct {
		EventBase
		Probability       float64 `json:"probability"`
		Threshold         float64 `json:"threshold"`
		InferenceDuration float64 `json:"inferenceDurationMs"`
		Delay             float64 `json:"delayMs"`
	}
	return json.Marshal(wire{EventBase: e.EventBase, Probability: e.Probability, Threshold: e.Threshold,
		InferenceDuration: float64(e.InferenceDuration) / float64(time.Millisecond), Delay: float64(e.Delay) / float64(time.Millisecond)})
}

type UserTurnExceededEvent struct {
	EventBase
	Transcript            string        `json:"transcript"`
	AccumulatedTranscript string        `json:"accumulatedTranscript"`
	AccumulatedWordCount  int           `json:"accumulatedWordCount"`
	Duration              time.Duration `json:"-"`
}

type ErrorEvent struct {
	EventBase
	Err    error `json:"-"`
	Source any   `json:"-"`
}

type CloseEvent struct {
	EventBase
	Err    error       `json:"-"`
	Reason CloseReason `json:"reason"`
}

type AgentFalseInterruptionEvent struct {
	EventBase
	Resumed bool `json:"resumed"`
}

type OverlappingSpeechEvent struct {
	EventBase
	DetectedAt         time.Time     `json:"-"`
	Interruption       bool          `json:"isInterruption"`
	AgentEnded         *bool         `json:"agentEnded,omitempty"`
	TotalDuration      time.Duration `json:"-"`
	PredictionDuration time.Duration `json:"-"`
	DetectionDelay     time.Duration `json:"-"`
	OverlapStartedAt   *time.Time    `json:"-"`
	SpeechInput        []int16       `json:"-"`
	Probabilities      []float64     `json:"probabilities,omitempty"`
	Probability        float64       `json:"probability"`
	NumRequests        int           `json:"numRequests"`
}

type DebugMessageEvent struct {
	EventBase
	Payload map[string]any `json:"payload"`
}

func NewErrorEvent(err error, source any, createdAt time.Time) ErrorEvent {
	return ErrorEvent{EventBase: newEventBase(EventError, createdAt), Err: err, Source: source}
}
func NewCloseEvent(reason CloseReason, err error, createdAt time.Time) CloseEvent {
	return CloseEvent{EventBase: newEventBase(EventClose, createdAt), Err: err, Reason: reason}
}
func NewSpeechCreatedEvent(handle *SpeechHandle, source SpeechSource, userInitiated bool, createdAt time.Time) SpeechCreatedEvent {
	return SpeechCreatedEvent{EventBase: newEventBase(EventSpeechCreated, createdAt), UserInitiated: userInitiated, Source: source, SpeechHandle: handle}
}
