// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"encoding/json"
	"time"
)

type Kind string

const (
	KindLLM          Kind = "llm_metrics"
	KindSTT          Kind = "stt_metrics"
	KindTTS          Kind = "tts_metrics"
	KindVAD          Kind = "vad_metrics"
	KindEOU          Kind = "eou_metrics"
	KindEOTInference Kind = "eot_inference_metrics"
	KindRealtime     Kind = "realtime_model_metrics"
	KindInterruption Kind = "interruption_metrics"
	KindAvatar       Kind = "avatar_metrics"
)

type Metadata struct {
	ModelProvider string `json:"modelProvider,omitempty"`
	ModelName     string `json:"modelName,omitempty"`
}

func (m Metadata) empty() bool { return m.ModelProvider == "" && m.ModelName == "" }

func metadataOrNil(m Metadata) *Metadata {
	if m.empty() {
		return nil
	}
	return &m
}

func durationMilliseconds(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

func unixMilliseconds(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

type Metric interface {
	MetricKind() Kind
	MetricTime() time.Time
	MetricMetadata() Metadata
}

type LLM struct {
	Label               string
	RequestID           string
	Timestamp           time.Time
	Duration            time.Duration
	TimeToFirstToken    time.Duration
	Cancelled           bool
	CompletionTokens    int64
	PromptTokens        int64
	PromptCachedTokens  int64
	CacheCreationTokens int64
	TotalTokens         int64
	TokensPerSecond     float64
	SpeechID            string
	Metadata            Metadata
}

func (m LLM) MetricKind() Kind         { return KindLLM }
func (m LLM) MetricTime() time.Time    { return m.Timestamp }
func (m LLM) MetricMetadata() Metadata { return m.Metadata }

func (m LLM) MarshalJSON() ([]byte, error) {
	type wire struct {
		Type                Kind      `json:"type"`
		Label               string    `json:"label"`
		RequestID           string    `json:"requestId"`
		Timestamp           int64     `json:"timestamp"`
		DurationMS          float64   `json:"durationMs"`
		TTFTMS              float64   `json:"ttftMs"`
		Cancelled           bool      `json:"cancelled"`
		CompletionTokens    int64     `json:"completionTokens"`
		PromptTokens        int64     `json:"promptTokens"`
		PromptCachedTokens  int64     `json:"promptCachedTokens"`
		CacheCreationTokens *int64    `json:"cacheCreationTokens,omitempty"`
		TotalTokens         int64     `json:"totalTokens"`
		TokensPerSecond     float64   `json:"tokensPerSecond"`
		SpeechID            string    `json:"speechId,omitempty"`
		Metadata            *Metadata `json:"metadata,omitempty"`
	}
	var cacheCreationTokens *int64
	if m.CacheCreationTokens != 0 {
		cacheCreationTokens = &m.CacheCreationTokens
	}
	return json.Marshal(wire{
		Type: KindLLM, Label: m.Label, RequestID: m.RequestID,
		Timestamp: unixMilliseconds(m.Timestamp), DurationMS: durationMilliseconds(m.Duration),
		TTFTMS: durationMilliseconds(m.TimeToFirstToken), Cancelled: m.Cancelled,
		CompletionTokens: m.CompletionTokens, PromptTokens: m.PromptTokens,
		PromptCachedTokens: m.PromptCachedTokens, CacheCreationTokens: cacheCreationTokens,
		TotalTokens: m.TotalTokens, TokensPerSecond: m.TokensPerSecond,
		SpeechID: m.SpeechID, Metadata: metadataOrNil(m.Metadata),
	})
}

type STT struct {
	Label         string
	RequestID     string
	Timestamp     time.Time
	Duration      time.Duration
	AudioDuration time.Duration
	InputTokens   int64
	OutputTokens  int64
	Streamed      bool
	Metadata      Metadata
}

func (m STT) MetricKind() Kind         { return KindSTT }
func (m STT) MetricTime() time.Time    { return m.Timestamp }
func (m STT) MetricMetadata() Metadata { return m.Metadata }

func (m STT) MarshalJSON() ([]byte, error) {
	type wire struct {
		Type            Kind      `json:"type"`
		Label           string    `json:"label"`
		RequestID       string    `json:"requestId"`
		Timestamp       int64     `json:"timestamp"`
		DurationMS      float64   `json:"durationMs"`
		AudioDurationMS float64   `json:"audioDurationMs"`
		InputTokens     int64     `json:"inputTokens,omitempty"`
		OutputTokens    int64     `json:"outputTokens,omitempty"`
		Streamed        bool      `json:"streamed"`
		Metadata        *Metadata `json:"metadata,omitempty"`
	}
	return json.Marshal(wire{
		Type: KindSTT, Label: m.Label, RequestID: m.RequestID,
		Timestamp: unixMilliseconds(m.Timestamp), DurationMS: durationMilliseconds(m.Duration),
		AudioDurationMS: durationMilliseconds(m.AudioDuration), InputTokens: m.InputTokens,
		OutputTokens: m.OutputTokens, Streamed: m.Streamed, Metadata: metadataOrNil(m.Metadata),
	})
}

type TTS struct {
	Label           string
	RequestID       string
	Timestamp       time.Time
	TimeToFirstByte time.Duration
	Duration        time.Duration
	AudioDuration   time.Duration
	Cancelled       bool
	CharactersCount int64
	InputTokens     int64
	OutputTokens    int64
	Streamed        bool
	SegmentID       string
	SpeechID        string
	Metadata        Metadata
}

func (m TTS) MetricKind() Kind         { return KindTTS }
func (m TTS) MetricTime() time.Time    { return m.Timestamp }
func (m TTS) MetricMetadata() Metadata { return m.Metadata }

func (m TTS) MarshalJSON() ([]byte, error) {
	type wire struct {
		Type            Kind      `json:"type"`
		Label           string    `json:"label"`
		RequestID       string    `json:"requestId"`
		Timestamp       int64     `json:"timestamp"`
		TTFBMS          float64   `json:"ttfbMs"`
		DurationMS      float64   `json:"durationMs"`
		AudioDurationMS float64   `json:"audioDurationMs"`
		Cancelled       bool      `json:"cancelled"`
		CharactersCount int64     `json:"charactersCount"`
		InputTokens     int64     `json:"inputTokens,omitempty"`
		OutputTokens    int64     `json:"outputTokens,omitempty"`
		Streamed        bool      `json:"streamed"`
		SegmentID       string    `json:"segmentId,omitempty"`
		SpeechID        string    `json:"speechId,omitempty"`
		Metadata        *Metadata `json:"metadata,omitempty"`
	}
	return json.Marshal(wire{
		Type: KindTTS, Label: m.Label, RequestID: m.RequestID,
		Timestamp: unixMilliseconds(m.Timestamp), TTFBMS: durationMilliseconds(m.TimeToFirstByte),
		DurationMS: durationMilliseconds(m.Duration), AudioDurationMS: durationMilliseconds(m.AudioDuration),
		Cancelled: m.Cancelled, CharactersCount: m.CharactersCount,
		InputTokens: m.InputTokens, OutputTokens: m.OutputTokens, Streamed: m.Streamed,
		SegmentID: m.SegmentID, SpeechID: m.SpeechID, Metadata: metadataOrNil(m.Metadata),
	})
}

type VAD struct {
	Label                  string
	Timestamp              time.Time
	IdleTime               time.Duration
	InferenceDurationTotal time.Duration
	InferenceCount         int64
	Metadata               Metadata
}

func (m VAD) MetricKind() Kind         { return KindVAD }
func (m VAD) MetricTime() time.Time    { return m.Timestamp }
func (m VAD) MetricMetadata() Metadata { return m.Metadata }

func (m VAD) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type                     Kind    `json:"type"`
		Label                    string  `json:"label"`
		Timestamp                int64   `json:"timestamp"`
		IdleTimeMS               float64 `json:"idleTimeMs"`
		InferenceDurationTotalMS float64 `json:"inferenceDurationTotalMs"`
		InferenceCount           int64   `json:"inferenceCount"`
	}{KindVAD, m.Label, unixMilliseconds(m.Timestamp), durationMilliseconds(m.IdleTime), durationMilliseconds(m.InferenceDurationTotal), m.InferenceCount})
}

type EOU struct {
	Timestamp                time.Time
	EndOfUtteranceDelay      time.Duration
	TranscriptionDelay       time.Duration
	OnUserTurnCompletedDelay time.Duration
	LastSpeakingTime         time.Time
	SpeechID                 string
	Metadata                 Metadata
}

func (m EOU) MetricKind() Kind         { return KindEOU }
func (m EOU) MetricTime() time.Time    { return m.Timestamp }
func (m EOU) MetricMetadata() Metadata { return m.Metadata }

func (m EOU) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type                       Kind    `json:"type"`
		Timestamp                  int64   `json:"timestamp"`
		EndOfUtteranceDelayMS      float64 `json:"endOfUtteranceDelayMs"`
		TranscriptionDelayMS       float64 `json:"transcriptionDelayMs"`
		OnUserTurnCompletedDelayMS float64 `json:"onUserTurnCompletedDelayMs"`
		LastSpeakingTimeMS         int64   `json:"lastSpeakingTimeMs"`
		SpeechID                   string  `json:"speechId,omitempty"`
	}{KindEOU, unixMilliseconds(m.Timestamp), durationMilliseconds(m.EndOfUtteranceDelay), durationMilliseconds(m.TranscriptionDelay), durationMilliseconds(m.OnUserTurnCompletedDelay), unixMilliseconds(m.LastSpeakingTime), m.SpeechID})
}

type CachedTokenDetails struct {
	Audio int64 `json:"audioTokens"`
	Text  int64 `json:"textTokens"`
	Image int64 `json:"imageTokens"`
}

type InputTokenDetails struct {
	Audio         int64              `json:"audioTokens"`
	Text          int64              `json:"textTokens"`
	Image         int64              `json:"imageTokens"`
	Cached        int64              `json:"cachedTokens"`
	CachedDetails CachedTokenDetails `json:"cachedTokensDetails,omitempty"`
}

type OutputTokenDetails struct {
	Text  int64 `json:"textTokens"`
	Audio int64 `json:"audioTokens"`
	Image int64 `json:"imageTokens"`
}

type Realtime struct {
	Label            string
	RequestID        string
	Timestamp        time.Time
	Duration         time.Duration
	SessionDuration  time.Duration
	TimeToFirstToken time.Duration
	Cancelled        bool
	InputTokens      int64
	OutputTokens     int64
	TotalTokens      int64
	TokensPerSecond  float64
	InputDetails     InputTokenDetails
	OutputDetails    OutputTokenDetails
	Metadata         Metadata
}

func (m Realtime) MetricKind() Kind         { return KindRealtime }
func (m Realtime) MetricTime() time.Time    { return m.Timestamp }
func (m Realtime) MetricMetadata() Metadata { return m.Metadata }

func (m Realtime) MarshalJSON() ([]byte, error) {
	type inputDetails struct {
		Audio         int64               `json:"audioTokens"`
		Text          int64               `json:"textTokens"`
		Image         int64               `json:"imageTokens"`
		Cached        int64               `json:"cachedTokens"`
		CachedDetails *CachedTokenDetails `json:"cachedTokensDetails,omitempty"`
	}
	type wire struct {
		Type              Kind               `json:"type"`
		Label             string             `json:"label"`
		RequestID         string             `json:"requestId"`
		Timestamp         int64              `json:"timestamp"`
		DurationMS        float64            `json:"durationMs"`
		SessionDurationMS float64            `json:"sessionDurationMs,omitempty"`
		TTFTMS            float64            `json:"ttftMs"`
		Cancelled         bool               `json:"cancelled"`
		InputTokens       int64              `json:"inputTokens"`
		OutputTokens      int64              `json:"outputTokens"`
		TotalTokens       int64              `json:"totalTokens"`
		TokensPerSecond   float64            `json:"tokensPerSecond"`
		InputDetails      inputDetails       `json:"inputTokenDetails"`
		OutputDetails     OutputTokenDetails `json:"outputTokenDetails"`
		Metadata          *Metadata          `json:"metadata,omitempty"`
	}
	var cachedDetails *CachedTokenDetails
	if m.InputDetails.CachedDetails != (CachedTokenDetails{}) {
		copy := m.InputDetails.CachedDetails
		cachedDetails = &copy
	}
	return json.Marshal(wire{
		Type: KindRealtime, Label: m.Label, RequestID: m.RequestID,
		Timestamp: unixMilliseconds(m.Timestamp), DurationMS: durationMilliseconds(m.Duration),
		SessionDurationMS: durationMilliseconds(m.SessionDuration), TTFTMS: durationMilliseconds(m.TimeToFirstToken),
		Cancelled: m.Cancelled, InputTokens: m.InputTokens, OutputTokens: m.OutputTokens,
		TotalTokens: m.TotalTokens, TokensPerSecond: m.TokensPerSecond,
		InputDetails:  inputDetails{Audio: m.InputDetails.Audio, Text: m.InputDetails.Text, Image: m.InputDetails.Image, Cached: m.InputDetails.Cached, CachedDetails: cachedDetails},
		OutputDetails: m.OutputDetails, Metadata: metadataOrNil(m.Metadata),
	})
}

type EOTInference struct {
	Timestamp          time.Time
	TotalDuration      time.Duration
	PredictionDuration time.Duration
	DetectionDelay     time.Duration
	NumRequests        int64
	Metadata           Metadata
}

func (m EOTInference) MetricKind() Kind         { return KindEOTInference }
func (m EOTInference) MetricTime() time.Time    { return m.Timestamp }
func (m EOTInference) MetricMetadata() Metadata { return m.Metadata }

func (m EOTInference) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type               Kind      `json:"type"`
		Timestamp          int64     `json:"timestamp"`
		TotalDuration      float64   `json:"totalDuration"`
		PredictionDuration float64   `json:"predictionDuration"`
		DetectionDelay     float64   `json:"detectionDelay"`
		NumRequests        int64     `json:"numRequests"`
		Metadata           *Metadata `json:"metadata,omitempty"`
	}{KindEOTInference, unixMilliseconds(m.Timestamp), durationMilliseconds(m.TotalDuration), durationMilliseconds(m.PredictionDuration), durationMilliseconds(m.DetectionDelay), m.NumRequests, metadataOrNil(m.Metadata)})
}

type Interruption struct {
	Timestamp          time.Time
	TotalDuration      time.Duration
	PredictionDuration time.Duration
	DetectionDelay     time.Duration
	NumInterruptions   int64
	NumBackchannels    int64
	NumRequests        int64
	Metadata           Metadata
}

func (m Interruption) MetricKind() Kind         { return KindInterruption }
func (m Interruption) MetricTime() time.Time    { return m.Timestamp }
func (m Interruption) MetricMetadata() Metadata { return m.Metadata }

func (m Interruption) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type               Kind      `json:"type"`
		Timestamp          int64     `json:"timestamp"`
		TotalDuration      float64   `json:"totalDuration"`
		PredictionDuration float64   `json:"predictionDuration"`
		DetectionDelay     float64   `json:"detectionDelay"`
		NumInterruptions   int64     `json:"numInterruptions"`
		NumBackchannels    int64     `json:"numBackchannels"`
		NumRequests        int64     `json:"numRequests"`
		Metadata           *Metadata `json:"metadata,omitempty"`
	}{KindInterruption, unixMilliseconds(m.Timestamp), durationMilliseconds(m.TotalDuration), durationMilliseconds(m.PredictionDuration), durationMilliseconds(m.DetectionDelay), m.NumInterruptions, m.NumBackchannels, m.NumRequests, metadataOrNil(m.Metadata)})
}

type Avatar struct {
	Timestamp       time.Time
	PlaybackLatency time.Duration
	SessionStarted  time.Time
	AvatarJoined    time.Time
	Metadata        Metadata
}

func (m Avatar) MetricKind() Kind         { return KindAvatar }
func (m Avatar) MetricTime() time.Time    { return m.Timestamp }
func (m Avatar) MetricMetadata() Metadata { return m.Metadata }

func (m Avatar) MarshalJSON() ([]byte, error) {
	type wire struct {
		Type              Kind      `json:"type"`
		Timestamp         int64     `json:"timestamp"`
		PlaybackLatencyMS float64   `json:"playbackLatencyMs,omitempty"`
		SessionStartedAt  *int64    `json:"sessionStartedAt,omitempty"`
		AvatarJoinedAt    *int64    `json:"avatarJoinedAt,omitempty"`
		Metadata          *Metadata `json:"metadata,omitempty"`
	}
	var sessionStartedAt, avatarJoinedAt *int64
	if !m.SessionStarted.IsZero() {
		value := m.SessionStarted.UnixMilli()
		sessionStartedAt = &value
	}
	if !m.AvatarJoined.IsZero() {
		value := m.AvatarJoined.UnixMilli()
		avatarJoinedAt = &value
	}
	return json.Marshal(wire{
		Type: KindAvatar, Timestamp: unixMilliseconds(m.Timestamp),
		PlaybackLatencyMS: durationMilliseconds(m.PlaybackLatency),
		SessionStartedAt:  sessionStartedAt, AvatarJoinedAt: avatarJoinedAt,
		Metadata: metadataOrNil(m.Metadata),
	})
}
