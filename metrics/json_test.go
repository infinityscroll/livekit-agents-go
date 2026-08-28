// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func TestMetricJSONMatchesAgentsJS(t *testing.T) {
	t.Parallel()
	timestamp := time.UnixMilli(1_700_000_000_123)
	tests := []struct {
		name   string
		metric Metric
		want   map[string]any
	}{
		{
			name: "llm",
			metric: LLM{Label: "openai.LLM", RequestID: "req", Timestamp: timestamp,
				Duration: 1500 * time.Microsecond, TimeToFirstToken: -time.Millisecond,
				Cancelled: true, CompletionTokens: 3, PromptTokens: 5, PromptCachedTokens: 2,
				CacheCreationTokens: 1, TotalTokens: 8, TokensPerSecond: 12.5,
				SpeechID: "speech", Metadata: Metadata{ModelProvider: "openai", ModelName: "gpt"}},
			want: map[string]any{
				"type": "llm_metrics", "label": "openai.LLM", "requestId": "req",
				"timestamp": float64(1_700_000_000_123), "durationMs": 1.5, "ttftMs": -1.0,
				"cancelled": true, "completionTokens": 3.0, "promptTokens": 5.0,
				"promptCachedTokens": 2.0, "cacheCreationTokens": 1.0, "totalTokens": 8.0,
				"tokensPerSecond": 12.5, "speechId": "speech",
				"metadata": map[string]any{"modelProvider": "openai", "modelName": "gpt"},
			},
		},
		{
			name: "stt",
			metric: STT{Label: "stt", RequestID: "req", Timestamp: timestamp, Duration: 2 * time.Millisecond,
				AudioDuration: 3*time.Second + 250*time.Microsecond, InputTokens: 4, OutputTokens: 5, Streamed: true},
			want: map[string]any{
				"type": "stt_metrics", "label": "stt", "requestId": "req",
				"timestamp": float64(1_700_000_000_123), "durationMs": 2.0,
				"audioDurationMs": 3000.25, "inputTokens": 4.0, "outputTokens": 5.0, "streamed": true,
			},
		},
		{
			name: "tts",
			metric: TTS{Label: "tts", RequestID: "req", Timestamp: timestamp, TimeToFirstByte: 2500 * time.Microsecond,
				Duration: 5 * time.Millisecond, AudioDuration: 2 * time.Second, Cancelled: true,
				CharactersCount: 12, InputTokens: 6, OutputTokens: 7, Streamed: true,
				SegmentID: "segment", SpeechID: "speech"},
			want: map[string]any{
				"type": "tts_metrics", "label": "tts", "requestId": "req",
				"timestamp": float64(1_700_000_000_123), "ttfbMs": 2.5, "durationMs": 5.0,
				"audioDurationMs": 2000.0, "cancelled": true, "charactersCount": 12.0,
				"inputTokens": 6.0, "outputTokens": 7.0, "streamed": true,
				"segmentId": "segment", "speechId": "speech",
			},
		},
		{
			name: "vad",
			metric: VAD{Label: "vad", Timestamp: timestamp, IdleTime: time.Millisecond,
				InferenceDurationTotal: 2250 * time.Microsecond, InferenceCount: 9,
				Metadata: Metadata{ModelProvider: "not-on-wire"}},
			want: map[string]any{"type": "vad_metrics", "label": "vad", "timestamp": float64(1_700_000_000_123),
				"idleTimeMs": 1.0, "inferenceDurationTotalMs": 2.25, "inferenceCount": 9.0},
		},
		{
			name: "eou",
			metric: EOU{Timestamp: timestamp, EndOfUtteranceDelay: time.Millisecond,
				TranscriptionDelay: 2 * time.Millisecond, OnUserTurnCompletedDelay: 3 * time.Millisecond,
				LastSpeakingTime: time.UnixMilli(1_700_000_000_000), SpeechID: "speech"},
			want: map[string]any{"type": "eou_metrics", "timestamp": float64(1_700_000_000_123),
				"endOfUtteranceDelayMs": 1.0, "transcriptionDelayMs": 2.0,
				"onUserTurnCompletedDelayMs": 3.0, "lastSpeakingTimeMs": float64(1_700_000_000_000), "speechId": "speech"},
		},
		{
			name: "realtime",
			metric: Realtime{Label: "rt", RequestID: "req", Timestamp: timestamp, Duration: time.Second,
				SessionDuration: 2 * time.Second, TimeToFirstToken: -time.Millisecond, Cancelled: true,
				InputTokens: 10, OutputTokens: 11, TotalTokens: 21, TokensPerSecond: 4.5,
				InputDetails: InputTokenDetails{Audio: 1, Text: 2, Image: 3, Cached: 4,
					CachedDetails: CachedTokenDetails{Audio: 1, Text: 2, Image: 3}},
				OutputDetails: OutputTokenDetails{Text: 4, Audio: 5, Image: 6}},
			want: map[string]any{
				"type": "realtime_model_metrics", "label": "rt", "requestId": "req",
				"timestamp": float64(1_700_000_000_123), "durationMs": 1000.0, "sessionDurationMs": 2000.0,
				"ttftMs": -1.0, "cancelled": true, "inputTokens": 10.0, "outputTokens": 11.0,
				"totalTokens": 21.0, "tokensPerSecond": 4.5,
				"inputTokenDetails": map[string]any{"audioTokens": 1.0, "textTokens": 2.0, "imageTokens": 3.0,
					"cachedTokens": 4.0, "cachedTokensDetails": map[string]any{"audioTokens": 1.0, "textTokens": 2.0, "imageTokens": 3.0}},
				"outputTokenDetails": map[string]any{"textTokens": 4.0, "audioTokens": 5.0, "imageTokens": 6.0},
			},
		},
		{
			name: "eot inference",
			metric: EOTInference{Timestamp: timestamp, TotalDuration: time.Millisecond,
				PredictionDuration: 2 * time.Millisecond, DetectionDelay: 3 * time.Millisecond, NumRequests: 4,
				Metadata: Metadata{ModelProvider: "livekit", ModelName: "eot"}},
			want: map[string]any{"type": "eot_inference_metrics", "timestamp": float64(1_700_000_000_123),
				"totalDuration": 1.0, "predictionDuration": 2.0, "detectionDelay": 3.0, "numRequests": 4.0,
				"metadata": map[string]any{"modelProvider": "livekit", "modelName": "eot"}},
		},
		{
			name: "interruption",
			metric: Interruption{Timestamp: timestamp, TotalDuration: time.Millisecond,
				PredictionDuration: 2 * time.Millisecond, DetectionDelay: 3 * time.Millisecond,
				NumInterruptions: 4, NumBackchannels: 5, NumRequests: 6},
			want: map[string]any{"type": "interruption_metrics", "timestamp": float64(1_700_000_000_123),
				"totalDuration": 1.0, "predictionDuration": 2.0, "detectionDelay": 3.0,
				"numInterruptions": 4.0, "numBackchannels": 5.0, "numRequests": 6.0},
		},
		{
			name: "avatar",
			metric: Avatar{Timestamp: timestamp, PlaybackLatency: 1500 * time.Microsecond,
				SessionStarted: time.UnixMilli(1_700_000_000_001), AvatarJoined: time.UnixMilli(1_700_000_000_002),
				Metadata: Metadata{ModelProvider: "avatar"}},
			want: map[string]any{"type": "avatar_metrics", "timestamp": float64(1_700_000_000_123),
				"playbackLatencyMs": 1.5, "sessionStartedAt": float64(1_700_000_000_001),
				"avatarJoinedAt": float64(1_700_000_000_002), "metadata": map[string]any{"modelProvider": "avatar"}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			encoded, err := json.Marshal(test.metric)
			if err != nil {
				t.Fatal(err)
			}
			var got map[string]any
			if err := json.Unmarshal(encoded, &got); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("JSON mismatch\n got: %#v\nwant: %#v\njson: %s", got, test.want, encoded)
			}
		})
	}
}

func TestMetricJSONOmitsOptionalZeroValues(t *testing.T) {
	t.Parallel()
	tests := []Metric{LLM{}, STT{}, TTS{}, Realtime{}, Avatar{}}
	for _, metric := range tests {
		encoded, err := json.Marshal(metric)
		if err != nil {
			t.Fatal(err)
		}
		var got map[string]any
		if err := json.Unmarshal(encoded, &got); err != nil {
			t.Fatal(err)
		}
		for _, key := range []string{"metadata", "speechId", "segmentId", "cacheCreationTokens", "sessionDurationMs", "cachedTokensDetails", "playbackLatencyMs", "sessionStartedAt", "avatarJoinedAt"} {
			if _, ok := got[key]; ok {
				t.Fatalf("%T unexpectedly encoded optional %q: %s", metric, key, encoded)
			}
		}
	}
}

func TestModelUsageJSONAndFilterZeroValues(t *testing.T) {
	t.Parallel()
	usage := LLMUsage{Provider: "openai", Model: "gpt", InputTokens: 10,
		InputCacheCreationTokens: 2, SessionDuration: 1500 * time.Millisecond}
	encoded, err := json.Marshal(usage)
	if err != nil {
		t.Fatal(err)
	}
	var full map[string]any
	if err := json.Unmarshal(encoded, &full); err != nil {
		t.Fatal(err)
	}
	if len(full) != 16 || full["type"] != "llm_usage" || full["inputTokens"] != 10.0 || full["sessionDurationMs"] != 1500.0 {
		t.Fatalf("unexpected full usage JSON: %s", encoded)
	}
	filtered := FilterZeroValues(usage)
	want := map[string]any{
		"type": UsageLLM, "provider": "openai", "model": "gpt", "inputTokens": int64(10),
		"inputCacheCreationTokens": int64(2), "sessionDurationMs": 1500.0,
	}
	if !reflect.DeepEqual(filtered, want) {
		t.Fatalf("FilterZeroValues() = %#v, want %#v", filtered, want)
	}
}
