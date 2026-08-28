// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"log/slog"
	"math"
)

func roundTwo(value float64) float64 { return math.Round(value*100) / 100 }

// LogMetric emits the concise structured summary used by agents-js. The full
// metric remains available to telemetry subscribers.
func LogMetric(logger *slog.Logger, metric Metric) {
	if logger == nil {
		logger = slog.Default()
	}
	switch m := metric.(type) {
	case LLM:
		logger.Info("LLM metrics", "ttft_ms", roundTwo(durationMilliseconds(m.TimeToFirstToken)), "input_tokens", m.PromptTokens,
			"prompt_cached_tokens", m.PromptCachedTokens, "cache_creation_tokens", m.CacheCreationTokens,
			"output_tokens", m.CompletionTokens, "tokens_per_second", roundTwo(m.TokensPerSecond))
	case Realtime:
		logger.Info("RealtimeModel metrics", "ttft_ms", roundTwo(durationMilliseconds(m.TimeToFirstToken)), "input_tokens", m.InputTokens,
			"cached_input_tokens", m.InputDetails.Cached, "output_tokens", m.OutputTokens, "total_tokens", m.TotalTokens,
			"tokens_per_second", roundTwo(m.TokensPerSecond))
	case TTS:
		logger.Info("TTS metrics", "ttfb_ms", roundTwo(durationMilliseconds(m.TimeToFirstByte)), "audio_duration_ms", math.Round(durationMilliseconds(m.AudioDuration)))
	case EOU:
		logger.Info("EOU metrics", "end_of_utterance_delay_ms", roundTwo(durationMilliseconds(m.EndOfUtteranceDelay)),
			"transcription_delay_ms", roundTwo(durationMilliseconds(m.TranscriptionDelay)),
			"on_user_turn_completed_delay_ms", roundTwo(durationMilliseconds(m.OnUserTurnCompletedDelay)))
	case VAD:
		logger.Info("VAD metrics", "idle_time_ms", math.Round(durationMilliseconds(m.IdleTime)),
			"inference_duration_total_ms", math.Round(durationMilliseconds(m.InferenceDurationTotal)), "inference_count", m.InferenceCount)
	case STT:
		logger.Info("STT metrics", "audio_duration_ms", math.Round(durationMilliseconds(m.AudioDuration)))
	case Interruption:
		logger.Info("Interruption metrics", "total_duration_ms", roundTwo(durationMilliseconds(m.TotalDuration)),
			"prediction_duration_ms", roundTwo(durationMilliseconds(m.PredictionDuration)), "detection_delay_ms", roundTwo(durationMilliseconds(m.DetectionDelay)),
			"num_interruptions", m.NumInterruptions, "num_backchannels", m.NumBackchannels, "num_requests", m.NumRequests)
	case Avatar:
		logger.Info("Avatar metrics", "provider", m.Metadata.ModelProvider, "model", m.Metadata.ModelName,
			"playback_latency_ms", roundTwo(durationMilliseconds(m.PlaybackLatency)),
			"avatar_join_latency_ms", roundTwo(durationMilliseconds(m.AvatarJoined.Sub(m.SessionStarted))))
	}
}

// LogMetrics is the TypeScript-compatible plural name.
func LogMetrics(logger *slog.Logger, metric Metric) { LogMetric(logger, metric) }
