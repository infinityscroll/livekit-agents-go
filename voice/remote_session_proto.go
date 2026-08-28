// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/livekit/agents-go/llm"
	"github.com/livekit/agents-go/metrics"
	agentpb "github.com/livekit/protocol/livekit/agent"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func encodeRemoteAgentState(value AgentState) agentpb.AgentState {
	switch value {
	case AgentStateInitializing:
		return agentpb.AgentState_AS_INITIALIZING
	case AgentStateIdle:
		return agentpb.AgentState_AS_IDLE
	case AgentStateListening:
		return agentpb.AgentState_AS_LISTENING
	case AgentStateThinking:
		return agentpb.AgentState_AS_THINKING
	case AgentStateSpeaking:
		return agentpb.AgentState_AS_SPEAKING
	default:
		return agentpb.AgentState_AS_IDLE
	}
}

func encodeRemoteUserState(value UserState) agentpb.UserState {
	switch value {
	case UserStateSpeaking:
		return agentpb.UserState_US_SPEAKING
	case UserStateListening:
		return agentpb.UserState_US_LISTENING
	case UserStateAway:
		return agentpb.UserState_US_AWAY
	default:
		return agentpb.UserState_US_LISTENING
	}
}

func encodeRemoteChatRole(value llm.ChatRole) agentpb.ChatRole {
	switch value {
	case llm.RoleDeveloper:
		return agentpb.ChatRole_DEVELOPER
	case llm.RoleSystem:
		return agentpb.ChatRole_SYSTEM
	case llm.RoleUser:
		return agentpb.ChatRole_USER
	case llm.RoleAssistant:
		return agentpb.ChatRole_ASSISTANT
	default:
		// The pinned protocol has no UNKNOWN role; its zero/default is
		// DEVELOPER, matching protobuf decoding of an omitted field.
		return agentpb.ChatRole_DEVELOPER
	}
}

func encodeRemoteAMDCategory(value AMDCategory) agentpb.AmdCategory {
	switch value {
	case AMDCategoryHuman:
		return agentpb.AmdCategory_AMD_HUMAN
	case AMDCategoryMachineIVR:
		return agentpb.AmdCategory_AMD_MACHINE_IVR
	case AMDCategoryMachineVM:
		return agentpb.AmdCategory_AMD_MACHINE_VM
	case AMDCategoryMachineUnavailable:
		return agentpb.AmdCategory_AMD_MACHINE_UNAVAILABLE
	case AMDCategoryUncertain:
		return agentpb.AmdCategory_AMD_UNCERTAIN
	default:
		return agentpb.AmdCategory_AMD_UNKNOWN
	}
}

func remoteTimestamp(value time.Time) *timestamppb.Timestamp {
	if value.IsZero() {
		value = time.Now()
	}
	return timestamppb.New(value)
}

func durationSeconds(value time.Duration) *float64 {
	seconds := value.Seconds()
	return &seconds
}

func encodeRemoteMetrics(value llm.MetricsReport) *agentpb.MetricsReport {
	report := &agentpb.MetricsReport{}
	if !value.StartedSpeakingAt.IsZero() {
		report.StartedSpeakingAt = remoteTimestamp(value.StartedSpeakingAt)
	}
	if !value.StoppedSpeakingAt.IsZero() {
		report.StoppedSpeakingAt = remoteTimestamp(value.StoppedSpeakingAt)
	}
	if value.TranscriptionDelay != 0 {
		report.TranscriptionDelay = durationSeconds(value.TranscriptionDelay)
	}
	if value.EndOfTurnDelay != 0 {
		report.EndOfTurnDelay = durationSeconds(value.EndOfTurnDelay)
	}
	if value.OnUserTurnCompletedDelay != 0 {
		report.OnUserTurnCompletedDelay = durationSeconds(value.OnUserTurnCompletedDelay)
	}
	if value.LLMNodeTTFT != 0 {
		report.LlmNodeTtft = durationSeconds(value.LLMNodeTTFT)
	}
	if value.TTSNodeTTFB != 0 {
		report.TtsNodeTtfb = durationSeconds(value.TTSNodeTTFB)
	}
	if value.EndToEndLatency != 0 {
		report.E2ELatency = durationSeconds(value.EndToEndLatency)
	}
	return report
}

func encodeRemoteChatItem(item llm.ChatItem) (*agentpb.ChatContext_ChatItem, error) {
	if item == nil {
		return nil, errorsNewRemoteChat("nil chat item")
	}
	switch value := item.(type) {
	case *llm.ChatMessage:
		contents := make([]*agentpb.ChatMessage_ChatContent, 0, len(value.Content))
		for _, content := range value.Content {
			var text string
			switch typed := content.(type) {
			case llm.TextContent:
				text = string(typed)
			case llm.InstructionContent:
				text = typed.Instructions.Value()
			default:
				continue
			}
			contents = append(contents, &agentpb.ChatMessage_ChatContent{Payload: &agentpb.ChatMessage_ChatContent_Text{Text: text}})
		}
		extra := make(map[string]string, len(value.Extra))
		for key, raw := range value.Extra {
			extra[key] = fmt.Sprint(raw)
		}
		message := &agentpb.ChatMessage{
			Id: value.ID, Role: encodeRemoteChatRole(value.Role), Content: contents, Interrupted: value.Interrupted,
			TranscriptConfidence: value.TranscriptConfidence, Extra: extra, Metrics: encodeRemoteMetrics(value.Metrics), CreatedAt: remoteTimestamp(value.CreatedAt),
		}
		return &agentpb.ChatContext_ChatItem{Item: &agentpb.ChatContext_ChatItem_Message{Message: message}}, nil
	case *llm.FunctionCall:
		return &agentpb.ChatContext_ChatItem{Item: &agentpb.ChatContext_ChatItem_FunctionCall{FunctionCall: &agentpb.FunctionCall{
			Id: value.ID, CallId: value.CallID, Name: value.Name, Arguments: value.Arguments, CreatedAt: remoteTimestamp(value.CreatedAt),
		}}}, nil
	case *llm.FunctionCallOutput:
		return &agentpb.ChatContext_ChatItem{Item: &agentpb.ChatContext_ChatItem_FunctionCallOutput{FunctionCallOutput: &agentpb.FunctionCallOutput{
			Id: value.ID, CallId: value.CallID, Name: value.Name, Output: value.Output, IsError: value.IsError, CreatedAt: remoteTimestamp(value.CreatedAt),
		}}}, nil
	case *llm.AgentHandoffItem:
		var old *string
		if value.OldAgentID != "" {
			copy := value.OldAgentID
			old = &copy
		}
		return &agentpb.ChatContext_ChatItem{Item: &agentpb.ChatContext_ChatItem_AgentHandoff{AgentHandoff: &agentpb.AgentHandoff{
			Id: value.ID, OldAgentId: old, NewAgentId: value.NewAgentID, CreatedAt: remoteTimestamp(value.CreatedAt),
		}}}, nil
	case *llm.AgentConfigUpdate:
		var instructions *string
		if value.Instructions != nil {
			rendered := value.Instructions.Value()
			instructions = &rendered
		}
		return &agentpb.ChatContext_ChatItem{Item: &agentpb.ChatContext_ChatItem_AgentConfigUpdate{AgentConfigUpdate: &agentpb.AgentConfigUpdate{
			Id: value.ID, Instructions: instructions, ToolsAdded: append([]string(nil), value.ToolsAdded...), ToolsRemoved: append([]string(nil), value.ToolsRemoved...), CreatedAt: remoteTimestamp(value.CreatedAt),
		}}}, nil
	default:
		return nil, errorsNewRemoteChat(fmt.Sprintf("unsupported chat item %T", item))
	}
}

type remoteChatEncodingError struct{ message string }

func (e remoteChatEncodingError) Error() string { return "voice remote session: " + e.message }
func errorsNewRemoteChat(message string) error  { return remoteChatEncodingError{message: message} }

func encodeRemoteChatItems(items []llm.ChatItem, includeConfig bool) ([]*agentpb.ChatContext_ChatItem, error) {
	result := make([]*agentpb.ChatContext_ChatItem, 0, len(items))
	for _, item := range items {
		if item == nil || !includeConfig && item.ItemType() == llm.ItemAgentConfigUpdate {
			continue
		}
		encoded, err := encodeRemoteChatItem(item)
		if err != nil {
			return nil, err
		}
		result = append(result, encoded)
	}
	return result, nil
}

func clampInt32(value int64) int32 {
	return int32(min(int64(math.MaxInt32), max(int64(math.MinInt32), value)))
}

func encodeRemoteUsage(value AgentSessionUsage) *agentpb.AgentSessionUsage {
	result := &agentpb.AgentSessionUsage{ModelUsage: make([]*agentpb.ModelUsage, 0, len(value.ModelUsage))}
	for _, usage := range value.ModelUsage {
		switch typed := usage.(type) {
		case metrics.LLMUsage:
			result.ModelUsage = append(result.ModelUsage, &agentpb.ModelUsage{Usage: &agentpb.ModelUsage_Llm{Llm: &agentpb.LLMModelUsage{
				Provider: typed.Provider, Model: typed.Model, InputTokens: clampInt32(typed.InputTokens), InputCachedTokens: clampInt32(typed.InputCachedTokens),
				InputAudioTokens: clampInt32(typed.InputAudioTokens), InputCachedAudioTokens: clampInt32(typed.InputCachedAudioTokens), InputTextTokens: clampInt32(typed.InputTextTokens),
				InputCachedTextTokens: clampInt32(typed.InputCachedTextTokens), InputImageTokens: clampInt32(typed.InputImageTokens), InputCachedImageTokens: clampInt32(typed.InputCachedImageTokens),
				OutputTokens: clampInt32(typed.OutputTokens), OutputAudioTokens: clampInt32(typed.OutputAudioTokens), OutputTextTokens: clampInt32(typed.OutputTextTokens), SessionDuration: typed.SessionDuration.Seconds(),
			}}})
		case metrics.TTSUsage:
			result.ModelUsage = append(result.ModelUsage, &agentpb.ModelUsage{Usage: &agentpb.ModelUsage_Tts{Tts: &agentpb.TTSModelUsage{
				Provider: typed.Provider, Model: typed.Model, InputTokens: clampInt32(typed.InputTokens), OutputTokens: clampInt32(typed.OutputTokens),
				CharactersCount: clampInt32(typed.CharactersCount), AudioDuration: typed.AudioDuration.Seconds(),
			}}})
		case metrics.STTUsage:
			result.ModelUsage = append(result.ModelUsage, &agentpb.ModelUsage{Usage: &agentpb.ModelUsage_Stt{Stt: &agentpb.STTModelUsage{
				Provider: typed.Provider, Model: typed.Model, InputTokens: clampInt32(typed.InputTokens), OutputTokens: clampInt32(typed.OutputTokens), AudioDuration: typed.AudioDuration.Seconds(),
			}}})
		case metrics.RequestUsage:
			if typed.Kind == metrics.UsageInterruption {
				result.ModelUsage = append(result.ModelUsage, &agentpb.ModelUsage{Usage: &agentpb.ModelUsage_Interruption{Interruption: &agentpb.InterruptionModelUsage{
					Provider: typed.Provider, Model: typed.Model, TotalRequests: clampInt32(typed.TotalRequests),
				}}})
			} else if typed.Kind == metrics.UsageEOT {
				result.ModelUsage = append(result.ModelUsage, &agentpb.ModelUsage{Usage: &agentpb.ModelUsage_Eot{Eot: &agentpb.EotModelUsage{
					Provider: typed.Provider, Model: typed.Model, TotalRequests: clampInt32(typed.TotalRequests),
				}}})
			}
		}
	}
	return result
}

func encodeRemoteSessionOptions(options resolvedSessionOptions) map[string]string {
	marshal := func(value any) string {
		encoded, err := json.Marshal(value)
		if err != nil {
			return "{}"
		}
		return string(encoded)
	}
	transcription := ""
	if options.transcriptionTimeoutEnabled {
		transcription = options.transcriptionTimeout.String()
	}
	away := ""
	if options.userAwayEnabled {
		away = options.userAwayTimeout.String()
	}
	return map[string]string{
		"endpointing":                marshal(options.turn.Endpointing),
		"interruption":               marshal(options.turn.Interruption),
		"max_tool_steps":             fmt.Sprintf("%d", options.maxToolSteps),
		"user_away_timeout":          away,
		"transcription_timeout":      transcription,
		"preemptive_generation":      marshal(options.turn.PreemptiveGeneration),
		"use_tts_aligned_transcript": fmt.Sprintf("%t", options.useTTSAlignedTranscript),
	}
}

func remoteDuration(value time.Duration) *durationpb.Duration {
	return durationpb.New(value)
}
