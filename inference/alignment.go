// SPDX-License-Identifier: Apache-2.0

package inference

import (
	"context"

	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/tts"
)

// TTSAlignmentDecoder is the provider-alignment extension seam used by the
// inference TTS adapter. Implementations must not retain event slices and
// should return a newly owned result. The context is cancelled with the
// synthesis stream.
type TTSAlignmentDecoder interface {
	DecodeTTSAlignment(context.Context, TTSServerEvent, string) ([]agents.TimedString, error)
}

// TTSAlignmentDecoderFunc adapts a function into TTSAlignmentDecoder.
type TTSAlignmentDecoderFunc func(context.Context, TTSServerEvent, string) ([]agents.TimedString, error)

func (f TTSAlignmentDecoderFunc) DecodeTTSAlignment(ctx context.Context, event TTSServerEvent, provider string) ([]agents.TimedString, error) {
	return f(ctx, event, provider)
}

type defaultTTSAlignmentDecoder struct{}

func (defaultTTSAlignmentDecoder) DecodeTTSAlignment(_ context.Context, event TTSServerEvent, provider string) ([]agents.TimedString, error) {
	return DecodeTTSAlignment(event, provider), nil
}

// DefaultTTSAlignmentDecoder returns the stateless Cloud Inference provider
// decoder. A function keeps the process-wide default immutable and race-free.
func DefaultTTSAlignmentDecoder() TTSAlignmentDecoder { return defaultTTSAlignmentDecoder{} }

// DecodeTTSAlignment converts a gateway alignment event into SDK timestamps.
// Word alignment takes precedence over character alignment. Cartesia's words
// are space-delimited by convention, so the separator is restored here.
func DecodeTTSAlignment(event TTSServerEvent, provider string) []agents.TimedString {
	if len(event.Words) != 0 {
		result := make([]agents.TimedString, len(event.Words))
		for i, word := range event.Words {
			text := word.Word
			if provider == tts.ProviderCartesia {
				text += " "
			}
			result[i] = agents.NewTimedString(text, inferenceSeconds(word.Start), inferenceSeconds(word.End))
		}
		return result
	}
	result := make([]agents.TimedString, len(event.Chars))
	for i, char := range event.Chars {
		result[i] = agents.NewTimedString(char.Char, inferenceSeconds(char.Start), inferenceSeconds(char.End))
	}
	return result
}

var _ TTSAlignmentDecoder = defaultTTSAlignmentDecoder{}
