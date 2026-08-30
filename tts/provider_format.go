// SPDX-License-Identifier: Apache-2.0

package tts

import (
	"fmt"
	"strings"

	"github.com/infinityscroll/livekit-agents-go/tokenize"
)

const (
	ProviderCartesia  = "cartesia"
	ProviderInworld   = "inworld"
	ProviderXAI       = "xai"
	ProviderFishAudio = "fishaudio"
)

// Toggle is a tri-state sparse override used by speech steering.
type Toggle uint8

const (
	ToggleDefault Toggle = iota
	ToggleEnabled
	ToggleDisabled
)

// Pace controls the requested overall delivery pace.
type Pace string

const (
	PaceDefault Pace = ""
	PaceSlow    Pace = "slow"
	PaceNormal  Pace = "normal"
	PaceFast    Pace = "fast"
)

// NonverbalOptions is a sparse opt-out. Unspecified categories remain enabled.
// All corresponds to the JS boolean form of nonverbalSounds.
type NonverbalOptions struct {
	All          Toggle
	Laughing     Toggle
	Breathing    Toggle
	Sighing      Toggle
	Crying       Toggle
	Vocalizing   Toggle
	MouthSounds  Toggle
	ReflexSounds Toggle
}

// SpeechSteeringOptions steers verbal delivery and non-verbal sounds.
type SpeechSteeringOptions struct {
	Disfluencies    Toggle
	NonverbalSounds NonverbalOptions
	Pace            Pace
}

// DefaultSpeechSteeringOptions enables light fillers and the full sound
// vocabulary. It is a value, so callers can copy it without shared mutation.
var DefaultSpeechSteeringOptions = SpeechSteeringOptions{Disfluencies: ToggleEnabled}

// NonverbalField names a queryable sound category.
type NonverbalField string

const (
	NonverbalLaughing     NonverbalField = "laughing"
	NonverbalBreathing    NonverbalField = "breathing"
	NonverbalSighing      NonverbalField = "sighing"
	NonverbalCrying       NonverbalField = "crying"
	NonverbalVocalizing   NonverbalField = "vocalizing"
	NonverbalMouthSounds  NonverbalField = "mouthSounds"
	NonverbalReflexSounds NonverbalField = "reflexSounds"
)

// MarkupInfo describes the expressive features supported by a provider.
type MarkupInfo struct {
	Nonverbals map[NonverbalField][]string
}

// ExpressiveTag is one expressive XML tag removed from a transcript.
type ExpressiveTag struct {
	Type  string
	Value string
}

var (
	inworldSounds = []string{"laugh", "sigh", "breathe", "clear throat", "cough", "yawn"}
	xaiInline     = []string{"breath", "inhale", "exhale", "sigh", "laugh", "chuckle", "giggle", "cry", "tsk", "tongue-click", "lip-smack", "hum-tune"}
	xaiWrapping   = []string{"emphasis", "whisper", "soft", "loud", "build-intensity", "decrease-intensity", "higher-pitch", "lower-pitch", "slow", "fast", "sing-song", "singing", "laugh-speak"}
	fishEmotions  = []string{"regretful", "hopeful", "happy", "excited", "curious", "surprised", "sad", "empathetic", "sarcastic", "calm", "angry", "worried", "nervous", "confident", "grateful", "delighted", "disappointed", "frustrated", "determined"}
	fishSounds    = []string{"laughing", "chuckling", "clear throat", "sighing", "gasping", "groaning", "yawning", "sobbing"}
	fishTones     = []string{"whispering", "soft", "shouting", "hurried"}
)

var nonverbalFields = [...]NonverbalField{
	NonverbalLaughing, NonverbalBreathing, NonverbalSighing, NonverbalCrying,
	NonverbalVocalizing, NonverbalMouthSounds, NonverbalReflexSounds,
}

// HasMarkupDialect reports whether provider supports the shared expr dialect.
// It performs no prompt rendering or allocation.
func HasMarkupDialect(provider string) bool {
	switch provider {
	case ProviderCartesia, ProviderInworld, ProviderXAI, ProviderFishAudio:
		return true
	default:
		return false
	}
}

// SupportedNonverbals returns a defensive copy of the provider capability map.
func SupportedNonverbals(provider string) map[NonverbalField][]string {
	out := make(map[NonverbalField][]string)
	for _, field := range nonverbalFields {
		if labels := soundLabels(provider, field); len(labels) != 0 {
			out[field] = append(out[field], labels...)
		}
		if labels := prosodyLabels(provider, field); len(labels) != 0 {
			out[field] = append(out[field], labels...)
		}
	}
	return out
}

// ProviderMarkupInfo returns the queryable markup matrix for a provider.
func ProviderMarkupInfo(provider string) MarkupInfo {
	return MarkupInfo{Nonverbals: SupportedNonverbals(provider)}
}

func categoryToggle(options NonverbalOptions, field NonverbalField) Toggle {
	switch field {
	case NonverbalLaughing:
		return options.Laughing
	case NonverbalBreathing:
		return options.Breathing
	case NonverbalSighing:
		return options.Sighing
	case NonverbalCrying:
		return options.Crying
	case NonverbalVocalizing:
		return options.Vocalizing
	case NonverbalMouthSounds:
		return options.MouthSounds
	case NonverbalReflexSounds:
		return options.ReflexSounds
	default:
		return ToggleDefault
	}
}

func steeringRemoved(prosody bool, provider string, steering *SpeechSteeringOptions) []string {
	if steering == nil {
		return nil
	}
	if steering.NonverbalSounds.All == ToggleEnabled {
		return nil
	}
	removed := make([]string, 0, 8)
	if steering.NonverbalSounds.All == ToggleDisabled {
		for _, field := range nonverbalFields {
			values := soundLabels(provider, field)
			if prosody {
				values = prosodyLabels(provider, field)
			}
			removed = append(removed, values...)
		}
		return removed
	}
	for _, field := range nonverbalFields {
		if categoryToggle(steering.NonverbalSounds, field) == ToggleDisabled {
			values := soundLabels(provider, field)
			if prosody {
				values = prosodyLabels(provider, field)
			}
			removed = append(removed, values...)
		}
	}
	return removed
}

func allowedSounds(provider string, steering *SpeechSteeringOptions) []string {
	removed := steeringRemoved(false, provider, steering)
	all := providerSoundVocabulary(provider)
	if len(removed) == 0 {
		return all
	}
	out := make([]string, 0, len(all))
	for _, label := range all {
		if !containsString(removed, label) {
			out = append(out, label)
		}
	}
	return out
}

func allowedProsody(provider string, steering *SpeechSteeringOptions) []string {
	if provider != ProviderXAI {
		return nil
	}
	removed := steeringRemoved(true, provider, steering)
	if len(removed) == 0 {
		return xaiWrapping
	}
	out := make([]string, 0, len(xaiWrapping))
	for _, label := range xaiWrapping {
		if !containsString(removed, label) {
			out = append(out, label)
		}
	}
	return out
}

func providerSoundVocabulary(provider string) []string {
	switch provider {
	case ProviderInworld:
		return inworldSounds
	case ProviderXAI:
		return xaiInline
	case ProviderFishAudio:
		return fishSounds
	default:
		return nil
	}
}

func soundLabels(provider string, field NonverbalField) []string {
	switch provider {
	case ProviderInworld:
		switch field {
		case NonverbalLaughing:
			return []string{"laugh"}
		case NonverbalBreathing:
			return []string{"breathe"}
		case NonverbalSighing:
			return []string{"sigh"}
		case NonverbalReflexSounds:
			return []string{"cough", "clear throat", "yawn"}
		}
	case ProviderXAI:
		switch field {
		case NonverbalLaughing:
			return []string{"laugh", "chuckle", "giggle"}
		case NonverbalBreathing:
			return []string{"breath", "inhale", "exhale"}
		case NonverbalSighing:
			return []string{"sigh"}
		case NonverbalCrying:
			return []string{"cry"}
		case NonverbalVocalizing:
			return []string{"hum-tune"}
		case NonverbalMouthSounds:
			return []string{"tsk", "tongue-click", "lip-smack"}
		}
	case ProviderFishAudio:
		switch field {
		case NonverbalLaughing:
			return []string{"laughing", "chuckling"}
		case NonverbalBreathing:
			return []string{"gasping"}
		case NonverbalSighing:
			return []string{"sighing"}
		case NonverbalCrying:
			return []string{"sobbing"}
		case NonverbalVocalizing:
			return []string{"groaning"}
		case NonverbalReflexSounds:
			return []string{"clear throat", "yawning"}
		}
	}
	return nil
}

func prosodyLabels(provider string, field NonverbalField) []string {
	if provider != ProviderXAI {
		return nil
	}
	switch field {
	case NonverbalLaughing:
		return []string{"laugh-speak"}
	case NonverbalVocalizing:
		return []string{"sing-song", "singing"}
	default:
		return nil
	}
}

func soundUsageHint(sound string) string {
	switch sound {
	case "laugh", "laughing":
		return "a laugh at something obviously funny"
	case "chuckle", "chuckling", "giggle":
		return "a chuckle at something subtly humorous"
	case "sigh", "sighing":
		return "a sigh when commiserating"
	case "inhale":
		return "a sharp inhale before a big reveal"
	case "gasping":
		return "a gasp at a sudden shock or reveal"
	case "lip-smack", "tongue-click":
		return "a lip-smack or tongue-click as a tiny beat of thought"
	case "tsk":
		return "a tsk for mock-disapproval"
	case "clear throat":
		return "a clear-throat when shifting to a new step or topic"
	case "groaning":
		return "a groan at a groan-worthy pun or an unwelcome chore"
	case "yawning":
		return "a yawn when tiredness itself is the topic"
	case "sobbing":
		return "a sob reserved for real heartbreak"
	default:
		return ""
	}
}

func soundGuidance(sounds []string) string {
	hints := make([]string, 0, len(sounds))
	for _, sound := range sounds {
		hint := soundUsageHint(sound)
		if hint == "" {
			continue
		}
		if !containsString(hints, hint) {
			hints = append(hints, hint)
		}
	}
	line := "Non-verbal sounds: use one only where the moment genuinely earns it"
	if len(hints) != 0 {
		line += " — " + strings.Join(hints, ", ")
	}
	return line + ". Most turns have none; never repeat the same sound twice in a row."
}

// SteeringInstructions renders only overrides that differ from the default.
func SteeringInstructions(provider string, steering SpeechSteeringOptions) string {
	lines := make([]string, 0, 3)
	removed := steeringRemoved(false, provider, &steering)
	if len(removed) != 0 {
		if sounds := allowedSounds(provider, &steering); len(sounds) != 0 {
			lines = append(lines, soundGuidance(sounds))
		}
	}
	switch steering.Disfluencies {
	case ToggleEnabled:
		lines = append(lines, "Sprinkle in natural fillers (um, uh) and openers (oh, well, so), zero to two per turn, never mechanical.")
	case ToggleDisabled:
		lines = append(lines, "No fillers (um, uh). Sound composed and fluent.")
	}
	if steering.Pace != PaceDefault && steering.Pace != PaceNormal {
		lines = append(lines, fmt.Sprintf("Keep a %s overall speaking pace.", steering.Pace))
	}
	if len(lines) == 0 {
		return ""
	}
	return "Delivery guidelines:\n- " + strings.Join(lines, "\n- ")
}

// MaxInputLen returns the provider hard chunk limit and whether one is known.
func MaxInputLen(provider string) (int, bool) {
	switch provider {
	case ProviderInworld:
		return 900, true
	case ProviderCartesia:
		return 400, true
	case ProviderXAI:
		return 1000, true
	default:
		return 0, false
	}
}

const (
	expressiveBatchLength      = 200
	expressiveFirstChunkLength = 20
)

// SentenceTokenizer returns the default streamed-input tokenizer for provider.
func SentenceTokenizer(provider string, expressive bool) *tokenize.SentenceTokenizer {
	maxLen, limited := MaxInputLen(provider)
	opts := tokenize.SentenceOptions{MaxTokenLength: maxLen, XMLAware: expressive}
	if expressive && limited {
		opts.MinTokenLength = min(expressiveBatchLength, maxLen)
		opts.FirstTokenLength = expressiveFirstChunkLength
	}
	return tokenize.NewSentenceTokenizer(opts)
}
