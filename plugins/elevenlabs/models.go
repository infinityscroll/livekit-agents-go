// SPDX-License-Identifier: Apache-2.0

package elevenlabs

import (
	"errors"
	"fmt"
)

type TTSModel string

// Compatibility aliases mirror the TypeScript plugin's exported union names.
type TTSModels = TTSModel

const (
	ElevenMonolingualV1  TTSModel = "eleven_monolingual_v1"
	ElevenMultilingualV1 TTSModel = "eleven_multilingual_v1"
	ElevenMultilingualV2 TTSModel = "eleven_multilingual_v2"
	ElevenFlashV2        TTSModel = "eleven_flash_v2"
	ElevenFlashV25       TTSModel = "eleven_flash_v2_5"
	ElevenTurboV2        TTSModel = "eleven_turbo_v2"
	ElevenTurboV25       TTSModel = "eleven_turbo_v2_5"
	ElevenV3             TTSModel = "eleven_v3"
)

type TTSEncoding string

const (
	MP32205032  TTSEncoding = "mp3_22050_32"
	MP34410032  TTSEncoding = "mp3_44100_32"
	MP34410064  TTSEncoding = "mp3_44100_64"
	MP34410096  TTSEncoding = "mp3_44100_96"
	MP344100128 TTSEncoding = "mp3_44100_128"
	MP344100192 TTSEncoding = "mp3_44100_192"
	PCM16000    TTSEncoding = "pcm_16000"
	PCM22050    TTSEncoding = "pcm_22050"
	PCM44100    TTSEncoding = "pcm_44100"
)

type STTModel string

type ElevenLabsSTTModels = STTModel

const (
	ScribeV1         STTModel = "scribe_v1"
	ScribeV2         STTModel = "scribe_v2"
	ScribeV2Realtime STTModel = "scribe_v2_realtime"
)

type STTRealtimeSampleRate int

type STTRealtimeSampleRates = STTRealtimeSampleRate

const (
	SampleRate8000  STTRealtimeSampleRate = 8000
	SampleRate16000 STTRealtimeSampleRate = 16000
	SampleRate22050 STTRealtimeSampleRate = 22050
	SampleRate24000 STTRealtimeSampleRate = 24000
	SampleRate44100 STTRealtimeSampleRate = 44100
	SampleRate48000 STTRealtimeSampleRate = 48000
)

type TextNormalization string

const (
	TextNormalizationAuto TextNormalization = "auto"
	TextNormalizationOn   TextNormalization = "on"
	TextNormalizationOff  TextNormalization = "off"
)

type PreferredAlignment string

const (
	NormalizedAlignment PreferredAlignment = "normalized"
	OriginalAlignment   PreferredAlignment = "original"
)

const (
	DefaultBaseURL               = "https://api.elevenlabs.io/v1"
	DefaultVoiceID               = "bIHbv24MWmeRgasZH58o"
	DefaultTTSModel     TTSModel = ElevenTurboV25
	DefaultEncoding              = PCM22050
	DefaultSTTModel     STTModel = ScribeV1
	AuthorizationHeader          = "xi-api-key"
)

var (
	ErrUnsupportedEncoding  = errors.New("elevenlabs: unsupported output encoding")
	ErrUnsupportedTransport = errors.New("elevenlabs: model is unsupported by this transport")
	ErrInvalidProtocol      = errors.New("elevenlabs: invalid provider protocol message")
	ErrReplayLimit          = errors.New("elevenlabs: uncommitted audio exceeds reconnect replay limit")
)

type UnsupportedEncodingError struct {
	Encoding TTSEncoding
	Reason   string
}

func (e *UnsupportedEncodingError) Error() string {
	if e.Reason == "" {
		return fmt.Sprintf("%v: %s", ErrUnsupportedEncoding, e.Encoding)
	}
	return fmt.Sprintf("%v %s: %s", ErrUnsupportedEncoding, e.Encoding, e.Reason)
}

func (e *UnsupportedEncodingError) Unwrap() error { return ErrUnsupportedEncoding }

type UnsupportedTransportError struct {
	Model     TTSModel
	Transport string
}

func (e *UnsupportedTransportError) Error() string {
	return fmt.Sprintf("%v: model %s on %s", ErrUnsupportedTransport, e.Model, e.Transport)
}

func (e *UnsupportedTransportError) Unwrap() error { return ErrUnsupportedTransport }

type ProtocolError struct {
	Message string
	Cause   error
}

func (e *ProtocolError) Error() string {
	if e == nil || e.Message == "" {
		return ErrInvalidProtocol.Error()
	}
	return ErrInvalidProtocol.Error() + ": " + e.Message
}

func (e *ProtocolError) Unwrap() error {
	if e.Cause != nil {
		return e.Cause
	}
	return ErrInvalidProtocol
}

func sampleRateFromEncoding(encoding TTSEncoding) (int, error) {
	switch encoding {
	case MP32205032:
		return 22050, nil
	case MP34410032, MP34410064, MP34410096, MP344100128, MP344100192:
		return 44100, nil
	case PCM16000:
		return 16000, nil
	case PCM22050:
		return 22050, nil
	case PCM44100:
		return 44100, nil
	default:
		return 0, &UnsupportedEncodingError{Encoding: encoding}
	}
}

func isPCMEncoding(encoding TTSEncoding) bool {
	switch encoding {
	case PCM16000, PCM22050, PCM44100:
		return true
	default:
		return false
	}
}

func validRealtimeSampleRate(rate STTRealtimeSampleRate) bool {
	switch rate {
	case SampleRate8000, SampleRate16000, SampleRate22050, SampleRate24000, SampleRate44100, SampleRate48000:
		return true
	default:
		return false
	}
}
