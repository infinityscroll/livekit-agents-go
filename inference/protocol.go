// SPDX-License-Identifier: Apache-2.0

package inference

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

const MaxControlMessageBytes = 4 << 20

var (
	ErrInvalidEvent    = errors.New("invalid inference event")
	ErrControlTooLarge = errors.New("inference control message too large")
)

type ConnectionSettings struct {
	TimeoutSeconds float64 `json:"timeout"`
	Retries        int     `json:"retries"`
}

type STTSettings struct {
	SampleRate string       `json:"sample_rate"`
	Encoding   string       `json:"encoding"`
	Language   string       `json:"language,omitempty"`
	Extra      ModelOptions `json:"extra"`
}

type STTFallback struct {
	Models []STTFallbackModel `json:"models"`
}

type STTSessionCreate struct {
	Type       string              `json:"type"`
	Model      string              `json:"model,omitempty"`
	Settings   STTSettings         `json:"settings"`
	Fallback   *STTFallback        `json:"fallback,omitempty"`
	Connection *ConnectionSettings `json:"connection,omitempty"`
}

func (event STTSessionCreate) MarshalJSON() ([]byte, error) {
	type alias STTSessionCreate
	if event.Type == "" {
		event.Type = "session.create"
	}
	if event.Settings.Extra == nil {
		event.Settings.Extra = ModelOptions{}
	}
	return json.Marshal(alias(event))
}

// STTUpdateSettings is the hot-path settings payload accepted by the gateway.
// All fields are optional so callers can update one setting without resetting
// the others.
type STTUpdateSettings struct {
	Model    string       `json:"model,omitempty"`
	Language string       `json:"language,omitempty"`
	Extra    ModelOptions `json:"extra,omitempty"`
}

type STTSessionUpdate struct {
	Type     string            `json:"type"`
	Settings STTUpdateSettings `json:"settings"`
}

func (event STTSessionUpdate) MarshalJSON() ([]byte, error) {
	type alias STTSessionUpdate
	if event.Type == "" {
		event.Type = "session.update"
	}
	return json.Marshal(alias(event))
}

type STTWord struct {
	Word       string          `json:"word"`
	Start      float64         `json:"start"`
	End        float64         `json:"end"`
	Confidence float64         `json:"confidence"`
	SpeakerID  *string         `json:"speaker_id,omitempty"`
	Extra      json.RawMessage `json:"extra,omitempty"`
}

type STTServerEvent struct {
	Type       string          `json:"type"`
	SessionID  string          `json:"session_id,omitempty"`
	Transcript string          `json:"transcript,omitempty"`
	Language   string          `json:"language,omitempty"`
	Start      float64         `json:"start,omitempty"`
	Duration   float64         `json:"duration,omitempty"`
	Confidence float64         `json:"confidence,omitempty"`
	Words      []STTWord       `json:"words,omitempty"`
	SpeakerID  *string         `json:"speaker_id,omitempty"`
	Extra      json.RawMessage `json:"extra,omitempty"`
	Message    string          `json:"message,omitempty"`
	Code       *int            `json:"code,omitempty"`
	Unknown    json.RawMessage `json:"-"`
}

func (event STTServerEvent) Known() bool {
	switch event.Type {
	case "session.created", "session.finalized", "session.closed", "interim_transcript",
		"final_transcript", "preflight_transcript", "start_of_speech", "error":
		return true
	default:
		return false
	}
}

func DecodeSTTServerEvent(data []byte) (STTServerEvent, error) {
	if err := checkControlMessage(data); err != nil {
		return STTServerEvent{}, err
	}
	event := STTServerEvent{Confidence: 1}
	if err := decodeStrictSingleValue(data, &event); err != nil {
		return STTServerEvent{}, fmt.Errorf("%w: %v", ErrInvalidEvent, err)
	}
	if event.Type == "" {
		return STTServerEvent{}, fmt.Errorf("%w: missing type", ErrInvalidEvent)
	}
	if !event.Known() {
		event.Unknown = append(event.Unknown[:0], data...)
	}
	return event, nil
}

type TTSGenerationConfig struct {
	Voice    string `json:"voice,omitempty"`
	Language string `json:"language,omitempty"`
	Model    string `json:"model,omitempty"`
}

type TTSFallback struct {
	Models []TTSFallbackModel `json:"models"`
}

type TTSSessionCreate struct {
	Type       string              `json:"type"`
	SampleRate string              `json:"sample_rate"`
	Encoding   string              `json:"encoding"`
	Model      string              `json:"model,omitempty"`
	Voice      string              `json:"voice,omitempty"`
	Language   string              `json:"language,omitempty"`
	Extra      ModelOptions        `json:"extra"`
	Transcript string              `json:"transcript,omitempty"`
	Fallback   *TTSFallback        `json:"fallback,omitempty"`
	Connection *ConnectionSettings `json:"connection,omitempty"`
}

func (event TTSSessionCreate) MarshalJSON() ([]byte, error) {
	type alias TTSSessionCreate
	if event.Type == "" {
		event.Type = "session.create"
	}
	if event.Extra == nil {
		event.Extra = ModelOptions{}
	}
	return json.Marshal(alias(event))
}

type TTSInputTranscript struct {
	Type             string               `json:"type"`
	Transcript       string               `json:"transcript"`
	GenerationConfig *TTSGenerationConfig `json:"generation_config,omitempty"`
	Extra            ModelOptions         `json:"extra,omitempty"`
}

func (event TTSInputTranscript) MarshalJSON() ([]byte, error) {
	type alias TTSInputTranscript
	if event.Type == "" {
		event.Type = "input_transcript"
	}
	return json.Marshal(alias(event))
}

type TTSWordTimestamp struct {
	Word  string  `json:"word"`
	Start float64 `json:"start"`
	End   float64 `json:"end"`
}

type TTSCharTimestamp struct {
	Char  string  `json:"char"`
	Start float64 `json:"start"`
	End   float64 `json:"end"`
}

type TTSServerEvent struct {
	Type      string             `json:"type"`
	SessionID string             `json:"session_id,omitempty"`
	Audio     string             `json:"audio,omitempty"`
	Message   string             `json:"message,omitempty"`
	Words     []TTSWordTimestamp `json:"words,omitempty"`
	Chars     []TTSCharTimestamp `json:"chars,omitempty"`
	Unknown   json.RawMessage    `json:"-"`
}

func (event TTSServerEvent) Known() bool {
	switch event.Type {
	case "session.created", "output_audio", "output_alignment", "done", "session.closed", "error":
		return true
	default:
		return false
	}
}

func DecodeTTSServerEvent(data []byte) (TTSServerEvent, error) {
	if err := checkControlMessage(data); err != nil {
		return TTSServerEvent{}, err
	}
	var event TTSServerEvent
	if err := decodeStrictSingleValue(data, &event); err != nil {
		return TTSServerEvent{}, fmt.Errorf("%w: %v", ErrInvalidEvent, err)
	}
	if event.Type == "" {
		return TTSServerEvent{}, fmt.Errorf("%w: missing type", ErrInvalidEvent)
	}
	if !event.Known() {
		event.Unknown = append(event.Unknown[:0], data...)
	}
	switch event.Type {
	case "session.created", "output_audio", "done", "session.closed":
		if event.SessionID == "" {
			return TTSServerEvent{}, fmt.Errorf("%w: %s missing session_id", ErrInvalidEvent, event.Type)
		}
	}
	return event, nil
}

func checkControlMessage(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("%w: empty payload", ErrInvalidEvent)
	}
	if len(data) > MaxControlMessageBytes {
		return ErrControlTooLarge
	}
	return nil
}

func decodeStrictSingleValue(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err == nil {
		return errors.New("multiple JSON values")
	} else if !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}
