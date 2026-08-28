// SPDX-License-Identifier: Apache-2.0

package inference

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestSessionCreateWireFormat(t *testing.T) {
	data, err := json.Marshal(STTSessionCreate{Settings: STTSettings{SampleRate: "16000", Encoding: "pcm_s16le"}})
	if err != nil {
		t.Fatal(err)
	}
	if got := string(data); got != `{"type":"session.create","settings":{"sample_rate":"16000","encoding":"pcm_s16le","extra":{}}}` {
		t.Fatalf("wire JSON = %s", got)
	}
	data, err = json.Marshal(TTSInputTranscript{Transcript: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if got := string(data); got != `{"type":"input_transcript","transcript":"hello"}` {
		t.Fatalf("wire JSON = %s", got)
	}
}

func TestSTTSessionUpdateWireFormat(t *testing.T) {
	data, err := json.Marshal(STTSessionUpdate{Settings: STTUpdateSettings{
		Model:    "deepgram/nova-3",
		Language: "en-US",
		Extra:    ModelOptions{"keyterm": []string{"LiveKit"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"type":"session.update","settings":{"model":"deepgram/nova-3","language":"en-US","extra":{"keyterm":["LiveKit"]}}}`
	if got := string(data); got != want {
		t.Fatalf("wire JSON = %s, want %s", got, want)
	}
}

func TestFallbackWireFormatKeepsEmptyExtraObjects(t *testing.T) {
	data, err := json.Marshal(STTSessionCreate{
		Settings: STTSettings{SampleRate: "16000", Encoding: "pcm_s16le"},
		Fallback: &STTFallback{Models: []STTFallbackModel{{Model: "deepgram/nova-3"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	const wantSTT = `{"type":"session.create","settings":{"sample_rate":"16000","encoding":"pcm_s16le","extra":{}},"fallback":{"models":[{"model":"deepgram/nova-3","extra":{}}]}}`
	if got := string(data); got != wantSTT {
		t.Fatalf("STT fallback JSON = %s, want %s", got, wantSTT)
	}
	data, err = json.Marshal(TTSSessionCreate{
		SampleRate: "16000", Encoding: "pcm_s16le", Model: "cartesia/sonic-3",
		Fallback: &TTSFallback{Models: []TTSFallbackModel{{Model: "elevenlabs/eleven_flash_v2", Voice: "voice-a"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	const wantTTS = `{"type":"session.create","sample_rate":"16000","encoding":"pcm_s16le","model":"cartesia/sonic-3","extra":{},"fallback":{"models":[{"model":"elevenlabs/eleven_flash_v2","voice":"voice-a","extra":{}}]}}`
	if got := string(data); got != wantTTS {
		t.Fatalf("TTS fallback JSON = %s, want %s", got, wantTTS)
	}
}

func TestDecodeSTTDefaultsAndUnknown(t *testing.T) {
	event, err := DecodeSTTServerEvent([]byte(`{"type":"final_transcript","transcript":"hello","words":null}`))
	if err != nil {
		t.Fatal(err)
	}
	if event.Confidence != 1 || event.Words != nil {
		t.Fatalf("defaults not applied: %+v", event)
	}
	unknown, err := DecodeSTTServerEvent([]byte(`{"type":"provider.new_event","future":true}`))
	if err != nil {
		t.Fatal(err)
	}
	if unknown.Known() || len(unknown.Unknown) == 0 {
		t.Fatalf("unknown event was not preserved: %+v", unknown)
	}
}

func TestDecodeTTSValidationAndLimits(t *testing.T) {
	if _, err := DecodeTTSServerEvent([]byte(`{"type":"output_audio","audio":"AA=="}`)); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("missing session ID error = %v", err)
	}
	event, err := DecodeTTSServerEvent([]byte(`{"type":"gateway.future","payload":1}`))
	if err != nil || event.Known() || len(event.Unknown) == 0 {
		t.Fatalf("unknown event: %+v, %v", event, err)
	}
	tooLarge := []byte(strings.Repeat(" ", MaxControlMessageBytes+1))
	if _, err := DecodeTTSServerEvent(tooLarge); !errors.Is(err, ErrControlTooLarge) {
		t.Fatalf("oversized error = %v", err)
	}
	if _, err := DecodeTTSServerEvent([]byte(`{"type":"done","session_id":"s"} {}`)); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("trailing JSON error = %v", err)
	}
}
