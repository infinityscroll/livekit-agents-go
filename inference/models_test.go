// SPDX-License-Identifier: Apache-2.0

package inference

import (
	"reflect"
	"testing"

	"github.com/infinityscroll/livekit-agents-go/stt"
)

func TestModelStringParsing(t *testing.T) {
	model, language := ParseSTTModelString("deepgram/nova-3:English")
	if model != "deepgram/nova-3" || language != "en" {
		t.Fatalf("got %q, %q", model, language)
	}
	model, voice := ParseTTSModelString("provider/model:voice:variant")
	if model != "provider/model:voice" || voice != "variant" {
		t.Fatalf("final-colon parse got %q, %q", model, voice)
	}
}

func TestCapabilities(t *testing.T) {
	if got := STTAlignedTranscript("deepgram/nova-3", "elevenlabs/scribe_v2_realtime"); got != stt.AlignedTranscriptWord {
		t.Fatalf("alignment = %q", got)
	}
	if got := STTAlignedTranscript("auto"); got != stt.AlignedTranscriptNone {
		t.Fatalf("unknown alignment = %q", got)
	}
	if !STTDiarizationEnabled(ModelOptions{"diarization": "speaker"}) || STTDiarizationEnabled(ModelOptions{"diarization": "none"}) {
		t.Fatal("diarization detection mismatch")
	}
	if !TTSHasAlignedTranscript("elevenlabs/eleven_flash_v2_5", ModelOptions{"sync_alignment": true}) {
		t.Fatal("expected ElevenLabs alignment")
	}
	if TTSHasAlignedTranscript("elevenlabs/eleven_flash_v2_5", nil) {
		t.Fatal("alignment must be opt-in")
	}
}

func TestMergeSTTKeyterms(t *testing.T) {
	base := ModelOptions{"keyterm": "LiveKit"}
	overlay, ok := MergeSTTKeyterms("deepgram/nova-3", base, []string{"Agents", "LiveKit"})
	if !ok || !reflect.DeepEqual(overlay["keyterm"], []string{"LiveKit", "Agents"}) {
		t.Fatalf("unexpected Deepgram overlay: %#v", overlay)
	}
	if base["keyterm"] != "LiveKit" {
		t.Fatal("input options mutated")
	}
	vocabulary, ok := MergeSTTKeyterms("speechmatics/enhanced", ModelOptions{
		"additional_vocab": []any{map[string]any{"content": "LiveKit", "sounds_like": []any{"live kit"}}},
	}, []string{"Agents", "LiveKit"})
	if !ok {
		t.Fatal("Speechmatics should support keyterms")
	}
	entries := vocabulary["additional_vocab"].([]map[string]any)
	if len(entries) != 2 || entries[1]["content"] != "Agents" {
		t.Fatalf("unexpected vocabulary: %#v", entries)
	}
	if _, ok := MergeSTTKeyterms("speechmatics/linden-1", nil, []string{"x"}); ok {
		t.Fatal("linden-1 must not advertise keyterms")
	}
}
