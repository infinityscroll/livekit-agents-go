// SPDX-License-Identifier: Apache-2.0

package elevenlabs

import (
	"errors"
	"math"
	"testing"
	"time"

	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/tokenize"
)

func TestDefaultsAndCredentialPrecedence(t *testing.T) {
	t.Setenv("ELEVEN_API_KEY", "livekit-key")
	t.Setenv("ELEVENLABS_API_KEY", "provider-key")

	speech, err := NewSTT(STTOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if speech.opts.apiKey != "livekit-key" || speech.opts.model != ScribeV1 || speech.opts.sampleRate != SampleRate16000 {
		t.Fatalf("unexpected STT defaults: %+v", speech.opts)
	}
	if !speech.opts.tagAudioEvents || !speech.opts.enableLogging || speech.opts.includeTimestamps {
		t.Fatalf("unexpected STT boolean defaults: %+v", speech.opts)
	}
	if !speech.Capabilities().Keyterms || speech.Capabilities().Streaming {
		t.Fatalf("unexpected STT capabilities: %+v", speech.Capabilities())
	}

	voice, err := NewTTS(TTSOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if voice.opts.apiKey != "livekit-key" || voice.opts.voiceID != DefaultVoiceID || voice.opts.model != DefaultTTSModel || voice.opts.encoding != PCM22050 {
		t.Fatalf("unexpected TTS defaults: %+v", voice.opts)
	}
	if voice.SampleRate() != 22050 || !voice.opts.autoMode || !voice.opts.syncAlignment || voice.opts.inactivityTimeout != 180*time.Second {
		t.Fatalf("unexpected TTS runtime defaults: %+v", voice.opts)
	}
	if _, ok := voice.opts.tokenizer.(*tokenize.SentenceTokenizer); !ok {
		t.Fatalf("default tokenizer is %T, want SentenceTokenizer", voice.opts.tokenizer)
	}
	if err := voice.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestProviderCredentialFallbackAndMissingCredential(t *testing.T) {
	t.Setenv("ELEVEN_API_KEY", "")
	t.Setenv("ELEVENLABS_API_KEY", "provider-key")
	speech, err := NewSTT(STTOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if speech.opts.apiKey != "provider-key" {
		t.Fatalf("got key %q", speech.opts.apiKey)
	}
	t.Setenv("ELEVENLABS_API_KEY", "")
	_, err = NewSTT(STTOptions{})
	var missing *agents.MissingCredentialsError
	if !errors.As(err, &missing) {
		t.Fatalf("expected MissingCredentialsError, got %T: %v", err, err)
	}
}

func TestOptionValidation(t *testing.T) {
	key := "key"
	tooMany := make([]string, maxRealtimeKeyterms+1)
	for i := range tooMany {
		tooMany[i] = "term"
	}
	if _, err := NewSTT(STTOptions{APIKey: key, Model: ScribeV2Realtime, Keyterms: tooMany}); err == nil {
		t.Fatal("expected realtime keyterm limit error")
	}
	if _, err := NewSTT(STTOptions{APIKey: key, Model: ScribeV2Realtime, Keyterms: []string{"123456789012345678901"}}); err == nil {
		t.Fatal("expected realtime 20-character keyterm limit error")
	}
	if _, err := NewSTT(STTOptions{APIKey: key, Model: ScribeV2, Keyterms: []string{"bad[keyterm"}}); err == nil {
		t.Fatal("expected unsupported batch keyterm character error")
	}
	badThreshold := 1.1
	if _, err := NewSTT(STTOptions{APIKey: key, ServerVAD: &VADOptions{VADThreshold: &badThreshold}}); err == nil {
		t.Fatal("expected invalid VAD threshold")
	}
	if _, err := NewTTS(TTSOptions{APIKey: key, InactivityTimeout: 181 * time.Second}); err == nil {
		t.Fatal("expected inactivity limit error")
	}
	if _, err := NewTTS(TTSOptions{APIKey: key, MaxActiveContexts: 6}); err == nil {
		t.Fatal("expected context limit error")
	}
	badSpeed := 2.0
	if _, err := NewTTS(TTSOptions{APIKey: key, VoiceSettings: &VoiceSettings{Speed: &badSpeed}}); err == nil {
		t.Fatal("expected voice speed error")
	}
	nan := math.NaN()
	if _, err := NewTTS(TTSOptions{APIKey: key, VoiceSettings: &VoiceSettings{Stability: nan}}); err == nil {
		t.Fatal("expected non-finite voice setting error")
	}
}

func TestMergedKeytermsStableUserPrecedence(t *testing.T) {
	got := mergedKeyterms([]string{" LiveKit ", "Agents"}, []string{"livekit", "Sanjana"})
	want := []string{"LiveKit", "Agents", "Sanjana"}
	if len(got) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v want %v", got, want)
		}
	}
}

func TestModelAndEncodingSurface(t *testing.T) {
	encodings := []TTSEncoding{MP32205032, MP34410032, MP34410064, MP34410096, MP344100128, MP344100192, PCM16000, PCM22050, PCM44100}
	for _, encoding := range encodings {
		if _, err := sampleRateFromEncoding(encoding); err != nil {
			t.Fatalf("advertised encoding %s rejected at configuration: %v", encoding, err)
		}
	}
	for _, rate := range []STTRealtimeSampleRate{SampleRate8000, SampleRate16000, SampleRate22050, SampleRate24000, SampleRate44100, SampleRate48000} {
		if !validRealtimeSampleRate(rate) {
			t.Fatalf("advertised sample rate %d rejected", rate)
		}
	}
}
