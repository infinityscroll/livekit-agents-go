// SPDX-License-Identifier: Apache-2.0

package inference

import (
	"errors"
	"testing"
)

func TestInferenceWebSocketURL(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		base, endpoint, want string
	}{
		{"https://agent-gateway.livekit.cloud/v1", "stt", "wss://agent-gateway.livekit.cloud/v1/stt"},
		{"http://localhost:8080/root/", "/tts", "ws://localhost:8080/root/tts"},
		{"wss://example.test/v1?secret=discard", "tts", "wss://example.test/v1/tts"},
	} {
		got, err := inferenceWebSocketURL(test.base, test.endpoint)
		if err != nil {
			t.Fatal(err)
		}
		if got != test.want {
			t.Fatalf("URL = %q, want %q", got, test.want)
		}
	}
	if _, err := inferenceWebSocketURL("file:///tmp/socket", "stt"); err == nil {
		t.Fatal("file URL was accepted")
	}
	if _, err := inferenceWebSocketURL("https://user:secret@example.test/v1", "stt"); err == nil {
		t.Fatal("URL userinfo was accepted")
	}
}

func FuzzInferenceEventDecoders(f *testing.F) {
	f.Add([]byte(`{"type":"session.created","session_id":"s"}`))
	f.Add([]byte(`{"type":"output_audio","session_id":"s","audio":"AA=="}`))
	f.Add([]byte(`{"type":"final_transcript","transcript":"hello"}`))
	f.Add([]byte{0xff, 0x00, '{'})
	f.Fuzz(func(t *testing.T, payload []byte) {
		_, sttErr := DecodeSTTServerEvent(payload)
		_, ttsErr := DecodeTTSServerEvent(payload)
		if len(payload) == 0 && (!errors.Is(sttErr, ErrInvalidEvent) || !errors.Is(ttsErr, ErrInvalidEvent)) {
			t.Fatalf("empty payload errors = %v, %v", sttErr, ttsErr)
		}
	})
}

func BenchmarkDecodeInferenceSTTFinal(b *testing.B) {
	payload := []byte(`{"type":"final_transcript","session_id":"s","transcript":"hello world","words":[{"word":"hello","start":0,"end":0.2,"confidence":0.99}]}`)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := DecodeSTTServerEvent(payload); err != nil {
			b.Fatal(err)
		}
	}
}
