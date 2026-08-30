// SPDX-License-Identifier: Apache-2.0

package elevenlabs

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/metrics"
	"github.com/infinityscroll/livekit-agents-go/tts"
)

func TestHTTPStreamingTTSRequestPCMAndMetrics(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/text-to-speech/voice-1/stream" || r.Method != http.MethodPost {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if r.URL.Query().Get("output_format") != string(PCM22050) || r.URL.Query().Get("enable_logging") != "false" || r.URL.Query().Get("optimize_streaming_latency") != "2" {
			t.Errorf("unexpected query: %s", r.URL.RawQuery)
		}
		if r.Header.Get(AuthorizationHeader) != "secret" || r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("unexpected headers: %v", r.Header)
		}
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("decode body: %v", err)
		}
		if payload["text"] != "hello" || payload["model_id"] != string(ElevenV3) || payload["language_code"] != "en" || payload["apply_text_normalization"] != string(TextNormalizationOn) {
			t.Errorf("unexpected payload: %#v", payload)
		}
		if dictionaries, ok := payload["pronunciation_dictionary_locators"].([]any); !ok || len(dictionaries) != 1 {
			t.Errorf("missing dictionaries: %#v", payload)
		}
		if attempts.Add(1) == 1 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"detail":"retry"}`))
			return
		}
		w.Header().Set("Content-Type", "audio/pcm; rate=22050")
		w.Header().Set("request-id", "tts-req")
		_, _ = w.Write([]byte{1, 0, 2})
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		_, _ = w.Write([]byte{0, 3, 0, 4, 0})
	}))
	defer server.Close()
	logging := false
	latency := 2
	client, err := NewTTS(TTSOptions{
		APIKey: "secret", BaseURL: server.URL + "/v1", VoiceID: "voice-1", Model: ElevenV3,
		Language: "en-US", Encoding: PCM22050, EnableLogging: &logging, StreamingLatency: &latency,
		ApplyTextNormalization:          TextNormalizationOn,
		PronunciationDictionaryLocators: []PronunciationDictionaryLocator{{PronunciationDictionaryID: "dict", VersionID: "v1"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	metricCh := make(chan metrics.TTS, 4)
	client.OnMetrics(func(metric metrics.TTS) { metricCh <- metric })
	value, err := client.Synthesize(t.Context(), "hello", tts.SynthesizeOptions{ConnectOptions: agents.APIConnectOptions{MaxRetries: 1, RetryInterval: time.Millisecond}})
	if err != nil {
		t.Fatal(err)
	}
	stream := value.(*ChunkedStream)
	audio, err := stream.Recv(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !audio.Final || audio.RequestID != "tts-req" || len(audio.Frame.Data) != 4 || audio.Frame.Data[0] != 1 || audio.Frame.Data[3] != 4 {
		t.Fatalf("unexpected audio: %+v", audio)
	}
	if _, err := stream.Recv(t.Context()); !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF, got %v", err)
	}
	if attempts.Load() != 2 {
		t.Fatalf("attempts=%d", attempts.Load())
	}
	// One metric is emitted for each completed attempt; the success metric must
	// carry decoded audio duration and v3 metadata.
	var success metrics.TTS
	for i := 0; i < 2; i++ {
		select {
		case metric := <-metricCh:
			if metric.AudioDuration > 0 {
				success = metric
			}
		case <-time.After(time.Second):
			t.Fatal("missing TTS metric")
		}
	}
	if success.AudioDuration != audio.Frame.Duration() || success.Metadata.ModelName != string(ElevenV3) || success.Streamed {
		t.Fatalf("unexpected success metric: %+v", success)
	}
	if err := client.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPStreamingTTSRejectsCompressedAndMismatchedContent(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "audio/mpeg")
		_, _ = w.Write([]byte("not pcm"))
	}))
	defer server.Close()

	mp3, err := NewTTS(TTSOptions{APIKey: "x", BaseURL: server.URL, Encoding: MP344100128})
	if err != nil {
		t.Fatal(err)
	}
	_, err = mp3.Synthesize(t.Context(), "hello", tts.SynthesizeOptions{})
	var unsupported *UnsupportedEncodingError
	if !errors.As(err, &unsupported) || requests.Load() != 0 {
		t.Fatalf("MP3 must fail before network I/O: %T %v requests=%d", err, err, requests.Load())
	}
	_ = mp3.Close(t.Context())

	pcm, err := NewTTS(TTSOptions{APIKey: "x", BaseURL: server.URL, Encoding: PCM22050})
	if err != nil {
		t.Fatal(err)
	}
	value, err := pcm.Synthesize(t.Context(), "hello", tts.SynthesizeOptions{ConnectOptions: agents.APIConnectOptions{MaxRetries: -1}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = value.Recv(t.Context())
	var protocol *ProtocolError
	if !errors.As(err, &protocol) {
		t.Fatalf("expected strict content type ProtocolError, got %T: %v", err, err)
	}
	_ = pcm.Close(t.Context())
}

func TestPCMContentTypeRejectsAmbiguousBinary(t *testing.T) {
	for _, contentType := range []string{"application/octet-stream", "audio/l16", "audio/raw", "audio/mpeg"} {
		if err := requirePCMContentType(contentType); err == nil {
			t.Fatalf("ambiguous/compressed content type %q was accepted as little-endian PCM", contentType)
		}
	}
	for _, contentType := range []string{"audio/pcm; rate=22050", "audio/x-pcm"} {
		if err := requirePCMContentType(contentType); err != nil {
			t.Fatalf("PCM content type %q rejected: %v", contentType, err)
		}
	}
}

func TestListVoicesTypedStatusAndSuccess(t *testing.T) {
	var fail atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/voices" {
			t.Errorf("path=%s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		if fail.Load() {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"detail":"unauthorized"}`))
			return
		}
		_, _ = w.Write([]byte(`{"voices":[{"voice_id":"v","name":"Voice","category":"premade"}]}`))
	}))
	defer server.Close()
	client, err := NewTTS(TTSOptions{APIKey: "x", BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	voices, err := client.ListVoices(t.Context())
	if err != nil || len(voices) != 1 || voices[0].ID != "v" {
		t.Fatalf("voices=%+v err=%v", voices, err)
	}
	fail.Store(true)
	_, err = client.ListVoices(t.Context())
	var status *agents.APIStatusError
	if !errors.As(err, &status) || status.StatusCode != http.StatusUnauthorized || status.Retryable() {
		t.Fatalf("unexpected status: %T %v", err, err)
	}
	_ = client.Close(t.Context())
}

func TestHTTPStreamingTTSCancellation(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer server.Close()
	defer close(release)
	client, err := NewTTS(TTSOptions{APIKey: "x", BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	stream, err := client.Synthesize(ctx, "hello", tts.SynthesizeOptions{ConnectOptions: agents.APIConnectOptions{MaxRetries: -1}})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	_, err = stream.Recv(t.Context())
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected cancellation result: %v", err)
	}
	_ = client.Close(t.Context())
}
