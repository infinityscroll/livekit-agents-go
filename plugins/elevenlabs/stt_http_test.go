// SPDX-License-Identifier: Apache-2.0

package elevenlabs

import (
	"context"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/metrics"
	"github.com/infinityscroll/livekit-agents-go/stt"
)

func TestBatchSTTRequestRetryMappingAndMetrics(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempt := attempts.Add(1)
		if r.Method != http.MethodPost || r.URL.Path != "/v1/speech-to-text" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get(AuthorizationHeader) != "secret" {
			t.Errorf("missing auth header")
		}
		if r.URL.Query().Get("enable_logging") != "false" {
			t.Errorf("unexpected enable_logging %q", r.URL.Query().Get("enable_logging"))
		}
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Errorf("multipart: %v", err)
			return
		}
		if got := r.FormValue("model_id"); got != string(ScribeV2) {
			t.Errorf("model_id=%q", got)
		}
		if got := r.FormValue("language_code"); got != "fr" {
			t.Errorf("language_code=%q", got)
		}
		if got := r.MultipartForm.Value["keyterms"]; !slices.Equal(got, []string{"LiveKit", "Agents", "Sanjana"}) {
			t.Errorf("keyterms=%v", got)
		}
		file, _, err := r.FormFile("file")
		if err != nil {
			t.Errorf("file: %v", err)
			return
		}
		defer file.Close()
		header := make([]byte, 44)
		if _, err := io.ReadFull(file, header); err != nil || string(header[:4]) != "RIFF" || string(header[8:12]) != "WAVE" {
			t.Errorf("invalid WAV header %q: %v", header[:12], err)
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		if attempt == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"detail":"retry"}`))
			return
		}
		w.Header().Set("request-id", "req-123")
		_, _ = w.Write([]byte(`{"text":"bonjour","language_code":"fr","words":[{"text":"bon","start":0.1,"end":0.2,"speaker_id":"speaker-1","type":"word","logprob":-0.2},{"text":"jour","start":0.2,"end":0.4,"type":"word","logprob":-0.4}]}`))
	}))
	defer server.Close()

	logging := false
	client, err := NewSTT(STTOptions{
		APIKey: "secret", BaseURL: server.URL + "/v1", Model: ScribeV2,
		Keyterms: []string{"LiveKit", "Agents"}, EnableLogging: &logging,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.UpdateSessionKeyterms([]string{"livekit", "Sanjana"}); err != nil {
		t.Fatal(err)
	}
	metricCh := make(chan metrics.STT, 1)
	unsubscribe := client.OnMetrics(func(metric metrics.STT) { metricCh <- metric })
	defer unsubscribe()
	frame, _ := agents.NewAudioFrame(make([]int16, 320), 16000, 1)
	event, err := client.Recognize(t.Context(), []agents.AudioFrame{frame}, stt.RecognizeOptions{
		Language: "French", ConnectOptions: agents.APIConnectOptions{MaxRetries: 1, RetryInterval: time.Millisecond, Timeout: time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	if attempts.Load() != 2 {
		t.Fatalf("attempts=%d", attempts.Load())
	}
	if event.RequestID != "req-123" || event.Type != stt.FinalTranscript || len(event.Alternatives) != 1 {
		t.Fatalf("unexpected event: %+v", event)
	}
	alternative := event.Alternatives[0]
	if alternative.Text != "bonjour" || alternative.Language != "fr" || alternative.SpeakerID != "speaker-1" {
		t.Fatalf("unexpected alternative: %+v", alternative)
	}
	wantConfidence := math.Exp(-0.3)
	if math.Abs(alternative.Confidence-wantConfidence) > 1e-9 || len(alternative.Words) != 2 {
		t.Fatalf("unexpected confidence/words: %+v", alternative)
	}
	select {
	case metric := <-metricCh:
		if metric.RequestID != "req-123" || metric.Streamed || metric.AudioDuration != frame.Duration() {
			t.Fatalf("unexpected metric: %+v", metric)
		}
	case <-time.After(time.Second):
		t.Fatal("missing metric")
	}
}

func TestBatchSTTPreservesStatusAndStrictContentType(t *testing.T) {
	tests := []struct {
		name        string
		status      int
		contentType string
		body        string
		wantStatus  bool
	}{
		{name: "status", status: http.StatusBadRequest, contentType: "application/json", body: `{"detail":"bad language"}`, wantStatus: true},
		{name: "content-type", status: http.StatusOK, contentType: "text/plain", body: `{"text":"no"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", test.contentType)
				w.WriteHeader(test.status)
				_, _ = w.Write([]byte(test.body))
			}))
			defer server.Close()
			client, err := NewSTT(STTOptions{APIKey: "x", BaseURL: server.URL})
			if err != nil {
				t.Fatal(err)
			}
			frame, _ := agents.NewAudioFrame(make([]int16, 160), 16000, 1)
			_, err = client.Recognize(t.Context(), []agents.AudioFrame{frame}, stt.RecognizeOptions{ConnectOptions: agents.APIConnectOptions{MaxRetries: -1}})
			if err == nil {
				t.Fatal("expected error")
			}
			var status *agents.APIStatusError
			if errors.As(err, &status) != test.wantStatus {
				t.Fatalf("status error=%v want %v: %T %v", errors.As(err, &status), test.wantStatus, err, err)
			}
			if test.wantStatus && (status.StatusCode != test.status || status.Message != "ElevenLabs API error: bad language") {
				t.Fatalf("unexpected status error: %+v", status)
			}
		})
	}
}

func TestBatchSTTDeadlineCancelsRequest(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer server.Close()
	defer close(release)
	client, err := NewSTT(STTOptions{APIKey: "x", BaseURL: server.URL})
	if err != nil {
		t.Fatal(err)
	}
	frame, _ := agents.NewAudioFrame(make([]int16, 160), 16000, 1)
	_, err = client.Recognize(context.Background(), []agents.AudioFrame{frame}, stt.RecognizeOptions{
		ConnectOptions: agents.APIConnectOptions{MaxRetries: -1, Timeout: 20 * time.Millisecond},
	})
	var timeout *agents.APITimeoutError
	if !errors.As(err, &timeout) {
		t.Fatalf("expected APITimeoutError, got %T: %v", err, err)
	}
}
