// SPDX-License-Identifier: Apache-2.0

package elevenlabs

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/stt"
)

var testUpgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

func TestRealtimeSTTWireProtocolAndDelayedLanguage(t *testing.T) {
	serverErr := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/speech-to-text/realtime" {
			serverErr <- errors.New("unexpected websocket path: " + r.URL.Path)
			http.Error(w, "path", http.StatusNotFound)
			return
		}
		query := r.URL.Query()
		if query.Get("model_id") != string(ScribeV2Realtime) || query.Get("audio_format") != "pcm_16000" || query.Get("commit_strategy") != "manual" || query.Get("include_language_detection") != "true" {
			serverErr <- errors.New("unexpected realtime query: " + r.URL.RawQuery)
		}
		if got := query["keyterms"]; len(got) != 2 || got[0] != "LiveKit" || got[1] != "Sanjana & Co" {
			serverErr <- errors.New("unexpected keyterms")
		}
		if r.Header.Get(AuthorizationHeader) != "secret" {
			serverErr <- errors.New("missing websocket auth")
		}
		conn, err := testUpgrader.Upgrade(w, r, nil)
		if err != nil {
			serverErr <- err
			return
		}
		defer conn.Close()
		seenAudio := false
		for {
			var packet sttAudioPacket
			if err := conn.ReadJSON(&packet); err != nil {
				serverErr <- err
				return
			}
			if packet.MessageType != "input_audio_chunk" || packet.SampleRate != 16000 {
				serverErr <- errors.New("invalid input packet")
				return
			}
			if packet.AudioBase64 != "" {
				audio, err := base64.StdEncoding.DecodeString(packet.AudioBase64)
				if err != nil || len(audio) != 1600 {
					serverErr <- errors.New("invalid 50ms audio packet")
					return
				}
				seenAudio = true
				_ = conn.WriteJSON(map[string]any{"message_type": "partial_transcript", "text": "hol"})
			}
			if packet.Commit {
				if !seenAudio {
					serverErr <- errors.New("commit preceded audio")
					return
				}
				_ = conn.WriteJSON(map[string]any{"message_type": "committed_transcript", "text": "hola"})
				_ = conn.WriteJSON(map[string]any{
					"message_type": "committed_transcript_with_timestamps", "text": "hola", "language_code": "es",
					"words": []map[string]any{{"text": "hola", "start": 0.0, "end": 0.05, "type": "word", "logprob": -0.1}},
				})
				serverErr <- nil
				return
			}
		}
	}))
	defer server.Close()

	client, err := NewSTT(STTOptions{
		APIKey: "secret", BaseURL: server.URL + "/v1", Model: ScribeV2Realtime,
		Keyterms: []string{"LiveKit", "Sanjana & Co"},
	})
	if err != nil {
		t.Fatal(err)
	}
	streamValue, err := client.Stream(t.Context(), stt.StreamOptions{ConnectOptions: agents.APIConnectOptions{MaxRetries: -1, Timeout: time.Second}})
	if err != nil {
		t.Fatal(err)
	}
	stream := streamValue.(*SpeechStream)
	frame, _ := agents.NewAudioFrame(make([]int16, 800), 16000, 1)
	if err := stream.Push(t.Context(), frame); err != nil {
		t.Fatal(err)
	}
	// EndInput commits pending audio; callers cannot accidentally strand the
	// provider websocket by omitting a separate Flush.
	if err := stream.EndInput(); err != nil {
		t.Fatal(err)
	}
	var eventTypes []stt.SpeechEventType
	var final stt.SpeechData
	readCtx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	for {
		event, err := stream.Recv(readCtx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if event.Type == stt.RecognitionUsageEvent {
			continue
		}
		eventTypes = append(eventTypes, event.Type)
		if event.Type == stt.FinalTranscript {
			final = event.Alternatives[0]
		}
	}
	if len(eventTypes) != 3 || eventTypes[0] != stt.StartOfSpeech || eventTypes[1] != stt.InterimTranscript || eventTypes[2] != stt.FinalTranscript {
		t.Fatalf("unexpected event order: %v", eventTypes)
	}
	if final.Text != "hola" || final.Language != "es" || len(final.Words) != 1 {
		t.Fatalf("unexpected final transcript: %+v", final)
	}
	if err := <-serverErr; err != nil {
		t.Fatal(err)
	}
	if err := client.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestRealtimeSTTProviderErrorsAreTerminalAndTyped(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := testUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.WriteJSON(map[string]any{"message_type": "auth_error", "message": "bad key", "details": "revoked"})
		<-time.After(20 * time.Millisecond)
	}))
	defer server.Close()
	client, err := NewSTT(STTOptions{APIKey: "bad", BaseURL: server.URL, Model: ScribeV2Realtime})
	if err != nil {
		t.Fatal(err)
	}
	errorEvent := make(chan stt.ErrorEvent, 1)
	client.OnError(func(event stt.ErrorEvent) { errorEvent <- event })
	value, err := client.Stream(t.Context(), stt.StreamOptions{ConnectOptions: agents.APIConnectOptions{MaxRetries: -1}})
	if err != nil {
		t.Fatal(err)
	}
	stream := value.(*SpeechStream)
	recvCtx, cancelRecv := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancelRecv()
	_, err = stream.Recv(recvCtx)
	var provider *ProviderError
	if !errors.As(err, &provider) || provider.Type != "auth_error" || provider.Retryable() {
		t.Fatalf("expected terminal auth ProviderError, got %T: %v", err, err)
	}
	select {
	case event := <-errorEvent:
		if event.Recoverable || !errors.As(event.Err, &provider) {
			t.Fatalf("unexpected error event: %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("missing STT error event")
	}
	_ = client.Close(t.Context())
}

func TestRealtimeSTTCurrentProviderEventVocabulary(t *testing.T) {
	event, err := decodeRealtimeSTTEvent([]byte(`{"message_type":"rate_limited","error":"slow down"}`))
	if err != nil || event.Message != "slow down" {
		t.Fatalf("error alias decode: event=%+v err=%v", event, err)
	}
	tests := []struct {
		name      string
		retryable bool
	}{
		{name: "auth_error"},
		{name: "unaccepted_terms"},
		{name: "chunk_size_exceeded"},
		{name: "commit_throttled", retryable: true},
		{name: "rate_limited", retryable: true},
		{name: "session_time_limit_exceeded", retryable: true},
		{name: "insufficient_audio_activity", retryable: true},
	}
	for _, test := range tests {
		if !providerErrorType(test.name) {
			t.Fatalf("documented provider event %q is not recognized", test.name)
		}
		provider := newSTTProviderError(realtimeSTTEvent{MessageType: test.name}).(*ProviderError)
		if provider.Retryable() != test.retryable {
			t.Fatalf("%s retryable=%v want %v", test.name, provider.Retryable(), test.retryable)
		}
	}
	state := realtimeSTTState{}
	stream := &SpeechStream{BaseStream: stt.NewBaseStream(t.Context(), 1)}
	accepted, err := stream.processRealtimeSTTEvent(t.Context(), resolvedSTTOptions{}, &state, realtimeSTTEvent{MessageType: "warning"})
	if err != nil || accepted {
		t.Fatalf("warning must be non-terminal: accepted=%v err=%v", accepted, err)
	}
	_ = stream.Close()
}

func TestRealtimeSTTRetriesAndReplaysUncommittedTurn(t *testing.T) {
	var connections atomic.Int32
	replayed := make(chan int, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		index := connections.Add(1)
		conn, err := testUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		audioPackets := 0
		for {
			var packet sttAudioPacket
			if err := conn.ReadJSON(&packet); err != nil {
				return
			}
			if packet.AudioBase64 != "" {
				audioPackets++
				if index == 1 {
					_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseInternalServerErr, "retry"), time.Now().Add(time.Second))
					return
				}
			}
			if packet.Commit && index == 2 {
				replayed <- audioPackets
				_ = conn.WriteJSON(map[string]any{"message_type": "committed_transcript", "text": "replayed", "language_code": "en"})
				return
			}
		}
	}))
	defer server.Close()
	client, err := NewSTT(STTOptions{APIKey: "x", BaseURL: server.URL, Model: ScribeV2Realtime, Language: "en"})
	if err != nil {
		t.Fatal(err)
	}
	value, err := client.Stream(t.Context(), stt.StreamOptions{ConnectOptions: agents.APIConnectOptions{MaxRetries: 2, RetryInterval: time.Millisecond}})
	if err != nil {
		t.Fatal(err)
	}
	stream := value.(*SpeechStream)
	frame, _ := agents.NewAudioFrame(make([]int16, 800), 16000, 1)
	_ = stream.Push(t.Context(), frame)
	_ = stream.EndInput()
	readCtx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	seenFinal := false
	for {
		event, err := stream.Recv(readCtx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if event.Type == stt.FinalTranscript && event.Alternatives[0].Text == "replayed" {
			seenFinal = true
		}
	}
	if !seenFinal || connections.Load() != 2 {
		t.Fatalf("final=%v connections=%d", seenFinal, connections.Load())
	}
	if got := <-replayed; got != 1 {
		t.Fatalf("replayed audio packets=%d", got)
	}
	_ = client.Close(t.Context())
}
