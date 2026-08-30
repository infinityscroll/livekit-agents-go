// SPDX-License-Identifier: Apache-2.0

package inference

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/stt"
)

func TestInferenceSTTStreamingWireAndEvents(t *testing.T) {
	t.Parallel()
	serverErrors := make(chan error, 1)
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/stt" {
			serverErrors <- &testError{"path", request.URL.Path}
			return
		}
		if request.Header.Get("Authorization") == "" {
			serverErrors <- errors.New("missing authorization header")
			return
		}
		conn, err := upgrader.Upgrade(writer, request, nil)
		if err != nil {
			serverErrors <- err
			return
		}
		defer conn.Close()
		var create STTSessionCreate
		if err := conn.ReadJSON(&create); err != nil {
			serverErrors <- err
			return
		}
		if create.Type != "session.create" || create.Model != "deepgram/nova-3" ||
			create.Settings.SampleRate != "16000" || create.Settings.Language != "en-US" {
			serverErrors <- &testError{"session.create", create}
			return
		}
		if err := conn.WriteJSON(map[string]any{"type": "session.created", "session_id": "stt-session"}); err != nil {
			serverErrors <- err
			return
		}
		var audio struct {
			Type  string `json:"type"`
			Audio string `json:"audio"`
		}
		if err := conn.ReadJSON(&audio); err != nil {
			serverErrors <- err
			return
		}
		pcm, err := base64.StdEncoding.DecodeString(audio.Audio)
		if err != nil || audio.Type != "input_audio" || len(pcm) != 1_600 {
			serverErrors <- &testError{"input_audio", map[string]any{"type": audio.Type, "bytes": len(pcm), "err": err}}
			return
		}
		var finalize map[string]any
		if err := conn.ReadJSON(&finalize); err != nil {
			serverErrors <- err
			return
		}
		if finalize["type"] != "session.finalize" {
			serverErrors <- &testError{"session.finalize", finalize}
			return
		}
		messages := []map[string]any{
			{"type": "start_of_speech"},
			{"type": "interim_transcript", "session_id": "stt-session", "transcript": "hello", "language": "en-US"},
			{
				"type": "final_transcript", "session_id": "stt-session", "transcript": "hello world",
				"language": "en-US", "start": 0.1, "duration": 0.5, "confidence": 0.9,
				"speaker_id": "speaker-a", "extra": map[string]any{"provider": "deepgram"},
				"words": []map[string]any{{"word": "hello", "start": 0.1, "end": 0.3, "confidence": 0.8, "speaker_id": "speaker-a"}},
			},
			{"type": "session.closed"},
		}
		for _, message := range messages {
			if err := conn.WriteJSON(message); err != nil {
				serverErrors <- err
				return
			}
		}
		serverErrors <- nil
	}))
	defer server.Close()

	client, err := NewSTT(STTOptions{
		Model: "deepgram/nova-3:en-US", BaseURL: server.URL + "/v1",
		Credentials: testCredentials(), ConnectOptions: agents.APIConnectOptions{MaxRetries: -1, Timeout: 2 * time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close(context.Background())
	streamValue, err := client.Stream(context.Background(), stt.StreamOptions{})
	if err != nil {
		t.Fatal(err)
	}
	stream := streamValue.(*SpeechStream)
	frame, _ := agents.NewAudioFrame(make([]int16, 800), 16_000, 1)
	if err := stream.Push(context.Background(), frame); err != nil {
		t.Fatal(err)
	}
	if err := stream.EndInput(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var events []stt.SpeechEvent
	for {
		event, err := stream.Recv(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, event)
	}
	wantTypes := []stt.SpeechEventType{stt.StartOfSpeech, stt.InterimTranscript, stt.RecognitionUsageEvent, stt.FinalTranscript, stt.EndOfSpeech}
	if len(events) != len(wantTypes) {
		t.Fatalf("events = %#v", events)
	}
	for i, want := range wantTypes {
		if events[i].Type != want {
			t.Fatalf("event %d type = %v, want %v", i, events[i].Type, want)
		}
	}
	if usage := events[2].RecognitionUsage; usage == nil || usage.AudioDuration != 50*time.Millisecond {
		t.Fatalf("usage = %+v", usage)
	}
	final := events[3]
	if final.RequestID != "stt-session" || len(final.Alternatives) != 1 {
		t.Fatalf("final = %+v", final)
	}
	data := final.Alternatives[0]
	if data.Text != "hello world" || data.SpeakerID != "speaker-a" || data.Metadata["provider"] != "deepgram" {
		t.Fatalf("speech data = %+v", data)
	}
	if len(data.Words) != 1 || data.Words[0].StartTime == nil || *data.Words[0].StartTime != 100*time.Millisecond ||
		data.Words[0].Confidence == nil || *data.Words[0].Confidence != 0.8 {
		t.Fatalf("word = %+v", data.Words)
	}
	if err := stream.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-serverErrors; err != nil {
		t.Fatal(err)
	}
}

func TestInferenceSTTLiveUpdateUsesSettingsEnvelope(t *testing.T) {
	t.Parallel()
	updateJSON := make(chan string, 1)
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		conn, err := upgrader.Upgrade(writer, request, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_, first, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var create STTSessionCreate
		if json.Unmarshal(first, &create) != nil {
			return
		}
		_, payload, err := conn.ReadMessage()
		if err == nil {
			updateJSON <- string(payload)
		}
	}))
	defer server.Close()
	client, err := NewSTT(STTOptions{Model: "deepgram/nova-3", BaseURL: server.URL, Credentials: testCredentials()})
	if err != nil {
		t.Fatal(err)
	}
	streamValue, err := client.Stream(context.Background(), stt.StreamOptions{})
	if err != nil {
		t.Fatal(err)
	}
	stream := streamValue.(*SpeechStream)
	language := agents.AsLanguageCode("fr-FR")
	if err := client.UpdateOptions(STTUpdateOptions{Language: &language, ModelOptions: ModelOptions{"keyterm": []string{"LiveKit"}}}); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-updateJSON:
		const want = `{"type":"session.update","settings":{"language":"fr-FR","extra":{"keyterm":["LiveKit"]}}}`
		if got != want {
			t.Fatalf("update JSON = %s, want %s", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for session.update")
	}
	_ = stream.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := stream.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkInferenceSTTPacketize50ms(b *testing.B) {
	frame, _ := agents.NewAudioFrame(make([]int16, 800), 16_000, 1)
	packetizer := newInferencePCMPacketizer(800)
	emit := func([]byte) error { return nil }
	b.ReportAllocs()
	b.SetBytes(int64(len(frame.Data) * 2))
	for b.Loop() {
		if err := packetizer.writeFrame(frame, emit); err != nil {
			b.Fatal(err)
		}
	}
}

type testError struct {
	field string
	got   any
}

func (e *testError) Error() string { return e.field + " mismatch" }
