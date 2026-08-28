// SPDX-License-Identifier: Apache-2.0

package inference

import (
	"context"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/metrics"
	protocol "github.com/livekit/protocol/livekit/agent"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func readTurnClientMessage(conn *websocket.Conn) (*protocol.ClientMessage, error) {
	messageType, payload, err := conn.ReadMessage()
	if err != nil {
		return nil, err
	}
	if messageType != websocket.BinaryMessage {
		return nil, errors.New("turn detector client message was not binary")
	}
	var message protocol.ClientMessage
	if err := proto.Unmarshal(payload, &message); err != nil {
		return nil, err
	}
	return &message, nil
}

func writeTurnServerMessage(conn *websocket.Conn, message *protocol.ServerMessage) error {
	payload, err := proto.Marshal(message)
	if err != nil {
		return err
	}
	return conn.WriteMessage(websocket.BinaryMessage, payload)
}

func TestCloudTurnTransportBinaryProtocolAndMetrics(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	ready := make(chan struct{})
	serverErrors := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/eot" {
			serverErrors <- errors.New("unexpected EOT path: " + request.URL.Path)
			return
		}
		if !strings.HasPrefix(request.Header.Get("Authorization"), "Bearer ") {
			serverErrors <- errors.New("missing EOT authorization")
			return
		}
		conn, err := upgrader.Upgrade(writer, request, nil)
		if err != nil {
			serverErrors <- err
			return
		}
		defer conn.Close()

		create, err := readTurnClientMessage(conn)
		if err != nil {
			serverErrors <- err
			return
		}
		settings := create.GetSessionCreate().GetSettings()
		if settings == nil || settings.GetSampleRate() != 16_000 || settings.GetEncoding() != protocol.AudioEncoding_AUDIO_ENCODING_PCM_S16LE || settings.GetTypeSettings() != nil {
			serverErrors <- errors.New("invalid EOT session.create")
			return
		}
		if err := writeTurnServerMessage(conn, &protocol.ServerMessage{
			Message: &protocol.ServerMessage_SessionCreated{SessionCreated: &protocol.SessionCreated{
				DefaultThresholds: map[string]float32{"en": .42, "fr": .36}, DefaultThreshold: .4,
				DefaultBackchannelThresholds: map[string]float32{"en": .8}, DefaultBackchannelThreshold: .75,
			}},
		}); err != nil {
			serverErrors <- err
			return
		}
		close(ready)

		start, err := readTurnClientMessage(conn)
		if err != nil {
			serverErrors <- err
			return
		}
		requestID := start.GetInferenceStart().GetRequestId()
		if requestID == "" {
			serverErrors <- errors.New("missing inference.start request ID")
			return
		}
		audio, err := readTurnClientMessage(conn)
		if err != nil {
			serverErrors <- err
			return
		}
		input := audio.GetInputAudio()
		if input == nil || input.GetNumSamples() != 320 || len(input.GetAudio()) != 640 || input.GetCreatedAt() == nil {
			serverErrors <- errors.New("invalid EOT input_audio")
			return
		}
		latest := timestamppb.New(time.Now().Add(-20 * time.Millisecond))
		if err := writeTurnServerMessage(conn, &protocol.ServerMessage{
			RequestId: &requestID,
			Message: &protocol.ServerMessage_EotPrediction{EotPrediction: &protocol.EotPrediction{
				Probability: .77, BackchannelProbability: .82,
				InferenceStats: &protocol.InferenceStats{
					LatestClientCreatedAt: latest,
					ClientE2ELatency:      durationpb.New(30 * time.Millisecond),
					ServerE2ELatency:      durationpb.New(9 * time.Millisecond),
				},
			}},
		}); err != nil {
			serverErrors <- err
			return
		}
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) || errors.Is(err, io.EOF) {
					serverErrors <- nil
					return
				}
				serverErrors <- nil // client teardown commonly surfaces close 1006 to the peer
				return
			}
		}
	}))
	defer server.Close()

	detector, err := NewTurnDetector(TurnDetectorOptions{
		Version: TurnDetectorV1, BaseURL: server.URL + "/v1", Credentials: testCredentials(),
		ConnectOptions: agents.APIConnectOptions{MaxRetries: -1, Timeout: 2 * time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	metricValues := make(chan metrics.EOTInference, 1)
	unsubscribe := detector.OnMetrics(func(metric metrics.EOTInference) { metricValues <- metric })
	defer unsubscribe()
	stream, err := detector.Stream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("cloud EOT session did not become ready")
	}
	prediction, err := stream.BeginPrediction(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	transport, ok := stream.transport.(*cloudTurnTransport)
	if !ok {
		t.Fatalf("transport = %T", stream.transport)
	}
	frame, _ := agents.NewAudioFrame(make([]int16, 320), 16_000, 1)
	if err := transport.PushFrame(context.Background(), frame); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	event, err := prediction.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(event.EndOfTurnProbability-.77) > 1e-6 || event.BackchannelProbability == nil || math.Abs(*event.BackchannelProbability-.82) > 1e-6 || event.InferenceDuration == nil || *event.InferenceDuration != 9*time.Millisecond || event.DetectionDelay == nil || *event.DetectionDelay < 15*time.Millisecond {
		t.Fatalf("EOT event = %+v", event)
	}
	if got, ok := detector.UnlikelyThreshold(agents.AsLanguageCode("en-US")); !ok || got != .42 {
		t.Fatalf("server threshold = %v, %t", got, ok)
	}
	select {
	case metric := <-metricValues:
		if metric.NumRequests != 1 || metric.PredictionDuration != 9*time.Millisecond || metric.TotalDuration != 30*time.Millisecond || metric.Metadata.ModelProvider != "livekit" || metric.Metadata.ModelName != string(TurnDetectorModelV1) {
			t.Fatalf("EOT metric = %+v", metric)
		}
	case <-ctx.Done():
		t.Fatal("missing EOT metric")
	}
	if err := detector.Close(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-serverErrors:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("EOT server did not stop")
	}
}

func BenchmarkLocalTurnAudioWindow(b *testing.B) {
	ring := newPCMRing(19_200)
	frame := make([]int16, 1_600)
	b.ReportAllocs()
	b.SetBytes(int64(len(frame) * 2))
	for b.Loop() {
		ring.push(frame)
		_ = ring.snapshot()
	}
}
