// SPDX-License-Identifier: Apache-2.0

package workerprotocol

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/livekit/protocol/livekit"
	"google.golang.org/protobuf/proto"
)

func TestAgentEndpoint(t *testing.T) {
	tests := []struct {
		input string
		token string
		want  string
	}{
		{"http://localhost:7880", "", "ws://localhost:7880/agent"},
		{"https://example.test/livekit/?region=in", "secret", "wss://example.test/livekit/agent?region=in&worker_token=secret"},
		{"ws://example.test/base", "", "ws://example.test/base/agent"},
	}
	for _, test := range tests {
		got, err := AgentEndpoint(test.input, test.token)
		if err != nil {
			t.Fatalf("AgentEndpoint(%q): %v", test.input, err)
		}
		if got.String() != test.want {
			t.Errorf("AgentEndpoint(%q) = %q, want %q", test.input, got, test.want)
		}
	}
	for _, invalid := range []string{"localhost:7880", "ftp://localhost:7880", "://bad"} {
		if _, err := AgentEndpoint(invalid, ""); err == nil {
			t.Errorf("AgentEndpoint(%q) succeeded", invalid)
		}
	}
}

func TestGorillaTransportBinaryProtocol(t *testing.T) {
	serverDone := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/agent" {
			http.NotFound(response, request)
			return
		}
		if got := request.Header.Get("Authorization"); got != "Bearer test-token" {
			serverDone <- errors.New("authorization header mismatch: " + got)
			return
		}
		conn, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(response, request, nil)
		if err != nil {
			serverDone <- err
			return
		}
		defer conn.Close()
		messageType, payload, err := conn.ReadMessage()
		if err != nil {
			serverDone <- err
			return
		}
		if messageType != websocket.BinaryMessage {
			serverDone <- errors.New("worker message was not binary")
			return
		}
		worker := new(livekit.WorkerMessage)
		if err := proto.Unmarshal(payload, worker); err != nil {
			serverDone <- err
			return
		}
		if worker.GetRegister().GetAgentName() != "test-agent" {
			serverDone <- errors.New("registration agent name mismatch")
			return
		}
		encoded, err := proto.Marshal(&livekit.ServerMessage{Message: &livekit.ServerMessage_Register{Register: &livekit.RegisterWorkerResponse{WorkerId: "worker-1"}}})
		if err == nil {
			err = conn.WriteMessage(websocket.BinaryMessage, encoded)
		}
		serverDone <- err
	}))
	defer server.Close()

	endpoint, err := AgentEndpoint(strings.Replace(server.URL, "http://", "ws://", 1), "")
	if err != nil {
		t.Fatal(err)
	}
	transport, err := (GorillaDialer{}).Dial(context.Background(), endpoint, http.Header{"Authorization": []string{"Bearer test-token"}})
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()
	register := &livekit.WorkerMessage{Message: &livekit.WorkerMessage_Register{Register: &livekit.RegisterWorkerRequest{AgentName: "test-agent"}}}
	if err := transport.Write(context.Background(), register); err != nil {
		t.Fatal(err)
	}
	response, err := transport.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if response.GetRegister().GetWorkerId() != "worker-1" {
		t.Fatalf("worker ID = %q", response.GetRegister().GetWorkerId())
	}
	if err := <-serverDone; err != nil {
		t.Fatal(err)
	}
}

func TestGorillaTransportReadHonorsContext(t *testing.T) {
	upgraded := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		conn, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(response, request, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		close(upgraded)
		<-request.Context().Done()
	}))
	defer server.Close()
	endpoint, _ := AgentEndpoint(strings.Replace(server.URL, "http://", "ws://", 1), "")
	transport, err := (GorillaDialer{}).Dial(context.Background(), endpoint, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()
	<-upgraded
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = transport.Read(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Read error = %v, want context deadline", err)
	}
}
