// SPDX-License-Identifier: Apache-2.0

package agents

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestWorkerHTTPServerHealthAndDescription(t *testing.T) {
	var ready atomic.Bool
	server := newWorkerHTTPServer("127.0.0.1", 0, func() workerHealth {
		if ready.Load() {
			return workerHealth{healthy: true, live: true, message: "OK"}
		}
		return workerHealth{live: true, message: "not connected to livekit"}
	}, func() workerDescription {
		return workerDescription{AgentName: "support", WorkerType: "JT_ROOM", ActiveJobs: 2, SDKVersion: "test", ProjectType: "go", ProtocolVersion: 1}
	})
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	t.Cleanup(func() { _ = server.Close(closeCtx) })
	baseURL := "http://" + server.Address()

	assertHTTP(t, baseURL+"/", http.StatusServiceUnavailable, "not connected to livekit")
	assertHTTP(t, baseURL+"/readyz", http.StatusServiceUnavailable, "not connected to livekit")
	assertHTTP(t, baseURL+"/livez", http.StatusOK, "not connected to livekit")
	ready.Store(true)
	assertHTTP(t, baseURL+"/", http.StatusOK, "OK")
	assertHTTP(t, baseURL+"/readyz", http.StatusOK, "OK")
	assertHTTP(t, baseURL+"/missing", http.StatusNotFound, "404 page not found")

	response, err := http.Get(baseURL + "/worker")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var description workerDescription
	if err := json.NewDecoder(response.Body).Decode(&description); err != nil {
		t.Fatal(err)
	}
	if description.AgentName != "support" || description.ActiveJobs != 2 || description.ProtocolVersion != 1 {
		t.Fatalf("worker description = %#v", description)
	}

	request, _ := http.NewRequest(http.MethodPost, baseURL+"/readyz", nil)
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("POST /readyz status = %d", response.StatusCode)
	}
}

func assertHTTP(t *testing.T, target string, status int, bodySubstring string) {
	t.Helper()
	response, err := http.Get(target)
	if err != nil {
		t.Fatal(err)
	}
	payload, readErr := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if readErr != nil {
		t.Fatal(readErr)
	}
	if response.StatusCode != status || !strings.Contains(string(payload), bodySubstring) {
		t.Fatalf("GET %s = (%d, %q), want status %d containing %q", target, response.StatusCode, payload, status, bodySubstring)
	}
}
