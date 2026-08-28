// SPDX-License-Identifier: Apache-2.0

package agents

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/livekit/protocol/livekit"
)

type workerHealth struct {
	healthy bool
	live    bool
	message string
}

type workerDescription struct {
	AgentName       string `json:"agent_name"`
	AgentNameIsEnv  bool   `json:"agent_name_is_env"`
	Deployment      string `json:"deployment"`
	WorkerType      string `json:"worker_type"`
	ActiveJobs      int    `json:"active_jobs"`
	SDKVersion      string `json:"sdk_version"`
	ProjectType     string `json:"project_type"`
	ProtocolVersion int    `json:"protocol_version"`
}

type workerHTTPServer struct {
	host string
	port int

	health func() workerHealth
	worker func() workerDescription

	mu       sync.RWMutex
	server   *http.Server
	listener net.Listener
	started  bool
}

func newWorkerHTTPServer(host string, port int, health func() workerHealth, worker func() workerDescription) *workerHTTPServer {
	return &workerHTTPServer{host: host, port: port, health: health, worker: worker}
}

func (s *workerHTTPServer) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		return errors.New("agents: worker HTTP server is already started")
	}
	listener, err := net.Listen("tcp", net.JoinHostPort(s.host, fmt.Sprint(s.port)))
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleRoot)
	mux.HandleFunc("/readyz", s.handleReady)
	mux.HandleFunc("/livez", s.handleLive)
	mux.HandleFunc("/worker", s.handleWorker)
	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       time.Minute,
		MaxHeaderBytes:    16 << 10,
	}
	s.server = server
	s.listener = listener
	s.started = true
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			// Readiness observes the listener state; there is no safe way to
			// return an asynchronous Serve error through Start.
			_ = listener.Close()
		}
	}()
	return nil
}

func (s *workerHTTPServer) Address() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

func (s *workerHTTPServer) Close(ctx context.Context) error {
	s.mu.RLock()
	server := s.server
	s.mu.RUnlock()
	if server == nil {
		return nil
	}
	return server.Shutdown(ctx)
}

func (s *workerHTTPServer) handleRoot(response http.ResponseWriter, request *http.Request) {
	if request.URL.Path != "/" {
		http.NotFound(response, request)
		return
	}
	s.writeHealth(response, request, false)
}

func (s *workerHTTPServer) handleReady(response http.ResponseWriter, request *http.Request) {
	s.writeHealth(response, request, false)
}

func (s *workerHTTPServer) handleLive(response http.ResponseWriter, request *http.Request) {
	s.writeHealth(response, request, true)
}

func (s *workerHTTPServer) writeHealth(response http.ResponseWriter, request *http.Request, liveness bool) {
	if request.Method != http.MethodGet && request.Method != http.MethodHead {
		response.Header().Set("Allow", "GET, HEAD")
		http.Error(response, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	result := s.health()
	healthy := result.healthy
	if liveness {
		healthy = result.live
	}
	status := http.StatusOK
	if !healthy {
		status = http.StatusServiceUnavailable
	}
	response.Header().Set("Content-Type", "text/plain; charset=utf-8")
	response.WriteHeader(status)
	if request.Method != http.MethodHead {
		_, _ = response.Write([]byte(result.message))
	}
}

func (s *workerHTTPServer) handleWorker(response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet && request.Method != http.MethodHead {
		response.Header().Set("Allow", "GET, HEAD")
		http.Error(response, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(http.StatusOK)
	if request.Method == http.MethodHead {
		return
	}
	_ = json.NewEncoder(response).Encode(s.worker())
}

func (s *AgentServer[T]) healthSnapshot() workerHealth {
	s.stateMu.RLock()
	started := s.started
	closed := s.closed
	connecting := s.connecting
	registered := s.registered
	s.stateMu.RUnlock()
	live := started && !closed
	if !live {
		return workerHealth{message: "worker is not running"}
	}
	if err := s.pool.Healthy(); err != nil {
		return workerHealth{live: true, message: err.Error()}
	}
	if connecting || !registered {
		return workerHealth{live: true, message: "not connected to livekit"}
	}
	return workerHealth{healthy: true, live: true, message: "OK"}
}

func (s *AgentServer[T]) workerSnapshot() workerDescription {
	return workerDescription{
		AgentName: s.opts.AgentName, AgentNameIsEnv: s.opts.AgentNameIsEnv,
		Deployment: s.opts.Deployment, WorkerType: s.opts.ServerType.String(),
		ActiveJobs: len(s.pool.ActiveJobs()), SDKVersion: Version, ProjectType: "go",
		ProtocolVersion: livekit.CurrentWorkerProtocol,
	}
}
