// SPDX-License-Identifier: Apache-2.0

package agentscli

import (
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	agents "github.com/infinityscroll/livekit-agents-go"
	agentpb "github.com/livekit/protocol/livekit/agent"
	"google.golang.org/protobuf/proto"
)

type devClient struct {
	once       sync.Once
	cancel     context.CancelFunc
	done       chan struct{}
	connection net.Conn
	mu         sync.Mutex
}

func startDevClient(address, agentName, url string, logger *slog.Logger) *devClient {
	if address == "" {
		return nil
	}
	if logger == nil {
		logger = slog.Default()
	}
	if _, _, err := net.SplitHostPort(address); err != nil {
		logger.Warn("invalid --cli-addr; skipping dev channel", "address", address, "error", err)
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	client := &devClient{cancel: cancel, done: make(chan struct{})}
	go client.run(ctx, address, agentName, url, logger)
	return client
}

func (c *devClient) run(ctx context.Context, address, agentName, url string, logger *slog.Logger) {
	defer close(c.done)
	dialer := net.Dialer{Timeout: 2 * time.Second, KeepAlive: 30 * time.Second}
	connection, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		logger.Debug("dev channel unavailable", "error", err)
		return
	}
	c.mu.Lock()
	c.connection = connection
	c.mu.Unlock()
	defer connection.Close()
	message := &agentpb.AgentDevMessage{Message: &agentpb.AgentDevMessage_ServerInfo{ServerInfo: &agentpb.ServerInfo{AgentName: agentName, Url: url}}}
	payload, err := proto.Marshal(message)
	if err != nil {
		logger.Debug("failed to encode dev server info", "error", err)
		return
	}
	if len(payload) > 1<<20 {
		logger.Debug("dev server info exceeds protocol limit")
		return
	}
	_ = connection.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if err := writeDevFrame(connection, payload); err != nil {
		logger.Debug("failed to report dev server info", "error", err)
	}
}

func writeDevFrame(writer io.Writer, payload []byte) error {
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
	buffers := net.Buffers{header[:], payload}
	_, err := buffers.WriteTo(writer)
	return err
}

func (c *devClient) Close() error {
	if c == nil {
		return nil
	}
	c.once.Do(func() {
		c.cancel()
		c.mu.Lock()
		if c.connection != nil {
			_ = c.connection.Close()
		}
		c.mu.Unlock()
	})
	select {
	case <-c.done:
		return nil
	case <-time.After(3 * time.Second):
		return context.DeadlineExceeded
	}
}

func effectiveAgentName[UserData any](options agents.ServerOptions[UserData], getenv func(string) string) string {
	if value := getenv("LIVEKIT_AGENT_NAME_OVERRIDE"); value != "" {
		return value
	}
	if options.AgentName != "" {
		return options.AgentName
	}
	return getenv("LIVEKIT_AGENT_NAME")
}

func effectiveURL[UserData any](options agents.ServerOptions[UserData], getenv func(string) string) string {
	if options.URL != "" {
		return options.URL
	}
	if options.WSURL != "" {
		return options.WSURL
	}
	if value := getenv("LIVEKIT_URL"); value != "" {
		return value
	}
	return "ws://localhost:7880"
}

var _ io.Closer = (*devClient)(nil)
