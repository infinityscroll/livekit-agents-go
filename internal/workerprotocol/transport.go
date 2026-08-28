// SPDX-License-Identifier: Apache-2.0

// Package workerprotocol implements the LiveKit agent worker WebSocket wire
// protocol. It deliberately exposes a small transport interface so the worker
// actor can be tested without a network connection.
package workerprotocol

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/livekit/protocol/livekit"
	"google.golang.org/protobuf/proto"
)

const (
	defaultHandshakeTimeout = 10 * time.Second
	defaultWriteTimeout     = 10 * time.Second
	defaultReadLimit        = 8 << 20
)

var (
	ErrUnexpectedMessageType = errors.New("worker protocol: expected a binary WebSocket message")
	ErrClosed                = errors.New("worker protocol: transport is closed")
)

// Transport permits one concurrent reader and one concurrent writer. Close is
// idempotent and unblocks an outstanding Read.
type Transport interface {
	Read(context.Context) (*livekit.ServerMessage, error)
	Write(context.Context, *livekit.WorkerMessage) error
	Close() error
}

// Dialer opens a worker protocol transport.
type Dialer interface {
	Dial(context.Context, *url.URL, http.Header) (Transport, error)
}

// GorillaDialer is the production WebSocket dialer. Zero fields use safe
// defaults. Compression stays disabled because protobuf worker messages are
// small and compression adds CPU and cross-message memory state.
type GorillaDialer struct {
	HandshakeTimeout time.Duration
	WriteTimeout     time.Duration
	ReadLimit        int64
	Proxy            func(*http.Request) (*url.URL, error)
}

func (d GorillaDialer) Dial(ctx context.Context, endpoint *url.URL, header http.Header) (Transport, error) {
	if endpoint == nil {
		return nil, errors.New("worker protocol: nil endpoint")
	}
	handshakeTimeout := d.HandshakeTimeout
	if handshakeTimeout <= 0 {
		handshakeTimeout = defaultHandshakeTimeout
	}
	writeTimeout := d.WriteTimeout
	if writeTimeout <= 0 {
		writeTimeout = defaultWriteTimeout
	}
	readLimit := d.ReadLimit
	if readLimit <= 0 {
		readLimit = defaultReadLimit
	}

	proxy := d.Proxy
	if proxy == nil {
		proxy = http.ProxyFromEnvironment
	}
	wd := websocket.Dialer{
		HandshakeTimeout:  handshakeTimeout,
		EnableCompression: false,
		Proxy:             proxy,
	}
	conn, resp, err := wd.DialContext(ctx, endpoint.String(), header)
	if err != nil {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		return nil, fmt.Errorf("worker protocol: dial %s: %w", endpoint.Redacted(), err)
	}
	conn.SetReadLimit(readLimit)
	return &websocketTransport{conn: conn, writeTimeout: writeTimeout, closed: make(chan struct{})}, nil
}

type websocketTransport struct {
	conn         *websocket.Conn
	writeTimeout time.Duration
	writeMu      sync.Mutex
	closeOnce    sync.Once
	closedInit   sync.Once
	closed       chan struct{}
}

func (t *websocketTransport) ensureClosedChannel() chan struct{} {
	// All production instances are constructed in this package. The lazy path
	// keeps a zero-value test instance from panicking.
	t.closedInit.Do(func() {
		if t.closed == nil {
			t.closed = make(chan struct{})
		}
	})
	return t.closed
}

func (t *websocketTransport) Read(ctx context.Context) (*livekit.ServerMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if t.conn == nil {
		return nil, ErrClosed
	}

	deadlineDone := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = t.conn.SetReadDeadline(time.Now())
		close(deadlineDone)
	})
	defer func() {
		if !stop() {
			<-deadlineDone
		}
		_ = t.conn.SetReadDeadline(time.Time{})
	}()

	messageType, payload, err := t.conn.ReadMessage()
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		select {
		case <-t.ensureClosedChannel():
			return nil, ErrClosed
		default:
		}
		return nil, fmt.Errorf("worker protocol: read: %w", err)
	}
	if messageType != websocket.BinaryMessage {
		return nil, fmt.Errorf("%w: type %d", ErrUnexpectedMessageType, messageType)
	}

	msg := new(livekit.ServerMessage)
	if err := proto.Unmarshal(payload, msg); err != nil {
		return nil, fmt.Errorf("worker protocol: decode server message: %w", err)
	}
	return msg, nil
}

func (t *websocketTransport) Write(ctx context.Context, msg *livekit.WorkerMessage) error {
	if msg == nil {
		return errors.New("worker protocol: nil worker message")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if t.conn == nil {
		return ErrClosed
	}
	payload, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("worker protocol: encode worker message: %w", err)
	}

	t.writeMu.Lock()
	defer t.writeMu.Unlock()
	select {
	case <-t.ensureClosedChannel():
		return ErrClosed
	default:
	}

	deadline := time.Now().Add(t.writeTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if err := t.conn.SetWriteDeadline(deadline); err != nil {
		return fmt.Errorf("worker protocol: set write deadline: %w", err)
	}
	if err := t.conn.WriteMessage(websocket.BinaryMessage, payload); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("worker protocol: write: %w", err)
	}
	return nil
}

func (t *websocketTransport) Close() error {
	var closeErr error
	t.closeOnce.Do(func() {
		close(t.ensureClosedChannel())
		if t.conn == nil {
			return
		}
		t.writeMu.Lock()
		deadline := time.Now().Add(time.Second)
		_ = t.conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), deadline)
		closeErr = t.conn.Close()
		t.writeMu.Unlock()
	})
	return closeErr
}

// AgentEndpoint converts an HTTP(S) or WebSocket LiveKit URL into the worker
// WebSocket endpoint and adds an optional Cloud worker token.
func AgentEndpoint(rawURL, workerToken string) (*url.URL, error) {
	u, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return nil, fmt.Errorf("worker protocol: parse URL: %w", err)
	}
	if u.Host == "" {
		return nil, errors.New("worker protocol: URL requires a host")
	}
	if u.Fragment != "" {
		return nil, errors.New("worker protocol: URL must not contain a fragment")
	}
	switch strings.ToLower(u.Scheme) {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	case "ws", "wss":
	default:
		return nil, fmt.Errorf("worker protocol: unsupported URL scheme %q", u.Scheme)
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/agent"
	u.RawPath = ""
	if workerToken != "" {
		q := u.Query()
		q.Set("worker_token", workerToken)
		u.RawQuery = q.Encode()
	}
	return u, nil
}
