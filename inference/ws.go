// SPDX-License-Identifier: Apache-2.0

package inference

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	agents "github.com/livekit/agents-go"
)

const maxWebSocketErrorBodyBytes = 1 << 20

type gatewayWSDialOptions struct {
	BaseURL      string
	Endpoint     string
	Credentials  Credentials
	Metadata     func(context.Context) RequestMetadata
	Dialer       *websocket.Dialer
	ReadLimit    int64
	WriteTimeout time.Duration
}

// gatewayWS serializes writers as required by gorilla/websocket. Exactly one
// wsReader may be active at a time.
type gatewayWS struct {
	conn         *websocket.Conn
	writeGate    chan struct{}
	writeTimeout time.Duration
	generation   uint64
	closeOnce    sync.Once
}

func dialGatewayWS(ctx context.Context, options gatewayWSDialOptions) (*gatewayWS, error) {
	target, err := inferenceWebSocketURL(options.BaseURL, options.Endpoint)
	if err != nil {
		return nil, err
	}
	token, err := AccessToken(options.Credentials, 0)
	if err != nil {
		return nil, err
	}
	var header http.Header
	if options.Metadata != nil {
		header = MetadataHeaders(options.Metadata(ctx))
	} else {
		header = MetadataHeaders(RequestMetadata{})
	}
	header.Set("Authorization", "Bearer "+token)

	dialer := options.Dialer
	if dialer == nil {
		dialer = websocket.DefaultDialer
	}
	conn, response, err := dialer.DialContext(ctx, target, header)
	if err != nil {
		return nil, gatewayDialError(ctx, response, err)
	}
	if options.ReadLimit <= 0 {
		options.ReadLimit = MaxControlMessageBytes
	}
	conn.SetReadLimit(options.ReadLimit)
	result := &gatewayWS{
		conn: conn, writeGate: make(chan struct{}, 1), writeTimeout: options.WriteTimeout,
	}
	result.writeGate <- struct{}{}
	return result, nil
}

func inferenceWebSocketURL(baseURL, endpoint string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil {
		return "", fmt.Errorf("invalid inference base URL %q: %w", baseURL, err)
	}
	switch parsed.Scheme {
	case "http":
		parsed.Scheme = "ws"
	case "https":
		parsed.Scheme = "wss"
	case "ws", "wss":
	default:
		return "", fmt.Errorf("invalid inference base URL scheme %q", parsed.Scheme)
	}
	if parsed.Host == "" {
		return "", fmt.Errorf("invalid inference base URL %q: missing host", baseURL)
	}
	if parsed.User != nil {
		return "", fmt.Errorf("invalid inference base URL %q: userinfo is not allowed", baseURL)
	}
	parsed.Path = path.Join(parsed.Path, endpoint)
	parsed.RawPath = ""
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String(), nil
}

func gatewayDialError(ctx context.Context, response *http.Response, cause error) error {
	if response != nil {
		defer response.Body.Close()
		body, readErr := readLimitedBody(response.Body, maxWebSocketErrorBodyBytes)
		if readErr != nil {
			body = []byte(readErr.Error())
		}
		var decoded any
		if len(body) != 0 && json.Unmarshal(body, &decoded) != nil {
			decoded = string(body)
		}
		message := http.StatusText(response.StatusCode)
		if object, ok := decoded.(map[string]any); ok {
			if value, _ := object["message"].(string); value != "" {
				message = value
			} else if value, _ := object["detail"].(string); value != "" {
				message = value
			}
		}
		retryable := response.StatusCode == http.StatusRequestTimeout ||
			response.StatusCode == http.StatusTooManyRequests || response.StatusCode >= 500
		return agents.NewAPIStatusError(
			"LiveKit Inference WebSocket rejected: "+message,
			response.StatusCode,
			gatewayRequestID(response.Header),
			decoded,
			retryable,
			cause,
		)
	}
	if err := context.Cause(ctx); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return agents.NewAPITimeoutError("LiveKit Inference WebSocket connection timed out", true, err)
		}
		return err
	}
	return agents.NewAPIConnectionError("LiveKit Inference WebSocket connection failed", true, cause)
}

func gatewayRequestID(header http.Header) string {
	for _, key := range [...]string{"request-id", "x-request-id", "x-livekit-request-id"} {
		if value := header.Get(key); value != "" {
			return value
		}
	}
	return ""
}

func readLimitedBody(reader io.Reader, limit int64) ([]byte, error) {
	limited := io.LimitReader(reader, limit+1)
	body, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, fmt.Errorf("inference response exceeds %d bytes", limit)
	}
	return body, nil
}

func (w *gatewayWS) WriteJSON(ctx context.Context, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode inference event: %w", err)
	}
	if len(payload) > MaxControlMessageBytes {
		return ErrControlTooLarge
	}
	return w.write(ctx, websocket.TextMessage, payload)
}

func (w *gatewayWS) write(ctx context.Context, messageType int, payload []byte) error {
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-w.writeGate:
	}
	defer func() { w.writeGate <- struct{}{} }()

	deadline := time.Time{}
	if w.writeTimeout > 0 {
		deadline = time.Now().Add(w.writeTimeout)
	}
	if contextDeadline, ok := ctx.Deadline(); ok && (deadline.IsZero() || contextDeadline.Before(deadline)) {
		deadline = contextDeadline
	}
	if err := w.conn.SetWriteDeadline(deadline); err != nil {
		return err
	}
	if err := w.conn.WriteMessage(messageType, payload); err != nil {
		if cause := context.Cause(ctx); cause != nil {
			return cause
		}
		return agents.NewAPIConnectionError("LiveKit Inference WebSocket write failed", true, err)
	}
	return nil
}

func (w *gatewayWS) Close() error {
	var result error
	w.closeOnce.Do(func() {
		deadline := time.Now().Add(time.Second)
		_ = w.conn.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), deadline)
		result = w.conn.Close()
	})
	return result
}

// wsReader delivers at most one unacknowledged message. Stop leaves the
// underlying connection untouched, allowing TTS sessions to return it to the
// pool without a blocked reader goroutine.
type wsReader struct {
	conn     *gatewayWS
	events   chan *wsReadResult
	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once
}

type wsReadResult struct {
	Data []byte
	Err  error
	ack  chan bool
	once sync.Once
}

func startWSReader(conn *gatewayWS) *wsReader {
	reader := &wsReader{
		conn: conn, events: make(chan *wsReadResult, 1),
		stop: make(chan struct{}), done: make(chan struct{}),
	}
	go reader.run()
	return reader
}

func (r *wsReader) run() {
	defer close(r.done)
	defer close(r.events)
	for {
		messageType, data, err := r.conn.conn.ReadMessage()
		if err != nil {
			select {
			case r.events <- &wsReadResult{Err: err}:
			case <-r.stop:
			}
			return
		}
		if messageType != websocket.TextMessage && messageType != websocket.BinaryMessage {
			continue
		}
		result := &wsReadResult{Data: data, ack: make(chan bool, 1)}
		select {
		case r.events <- result:
		case <-r.stop:
			return
		}
		select {
		case resume := <-result.ack:
			if !resume {
				return
			}
		case <-r.stop:
			return
		}
	}
}

func (r *wsReadResult) Resume() {
	if r == nil || r.ack == nil {
		return
	}
	r.once.Do(func() { r.ack <- true })
}

func (r *wsReadResult) Stop() {
	if r == nil || r.ack == nil {
		return
	}
	r.once.Do(func() { r.ack <- false })
}

// Stop terminates the reader after its current ReadMessage returns or its
// current delivery is acknowledged. Callers returning a healthy pooled
// connection should first Stop the delivered wsReadResult, which guarantees
// there is no outstanding ReadMessage.
func (r *wsReader) Stop() { r.stopOnce.Do(func() { close(r.stop) }) }

func (r *wsReader) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-r.done:
		return nil
	}
}
