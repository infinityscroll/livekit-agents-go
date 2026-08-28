// SPDX-License-Identifier: Apache-2.0

package workerprotocol

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

const MaxIPCFrameSize = 16 << 20

const (
	IPCTypeHello      = "hello"
	IPCTypeReady      = "ready"
	IPCTypeInitialize = "initialize"
	IPCTypeStart      = "start"
	IPCTypeStop       = "stop"
	IPCTypeStatus     = "status"
	IPCTypeDone       = "done"

	IPCTypeInferenceRequest  = "inference_request"
	IPCTypeInferenceResponse = "inference_response"
	IPCTypeInferenceCancel   = "inference_cancel"
)

// IPCMessage is the authenticated, length-delimited control protocol between
// the worker and its one-shot job subprocess. Job contains a protobuf-encoded
// livekit.Job. Secrets never appear in command-line arguments.
type IPCMessage struct {
	Type string `json:"type"`

	Token string `json:"token,omitempty"`
	Error string `json:"error,omitempty"`
	// ErrorCode carries a stable machine-readable category for errors that
	// cross a process boundary. Error remains the human-readable diagnostic.
	ErrorCode string `json:"error_code,omitempty"`
	Reason    string `json:"reason,omitempty"`
	Status    int32  `json:"status,omitempty"`
	Success   bool   `json:"success,omitempty"`
	// RequestID, Method, and Data multiplex shared local inference over the
	// authenticated job control connection. Data is raw JSON so the framing
	// layer never performs a lossy interface{} round trip.
	RequestID string          `json:"request_id,omitempty"`
	Method    string          `json:"method,omitempty"`
	Methods   []string        `json:"methods,omitempty"`
	Data      json.RawMessage `json:"data,omitempty"`
	// DeadlineUnixNano preserves a request context deadline across both the
	// job-control and supervised-inference process hops.
	DeadlineUnixNano int64 `json:"deadline_unix_nano,omitempty"`

	Job                   []byte            `json:"job,omitempty"`
	URL                   string            `json:"url,omitempty"`
	RoomToken             string            `json:"room_token,omitempty"`
	WorkerID              string            `json:"worker_id,omitempty"`
	APIKey                string            `json:"api_key,omitempty"`
	APISecret             string            `json:"api_secret,omitempty"`
	FakeJob               bool              `json:"fake_job,omitempty"`
	ParticipantName       string            `json:"participant_name,omitempty"`
	ParticipantIdentity   string            `json:"participant_identity,omitempty"`
	ParticipantMetadata   string            `json:"participant_metadata,omitempty"`
	ParticipantAttributes map[string]string `json:"participant_attributes,omitempty"`
}

// FramedConn serializes JSON messages over a stream connection with a fixed
// four-byte big-endian size prefix. It permits one concurrent reader and one
// concurrent writer.
type FramedConn struct {
	conn    net.Conn
	readMu  sync.Mutex
	writeMu sync.Mutex
	once    sync.Once
}

func NewFramedConn(conn net.Conn) *FramedConn {
	return &FramedConn{conn: conn}
}

func (c *FramedConn) Read(ctx context.Context) (IPCMessage, error) {
	var out IPCMessage
	if c == nil || c.conn == nil {
		return out, io.ErrClosedPipe
	}
	if err := ctx.Err(); err != nil {
		return out, err
	}

	c.readMu.Lock()
	defer c.readMu.Unlock()
	reset := applyConnDeadline(ctx, c.conn.SetReadDeadline)
	defer reset()

	var header [4]byte
	if _, err := io.ReadFull(c.conn, header[:]); err != nil {
		return out, normalizeConnError(ctx, err)
	}
	size := binary.BigEndian.Uint32(header[:])
	if size == 0 || size > MaxIPCFrameSize {
		return out, fmt.Errorf("worker IPC: invalid frame size %d", size)
	}
	payload := make([]byte, int(size))
	if _, err := io.ReadFull(c.conn, payload); err != nil {
		return out, normalizeConnError(ctx, err)
	}
	if err := json.Unmarshal(payload, &out); err != nil {
		return out, fmt.Errorf("worker IPC: decode frame: %w", err)
	}
	if out.Type == "" {
		return out, errors.New("worker IPC: missing message type")
	}
	return out, nil
}

func (c *FramedConn) Write(ctx context.Context, msg IPCMessage) error {
	if c == nil || c.conn == nil {
		return io.ErrClosedPipe
	}
	if msg.Type == "" {
		return errors.New("worker IPC: missing message type")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("worker IPC: encode frame: %w", err)
	}
	if len(payload) == 0 || len(payload) > MaxIPCFrameSize {
		return fmt.Errorf("worker IPC: frame size %d exceeds limit", len(payload))
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	reset := applyConnDeadline(ctx, c.conn.SetWriteDeadline)
	defer reset()

	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
	if err := writeFull(c.conn, header[:]); err != nil {
		return normalizeConnError(ctx, err)
	}
	if err := writeFull(c.conn, payload); err != nil {
		return normalizeConnError(ctx, err)
	}
	return nil
}

func (c *FramedConn) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	var err error
	c.once.Do(func() { err = c.conn.Close() })
	return err
}

func writeFull(w io.Writer, payload []byte) error {
	for len(payload) > 0 {
		n, err := w.Write(payload)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		payload = payload[n:]
	}
	return nil
}

func applyConnDeadline(ctx context.Context, set func(time.Time) error) func() {
	deadline, ok := ctx.Deadline()
	if ok {
		_ = set(deadline)
	}
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = set(time.Now())
		close(done)
	})
	return func() {
		if !stop() {
			<-done
		}
		_ = set(time.Time{})
	}
}

func normalizeConnError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return err
}
