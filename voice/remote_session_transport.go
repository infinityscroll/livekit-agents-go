// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	agentpb "github.com/livekit/protocol/livekit/agent"
	lksdk "github.com/livekit/server-sdk-go/v2"
	"google.golang.org/protobuf/proto"
)

const (
	SessionMessagesTopic         = "lk.agent.session"
	DefaultSessionMaxMessageSize = 1 << 20
	DefaultRoomSessionQueueSize  = 32
	DefaultTCPDialTimeout        = 10 * time.Second
)

var (
	ErrSessionTransportClosed = errors.New("voice session transport is closed")
	ErrSessionFrameTooLarge   = errors.New("voice session transport frame is too large")
	ErrSessionQueueFull       = errors.New("voice session transport queue is full")
)

// SessionTransport carries the current LiveKit AgentSession protobuf. Recv has
// exactly one consumer; SendMessage is safe for concurrent callers and retains
// complete protobuf-frame ordering.
type SessionTransport interface {
	Start(context.Context) error
	SendMessage(context.Context, *agentpb.AgentSessionMessage) error
	Recv(context.Context) (*agentpb.AgentSessionMessage, error)
	Close(context.Context) error
}

type TCPSessionTransportOptions struct {
	Host           string
	Port           int
	DialTimeout    time.Duration
	MaxMessageSize uint32
	Dialer         *net.Dialer
}

// TcpSessionTransportOptions is the mechanical-migration spelling used by
// agents-js and Python. New Go code should use TCPSessionTransportOptions.
type TcpSessionTransportOptions = TCPSessionTransportOptions

// TCPSessionTransport uses the broker-compatible four-byte, big-endian length
// prefix. The wire has no authentication or TLS handshake; callers should bind
// it to loopback or place it behind an authenticated tunnel.
type TCPSessionTransport struct {
	opts TCPSessionTransportOptions

	startMu sync.Mutex
	mu      sync.RWMutex
	conn    net.Conn
	started bool
	closed  bool

	readMu    sync.Mutex
	writeGate chan struct{}
	closeOnce sync.Once
	closeErr  error
}

// TcpSessionTransport is the mechanical-migration spelling used by agents-js
// and Python. New Go code should use TCPSessionTransport.
type TcpSessionTransport = TCPSessionTransport

func NewTCPSessionTransport(host string, port int) (*TCPSessionTransport, error) {
	return NewTCPSessionTransportWithOptions(TCPSessionTransportOptions{Host: host, Port: port})
}

// NewTcpSessionTransport preserves the spelling used by agents-js.
func NewTcpSessionTransport(host string, port int) (*TCPSessionTransport, error) {
	return NewTCPSessionTransport(host, port)
}

func NewTCPSessionTransportWithOptions(options TCPSessionTransportOptions) (*TCPSessionTransport, error) {
	if options.Host == "" {
		return nil, errors.New("voice TCP session transport requires a host")
	}
	if options.Port < 1 || options.Port > 65535 {
		return nil, fmt.Errorf("voice TCP session transport port must be between 1 and 65535: %d", options.Port)
	}
	if options.DialTimeout == 0 {
		options.DialTimeout = DefaultTCPDialTimeout
	}
	if options.DialTimeout < 0 {
		return nil, errors.New("voice TCP session transport dial timeout must not be negative")
	}
	if options.MaxMessageSize == 0 {
		options.MaxMessageSize = DefaultSessionMaxMessageSize
	}
	if options.Dialer == nil {
		options.Dialer = &net.Dialer{Timeout: options.DialTimeout, KeepAlive: 30 * time.Second}
	}
	transport := &TCPSessionTransport{opts: options, writeGate: make(chan struct{}, 1)}
	transport.writeGate <- struct{}{}
	return transport, nil
}

func (t *TCPSessionTransport) Start(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	t.startMu.Lock()
	defer t.startMu.Unlock()
	t.mu.RLock()
	if t.closed {
		t.mu.RUnlock()
		return ErrSessionTransportClosed
	}
	if t.started {
		t.mu.RUnlock()
		return nil
	}
	t.mu.RUnlock()

	address := net.JoinHostPort(t.opts.Host, fmt.Sprintf("%d", t.opts.Port))
	conn, err := t.opts.Dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return fmt.Errorf("connect TCP session transport: %w", err)
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
		_ = tcp.SetKeepAlive(true)
	}
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		_ = conn.Close()
		return ErrSessionTransportClosed
	}
	if t.started {
		t.mu.Unlock()
		_ = conn.Close()
		return nil
	}
	t.conn, t.started = conn, true
	t.mu.Unlock()
	return nil
}

func (t *TCPSessionTransport) SendMessage(ctx context.Context, message *agentpb.AgentSessionMessage) error {
	if message == nil {
		return errors.New("voice TCP session transport cannot send a nil message")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	payload, err := proto.Marshal(message)
	if err != nil {
		return fmt.Errorf("marshal TCP session message: %w", err)
	}
	if uint64(len(payload)) > uint64(t.opts.MaxMessageSize) {
		return fmt.Errorf("%w: %d > %d", ErrSessionFrameTooLarge, len(payload), t.opts.MaxMessageSize)
	}
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-t.writeGate:
	}
	defer func() { t.writeGate <- struct{}{} }()

	conn, err := t.connection()
	if err != nil {
		return err
	}
	stopDeadline := armConnectionDeadline(ctx, conn.SetWriteDeadline)
	defer stopDeadline()
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
	buffers := net.Buffers{header[:], payload}
	_, err = buffers.WriteTo(conn)
	if err != nil {
		if cause := context.Cause(ctx); cause != nil {
			return cause
		}
		if t.Closed() {
			return ErrSessionTransportClosed
		}
		return fmt.Errorf("write TCP session message: %w", err)
	}
	return nil
}

func (t *TCPSessionTransport) Recv(ctx context.Context) (*agentpb.AgentSessionMessage, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	t.readMu.Lock()
	defer t.readMu.Unlock()
	conn, err := t.connection()
	if err != nil {
		return nil, err
	}
	stopDeadline := armConnectionDeadline(ctx, conn.SetReadDeadline)
	defer stopDeadline()
	var header [4]byte
	if _, err := io.ReadFull(conn, header[:]); err != nil {
		return nil, t.readError(ctx, err)
	}
	length := binary.BigEndian.Uint32(header[:])
	if length > t.opts.MaxMessageSize {
		_ = t.Close(context.Background())
		return nil, fmt.Errorf("%w: %d > %d", ErrSessionFrameTooLarge, length, t.opts.MaxMessageSize)
	}
	if uint64(length) > uint64(maxInt()) {
		_ = t.Close(context.Background())
		return nil, fmt.Errorf("%w: %d exceeds platform allocation limit", ErrSessionFrameTooLarge, length)
	}
	payload := make([]byte, int(length))
	if _, err := io.ReadFull(conn, payload); err != nil {
		return nil, t.readError(ctx, err)
	}
	message := new(agentpb.AgentSessionMessage)
	if err := proto.Unmarshal(payload, message); err != nil {
		return nil, fmt.Errorf("decode TCP session message: %w", err)
	}
	return message, nil
}

func (t *TCPSessionTransport) readError(ctx context.Context, err error) error {
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	if t.Closed() || errors.Is(err, net.ErrClosed) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return ErrSessionTransportClosed
	}
	return fmt.Errorf("read TCP session message: %w", err)
}

func (t *TCPSessionTransport) connection() (net.Conn, error) {
	t.mu.RLock()
	conn, started, closed := t.conn, t.started, t.closed
	t.mu.RUnlock()
	if closed || !started || conn == nil {
		return nil, ErrSessionTransportClosed
	}
	return conn, nil
}

func (t *TCPSessionTransport) Closed() bool {
	t.mu.RLock()
	closed := t.closed
	t.mu.RUnlock()
	return closed
}

func (t *TCPSessionTransport) Close(context.Context) error {
	t.closeOnce.Do(func() {
		t.mu.Lock()
		t.closed = true
		conn := t.conn
		t.conn = nil
		t.mu.Unlock()
		if conn != nil {
			t.closeErr = conn.Close()
		}
	})
	if errors.Is(t.closeErr, net.ErrClosed) {
		return nil
	}
	return t.closeErr
}

func armConnectionDeadline(ctx context.Context, set func(time.Time) error) func() {
	if ctx.Done() == nil {
		_ = set(time.Time{})
		return func() {}
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = set(deadline)
	} else {
		_ = set(time.Time{})
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

func maxInt() int { return int(^uint(0) >> 1) }

type RoomSessionTransportOptions struct {
	Room              *lksdk.Room
	RemoteIdentity    func() string
	QueueCapacity     int
	MaxMessageSize    uint32
	Topic             string
	Destination       func() []string
	SuppressSizeCheck bool
}

// RoomSessionTransport uses LiveKit byte streams. The server SDK's reader and
// writer are not context-aware and buffer a stream internally; declared stream
// sizes and this transport's bounded reader queue provide the limits available
// with server-sdk-go v2.18.1.
type RoomSessionTransport struct {
	opts RoomSessionTransportOptions

	mu        sync.RWMutex
	started   bool
	closed    bool
	incoming  chan *lksdk.ByteStreamReader
	failures  chan error
	sendGate  chan struct{}
	done      chan struct{}
	closeOnce sync.Once
}

func NewRoomSessionTransport(options RoomSessionTransportOptions) (*RoomSessionTransport, error) {
	if options.Room == nil || options.Room.LocalParticipant == nil {
		return nil, errors.New("voice room session transport requires a Room with a local participant")
	}
	if options.QueueCapacity == 0 {
		options.QueueCapacity = DefaultRoomSessionQueueSize
	}
	if options.QueueCapacity < 1 {
		return nil, errors.New("voice room session transport queue capacity must be positive")
	}
	if options.MaxMessageSize == 0 {
		options.MaxMessageSize = DefaultSessionMaxMessageSize
	}
	if options.Topic == "" {
		options.Topic = SessionMessagesTopic
	}
	transport := &RoomSessionTransport{
		opts: options, incoming: make(chan *lksdk.ByteStreamReader, options.QueueCapacity),
		failures: make(chan error, 1), sendGate: make(chan struct{}, 1), done: make(chan struct{}),
	}
	transport.sendGate <- struct{}{}
	return transport, nil
}

// NewRemoteSessionFromRoom is the Go migration equivalent of Python's
// RemoteSession.from_room. For dynamic participant linking, construct a
// RoomSessionTransport with RemoteIdentity and pass it to NewRemoteSession.
func NewRemoteSessionFromRoom(room *lksdk.Room, remoteIdentity string, options ...RemoteSessionOptions) (*RemoteSession, error) {
	transport, err := NewRoomSessionTransport(RoomSessionTransportOptions{
		Room: room,
		RemoteIdentity: func() string {
			return remoteIdentity
		},
	})
	if err != nil {
		return nil, err
	}
	return NewRemoteSession(transport, options...)
}

func (t *RoomSessionTransport) Start(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return ErrSessionTransportClosed
	}
	if t.started {
		return nil
	}
	err := t.opts.Room.RegisterByteStreamHandler(t.opts.Topic, func(reader *lksdk.ByteStreamReader, identity string) {
		if expected := t.remoteIdentity(); expected != "" && identity != expected {
			return
		}
		if reader == nil {
			return
		}
		if !t.opts.SuppressSizeCheck && reader.Info.Size != nil && *reader.Info.Size > uint64(t.opts.MaxMessageSize) {
			t.fail(fmt.Errorf("%w: declared %d > %d", ErrSessionFrameTooLarge, *reader.Info.Size, t.opts.MaxMessageSize))
			return
		}
		t.mu.RLock()
		closed := t.closed
		t.mu.RUnlock()
		if closed {
			return
		}
		select {
		case t.incoming <- reader:
		default:
			// The server SDK owns the reader and offers no cancellation API. Drop
			// before ReadAll and terminate the protocol rather than silently losing
			// a request whose caller would otherwise wait for its full timeout.
			t.fail(ErrSessionQueueFull)
		}
	})
	if err != nil {
		return fmt.Errorf("register room session stream handler: %w", err)
	}
	t.started = true
	return nil
}

func (t *RoomSessionTransport) SendMessage(ctx context.Context, message *agentpb.AgentSessionMessage) error {
	if message == nil {
		return errors.New("voice room session transport cannot send a nil message")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	payload, err := proto.Marshal(message)
	if err != nil {
		return fmt.Errorf("marshal room session message: %w", err)
	}
	if uint64(len(payload)) > uint64(t.opts.MaxMessageSize) {
		return fmt.Errorf("%w: %d > %d", ErrSessionFrameTooLarge, len(payload), t.opts.MaxMessageSize)
	}
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-t.sendGate:
	}
	defer func() { t.sendGate <- struct{}{} }()
	t.mu.RLock()
	closed, started := t.closed, t.started
	t.mu.RUnlock()
	if closed || !started || t.opts.Room.ConnectionState() != lksdk.ConnectionStateConnected {
		return ErrSessionTransportClosed
	}
	destinations := t.destinationIdentities()
	writer := t.opts.Room.LocalParticipant.StreamBytes(lksdk.StreamBytesOptions{
		Topic: t.opts.Topic, TotalSize: uint64(len(payload)), DestinationIdentities: destinations,
	})
	if writer == nil {
		return errors.New("voice room session transport could not create a byte stream")
	}
	// server-sdk-go Write/Close have no context or completion result. The send
	// gate still prevents interleaving and bounds concurrent SDK writers.
	writer.Write(payload, nil)
	writer.Close()
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	return nil
}

func (t *RoomSessionTransport) Recv(ctx context.Context) (*agentpb.AgentSessionMessage, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	case <-t.done:
		return nil, ErrSessionTransportClosed
	case err := <-t.failures:
		return nil, err
	case reader := <-t.incoming:
		// ReadAll has no context/cancel surface in server-sdk-go v2.18.1.
		payload := reader.ReadAll()
		if uint64(len(payload)) > uint64(t.opts.MaxMessageSize) {
			return nil, fmt.Errorf("%w: %d > %d", ErrSessionFrameTooLarge, len(payload), t.opts.MaxMessageSize)
		}
		message := new(agentpb.AgentSessionMessage)
		if err := proto.Unmarshal(payload, message); err != nil {
			return nil, fmt.Errorf("decode room session message: %w", err)
		}
		return message, nil
	}
}

func (t *RoomSessionTransport) fail(err error) {
	if err == nil {
		return
	}
	select {
	case t.failures <- err:
	default:
	}
}

func (t *RoomSessionTransport) Close(context.Context) error {
	t.closeOnce.Do(func() {
		t.mu.Lock()
		t.closed = true
		started := t.started
		t.started = false
		t.mu.Unlock()
		close(t.done)
		if started {
			t.opts.Room.UnregisterByteStreamHandler(t.opts.Topic)
		}
	})
	return nil
}

func (t *RoomSessionTransport) remoteIdentity() string {
	if t.opts.RemoteIdentity == nil {
		return ""
	}
	return t.opts.RemoteIdentity()
}

func (t *RoomSessionTransport) destinationIdentities() []string {
	if t.opts.Destination != nil {
		return append([]string(nil), t.opts.Destination()...)
	}
	if identity := t.remoteIdentity(); identity != "" {
		return []string{identity}
	}
	return nil
}

var _ SessionTransport = (*TCPSessionTransport)(nil)
var _ SessionTransport = (*RoomSessionTransport)(nil)
