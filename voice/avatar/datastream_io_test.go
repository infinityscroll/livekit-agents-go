// SPDX-License-Identifier: Apache-2.0

package avatar

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/infinityscroll/livekit-agents-go/voice"
	lksdk "github.com/livekit/server-sdk-go/v2"
)

type fakeByteStreamWriter struct {
	mu       sync.Mutex
	writes   [][]byte
	closed   int
	block    <-chan struct{}
	stop     chan struct{}
	stopOnce sync.Once
	closeErr error
}

func (w *fakeByteStreamWriter) Write(ctx context.Context, data []byte) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if w.block != nil {
		select {
		case <-w.block:
		case <-w.stop:
			return io.EOF
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
	w.mu.Lock()
	w.writes = append(w.writes, append([]byte(nil), data...))
	w.mu.Unlock()
	return nil
}
func (w *fakeByteStreamWriter) Close(context.Context) error {
	w.mu.Lock()
	w.closed++
	w.mu.Unlock()
	if w.stop != nil {
		w.stopOnce.Do(func() { close(w.stop) })
	}
	return w.closeErr
}

type fakeDataStreamTransport struct {
	readyCalls atomic.Int32
	readyBlock <-chan struct{}
	readyErr   error
	writer     *fakeByteStreamWriter

	mu           sync.Mutex
	opened       []ByteStreamOptions
	handlers     map[string]map[string]func(RPCInvocation) string
	unregistered int
	performErr   error
	performed    []string
}

func newFakeDataStreamTransport() *fakeDataStreamTransport {
	return &fakeDataStreamTransport{writer: &fakeByteStreamWriter{}, handlers: make(map[string]map[string]func(RPCInvocation) string)}
}
func (f *fakeDataStreamTransport) WaitReady(ctx context.Context, _ string, _ lksdk.TrackKind) error {
	f.readyCalls.Add(1)
	if f.readyBlock != nil {
		select {
		case <-f.readyBlock:
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
	return f.readyErr
}
func (f *fakeDataStreamTransport) OpenByteStream(ctx context.Context, options ByteStreamOptions) (ByteStreamWriter, error) {
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	f.mu.Lock()
	f.opened = append(f.opened, options)
	f.mu.Unlock()
	return f.writer, nil
}
func (f *fakeDataStreamTransport) PerformRPC(ctx context.Context, _ string, method, _ string) (string, error) {
	if err := context.Cause(ctx); err != nil {
		return "", err
	}
	f.mu.Lock()
	f.performed = append(f.performed, method)
	err := f.performErr
	f.mu.Unlock()
	return "ok", err
}
func (f *fakeDataStreamTransport) RegisterRPC(method, identity string, handler func(RPCInvocation) string) (func(), error) {
	f.mu.Lock()
	if f.handlers[method] == nil {
		f.handlers[method] = make(map[string]func(RPCInvocation) string)
	}
	f.handlers[method][identity] = handler
	f.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			f.mu.Lock()
			delete(f.handlers[method], identity)
			f.unregistered++
			f.mu.Unlock()
		})
	}, nil
}
func (f *fakeDataStreamTransport) invoke(method, identity, payload string) string {
	f.mu.Lock()
	handler := f.handlers[method][identity]
	f.mu.Unlock()
	if handler == nil {
		return "reject"
	}
	return handler(RPCInvocation{CallerIdentity: identity, Payload: payload})
}

func TestDataStreamAudioOutputPCMProtocolAndPlayback(t *testing.T) {
	transport := newFakeDataStreamTransport()
	output, err := NewDataStreamAudioOutput(DataStreamAudioOutputOptions{
		Transport: transport, DestinationIdentity: "avatar", SampleRate: 16000,
		WaitRemoteTrack: lksdk.TrackKindVideo,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer output.Close(t.Context())
	started := make(chan struct{}, 1)
	output.OnPlaybackStarted(func(voice.PlaybackStartedEvent) { started <- struct{}{} })
	frame := avatarTestFrame()
	if err := output.CaptureFrame(t.Context(), frame); err != nil {
		t.Fatal(err)
	}
	if err := output.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("missing eager start")
	}
	transport.mu.Lock()
	opened := transport.opened
	transport.mu.Unlock()
	if len(opened) != 1 || opened[0].Topic != AudioStreamTopic || opened[0].DestinationIdentity != "avatar" || opened[0].Attributes["sample_rate"] != "16000" || opened[0].Attributes["num_channels"] != "1" {
		t.Fatalf("open options = %#v", opened)
	}
	transport.writer.mu.Lock()
	writes, closed := transport.writer.writes, transport.writer.closed
	transport.writer.mu.Unlock()
	want := []byte{1, 0, 254, 255, 3, 0, 252, 255}
	if len(writes) != 1 || string(writes[0]) != string(want) || closed != 1 {
		t.Fatalf("writes=%v closed=%d", writes, closed)
	}
	if response := transport.invoke(RPCPlaybackFinished, "avatar", `{broken`); response != "ok" {
		t.Fatalf("RPC response = %q", response)
	}
	event, err := output.WaitForPlayout(t.Context())
	if err != nil || event.PlaybackPosition != 0 || event.Interrupted {
		t.Fatalf("WaitForPlayout = %#v, %v", event, err)
	}
}

func TestDataStreamAudioOutputWaitPlaybackStartAndFormatInvariant(t *testing.T) {
	transport := newFakeDataStreamTransport()
	output, err := NewDataStreamAudioOutput(DataStreamAudioOutputOptions{
		Transport: transport, DestinationIdentity: "avatar", WaitPlaybackStart: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer output.Close(t.Context())
	started := make(chan struct{}, 1)
	output.OnPlaybackStarted(func(voice.PlaybackStartedEvent) { started <- struct{}{} })
	if err := output.CaptureFrame(t.Context(), avatarTestFrame()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
		t.Fatal("started eagerly")
	default:
	}
	if response := transport.invoke(RPCPlaybackStarted, "avatar", ""); response != "ok" {
		t.Fatalf("start response = %q", response)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("missing remote start")
	}
	changed := avatarTestFrame()
	changed.SampleRate = 24000
	if err := output.CaptureFrame(t.Context(), changed); err == nil {
		t.Fatal("expected within-segment format error")
	}
}

func TestDataStreamAudioOutputConcurrentStartAndCancellation(t *testing.T) {
	ready := make(chan struct{})
	transport := newFakeDataStreamTransport()
	transport.readyBlock = ready
	output, err := NewDataStreamAudioOutput(DataStreamAudioOutputOptions{Transport: transport, DestinationIdentity: "avatar"})
	if err != nil {
		t.Fatal(err)
	}
	defer output.Close(t.Context())
	results := make(chan error, 2)
	go func() { results <- output.CaptureFrame(context.Background(), avatarTestFrame()) }()
	go func() { results <- output.CaptureFrame(context.Background(), avatarTestFrame()) }()
	deadline := time.After(time.Second)
	for transport.readyCalls.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("readiness was not requested")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	close(ready)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if transport.readyCalls.Load() != 1 {
		t.Fatalf("ready calls = %d", transport.readyCalls.Load())
	}

	blocked := make(chan struct{})
	other := newFakeDataStreamTransport()
	other.readyBlock = blocked
	cancelOutput, err := NewDataStreamAudioOutput(DataStreamAudioOutputOptions{Transport: other, DestinationIdentity: "avatar"})
	if err != nil {
		t.Fatal(err)
	}
	defer cancelOutput.Close(t.Context())
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	if err := cancelOutput.CaptureFrame(ctx, avatarTestFrame()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("CaptureFrame error = %v", err)
	}
}

func TestDataStreamAudioOutputClearTimeoutAndRPCFailure(t *testing.T) {
	for _, test := range []struct {
		name       string
		performErr error
		timeout    time.Duration
	}{
		{name: "timeout", timeout: 10 * time.Millisecond},
		{name: "rpc failure", performErr: io.ErrUnexpectedEOF, timeout: time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			transport := newFakeDataStreamTransport()
			transport.performErr = test.performErr
			output, err := NewDataStreamAudioOutput(DataStreamAudioOutputOptions{
				Transport: transport, DestinationIdentity: "avatar", ClearBufferTimeout: test.timeout,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer output.Close(t.Context())
			frame := avatarTestFrame()
			if err := output.CaptureFrame(t.Context(), frame); err != nil {
				t.Fatal(err)
			}
			clearErr := output.ClearBuffer(t.Context())
			if test.performErr == nil && clearErr != nil || test.performErr != nil && clearErr == nil {
				t.Fatalf("ClearBuffer error = %v", clearErr)
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			event, err := output.WaitForPlayout(ctx)
			if err != nil || !event.Interrupted || event.PlaybackPosition != frame.Duration() {
				t.Fatalf("WaitForPlayout = %#v, %v", event, err)
			}
		})
	}
}

func TestDataStreamAudioOutputCloseUnregistersOnce(t *testing.T) {
	transport := newFakeDataStreamTransport()
	output, err := NewDataStreamAudioOutput(DataStreamAudioOutputOptions{
		Transport: transport, DestinationIdentity: "avatar", WaitPlaybackStart: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := output.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := output.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	transport.mu.Lock()
	unregistered := transport.unregistered
	transport.mu.Unlock()
	if unregistered != 2 {
		t.Fatalf("unregistered = %d", unregistered)
	}
}

func TestDataStreamAudioOutputCloseUnblocksWriterAndRetainsError(t *testing.T) {
	blocked := make(chan struct{})
	sentinel := errors.New("close failed")
	transport := newFakeDataStreamTransport()
	transport.writer.block = blocked
	transport.writer.stop = make(chan struct{})
	transport.writer.closeErr = sentinel
	output, err := NewDataStreamAudioOutput(DataStreamAudioOutputOptions{Transport: transport, DestinationIdentity: "avatar"})
	if err != nil {
		t.Fatal(err)
	}
	captureDone := make(chan error, 1)
	go func() { captureDone <- output.CaptureFrame(context.Background(), avatarTestFrame()) }()
	deadline := time.After(time.Second)
	for {
		transport.mu.Lock()
		opened := len(transport.opened) != 0
		transport.mu.Unlock()
		if opened {
			break
		}
		select {
		case <-deadline:
			t.Fatal("writer did not open")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	closeResults := make(chan error, 2)
	go func() { closeResults <- output.Close(context.Background()) }()
	go func() { closeResults <- output.Close(context.Background()) }()
	if err := <-captureDone; !errors.Is(err, io.EOF) {
		t.Fatalf("capture error = %v", err)
	}
	for range 2 {
		if err := <-closeResults; !errors.Is(err, sentinel) {
			t.Fatalf("close error = %v", err)
		}
	}
}
