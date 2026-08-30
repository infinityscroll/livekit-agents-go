// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/stream"
	agentpb "github.com/livekit/protocol/livekit/agent"
	"google.golang.org/protobuf/proto"
)

type fakeSessionTransport struct {
	mu       sync.Mutex
	startErr error
	started  int
	closed   bool
	sendHook func(*agentpb.AgentSessionMessage) error
	inbound  *stream.Channel[*agentpb.AgentSessionMessage]
	sent     *stream.Channel[*agentpb.AgentSessionMessage]
	once     sync.Once
}

type fakeConsoleRecording struct{ closes atomic.Int32 }

func (r *fakeConsoleRecording) Close(context.Context) error {
	r.closes.Add(1)
	return nil
}

type metadataConsoleRecording struct {
	fakeConsoleRecording
	path    string
	started time.Time
}

type delayedConsoleRecording struct {
	closes  atomic.Int32
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (r *delayedConsoleRecording) Close(ctx context.Context) error {
	r.closes.Add(1)
	r.once.Do(func() { close(r.started) })
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-r.release:
		return nil
	}
}

func (r *metadataConsoleRecording) OutputPath() (string, bool) { return r.path, r.path != "" }
func (r *metadataConsoleRecording) RecordingStartedAt() (time.Time, bool) {
	return r.started, !r.started.IsZero()
}

type wrappedConsoleInput struct{ AudioInput }
type wrappedConsoleOutput struct{ AudioOutput }

func newFakeSessionTransport(capacity int) *fakeSessionTransport {
	return &fakeSessionTransport{inbound: stream.NewChannel[*agentpb.AgentSessionMessage](capacity), sent: stream.NewChannel[*agentpb.AgentSessionMessage](capacity)}
}

func (f *fakeSessionTransport) Start(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.started++
	if f.closed {
		return ErrSessionTransportClosed
	}
	return f.startErr
}

func (f *fakeSessionTransport) SendMessage(ctx context.Context, message *agentpb.AgentSessionMessage) error {
	f.mu.Lock()
	closed, hook := f.closed, f.sendHook
	f.mu.Unlock()
	if closed {
		return ErrSessionTransportClosed
	}
	cloned := proto.Clone(message).(*agentpb.AgentSessionMessage)
	if hook != nil {
		if err := hook(cloned); err != nil {
			return err
		}
	}
	return f.sent.Send(ctx, cloned)
}

func (f *fakeSessionTransport) Recv(ctx context.Context) (*agentpb.AgentSessionMessage, error) {
	return f.inbound.Recv(ctx)
}

func (f *fakeSessionTransport) Close(context.Context) error {
	f.once.Do(func() {
		f.mu.Lock()
		f.closed = true
		f.mu.Unlock()
		_ = f.inbound.Abort(ErrSessionTransportClosed)
		_ = f.sent.Close()
	})
	return nil
}

func (f *fakeSessionTransport) push(ctx context.Context, message *agentpb.AgentSessionMessage) error {
	return f.inbound.Send(ctx, proto.Clone(message).(*agentpb.AgentSessionMessage))
}

func TestTCPSessionTransportFramingAndFragmentation(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	host, portText, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	port, _ := strconv.Atoi(portText)
	accepted := make(chan net.Conn, 1)
	go func() {
		connection, acceptErr := listener.Accept()
		if acceptErr == nil {
			accepted <- connection
		}
	}()
	transport, err := NewTCPSessionTransport(host, port)
	if err != nil {
		t.Fatal(err)
	}
	if err := transport.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer transport.Close(context.Background())
	connection := <-accepted
	defer connection.Close()

	inbound := &agentpb.AgentSessionMessage{Message: &agentpb.AgentSessionMessage_Request{Request: &agentpb.SessionRequest{RequestId: "fragmented", Request: &agentpb.SessionRequest_Ping_{Ping: &agentpb.SessionRequest_Ping{}}}}}
	payload, _ := proto.Marshal(inbound)
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(payload)))
	for _, piece := range [][]byte{header[:1], header[1:3], header[3:], payload[:1], payload[1:]} {
		if _, err := connection.Write(piece); err != nil {
			t.Fatal(err)
		}
	}
	got, err := transport.Recv(t.Context())
	if err != nil || !proto.Equal(got, inbound) {
		t.Fatalf("Recv()=%v err=%v", got, err)
	}

	outbound := &agentpb.AgentSessionMessage{Message: &agentpb.AgentSessionMessage_Response{Response: &agentpb.SessionResponse{RequestId: "fragmented", Response: &agentpb.SessionResponse_Pong_{Pong: &agentpb.SessionResponse_Pong{}}}}}
	if err := transport.SendMessage(t.Context(), outbound); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(connection, header); err != nil {
		t.Fatal(err)
	}
	encoded := make([]byte, binary.BigEndian.Uint32(header))
	if _, err := io.ReadFull(connection, encoded); err != nil {
		t.Fatal(err)
	}
	decoded := new(agentpb.AgentSessionMessage)
	if err := proto.Unmarshal(encoded, decoded); err != nil || !proto.Equal(decoded, outbound) {
		t.Fatalf("decoded=%v err=%v", decoded, err)
	}
}

func TestTCPSessionTransportRejectsOversizeBeforeAllocation(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	host, portText, _ := net.SplitHostPort(listener.Addr().String())
	port, _ := strconv.Atoi(portText)
	accepted := make(chan net.Conn, 1)
	go func() { connection, _ := listener.Accept(); accepted <- connection }()
	transport, err := NewTCPSessionTransportWithOptions(TCPSessionTransportOptions{Host: host, Port: port, MaxMessageSize: 16})
	if err != nil {
		t.Fatal(err)
	}
	if err := transport.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	connection := <-accepted
	defer connection.Close()
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, 17)
	_, _ = connection.Write(header)
	if _, err := transport.Recv(t.Context()); !errors.Is(err, ErrSessionFrameTooLarge) {
		t.Fatalf("Recv error=%v", err)
	}
}

func TestRemoteSessionRequestResponseAndClosePending(t *testing.T) {
	transport := newFakeSessionTransport(8)
	remote, err := NewRemoteSession(transport, RemoteSessionOptions{RequestTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	pingDone := make(chan error, 1)
	go func() { pingDone <- remote.Ping(context.Background()) }()
	requestMessage, err := transport.sent.Recv(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	request := requestMessage.GetRequest()
	if request.GetPing() == nil || request.RequestId == "" {
		t.Fatalf("request=%v", request)
	}
	if err := transport.push(t.Context(), &agentpb.AgentSessionMessage{Message: &agentpb.AgentSessionMessage_Response{Response: &agentpb.SessionResponse{RequestId: request.RequestId, Response: &agentpb.SessionResponse_Pong_{Pong: &agentpb.SessionResponse_Pong{}}}}}); err != nil {
		t.Fatal(err)
	}
	if err := <-pingDone; err != nil {
		t.Fatal(err)
	}

	pending := make(chan error, 1)
	go func() { pending <- remote.Ping(context.Background()) }()
	if _, err := transport.sent.Recv(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := remote.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := <-pending; !errors.Is(err, ErrRemoteSessionClosed) {
		t.Fatalf("pending Ping error=%v", err)
	}
}

func TestRemoteSessionCloseBeforeStartIsPrompt(t *testing.T) {
	remote, err := NewRemoteSession(newFakeSessionTransport(1))
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err := remote.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("Close before Start took %s", elapsed)
	}
}

func TestRemoteSessionSlowEventSubscriberCannotBlockRPC(t *testing.T) {
	transport := newFakeSessionTransport(16)
	remote, err := NewRemoteSession(transport, RemoteSessionOptions{EventCapacity: 1, RequestTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer remote.Close(context.Background())
	subscription, err := remote.SubscribeEvents(1)
	if err != nil {
		t.Fatal(err)
	}
	for index := range 2 {
		event := &agentpb.AgentSessionEvent{Event: &agentpb.AgentSessionEvent_AgentStateChanged_{AgentStateChanged: &agentpb.AgentSessionEvent_AgentStateChanged{OldState: agentpb.AgentState_AS_IDLE, NewState: agentpb.AgentState_AS_LISTENING}}}
		if err := transport.push(t.Context(), &agentpb.AgentSessionMessage{Message: &agentpb.AgentSessionMessage_Event{Event: event}}); err != nil {
			t.Fatal(err)
		}
		_ = index
	}
	pingDone := make(chan error, 1)
	go func() { pingDone <- remote.Ping(context.Background()) }()
	requestMessage, err := transport.sent.Recv(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	requestID := requestMessage.GetRequest().GetRequestId()
	if err := transport.push(t.Context(), &agentpb.AgentSessionMessage{Message: &agentpb.AgentSessionMessage_Response{Response: &agentpb.SessionResponse{RequestId: requestID, Response: &agentpb.SessionResponse_Pong_{Pong: &agentpb.SessionResponse_Pong{}}}}}); err != nil {
		t.Fatal(err)
	}
	if err := <-pingDone; err != nil {
		t.Fatalf("RPC stalled behind subscriber: %v", err)
	}
	if _, err := subscription.Recv(t.Context()); err != nil {
		t.Fatalf("buffered event missing: %v", err)
	}
	if _, err := subscription.Recv(t.Context()); !errors.Is(err, ErrRemoteEventOverflow) {
		t.Fatalf("overflow error=%v", err)
	}
}

func TestRemoteSessionSubscribeCloseIsAtomic(t *testing.T) {
	for range 200 {
		remote, err := NewRemoteSession(newFakeSessionTransport(1))
		if err != nil {
			t.Fatal(err)
		}
		type subscribeResult struct {
			sub *RemoteEventSubscription
			err error
		}
		result := make(chan subscribeResult, 1)
		go func() {
			sub, subscribeErr := remote.SubscribeEvents(1)
			result <- subscribeResult{sub: sub, err: subscribeErr}
		}()
		if err := remote.Close(t.Context()); err != nil {
			t.Fatal(err)
		}
		subscribed := <-result
		if subscribed.err != nil {
			if !errors.Is(subscribed.err, ErrRemoteSessionClosed) {
				t.Fatalf("SubscribeEvents error=%v", subscribed.err)
			}
			continue
		}
		recvCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		_, recvErr := subscribed.sub.Recv(recvCtx)
		cancel()
		if recvErr == nil || errors.Is(recvErr, context.DeadlineExceeded) {
			t.Fatalf("subscription survived Close: %v", recvErr)
		}
	}
}

func TestRemoteEventOwnsClonedValue(t *testing.T) {
	event := &agentpb.AgentSessionEvent{Event: &agentpb.AgentSessionEvent_UserInputTranscribed_{UserInputTranscribed: &agentpb.AgentSessionEvent_UserInputTranscribed{Transcript: "original", IsFinal: true}}}
	converted, ok := remoteEventFromProto(event)
	if !ok {
		t.Fatal("event was not converted")
	}
	event.GetUserInputTranscribed().Transcript = "mutated"
	value, ok := converted.Value.(*agentpb.AgentSessionEvent_UserInputTranscribed)
	if !ok || value.Transcript != "original" || converted.Event.GetUserInputTranscribed().Transcript != "original" {
		t.Fatalf("converted event aliases source: %#v", converted)
	}
}

func TestConnectionDeadlineCleanupWaitsForCancellationCallback(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	entered := make(chan struct{})
	release := make(chan struct{})
	cleanup := armConnectionDeadline(ctx, func(deadline time.Time) error {
		if !deadline.IsZero() {
			close(entered)
			<-release
		}
		return nil
	})
	cancel()
	<-entered
	done := make(chan struct{})
	go func() {
		cleanup()
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("deadline cleanup returned while cancellation callback was active")
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("deadline cleanup did not finish")
	}
}

func TestStreamingPCMResamplerIsFrameBoundaryInvariant(t *testing.T) {
	input := make([]int16, 1001)
	for index := range input {
		input[index] = int16(index%401 - 200)
	}
	whole := newLinearPCMResampler(48_000, 24_000)
	want := append(whole.Push(input), whole.Flush()...)
	split := newLinearPCMResampler(48_000, 24_000)
	var got []int16
	for _, part := range [][]int16{input[:1], input[1:117], input[117:600], input[600:]} {
		got = append(got, split.Push(part)...)
	}
	got = append(got, split.Flush()...)
	if len(got) != len(want) {
		t.Fatalf("split samples=%d whole=%d", len(got), len(want))
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("sample %d: split=%d whole=%d", index, got[index], want[index])
		}
	}
}

func TestStreamingPCMResamplerExactRatesAndStereoDownmix(t *testing.T) {
	for _, test := range []struct {
		inputRate, outputRate int
		inputSamples, want    int
	}{
		{inputRate: 48_000, outputRate: 24_000, inputSamples: 48_000, want: 24_000},
		{inputRate: 24_000, outputRate: 48_000, inputSamples: 24_000, want: 48_000},
	} {
		resampler := newLinearPCMResampler(test.inputRate, test.outputRate)
		input := make([]int16, test.inputSamples)
		got := append(resampler.Push(input), resampler.Flush()...)
		if len(got) != test.want {
			t.Fatalf("%d->%d produced %d samples, want %d", test.inputRate, test.outputRate, len(got), test.want)
		}
	}
	stereo, err := agents.NewAudioFrame([]int16{1000, -500, -1000, 500}, 48_000, 2)
	if err != nil {
		t.Fatal(err)
	}
	converter := newStreamingPCMConverter(48_000)
	frames, err := converter.Push(stereo)
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 1 || len(frames[0].Data) != 2 || frames[0].Data[0] != 250 || frames[0].Data[1] != -250 {
		t.Fatalf("stereo downmix=%#v", frames)
	}
}

func TestTCPAudioOutputFlushHandshakeAndFirstWriteFailure(t *testing.T) {
	transport := newFakeSessionTransport(16)
	output, err := NewTCPAudioOutput(transport)
	if err != nil {
		t.Fatal(err)
	}
	frame := agents.AudioFrame{Data: make([]int16, 240), SampleRate: 24_000, Channels: 1, SamplesPerChannel: 240}
	if err := output.CaptureFrame(t.Context(), frame); err != nil {
		t.Fatal(err)
	}
	if err := output.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	if output.PendingPlayoutSegments() != 1 {
		t.Fatalf("pending=%d", output.PendingPlayoutSegments())
	}
	if err := output.NotifyPlayoutFinished(); err != nil {
		t.Fatal(err)
	}
	event, err := output.WaitForPlayout(t.Context())
	if err != nil || event.Interrupted {
		t.Fatalf("WaitForPlayout=%#v err=%v", event, err)
	}

	failing := newFakeSessionTransport(2)
	failing.sendHook = func(*agentpb.AgentSessionMessage) error { return errors.New("write failed") }
	failedOutput, _ := NewTCPAudioOutput(failing)
	if err := failedOutput.CaptureFrame(t.Context(), frame); err == nil {
		t.Fatal("CaptureFrame succeeded")
	}
	if failedOutput.PendingPlayoutSegments() != 0 {
		t.Fatalf("failed capture left %d pending segments", failedOutput.PendingPlayoutSegments())
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := failedOutput.WaitForPlayout(waitCtx); err != nil {
		t.Fatalf("failed first write left waiter blocked: %v", err)
	}
}

func TestTCPAudioOutputPauseResumeIsCancellationAware(t *testing.T) {
	transport := newFakeSessionTransport(8)
	output, _ := NewTCPAudioOutput(transport)
	if err := output.Pause(t.Context()); err != nil {
		t.Fatal(err)
	}
	frame := agents.AudioFrame{Data: make([]int16, 240), SampleRate: 24_000, Channels: 1, SamplesPerChannel: 240}
	done := make(chan error, 1)
	go func() { done <- output.CaptureFrame(context.Background(), frame) }()
	select {
	case err := <-done:
		t.Fatalf("paused capture returned early: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if err := output.Resume(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("capture did not resume")
	}
	if err := output.ClearBuffer(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := output.Pause(t.Context()); err != nil {
		t.Fatal(err)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := output.CaptureFrame(waitCtx, frame); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("paused capture cancellation error=%v", err)
	}
	if err := output.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestTCPAudioOutputSecondWriteFailureKeepsOneSegment(t *testing.T) {
	transport := newFakeSessionTransport(16)
	audioWrites := 0
	transport.sendHook = func(message *agentpb.AgentSessionMessage) error {
		if message.GetAudioOutput() != nil {
			audioWrites++
			if audioWrites == 2 {
				return errors.New("second write failed")
			}
		}
		return nil
	}
	output, _ := NewTCPAudioOutput(transport)
	frame := agents.AudioFrame{Data: make([]int16, 240), SampleRate: 24_000, Channels: 1, SamplesPerChannel: 240}
	if err := output.CaptureFrame(t.Context(), frame); err != nil {
		t.Fatal(err)
	}
	if err := output.CaptureFrame(t.Context(), frame); err == nil {
		t.Fatal("second CaptureFrame succeeded")
	}
	if got := output.PendingPlayoutSegments(); got != 1 {
		t.Fatalf("pending after second-frame failure=%d", got)
	}
	transport.sendHook = nil
	if err := output.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := output.NotifyPlayoutFinished(); err != nil {
		t.Fatal(err)
	}
	if _, err := output.WaitForPlayout(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := output.CapturedPlayoutSegments(); got != 1 {
		t.Fatalf("captured segments=%d", got)
	}
}

func TestTCPAudioOutputFlushSendFailureSettlesSegment(t *testing.T) {
	transport := newFakeSessionTransport(16)
	transport.sendHook = func(message *agentpb.AgentSessionMessage) error {
		if message.GetAudioPlaybackFlush() != nil {
			return errors.New("flush write failed")
		}
		return nil
	}
	output, _ := NewTCPAudioOutput(transport)
	frame := agents.AudioFrame{Data: make([]int16, 240), SampleRate: 24_000, Channels: 1, SamplesPerChannel: 240}
	if err := output.CaptureFrame(t.Context(), frame); err != nil {
		t.Fatal(err)
	}
	if err := output.Flush(t.Context()); err == nil {
		t.Fatal("Flush succeeded")
	}
	event, err := output.WaitForPlayout(t.Context())
	if err != nil || !event.Interrupted || output.PendingPlayoutSegments() != 0 {
		t.Fatalf("settled event=%#v pending=%d err=%v", event, output.PendingPlayoutSegments(), err)
	}
	transport.sendHook = nil
	writeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := output.CaptureFrame(writeCtx, frame); err != nil {
		t.Fatalf("CaptureFrame after failed flush=%v", err)
	}
	if err := output.ClearBuffer(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestSessionHostRoutesPingAudioUpdateAndEvents(t *testing.T) {
	transport := newFakeSessionTransport(32)
	input, _ := NewTCPAudioInput(4)
	output, _ := NewTCPAudioOutput(transport)
	session, err := NewAgentSession(AgentSessionOptions[struct{}]{DisableUserAwayTimeout: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close(context.Background()) })
	host, err := NewSessionHost(transport, SessionHostOptions[struct{}]{AudioInput: input, AudioOutput: output})
	if err != nil {
		t.Fatal(err)
	}
	if err := host.RegisterSession(session); err != nil {
		t.Fatal(err)
	}
	if err := host.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer host.Close(context.Background())
	if err := transport.push(t.Context(), &agentpb.AgentSessionMessage{Message: &agentpb.AgentSessionMessage_Request{Request: &agentpb.SessionRequest{RequestId: "ping", Request: &agentpb.SessionRequest_Ping_{Ping: &agentpb.SessionRequest_Ping{}}}}}); err != nil {
		t.Fatal(err)
	}
	message, err := transport.sent.Recv(t.Context())
	if err != nil || message.GetResponse().GetPong() == nil {
		t.Fatalf("ping response=%v err=%v", message, err)
	}
	frame := agents.AudioFrame{Data: make([]int16, 480), SampleRate: 48_000, Channels: 1, SamplesPerChannel: 480}
	wire, _ := audioToConsoleProto(frame)
	if err := transport.push(t.Context(), &agentpb.AgentSessionMessage{Message: &agentpb.AgentSessionMessage_AudioInput{AudioInput: wire}}); err != nil {
		t.Fatal(err)
	}
	converted, err := input.Recv(t.Context())
	if err != nil || converted.SampleRate != 24_000 || converted.Channels != 1 {
		t.Fatalf("audio=%#v err=%v", converted, err)
	}
	value := false
	update := &agentpb.SessionRequest_UpdateIO{Input: &agentpb.SessionRequest_UpdateIO_Input{AudioEnabled: &value}}
	if err := transport.push(t.Context(), &agentpb.AgentSessionMessage{Message: &agentpb.AgentSessionMessage_Request{Request: &agentpb.SessionRequest{RequestId: "update", Request: &agentpb.SessionRequest_UpdateIo{UpdateIo: update}}}}); err != nil {
		t.Fatal(err)
	}
	message, err = transport.sent.Recv(t.Context())
	if err != nil || message.GetResponse().GetUpdateIo() == nil || session.Input().AudioEnabled() {
		t.Fatalf("update response=%v enabled=%t err=%v", message, session.Input().AudioEnabled(), err)
	}
	if err := session.events.Publish(t.Context(), NewAgentStateChangedEvent(AgentStateIdle, AgentStateListening, time.Now())); err != nil {
		t.Fatal(err)
	}
	message, err = transport.sent.Recv(t.Context())
	if err != nil || message.GetEvent().GetAgentStateChanged() == nil {
		t.Fatalf("event=%v err=%v", message, err)
	}
}

func TestSimulationEndPanicBecomesResponseError(t *testing.T) {
	simulation, err := agents.NewSimulationContext[struct{}](&agents.SimulationDispatch{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = callSimulationEnd(t.Context(), func(context.Context, *agents.SimulationContext[struct{}]) error {
		panic("callback exploded")
	}, simulation)
	if err == nil || !strings.Contains(err.Error(), "callback exploded") {
		t.Fatalf("panic error=%v", err)
	}
}

func TestRemoteConsoleComponentsConcurrentClose(t *testing.T) {
	t.Run("remote", func(t *testing.T) {
		remote, err := NewRemoteSession(newFakeSessionTransport(8))
		if err != nil {
			t.Fatal(err)
		}
		if err := remote.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		runConcurrentClose(t, func() error { return remote.Close(context.Background()) })
	})

	t.Run("host", func(t *testing.T) {
		transport := newFakeSessionTransport(8)
		session, err := NewAgentSession(AgentSessionOptions[struct{}]{DisableUserAwayTimeout: true})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = session.Close(context.Background()) })
		host, err := NewSessionHost(transport, SessionHostOptions[struct{}]{ShutdownDrainTimeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		if err := host.RegisterSession(session); err != nil {
			t.Fatal(err)
		}
		if err := host.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		runConcurrentClose(t, func() error { return host.Close(context.Background()) })
	})

	t.Run("audio_output", func(t *testing.T) {
		transport := newFakeSessionTransport(8)
		output, _ := NewTCPAudioOutput(transport)
		frame := agents.AudioFrame{Data: make([]int16, 240), SampleRate: 24_000, Channels: 1, SamplesPerChannel: 240}
		if err := output.CaptureFrame(t.Context(), frame); err != nil {
			t.Fatal(err)
		}
		runConcurrentClose(t, func() error { return output.Close(context.Background()) })
		if output.PendingPlayoutSegments() != 0 {
			t.Fatalf("Close left %d pending segments", output.PendingPlayoutSegments())
		}
	})

	t.Run("console", func(t *testing.T) {
		console, err := NewAgentsConsole(AgentsConsoleOptions{Enabled: true, Transport: newFakeSessionTransport(8)})
		if err != nil {
			t.Fatal(err)
		}
		runConcurrentClose(t, func() error { return console.Close(context.Background()) })
	})
}

func runConcurrentClose(t *testing.T, closeFn func() error) {
	t.Helper()
	const callers = 32
	errorsSeen := make(chan error, callers)
	var wait sync.WaitGroup
	wait.Add(callers)
	for range callers {
		go func() {
			defer wait.Done()
			errorsSeen <- closeFn()
		}()
	}
	wait.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		if err != nil {
			t.Fatalf("concurrent Close error=%v", err)
		}
	}
}

func TestConsoleAcquisitionRollbackAndRetry(t *testing.T) {
	transport := newFakeSessionTransport(16)
	transport.startErr = errors.New("dial failed")
	console, err := NewAgentsConsole(AgentsConsoleOptions{Enabled: true, Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	restore := SetDefaultAgentsConsole(console)
	defer restore()
	session, err := NewAgentSession(AgentSessionOptions[struct{}]{DisableUserAwayTimeout: true})
	if err != nil {
		t.Fatal(err)
	}
	agent := MustAgent(AgentOptions[struct{}]{ID: "console_agent"})
	if err := session.Start(t.Context(), agent); err == nil {
		t.Fatal("Start succeeded despite transport failure")
	}
	if session.Started() || console.IOAcquired() || session.Input().Audio() != nil || session.Output().Audio() != nil {
		t.Fatal("failed Start did not roll back console acquisition")
	}
	transport.mu.Lock()
	transport.startErr = nil
	transport.mu.Unlock()
	if err := session.Start(t.Context(), agent); err != nil {
		t.Fatal(err)
	}
	if !console.IOAcquired() || session.Input().Audio() == nil || session.Output().Audio() == nil {
		t.Fatal("successful retry did not acquire console IO")
	}
	if err := session.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := console.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestConsoleAcquisitionRollsBackLaterActivityFailure(t *testing.T) {
	agent := MustAgent(AgentOptions[struct{}]{ID: "shared_agent"})
	first, err := NewAgentSession(AgentSessionOptions[struct{}]{DisableUserAwayTimeout: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Start(t.Context(), agent); err != nil {
		t.Fatal(err)
	}

	transport := newFakeSessionTransport(16)
	console, err := NewAgentsConsole(AgentsConsoleOptions{Enabled: true, Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	restore := SetDefaultAgentsConsole(console)
	defer restore()
	second, err := NewAgentSession(AgentSessionOptions[struct{}]{DisableUserAwayTimeout: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Start(t.Context(), agent); !errors.Is(err, ErrAgentAlreadyRunning) {
		t.Fatalf("Start error=%v", err)
	}
	if console.IOAcquired() || second.Input().Audio() != nil || second.Output().Audio() != nil {
		t.Fatal("activity failure left console IO acquired")
	}
	transport.mu.Lock()
	closed := transport.closed
	transport.mu.Unlock()
	if closed {
		t.Fatal("activity rollback closed the externally owned transport")
	}
	if err := first.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := second.Start(t.Context(), agent); err != nil {
		t.Fatal(err)
	}
	if err := second.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := console.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestConsoleRecordingDecoratorsAndOwnership(t *testing.T) {
	transport := newFakeSessionTransport(16)
	input, _ := NewTCPAudioInput(4)
	output, _ := NewTCPAudioOutput(transport)
	console, err := NewAgentsConsole(AgentsConsoleOptions{Enabled: true, Record: true, Transport: transport, AudioInput: input, AudioOutput: output})
	if err != nil {
		t.Fatal(err)
	}
	recording := new(fakeConsoleRecording)
	wrappedInput := &wrappedConsoleInput{AudioInput: input}
	wrappedOutput := &wrappedConsoleOutput{AudioOutput: output}
	if err := console.SetRecordingIO(wrappedInput, wrappedOutput, recording); err != nil {
		t.Fatal(err)
	}
	restore := SetDefaultAgentsConsole(console)
	defer restore()
	session, err := NewAgentSession(AgentSessionOptions[struct{}]{DisableUserAwayTimeout: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := session.Start(t.Context(), MustAgent(AgentOptions[struct{}]{ID: "recording_agent"})); err != nil {
		t.Fatal(err)
	}
	if session.Input().Audio() != wrappedInput || session.Output().Audio() != wrappedOutput {
		t.Fatalf("session did not acquire recording decorators: input=%T output=%T", session.Input().Audio(), session.Output().Audio())
	}
	if err := session.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := console.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if recording.closes.Load() != 1 {
		t.Fatalf("recording Close calls=%d", recording.closes.Load())
	}
	if err := console.SetRecordingIO(wrappedInput, wrappedOutput, recording); !errors.Is(err, ErrConsoleIOClosed) {
		t.Fatalf("SetRecordingIO after Close error=%v", err)
	}
}

func TestConsoleRecordingMetadataAndConcurrentFinalize(t *testing.T) {
	transport := newFakeSessionTransport(4)
	console, err := NewAgentsConsole(AgentsConsoleOptions{Enabled: true, Record: true, Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	started := time.Unix(1_700_000_000, 0)
	recording := &metadataConsoleRecording{path: "/tmp/audio.ogg", started: started}
	input, _ := NewTCPAudioInput(2)
	output, _ := NewTCPAudioOutput(transport)
	if err := console.SetRecordingIO(input, output, recording); err != nil {
		t.Fatal(err)
	}
	info := console.RecordingInfo()
	if info.OutputPath != recording.path || !info.HasStarted || !info.StartedAt.Equal(started) {
		t.Fatalf("RecordingInfo=%#v", info)
	}
	const callers = 16
	errCh := make(chan error, callers)
	for range callers {
		go func() { errCh <- console.CloseRecording(t.Context()) }()
	}
	for range callers {
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
	}
	if err := console.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if recording.closes.Load() != 1 {
		t.Fatalf("recording Close calls=%d", recording.closes.Load())
	}
}

func TestConsoleRecordingCallerTimeoutStillJoinsEventualCleanup(t *testing.T) {
	console, err := NewAgentsConsole(AgentsConsoleOptions{Enabled: false, Record: true})
	if err != nil {
		t.Fatal(err)
	}
	input := NewBaseAudioInput(t.Context(), 1, nil)
	output, err := NewManagedAudioOutput(AudioOutputOptions{SampleRate: 48_000})
	if err != nil {
		t.Fatal(err)
	}
	recording := &delayedConsoleRecording{started: make(chan struct{}), release: make(chan struct{})}
	if err := console.SetRecordingIO(input, output, recording); err != nil {
		t.Fatal(err)
	}

	timed, cancel := context.WithTimeout(t.Context(), 5*time.Millisecond)
	err = console.CloseRecording(timed)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("first CloseRecording error=%v", err)
	}
	select {
	case <-recording.started:
	default:
		t.Fatal("recording cleanup worker did not start")
	}
	close(recording.release)
	joined, cancelJoined := context.WithTimeout(t.Context(), time.Second)
	defer cancelJoined()
	if err := console.CloseRecording(joined); err != nil {
		t.Fatal(err)
	}
	if recording.closes.Load() != 1 {
		t.Fatalf("recording Close calls=%d", recording.closes.Load())
	}
}
