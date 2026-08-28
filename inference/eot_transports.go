// SPDX-License-Identifier: Apache-2.0

package inference

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/metrics"
	"github.com/livekit/agents-go/stream"
	protocol "github.com/livekit/protocol/livekit/agent"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const defaultTurnSendCapacity = 64

// CloudTurnTransportOptions contains only cloud-specific state. Credentials
// are intentionally omitted from JSON snapshots by TurnDetector.MarshalJSON.
type CloudTurnTransportOptions struct {
	BaseURL         string
	Credentials     Credentials
	ConnectOptions  agents.APIConnectOptions
	Metadata        func(context.Context) RequestMetadata
	WebSocketDialer *websocket.Dialer
	SendCapacity    int
	WriteTimeout    time.Duration
}

type cloudTurnTransport struct {
	detector *TurnDetector
	opts     CloudTurnTransportOptions

	mu     sync.RWMutex
	stream *TurnDetectorStream
	ws     *gatewayWS
	send   *stream.Channel[*protocol.ClientMessage]
	cancel context.CancelCauseFunc
	closed bool
}

func newCloudTurnTransport(detector *TurnDetector, options *CloudTurnTransportOptions) *cloudTurnTransport {
	opts := *options
	if opts.SendCapacity <= 0 {
		opts.SendCapacity = defaultTurnSendCapacity
	}
	if opts.WriteTimeout <= 0 {
		opts.WriteTimeout = opts.ConnectOptions.Resolve().Timeout
	}
	return &cloudTurnTransport{detector: detector, opts: opts}
}

func (t *cloudTurnTransport) Attach(value *TurnDetectorStream) {
	t.mu.Lock()
	t.stream = value
	t.mu.Unlock()
}

func (t *cloudTurnTransport) Ready() bool {
	t.mu.RLock()
	ready := t.ws != nil && !t.closed
	t.mu.RUnlock()
	return ready
}

func (t *cloudTurnTransport) RunInference(ctx context.Context, requestID string) error {
	return t.enqueue(ctx, &protocol.ClientMessage{Message: &protocol.ClientMessage_InferenceStart{
		InferenceStart: &protocol.InferenceStart{RequestId: requestID},
	}})
}

func (t *cloudTurnTransport) PushFrame(ctx context.Context, frame agents.AudioFrame) error {
	if len(frame.Data) == 0 {
		return nil
	}
	payload := pcm16LittleEndian(frame.Data)
	return t.enqueue(ctx, &protocol.ClientMessage{Message: &protocol.ClientMessage_InputAudio{
		InputAudio: &protocol.InputAudio{
			Audio: payload, NumSamples: uint32(frame.SamplesPerChannel), CreatedAt: timestamppb.Now(),
		},
	}})
}

func (t *cloudTurnTransport) Flush(ctx context.Context, _ FlushSentinel) error {
	return t.enqueue(ctx, &protocol.ClientMessage{Message: &protocol.ClientMessage_SessionFlush{
		SessionFlush: &protocol.SessionFlush{},
	}})
}

func (t *cloudTurnTransport) enqueue(ctx context.Context, message *protocol.ClientMessage) error {
	t.mu.RLock()
	queue, ws, closed := t.send, t.ws, t.closed
	t.mu.RUnlock()
	// Match agents-js: control calls before the WebSocket is ready or after a
	// transport swap are harmless. Audio remains in the stream queue until Run
	// is active, so only synchronous prediction hooks can take this path.
	if closed || queue == nil || ws == nil {
		return nil
	}
	return queue.Send(ctx, message)
}

func (t *cloudTurnTransport) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	t.mu.RLock()
	closed := t.closed
	t.mu.RUnlock()
	if closed {
		return stream.ErrClosed
	}
	connect := t.opts.ConnectOptions.Resolve()
	connectCtx, connectCancel := context.WithTimeout(ctx, connect.Timeout)
	ws, err := dialGatewayWS(connectCtx, gatewayWSDialOptions{
		BaseURL: t.opts.BaseURL, Endpoint: "eot", Credentials: t.opts.Credentials,
		Metadata: t.opts.Metadata, Dialer: t.opts.WebSocketDialer,
		ReadLimit: MaxControlMessageBytes, WriteTimeout: t.opts.WriteTimeout,
	})
	connectCancel()
	if err != nil {
		return nonRetryableTurnConnectError(err)
	}

	runCtx, cancel := context.WithCancelCause(ctx)
	queue := stream.NewChannel[*protocol.ClientMessage](t.opts.SendCapacity)
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		cancel(stream.ErrClosed)
		_ = queue.Abort(stream.ErrClosed)
		_ = ws.Close()
		return stream.ErrClosed
	}
	t.ws, t.send, t.cancel = ws, queue, cancel
	attached := t.stream
	t.mu.Unlock()
	if attached == nil {
		err := errors.New("cloud turn transport is not attached")
		cancel(err)
		_ = t.Close()
		return err
	}

	create := &protocol.ClientMessage{
		CreatedAt: timestamppb.Now(),
		Message: &protocol.ClientMessage_SessionCreate{SessionCreate: &protocol.SessionCreate{
			Settings: &protocol.SessionSettings{
				SampleRate: uint32(attached.detector.opts.sampleRate),
				Encoding:   protocol.AudioEncoding_AUDIO_ENCODING_PCM_S16LE,
			},
		}},
	}
	if err := writeTurnClientMessage(runCtx, ws, create); err != nil {
		t.clearRun(ws, queue)
		cancel(err)
		_ = ws.Close()
		return err
	}

	results := make(chan error, 3)
	go func() { results <- t.sendLoop(runCtx, ws, queue) }()
	go func() { results <- t.receiveLoop(runCtx, ws) }()
	go func() {
		err := attached.drain(runCtx, t)
		if errors.Is(err, io.EOF) {
			_ = writeTurnClientMessage(runCtx, ws, &protocol.ClientMessage{
				Message: &protocol.ClientMessage_SessionClose{SessionClose: &protocol.SessionClose{}},
			})
		}
		results <- err
	}()

	first := <-results
	cancel(first)
	_ = queue.Abort(first)
	_ = ws.Close()
	for range 2 {
		<-results
	}
	t.clearRun(ws, queue)
	if errors.Is(first, context.Canceled) && context.Cause(ctx) != nil {
		return context.Cause(ctx)
	}
	return first
}

func (t *cloudTurnTransport) clearRun(ws *gatewayWS, queue *stream.Channel[*protocol.ClientMessage]) {
	t.mu.Lock()
	if t.ws == ws {
		t.ws = nil
	}
	if t.send == queue {
		t.send = nil
	}
	t.cancel = nil
	t.mu.Unlock()
}

func (t *cloudTurnTransport) sendLoop(ctx context.Context, ws *gatewayWS, queue *stream.Channel[*protocol.ClientMessage]) error {
	for {
		message, err := queue.Recv(ctx)
		if err != nil {
			return err
		}
		if message.CreatedAt == nil {
			message.CreatedAt = timestamppb.Now()
		}
		if err := writeTurnClientMessage(ctx, ws, message); err != nil {
			return err
		}
	}
}

func writeTurnClientMessage(ctx context.Context, ws *gatewayWS, message *protocol.ClientMessage) error {
	payload, err := proto.Marshal(message)
	if err != nil {
		return fmt.Errorf("marshal turn detector message: %w", err)
	}
	return ws.write(ctx, websocket.BinaryMessage, payload)
}

func (t *cloudTurnTransport) receiveLoop(ctx context.Context, ws *gatewayWS) error {
	for {
		messageType, data, err := ws.conn.ReadMessage()
		if err != nil {
			if context.Cause(ctx) != nil {
				return context.Cause(ctx)
			}
			return agents.NewAPIConnectionError("turn detector connection closed unexpectedly", false, err)
		}
		if messageType != websocket.BinaryMessage {
			continue
		}
		if len(data) > MaxControlMessageBytes {
			return ErrControlTooLarge
		}
		var message protocol.ServerMessage
		if err := proto.Unmarshal(data, &message); err != nil {
			return fmt.Errorf("decode turn detector message: %w", err)
		}
		if err := t.processServerMessage(&message); err != nil {
			return err
		}
	}
}

func (t *cloudTurnTransport) processServerMessage(message *protocol.ServerMessage) error {
	t.mu.RLock()
	attached := t.stream
	t.mu.RUnlock()
	if attached == nil {
		return nil
	}
	if created := message.GetSessionCreated(); created != nil {
		if err := attached.detector.opts.thresholds.UpdateServerDefaults(
			created.DefaultThresholds, created.DefaultThreshold,
			created.DefaultBackchannelThresholds, created.DefaultBackchannelThreshold,
		); err != nil {
			return err
		}
		t.warnTransportLatency(message)
		return nil
	}
	if prediction := message.GetEotPrediction(); prediction != nil {
		stats := prediction.InferenceStats
		now := time.Now()
		var detectionDelay, inferenceDuration *time.Duration
		if stats != nil && stats.LatestClientCreatedAt != nil && stats.LatestClientCreatedAt.IsValid() {
			value := max(time.Duration(0), now.Sub(stats.LatestClientCreatedAt.AsTime()))
			detectionDelay = &value
		}
		if stats != nil && stats.ServerE2ELatency != nil && stats.ServerE2ELatency.IsValid() {
			value := stats.ServerE2ELatency.AsDuration()
			inferenceDuration = &value
		}
		backchannel := float64(prediction.BackchannelProbability)
		attached.ResolvePrediction(message.GetRequestId(), float64(prediction.Probability), TurnPredictionDetails{
			DetectionDelay: detectionDelay, InferenceDuration: inferenceDuration,
			BackchannelProbability: &backchannel,
		})
		metric := metrics.EOTInference{
			Timestamp: now, PredictionDuration: durationValue(inferenceDuration),
			DetectionDelay: durationValue(detectionDelay), NumRequests: 1,
			Metadata: metrics.Metadata{ModelProvider: attached.Provider(), ModelName: attached.Model()},
		}
		if stats != nil && stats.ClientE2ELatency != nil && stats.ClientE2ELatency.IsValid() {
			metric.TotalDuration = stats.ClientE2ELatency.AsDuration()
		}
		t.detector.metrics.Emit(metric)
		return nil
	}
	if inferenceErr := message.GetError(); inferenceErr != nil {
		return agents.NewAPIStatusError(
			inferenceErr.Message, int(inferenceErr.Code), message.GetRequestId(), nil, false, nil,
		)
	}
	if message.GetSessionClosed() != nil || message.GetInferenceStarted() != nil || message.GetInferenceStopped() != nil {
		t.warnTransportLatency(message)
	}
	return nil
}

func (t *cloudTurnTransport) warnTransportLatency(message *protocol.ServerMessage) {
	if timestamp := message.ClientCreatedAt; timestamp != nil && timestamp.IsValid() {
		latency := time.Since(timestamp.AsTime())
		if latency > 500*time.Millisecond {
			slog.Warn("turn detection transport latency is too high", "latency", latency)
		}
	}
}

func durationValue(value *time.Duration) time.Duration {
	if value == nil {
		return 0
	}
	return *value
}

func nonRetryableTurnConnectError(err error) error {
	var status *agents.APIStatusError
	if errors.As(err, &status) {
		return agents.NewAPIStatusError(status.Message, status.StatusCode, status.RequestID, status.Body, false, status)
	}
	var timeout *agents.APITimeoutError
	if errors.As(err, &timeout) {
		return agents.NewAPITimeoutError("turn detector connection timed out", false, timeout)
	}
	return agents.NewAPIConnectionError("failed to connect to turn detector", false, err)
}

func (t *cloudTurnTransport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	ws, queue, cancel := t.ws, t.send, t.cancel
	t.ws, t.send, t.cancel = nil, nil, nil
	t.mu.Unlock()
	if cancel != nil {
		cancel(stream.ErrClosed)
	}
	if queue != nil {
		_ = queue.Abort(stream.ErrClosed)
	}
	if ws != nil {
		return ws.Close()
	}
	return nil
}

type pcmRing struct {
	data   []int16
	write  int
	filled int
}

func newPCMRing(capacity int) pcmRing { return pcmRing{data: make([]int16, capacity)} }

func (r *pcmRing) push(data []int16) {
	if len(r.data) == 0 || len(data) == 0 {
		return
	}
	if len(data) >= len(r.data) {
		copy(r.data, data[len(data)-len(r.data):])
		r.write, r.filled = 0, len(r.data)
		return
	}
	first := min(len(data), len(r.data)-r.write)
	copy(r.data[r.write:], data[:first])
	copy(r.data, data[first:])
	r.write = (r.write + len(data)) % len(r.data)
	r.filled = min(len(r.data), r.filled+len(data))
}

func (r *pcmRing) snapshot() []int16 {
	result := make([]int16, r.filled)
	start := (r.write - r.filled + len(r.data)) % len(r.data)
	first := min(r.filled, len(r.data)-start)
	copy(result, r.data[start:start+first])
	copy(result[first:], r.data[:r.filled-first])
	return result
}

func (r *pcmRing) reset() { r.write, r.filled = 0, 0 }

type localTurnTransport struct {
	sampleRate int
	provider   *sharedLocalPredictor
	buffer     pcmRing

	mu      sync.Mutex
	stream  *TurnDetectorStream
	ctx     context.Context
	cancel  context.CancelCauseFunc
	predict context.CancelCauseFunc
	closed  bool
	wg      sync.WaitGroup
}

func newLocalTurnTransport(sampleRate int, provider *sharedLocalPredictor) *localTurnTransport {
	capacity := int(localTurnAudioWindow * time.Duration(sampleRate) / time.Second)
	return &localTurnTransport{sampleRate: sampleRate, provider: provider, buffer: newPCMRing(capacity)}
}

func (t *localTurnTransport) Attach(value *TurnDetectorStream) {
	t.mu.Lock()
	t.stream = value
	t.mu.Unlock()
}

func (t *localTurnTransport) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancelCause(ctx)
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		cancel(stream.ErrClosed)
		return stream.ErrClosed
	}
	t.ctx, t.cancel = runCtx, cancel
	attached := t.stream
	t.mu.Unlock()
	if attached == nil {
		cancel(errors.New("local turn transport is not attached"))
		return errors.New("local turn transport is not attached")
	}
	err := attached.drain(runCtx, t)
	cancel(err)
	t.wg.Wait()
	return err
}

func (t *localTurnTransport) RunInference(ctx context.Context, requestID string) error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return stream.ErrClosed
	}
	if t.predict != nil {
		t.predict(ErrTurnPredictionSuperseded)
	}
	parent := t.ctx
	if parent == nil {
		parent = context.Background()
	}
	predictCtx, cancel := context.WithCancelCause(parent)
	t.predict = cancel
	snapshot := t.buffer.snapshot()
	attached := t.stream
	t.wg.Add(1)
	t.mu.Unlock()
	go func() {
		defer t.wg.Done()
		started := time.Now()
		probability, err := t.provider.predict(predictCtx, snapshot)
		if errors.Is(err, ErrLocalInferenceUnavailable) {
			t.provider.warnedUnavailable.Do(func() {
				slog.Warn("local audio EOT inference is unavailable; defaulting predictions to 1.0")
			})
			probability, err = 1, nil
		}
		if err != nil {
			slog.Warn("local audio EOT prediction failed", "error", err)
			probability = 0
		}
		if context.Cause(predictCtx) != nil || attached == nil {
			return
		}
		duration := time.Since(started)
		attached.ResolvePrediction(requestID, probability, TurnPredictionDetails{InferenceDuration: &duration})
	}()
	return nil
}

func (t *localTurnTransport) PushFrame(_ context.Context, frame agents.AudioFrame) error {
	if frame.SampleRate != t.sampleRate || frame.Channels != 1 {
		return fmt.Errorf("%w: local turn detector expects %d Hz mono PCM", agents.ErrInvalidAudioFormat, t.sampleRate)
	}
	t.mu.Lock()
	if !t.closed {
		t.buffer.push(frame.Data)
	}
	t.mu.Unlock()
	return nil
}

func (t *localTurnTransport) Flush(context.Context, FlushSentinel) error {
	t.mu.Lock()
	t.buffer.reset()
	t.mu.Unlock()
	return nil
}

func (t *localTurnTransport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	cancel, predict := t.cancel, t.predict
	t.mu.Unlock()
	if predict != nil {
		predict(stream.ErrClosed)
	}
	if cancel != nil {
		cancel(stream.ErrClosed)
	}
	return nil
}

func (s *sharedLocalPredictor) predict(ctx context.Context, pcm []int16) (float64, error) {
	s.mu.Lock()
	if s.ready == nil && !s.loading && !s.loaded {
		// no-op; get performs initialization below
	}
	gate := s.gate
	if gate == nil {
		gate = make(chan struct{}, 1)
		gate <- struct{}{}
		s.gate = gate
	}
	s.mu.Unlock()
	select {
	case <-ctx.Done():
		return 0, context.Cause(ctx)
	case <-gate:
	}
	defer func() { gate <- struct{}{} }()
	predictor, err := s.get(ctx)
	if err != nil {
		return 0, err
	}
	return predictor.PredictEndOfTurn(ctx, pcm)
}

// turnAudioConverter converts arbitrary, stable PCM input into target-rate
// mono frames. It retains interpolation state across pushes and resets at a
// turn boundary.
type turnAudioConverter struct {
	mu       sync.Mutex
	inRate   int
	channels int
	resample *linearMonoResampler
}

func (c *turnAudioConverter) push(frame agents.AudioFrame, targetRate int) ([]agents.AudioFrame, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if frame.SampleRate <= 0 || frame.Channels <= 0 || len(frame.Data)%frame.Channels != 0 {
		return nil, agents.ErrInvalidAudioFormat
	}
	if c.inRate == 0 {
		c.inRate, c.channels = frame.SampleRate, frame.Channels
		if c.inRate != targetRate {
			c.resample = newLinearMonoResampler(c.inRate, targetRate)
		}
	} else if c.inRate != frame.SampleRate || c.channels != frame.Channels {
		return nil, fmt.Errorf("%w: audio format changed from %d Hz/%d channels to %d Hz/%d channels",
			agents.ErrInvalidAudioFormat, c.inRate, c.channels, frame.SampleRate, frame.Channels)
	}
	mono := frame
	if frame.Channels != 1 {
		data := downmixMono(frame.Data, frame.Channels)
		mono, _ = agents.NewAudioFrame(data, frame.SampleRate, 1)
		mono.UserData = frame.UserData
	}
	if c.resample == nil {
		return []agents.AudioFrame{mono}, nil
	}
	data := c.resample.push(mono.Data, false)
	if len(data) == 0 {
		return nil, nil
	}
	result, _ := agents.NewAudioFrame(data, targetRate, 1)
	result.UserData = frame.UserData
	return []agents.AudioFrame{result}, nil
}

func (c *turnAudioConverter) flush(targetRate int) []agents.AudioFrame {
	c.mu.Lock()
	defer c.mu.Unlock()
	var result []agents.AudioFrame
	if c.resample != nil {
		if data := c.resample.push(nil, true); len(data) != 0 {
			frame, _ := agents.NewAudioFrame(data, targetRate, 1)
			result = append(result, frame)
		}
	}
	c.inRate, c.channels, c.resample = 0, 0, nil
	return result
}

func downmixMono(input []int16, channels int) []int16 {
	result := make([]int16, len(input)/channels)
	for sample := range result {
		var total int64
		base := sample * channels
		for channel := range channels {
			total += int64(input[base+channel])
		}
		result[sample] = int16(total / int64(channels))
	}
	return result
}

type linearMonoResampler struct {
	inRate, outRate int64
	base, totalIn   int64
	nextOut         int64
	buffer          []int16
}

func newLinearMonoResampler(inRate, outRate int) *linearMonoResampler {
	return &linearMonoResampler{inRate: int64(inRate), outRate: int64(outRate)}
}

func (r *linearMonoResampler) push(input []int16, flush bool) []int16 {
	r.buffer = append(r.buffer, input...)
	r.totalIn += int64(len(input))
	estimated := int((r.totalIn*r.outRate)/r.inRate - r.nextOut)
	if estimated < 0 {
		estimated = 0
	}
	result := make([]int16, 0, estimated)
	limit := int64(-1)
	if flush {
		limit = (r.totalIn*r.outRate + r.inRate/2) / r.inRate
	}
	for {
		if flush && r.nextOut >= limit {
			break
		}
		position := r.nextOut * r.inRate
		left, fraction := position/r.outRate, position%r.outRate
		if left >= r.totalIn {
			break
		}
		right := left
		if fraction != 0 {
			right++
			if right >= r.totalIn {
				if !flush {
					break
				}
				right = left
			}
		}
		li, ri := int(left-r.base), int(right-r.base)
		if li < 0 || ri >= len(r.buffer) {
			break
		}
		value := (int64(r.buffer[li])*(r.outRate-fraction) + int64(r.buffer[ri])*fraction) / r.outRate
		result = append(result, int16(value))
		r.nextOut++
	}
	needed := (r.nextOut * r.inRate) / r.outRate
	if needed > r.base {
		drop := min(needed-r.base, int64(len(r.buffer)))
		copy(r.buffer, r.buffer[int(drop):])
		r.buffer = r.buffer[:len(r.buffer)-int(drop)]
		r.base += drop
	}
	if flush {
		r.buffer = r.buffer[:0]
		r.base = r.totalIn
	}
	return result
}

func pcm16LittleEndian(samples []int16) []byte {
	result := make([]byte, len(samples)*2)
	for i, sample := range samples {
		binary.LittleEndian.PutUint16(result[i*2:], uint16(sample))
	}
	return result
}
