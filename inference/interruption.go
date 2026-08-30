// SPDX-License-Identifier: Apache-2.0

package inference

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/metrics"
	"github.com/infinityscroll/livekit-agents-go/stream"
)

// Adaptive interruption wire and timing defaults.
const (
	AdaptiveInterruptionSampleRate            = 16000
	DefaultMinimumInterruptionDuration        = 50 * time.Millisecond
	DefaultMaximumInterruptionAudioDuration   = 3 * time.Second
	DefaultInterruptionAudioPrefixDuration    = time.Second
	DefaultInterruptionDetectionInterval      = 100 * time.Millisecond
	DefaultRemoteInterruptionInferenceTimeout = 700 * time.Millisecond
	adaptiveInterruptionFrameDuration         = 25 * time.Millisecond
	defaultInterruptionInputCapacity          = 64
	defaultInterruptionOutputCapacity         = 32
	interruptionCacheCapacity                 = 10
)

var (
	// ErrInterruptionDetectorClosed reports use after detector shutdown.
	ErrInterruptionDetectorClosed = errors.New("adaptive interruption detector is closed")
	errInterruptionReconnect      = errors.New("adaptive interruption options changed")
)

// InterruptionDetectionError is emitted separately from stream termination so
// applications can observe recoverable retries without consuming the stream.
type InterruptionDetectionError struct {
	Timestamp   time.Time
	Label       string
	Err         error
	Recoverable bool
}

func (e InterruptionDetectionError) Error() string {
	return fmt.Sprintf("interruption detection failed (label=%s, recoverable=%t): %v", e.Label, e.Recoverable, e.Err)
}
func (e InterruptionDetectionError) Unwrap() error { return e.Err }

// OverlappingSpeechEvent is the adaptive detector's interruption/backchannel
// verdict. Raw inference input is retained for diagnostics but omitted from
// JSON, matching the Python serializer.
type OverlappingSpeechEvent struct {
	Type               string        `json:"type"`
	CreatedAt          time.Time     `json:"createdAt"`
	DetectedAt         time.Time     `json:"detectedAt"`
	IsInterruption     bool          `json:"isInterruption"`
	AgentEnded         bool          `json:"agentEnded,omitempty"`
	TotalDuration      time.Duration `json:"-"`
	PredictionDuration time.Duration `json:"-"`
	DetectionDelay     time.Duration `json:"-"`
	OverlapStartedAt   *time.Time    `json:"-"`
	SpeechInput        []int16       `json:"-"`
	Probabilities      []float32     `json:"-"`
	Probability        float64       `json:"probability"`
	NumRequests        int64         `json:"numRequests"`
}

func (event OverlappingSpeechEvent) MarshalJSON() ([]byte, error) {
	type wire struct {
		Type                 string  `json:"type"`
		DetectedAt           int64   `json:"detectedAt"`
		IsInterruption       bool    `json:"isInterruption"`
		AgentEnded           bool    `json:"agentEnded,omitempty"`
		TotalDurationSeconds float64 `json:"totalDurationInS"`
		PredictionSeconds    float64 `json:"predictionDurationInS"`
		DetectionDelay       float64 `json:"detectionDelayInS"`
		OverlapStartedAt     *int64  `json:"overlapStartedAt,omitempty"`
		Probability          float64 `json:"probability"`
		NumRequests          int64   `json:"numRequests"`
	}
	var overlapStartedAt *int64
	if event.OverlapStartedAt != nil && !event.OverlapStartedAt.IsZero() {
		value := event.OverlapStartedAt.UnixMilli()
		overlapStartedAt = &value
	}
	var detectedAt int64
	if !event.DetectedAt.IsZero() {
		detectedAt = event.DetectedAt.UnixMilli()
	}
	return json.Marshal(wire{
		Type: event.Type, DetectedAt: detectedAt,
		IsInterruption: event.IsInterruption, AgentEnded: event.AgentEnded,
		TotalDurationSeconds: event.TotalDuration.Seconds(),
		PredictionSeconds:    event.PredictionDuration.Seconds(),
		DetectionDelay:       event.DetectionDelay.Seconds(),
		OverlapStartedAt:     overlapStartedAt,
		Probability:          event.Probability,
		NumRequests:          event.NumRequests,
	})
}

// AdaptiveInterruptionDetectorOptions configures the LiveKit barge-in model.
// A nil Threshold adopts the server-calibrated default.
type AdaptiveInterruptionDetectorOptions struct {
	Threshold                   *float64
	MinimumInterruptionDuration time.Duration
	MaximumAudioDuration        time.Duration
	AudioPrefixDuration         time.Duration
	DetectionInterval           time.Duration
	InferenceTimeout            time.Duration
	BaseURL                     string
	Credentials                 Credentials
	ConnectOptions              agents.APIConnectOptions
	Metadata                    func(context.Context) RequestMetadata
	WebSocketDialer             *websocket.Dialer
	InputCapacity               int
	OutputCapacity              int
	WriteTimeout                time.Duration
}

type resolvedInterruptionOptions struct {
	threshold      *float64
	minFrames      int
	minDuration    time.Duration
	maxAudio       time.Duration
	prefix         time.Duration
	interval       time.Duration
	timeout        time.Duration
	baseURL        string
	credentials    Credentials
	connect        agents.APIConnectOptions
	metadata       func(context.Context) RequestMetadata
	dialer         *websocket.Dialer
	inputCapacity  int
	outputCapacity int
	writeTimeout   time.Duration
}

func resolveInterruptionOptions(options AdaptiveInterruptionDetectorOptions) (resolvedInterruptionOptions, error) {
	if options.MinimumInterruptionDuration == 0 {
		options.MinimumInterruptionDuration = DefaultMinimumInterruptionDuration
	}
	if options.MaximumAudioDuration == 0 {
		options.MaximumAudioDuration = DefaultMaximumInterruptionAudioDuration
	}
	if options.AudioPrefixDuration == 0 {
		options.AudioPrefixDuration = DefaultInterruptionAudioPrefixDuration
	}
	if options.DetectionInterval == 0 {
		options.DetectionInterval = DefaultInterruptionDetectionInterval
	}
	if options.InferenceTimeout == 0 {
		options.InferenceTimeout = DefaultRemoteInterruptionInferenceTimeout
	}
	if options.MaximumAudioDuration <= 0 || options.MaximumAudioDuration > 3*time.Second {
		return resolvedInterruptionOptions{}, errors.New("maximum interruption audio duration must be in (0, 3s]")
	}
	if options.MinimumInterruptionDuration < 0 || options.AudioPrefixDuration < 0 ||
		options.AudioPrefixDuration > options.MaximumAudioDuration || options.DetectionInterval <= 0 || options.InferenceTimeout < 0 {
		return resolvedInterruptionOptions{}, errors.New("invalid adaptive interruption durations")
	}
	if options.Threshold != nil && (math.IsNaN(*options.Threshold) || math.IsInf(*options.Threshold, 0)) {
		return resolvedInterruptionOptions{}, errors.New("interruption threshold must be finite")
	}
	credentials, err := options.Credentials.Resolve()
	if err != nil {
		return resolvedInterruptionOptions{}, err
	}
	if options.BaseURL == "" {
		options.BaseURL = DefaultURLFromEnvironment()
	}
	if options.InputCapacity == 0 {
		options.InputCapacity = defaultInterruptionInputCapacity
	}
	if options.OutputCapacity == 0 {
		options.OutputCapacity = defaultInterruptionOutputCapacity
	}
	if options.InputCapacity < 1 || options.OutputCapacity < 1 {
		return resolvedInterruptionOptions{}, errors.New("adaptive interruption capacities must be positive")
	}
	connect := options.ConnectOptions.Resolve()
	if options.WriteTimeout <= 0 {
		options.WriteTimeout = connect.Timeout
	}
	return resolvedInterruptionOptions{
		threshold:   cloneFloat64Pointer(options.Threshold),
		minFrames:   int(math.Ceil(float64(options.MinimumInterruptionDuration) / float64(adaptiveInterruptionFrameDuration))),
		minDuration: options.MinimumInterruptionDuration, maxAudio: options.MaximumAudioDuration,
		prefix: options.AudioPrefixDuration, interval: options.DetectionInterval,
		timeout: options.InferenceTimeout, baseURL: options.BaseURL, credentials: credentials,
		connect: connect, metadata: options.Metadata, dialer: options.WebSocketDialer,
		inputCapacity: options.InputCapacity, outputCapacity: options.OutputCapacity,
		writeTimeout: options.WriteTimeout,
	}, nil
}

func cloneFloat64Pointer(value *float64) *float64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

// AdaptiveInterruptionDetector classifies overlap as interruption or backchannel.
type AdaptiveInterruptionDetector struct {
	mu      sync.RWMutex
	opts    resolvedInterruptionOptions
	streams map[*InterruptionStream]struct{}
	closed  bool

	overlap agents.EventEmitter[OverlappingSpeechEvent]
	metrics agents.EventEmitter[metrics.Interruption]
	errors  agents.EventEmitter[InterruptionDetectionError]

	closeOnce sync.Once
	closeDone chan struct{}
}

// NewAdaptiveInterruptionDetector validates credentials and options without dialing.
func NewAdaptiveInterruptionDetector(options AdaptiveInterruptionDetectorOptions) (*AdaptiveInterruptionDetector, error) {
	resolved, err := resolveInterruptionOptions(options)
	if err != nil {
		return nil, err
	}
	return &AdaptiveInterruptionDetector{
		opts: resolved, streams: make(map[*InterruptionStream]struct{}), closeDone: make(chan struct{}),
	}, nil
}

func (d *AdaptiveInterruptionDetector) Model() string    { return "adaptive interruption" }
func (d *AdaptiveInterruptionDetector) Provider() string { return "livekit" }
func (d *AdaptiveInterruptionDetector) Label() string {
	return "AdaptiveInterruptionDetector"
}
func (d *AdaptiveInterruptionDetector) SampleRate() int { return AdaptiveInterruptionSampleRate }
func (d *AdaptiveInterruptionDetector) OnOverlappingSpeech(fn func(OverlappingSpeechEvent)) func() {
	return d.overlap.Subscribe(fn)
}
func (d *AdaptiveInterruptionDetector) OnMetrics(fn func(metrics.Interruption)) func() {
	return d.metrics.Subscribe(fn)
}
func (d *AdaptiveInterruptionDetector) OnError(fn func(InterruptionDetectionError)) func() {
	return d.errors.Subscribe(fn)
}

func (d *AdaptiveInterruptionDetector) options() resolvedInterruptionOptions {
	d.mu.RLock()
	result := d.opts
	result.threshold = cloneFloat64Pointer(d.opts.threshold)
	d.mu.RUnlock()
	return result
}

// AdaptiveInterruptionUpdateOptions is a sparse live update that reconnects streams.
type AdaptiveInterruptionUpdateOptions struct {
	Threshold                   *float64
	MinimumInterruptionDuration *time.Duration
}

func (d *AdaptiveInterruptionDetector) UpdateOptions(options AdaptiveInterruptionUpdateOptions) error {
	if options.Threshold != nil && (math.IsNaN(*options.Threshold) || math.IsInf(*options.Threshold, 0)) {
		return errors.New("interruption threshold must be finite")
	}
	if options.MinimumInterruptionDuration != nil && *options.MinimumInterruptionDuration < 0 {
		return errors.New("minimum interruption duration must not be negative")
	}
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return ErrInterruptionDetectorClosed
	}
	if options.Threshold != nil {
		d.opts.threshold = cloneFloat64Pointer(options.Threshold)
	}
	if options.MinimumInterruptionDuration != nil {
		d.opts.minDuration = *options.MinimumInterruptionDuration
		d.opts.minFrames = int(math.Ceil(float64(*options.MinimumInterruptionDuration) / float64(adaptiveInterruptionFrameDuration)))
	}
	active := make([]*InterruptionStream, 0, len(d.streams))
	for value := range d.streams {
		active = append(active, value)
	}
	d.mu.Unlock()
	for _, value := range active {
		value.requestReconnect()
	}
	return nil
}

// InterruptionStreamOptions overrides per-stream connection and queue settings.
type InterruptionStreamOptions struct {
	ConnectOptions agents.APIConnectOptions
	InputCapacity  int
	OutputCapacity int
}

func (d *AdaptiveInterruptionDetector) Stream(parent context.Context, options ...InterruptionStreamOptions) (*InterruptionStream, error) {
	if len(options) > 1 {
		return nil, errors.New("adaptive interruption Stream accepts at most one options value")
	}
	if parent == nil {
		parent = context.Background()
	}
	resolved := d.options()
	if len(options) == 1 {
		if options[0].ConnectOptions != (agents.APIConnectOptions{}) {
			resolved.connect = options[0].ConnectOptions.Resolve()
		}
		if options[0].InputCapacity > 0 {
			resolved.inputCapacity = options[0].InputCapacity
		}
		if options[0].OutputCapacity > 0 {
			resolved.outputCapacity = options[0].OutputCapacity
		}
	}
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil, ErrInterruptionDetectorClosed
	}
	ctx, cancel := context.WithCancelCause(parent)
	result := &InterruptionStream{
		detector: d, ctx: ctx, cancel: cancel, opts: resolved,
		input:   stream.NewChannel[interruptionInput](resolved.inputCapacity),
		output:  stream.NewChannel[OverlappingSpeechEvent](resolved.outputCapacity),
		updates: make(chan struct{}, 1), done: make(chan struct{}),
	}
	d.streams[result] = struct{}{}
	d.mu.Unlock()
	go result.run()
	return result, nil
}

func (d *AdaptiveInterruptionDetector) unregister(value *InterruptionStream) {
	d.mu.Lock()
	delete(d.streams, value)
	d.mu.Unlock()
}

func (d *AdaptiveInterruptionDetector) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	d.closeOnce.Do(func() {
		d.mu.Lock()
		d.closed = true
		active := make([]*InterruptionStream, 0, len(d.streams))
		for value := range d.streams {
			active = append(active, value)
		}
		d.mu.Unlock()
		for _, value := range active {
			_ = value.Close()
		}
		go func() {
			for _, value := range active {
				<-value.done
			}
			close(d.closeDone)
		}()
	})
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-d.closeDone:
		return nil
	}
}

type interruptionInputKind uint8

const (
	interruptionAudio interruptionInputKind = iota
	interruptionAgentStarted
	interruptionAgentEnded
	interruptionOverlapStarted
	interruptionOverlapEnded
	interruptionFlush
)

type interruptionInput struct {
	kind           interruptionInputKind
	frame          agents.AudioFrame
	speechDuration time.Duration
	at             time.Time
	agentEnded     bool
}

type interruptionInputResult struct {
	value interruptionInput
	err   error
}

type interruptionCacheEntry struct {
	id                 uint64
	requestStarted     time.Time
	speechInput        []int16
	totalDuration      time.Duration
	predictionDuration time.Duration
	detectionDelay     time.Duration
	probabilities      []float32
	isInterruption     bool
	responded          bool
}

func (e interruptionCacheEntry) probability(window time.Duration) float64 {
	return EstimateInterruptionProbability(e.probabilities, window)
}

type fixedPCMWindow struct {
	data []int16
	len  int
}

func newFixedPCMWindow(capacity int) fixedPCMWindow {
	return fixedPCMWindow{data: make([]int16, capacity)}
}
func (w *fixedPCMWindow) reset() { w.len = 0 }
func (w *fixedPCMWindow) push(input []int16) int {
	written := len(input)
	if len(input) >= len(w.data) {
		copy(w.data, input[len(input)-len(w.data):])
		w.len = len(w.data)
		return written
	}
	if overflow := w.len + len(input) - len(w.data); overflow > 0 {
		copy(w.data, w.data[overflow:w.len])
		w.len -= overflow
	}
	copy(w.data[w.len:], input)
	w.len += len(input)
	return written
}
func (w *fixedPCMWindow) keepLast(samples int) {
	samples = min(max(samples, 0), w.len)
	copy(w.data, w.data[w.len-samples:w.len])
	w.len = samples
}
func (w *fixedPCMWindow) snapshot() []int16 { return slices.Clone(w.data[:w.len]) }

type interruptionActorState struct {
	agentSpeaking  bool
	overlap        bool
	overlapStarted time.Time
	overlapCount   int
	accumulated    int
	requests       int64
	audio          fixedPCMWindow
	cache          []interruptionCacheEntry
	pendingEnd     *interruptionPendingEnd
}

type interruptionPendingEnd struct {
	requestID  uint64
	endedAt    time.Time
	agentEnded bool
	deadline   time.Time
}

func newInterruptionActorState(options resolvedInterruptionOptions) interruptionActorState {
	return interruptionActorState{audio: newFixedPCMWindow(int(options.maxAudio * AdaptiveInterruptionSampleRate / time.Second))}
}
func (s *interruptionActorState) resetAgent() {
	s.agentSpeaking, s.overlap, s.overlapStarted = false, false, time.Time{}
	s.overlapCount, s.accumulated, s.requests = 0, 0, 0
	s.audio.reset()
	s.cache = s.cache[:0]
	s.pendingEnd = nil
}
func (s *interruptionActorState) cacheAdd(entry interruptionCacheEntry) {
	if len(s.cache) == interruptionCacheCapacity {
		copy(s.cache, s.cache[1:])
		s.cache = s.cache[:len(s.cache)-1]
	}
	s.cache = append(s.cache, entry)
}
func (s *interruptionActorState) cacheEntry(id uint64) *interruptionCacheEntry {
	for i := range s.cache {
		if s.cache[i].id == id {
			return &s.cache[i]
		}
	}
	return nil
}
func (s *interruptionActorState) latestResponded() interruptionCacheEntry {
	for i := len(s.cache) - 1; i >= 0; i-- {
		if s.cache[i].responded {
			return s.cache[i]
		}
	}
	return interruptionCacheEntry{}
}

// InterruptionStream is a bounded, cancellation-aware overlap detector stream.
type InterruptionStream struct {
	detector *AdaptiveInterruptionDetector
	ctx      context.Context
	cancel   context.CancelCauseFunc
	opts     resolvedInterruptionOptions
	input    *stream.Channel[interruptionInput]
	output   *stream.Channel[OverlappingSpeechEvent]
	updates  chan struct{}
	format   turnAudioConverter

	done      chan struct{}
	closeOnce sync.Once
}

func (s *InterruptionStream) Recv(ctx context.Context) (OverlappingSpeechEvent, error) {
	return s.output.Recv(interruptionContext(ctx))
}

func (s *InterruptionStream) PushAudio(ctx context.Context, frame agents.AudioFrame) error {
	ctx = interruptionContext(ctx)
	frames, err := s.format.push(frame, AdaptiveInterruptionSampleRate)
	if err != nil {
		return err
	}
	for _, converted := range frames {
		if err := s.input.Send(ctx, interruptionInput{kind: interruptionAudio, frame: converted}); err != nil {
			return err
		}
	}
	return nil
}
func (s *InterruptionStream) AgentSpeechStarted(ctx context.Context) error {
	return s.input.Send(interruptionContext(ctx), interruptionInput{kind: interruptionAgentStarted, at: time.Now()})
}
func (s *InterruptionStream) AgentSpeechEnded(ctx context.Context) error {
	return s.input.Send(interruptionContext(ctx), interruptionInput{kind: interruptionAgentEnded, at: time.Now()})
}
func (s *InterruptionStream) OverlapSpeechStarted(ctx context.Context, speechDuration time.Duration, startedAt time.Time) error {
	if startedAt.IsZero() {
		startedAt = time.Now()
	}
	return s.input.Send(interruptionContext(ctx), interruptionInput{kind: interruptionOverlapStarted, speechDuration: speechDuration, at: startedAt})
}
func (s *InterruptionStream) OverlapSpeechEnded(ctx context.Context, endedAt time.Time, agentEnded bool) error {
	if endedAt.IsZero() {
		endedAt = time.Now()
	}
	return s.input.Send(interruptionContext(ctx), interruptionInput{kind: interruptionOverlapEnded, at: endedAt, agentEnded: agentEnded})
}
func (s *InterruptionStream) Flush(ctx context.Context) error {
	ctx = interruptionContext(ctx)
	return s.input.Send(ctx, interruptionInput{kind: interruptionFlush})
}
func (s *InterruptionStream) EndInput(ctx context.Context) error {
	ctx = interruptionContext(ctx)
	if !s.input.Closed() {
		if err := s.Flush(ctx); err != nil && !errors.Is(err, stream.ErrClosed) {
			return err
		}
	}
	return s.input.Close()
}

func interruptionContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func (s *InterruptionStream) requestReconnect() {
	select {
	case s.updates <- struct{}{}:
	default:
	}
}

func (s *InterruptionStream) inputPump(output chan<- interruptionInputResult) {
	for {
		value, err := s.input.Recv(s.ctx)
		select {
		case output <- interruptionInputResult{value: value, err: err}:
		case <-s.ctx.Done():
			return
		}
		if err != nil {
			return
		}
	}
}

func (s *InterruptionStream) run() {
	defer close(s.done)
	defer s.detector.unregister(s)
	inputs := make(chan interruptionInputResult, 1)
	pumpDone := make(chan struct{})
	go func() {
		defer close(pumpDone)
		s.inputPump(inputs)
	}()
	state := newInterruptionActorState(s.opts)
	var terminal error
	defer func() {
		cause := terminal
		if cause == nil {
			cause = io.EOF
		}
		s.cancel(cause)
		<-pumpDone
	}()
	defer func() {
		if terminal != nil && !errors.Is(terminal, context.Canceled) && !errors.Is(terminal, stream.ErrClosed) {
			_ = s.output.Abort(terminal)
		} else {
			_ = s.output.Close()
		}
	}()
	retries := 0
	for {
		options := s.detector.options()
		ws, reader, err := s.connect(options)
		if err == nil {
			retries = 0
			err = s.serveConnection(ws, reader, inputs, &state, options)
			reader.Stop()
			_ = ws.Close()
			waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			_ = reader.Wait(waitCtx)
			cancel()
		}
		if errors.Is(err, errInterruptionReconnect) {
			state.cache = state.cache[:0]
			continue
		}
		if context.Cause(s.ctx) != nil {
			terminal = context.Cause(s.ctx)
			return
		}
		if errors.Is(err, io.EOF) {
			terminal = nil
			return
		}
		if !interruptionRetryable(err) || retries >= options.connect.MaxRetries {
			terminal = err
			s.emitError(err, false)
			return
		}
		s.emitError(err, true)
		delay := options.connect.RetryDelay(retries)
		retries++
		timer := time.NewTimer(delay)
		select {
		case <-s.ctx.Done():
			timer.Stop()
			terminal = context.Cause(s.ctx)
			return
		case <-s.updates:
			timer.Stop()
			retries = 0
		case <-timer.C:
		}
	}
}

func (s *InterruptionStream) connect(options resolvedInterruptionOptions) (*gatewayWS, *wsReader, error) {
	ctx, cancel := context.WithTimeout(s.ctx, options.connect.Timeout)
	ws, err := dialGatewayWS(ctx, gatewayWSDialOptions{
		BaseURL: options.baseURL, Endpoint: "bargein", Credentials: options.credentials,
		Metadata: options.metadata, Dialer: options.dialer,
		ReadLimit: MaxControlMessageBytes, WriteTimeout: options.writeTimeout,
	})
	cancel()
	if err != nil {
		return nil, nil, normalizeInterruptionConnectError(err)
	}
	settings := map[string]any{
		"sample_rate": AdaptiveInterruptionSampleRate, "num_channels": 1,
		"min_frames": options.minFrames, "encoding": "s16le",
	}
	if options.threshold != nil {
		settings["threshold"] = *options.threshold
	}
	if err := ws.WriteJSON(s.ctx, map[string]any{"type": "session.create", "settings": settings}); err != nil {
		_ = ws.Close()
		return nil, nil, err
	}
	return ws, startWSReader(ws), nil
}

func normalizeInterruptionConnectError(err error) error {
	var status *agents.APIStatusError
	if errors.As(err, &status) && status.StatusCode == 429 {
		return agents.NewAPIStatusError("LiveKit Adaptive Interruption quota exceeded", 429, status.RequestID, status.Body, false, status)
	}
	var timeout *agents.APITimeoutError
	if errors.As(err, &timeout) {
		return agents.NewAPITimeoutError("adaptive interruption connection timed out", false, timeout)
	}
	return err
}

func interruptionRetryable(err error) bool {
	if err == nil {
		return false
	}
	type retryable interface{ Retryable() bool }
	var value retryable
	return errors.As(err, &value) && value.Retryable()
}

func (s *InterruptionStream) serveConnection(ws *gatewayWS, reader *wsReader, inputs <-chan interruptionInputResult, state *interruptionActorState, options resolvedInterruptionOptions) error {
	var pendingTimer *time.Timer
	var pendingDeadline time.Time
	defer func() {
		if pendingTimer != nil {
			pendingTimer.Stop()
		}
	}()
	for {
		inputSource := inputs
		updateSource := (<-chan struct{})(s.updates)
		var timeoutSource <-chan time.Time
		if pending := state.pendingEnd; pending != nil {
			// Preserve actor ordering while the last inference request is in
			// flight. The bounded input path provides backpressure without a
			// polling goroutine, and option reconnects wait for the same bounded
			// deadline so they cannot discard the final verdict.
			inputSource = nil
			updateSource = nil
			if pendingTimer == nil {
				pendingTimer = time.NewTimer(max(time.Until(pending.deadline), 0))
				pendingDeadline = pending.deadline
			} else if pendingDeadline != pending.deadline {
				if !pendingTimer.Stop() {
					select {
					case <-pendingTimer.C:
					default:
					}
				}
				pendingTimer.Reset(max(time.Until(pending.deadline), 0))
				pendingDeadline = pending.deadline
			}
			timeoutSource = pendingTimer.C
		} else if pendingTimer != nil {
			if !pendingTimer.Stop() {
				select {
				case <-pendingTimer.C:
				default:
				}
			}
			pendingDeadline = time.Time{}
		}
		select {
		case <-s.ctx.Done():
			return context.Cause(s.ctx)
		case <-updateSource:
			return errInterruptionReconnect
		case <-timeoutSource:
			pending := state.pendingEnd
			if pending == nil {
				continue
			}
			return agents.NewAPIStatusError(
				fmt.Sprintf("interruption inference timed out after %.1fs (ws)", options.timeout.Seconds()),
				408, "", nil, false, nil,
			)
		case result := <-inputSource:
			if result.err != nil {
				if errors.Is(result.err, io.EOF) {
					_ = ws.WriteJSON(s.ctx, map[string]string{"type": "session.close"})
					return io.EOF
				}
				return result.err
			}
			if err := s.handleInput(ws, state, options, result.value); err != nil {
				return err
			}
		case result, ok := <-reader.events:
			if !ok {
				return agents.NewAPIConnectionError("adaptive interruption connection closed unexpectedly", true, nil)
			}
			if result.Err != nil {
				result.Stop()
				return agents.NewAPIConnectionError("adaptive interruption WebSocket read failed", true, result.Err)
			}
			err := s.handleServerMessage(state, options, result.Data)
			if err != nil {
				result.Stop()
				return err
			}
			result.Resume()
		}
	}
}

func (s *InterruptionStream) handleInput(ws *gatewayWS, state *interruptionActorState, options resolvedInterruptionOptions, input interruptionInput) error {
	switch input.kind {
	case interruptionAgentStarted:
		state.resetAgent()
		state.agentSpeaking = true
	case interruptionAgentEnded:
		state.resetAgent()
	case interruptionOverlapStarted:
		if !state.agentSpeaking {
			return nil
		}
		state.overlap, state.overlapStarted, state.accumulated = true, input.at, 0
		state.overlapCount++
		if state.overlapCount == 1 {
			keep := int((input.speechDuration + options.prefix) * AdaptiveInterruptionSampleRate / time.Second)
			state.audio.keepLast(keep)
		}
		state.cache = state.cache[:0]
	case interruptionOverlapEnded:
		if state.overlap {
			if len(state.cache) > 0 && !state.cache[len(state.cache)-1].responded {
				entry := state.cache[len(state.cache)-1]
				deadline := entry.requestStarted.Add(options.timeout)
				if options.timeout <= 0 || !deadline.After(time.Now()) {
					return agents.NewAPIStatusError(
						fmt.Sprintf("interruption inference timed out after %.1fs (ws)", options.timeout.Seconds()),
						408, "", nil, false, nil,
					)
				}
				state.pendingEnd = &interruptionPendingEnd{
					requestID: entry.id, endedAt: input.at, agentEnded: input.agentEnded, deadline: deadline,
				}
				return nil
			}
			return s.finishOverlap(state, input.at, input.agentEnded, options)
		}
		state.overlap, state.overlapStarted, state.accumulated, state.pendingEnd = false, time.Time{}, 0, nil
	case interruptionAudio:
		if !state.agentSpeaking {
			return nil
		}
		if len(input.frame.Data) > len(state.audio.data) {
			return fmt.Errorf("adaptive interruption audio frame has %d samples; maximum window is %d", len(input.frame.Data), len(state.audio.data))
		}
		state.accumulated += state.audio.push(input.frame.Data)
		batch := int(options.interval * AdaptiveInterruptionSampleRate / time.Second)
		if state.overlap && state.accumulated >= batch {
			state.accumulated = 0
			return s.sendInterruptionAudio(ws, state, options)
		}
	case interruptionFlush:
		// A flush is intentionally a no-op for interruption overlap state.
	}
	return nil
}

var interruptionRequestSequence atomic.Uint64

func (s *InterruptionStream) sendInterruptionAudio(ws *gatewayWS, state *interruptionActorState, options resolvedInterruptionOptions) error {
	now := time.Now()
	if options.timeout > 0 {
		for i := range state.cache {
			entry := &state.cache[i]
			if entry.responded {
				continue
			}
			if now.Sub(entry.requestStarted) > options.timeout {
				return agents.NewAPIStatusError(
					fmt.Sprintf("interruption inference timed out after %.1fs (ws)", now.Sub(entry.requestStarted).Seconds()),
					408, "", nil, false, nil,
				)
			}
			break
		}
	}
	id := interruptionRequestSequence.Add(1)
	audio := state.audio.snapshot()
	payload := make([]byte, 8+len(audio)*2)
	binary.LittleEndian.PutUint64(payload, id)
	for i, sample := range audio {
		binary.LittleEndian.PutUint16(payload[8+i*2:], uint16(sample))
	}
	if err := ws.write(s.ctx, websocket.BinaryMessage, payload); err != nil {
		return err
	}
	state.cacheAdd(interruptionCacheEntry{id: id, requestStarted: now, speechInput: audio})
	state.requests++
	return nil
}

type interruptionServerMessage struct {
	Type             string    `json:"type"`
	DefaultThreshold *float64  `json:"default_threshold"`
	CreatedAt        uint64    `json:"created_at"`
	Probabilities    []float32 `json:"probabilities"`
	Prediction       float64   `json:"prediction_duration"`
	IsBargeIn        *bool     `json:"is_bargein"`
	Message          string    `json:"message"`
	Code             int       `json:"code"`
}

func (s *InterruptionStream) handleServerMessage(state *interruptionActorState, options resolvedInterruptionOptions, data []byte) error {
	if len(data) == 0 || len(data) > MaxControlMessageBytes {
		return ErrInvalidEvent
	}
	var message interruptionServerMessage
	if err := json.Unmarshal(data, &message); err != nil {
		return fmt.Errorf("decode adaptive interruption message: %w", err)
	}
	switch message.Type {
	case "session.created":
		if options.threshold == nil && message.DefaultThreshold == nil {
			return agents.NewAPIStatusError("adaptive interruption session created without a threshold", 500, "", nil, false, nil)
		}
	case "session.closed":
		return io.EOF
	case "bargein_detected", "inference_done":
		if !state.overlap || state.overlapStarted.IsZero() {
			return nil
		}
		entry := state.cacheEntry(message.CreatedAt)
		if entry == nil {
			return nil
		}
		entry.totalDuration = time.Since(entry.requestStarted)
		entry.predictionDuration = secondsDuration(message.Prediction)
		entry.detectionDelay = time.Since(state.overlapStarted)
		entry.probabilities = slices.Clone(message.Probabilities)
		entry.responded = true
		entry.isInterruption = message.Type == "bargein_detected"
		if message.IsBargeIn != nil {
			entry.isInterruption = *message.IsBargeIn
		}
		if message.Type == "bargein_detected" {
			event := interruptionEvent(*entry, true, state.overlapStarted, time.Now(), false, state.requests, options.minDuration)
			state.requests, state.overlap, state.pendingEnd = 0, false, nil
			return s.emitEvent(event)
		}
		if pending := state.pendingEnd; pending != nil && pending.requestID == message.CreatedAt {
			return s.finishOverlap(state, pending.endedAt, pending.agentEnded, options)
		}
	case "error":
		return agents.NewAPIStatusError("LiveKit Adaptive Interruption returned error: "+message.Message, message.Code, "", message, message.Code >= 500, nil)
	default:
		// Forward compatibility: unknown control messages are ignored.
	}
	return nil
}

func (s *InterruptionStream) finishOverlap(state *interruptionActorState, endedAt time.Time, agentEnded bool, options resolvedInterruptionOptions) error {
	entry := state.latestResponded()
	event := interruptionEvent(entry, entry.isInterruption, state.overlapStarted, endedAt, agentEnded, state.requests, options.minDuration)
	state.requests = 0
	state.overlap, state.overlapStarted, state.accumulated, state.pendingEnd = false, time.Time{}, 0, nil
	return s.emitEvent(event)
}

func secondsDuration(seconds float64) time.Duration {
	if math.IsNaN(seconds) || math.IsInf(seconds, 0) {
		return 0
	}
	return time.Duration(seconds * float64(time.Second))
}

func interruptionEvent(entry interruptionCacheEntry, interruption bool, started, detected time.Time, agentEnded bool, requests int64, window time.Duration) OverlappingSpeechEvent {
	created := time.Now()
	if detected.IsZero() {
		detected = created
	}
	var startedPointer *time.Time
	if !started.IsZero() {
		copy := started
		startedPointer = &copy
	}
	return OverlappingSpeechEvent{
		Type: "overlapping_speech", CreatedAt: created, DetectedAt: detected,
		IsInterruption: interruption, AgentEnded: agentEnded,
		TotalDuration: entry.totalDuration, PredictionDuration: entry.predictionDuration,
		DetectionDelay: entry.detectionDelay, OverlapStartedAt: startedPointer,
		SpeechInput: slices.Clone(entry.speechInput), Probabilities: slices.Clone(entry.probabilities),
		Probability: entry.probability(window), NumRequests: requests,
	}
}

func (s *InterruptionStream) emitEvent(event OverlappingSpeechEvent) error {
	metric := metrics.Interruption{
		Timestamp: event.DetectedAt, TotalDuration: event.TotalDuration,
		PredictionDuration: event.PredictionDuration, DetectionDelay: event.DetectionDelay,
		NumRequests: event.NumRequests,
		Metadata:    metrics.Metadata{ModelProvider: s.detector.Provider(), ModelName: s.detector.Model()},
	}
	if event.IsInterruption {
		metric.NumInterruptions = 1
	} else if !event.AgentEnded {
		metric.NumBackchannels = 1
	}
	s.detector.overlap.Emit(event)
	s.detector.metrics.Emit(metric)
	return s.output.Send(s.ctx, event)
}

func (s *InterruptionStream) emitError(err error, recoverable bool) {
	if err == nil {
		err = errors.New("unknown interruption detection error")
	}
	s.detector.errors.Emit(InterruptionDetectionError{
		Timestamp: time.Now(), Label: s.detector.Label(), Err: err, Recoverable: recoverable,
	})
}

// EstimateInterruptionProbability returns the n-th largest frame probability,
// where n is the number of 25 ms frames in the minimum-duration window. This
// is the conservative estimator used by the Python and TypeScript SDKs.
func EstimateInterruptionProbability(probabilities []float32, window time.Duration) float64 {
	if window <= 0 {
		window = DefaultMinimumInterruptionDuration
	}
	n := int(math.Ceil(float64(window) / float64(adaptiveInterruptionFrameDuration)))
	if n < 1 {
		n = 1
	}
	if len(probabilities) < n {
		return 0
	}
	if n == 1 {
		largest := float32(-math.MaxFloat32)
		for _, value := range probabilities {
			if value > largest {
				largest = value
			}
		}
		return float64(largest)
	}
	if n == 2 {
		first, second := float32(-math.MaxFloat32), float32(-math.MaxFloat32)
		for _, value := range probabilities {
			if value >= first {
				second, first = first, value
			} else if value > second {
				second = value
			}
		}
		return float64(second)
	}
	copy := slices.Clone(probabilities)
	slices.SortFunc(copy, func(a, b float32) int {
		switch {
		case a > b:
			return -1
		case a < b:
			return 1
		default:
			return 0
		}
	})
	return float64(copy[n-1])
}

func (s *InterruptionStream) Close() error {
	s.closeOnce.Do(func() {
		s.cancel(stream.ErrClosed)
		_ = s.input.Abort(stream.ErrClosed)
	})
	return nil
}

func (s *InterruptionStream) Wait(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-s.done:
		return nil
	}
}

var _ stream.Reader[OverlappingSpeechEvent] = (*InterruptionStream)(nil)
