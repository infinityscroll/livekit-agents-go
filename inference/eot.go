// SPDX-License-Identifier: Apache-2.0

package inference

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/ipc"
	"github.com/livekit/agents-go/metrics"
	"github.com/livekit/agents-go/stream"
)

// Audio end-of-turn protocol and production defaults.
const (
	EOTInferenceMethod                 = "lk_eot_audio"
	DefaultTurnDetectorSampleRate      = 16000
	MinimumTurnDetectorSilenceDuration = 200 * time.Millisecond
	DefaultTurnPredictionTimeout       = time.Second
	defaultTurnInputCapacity           = 64
	localTurnAudioWindow               = 1200 * time.Millisecond
)

// EOTInferenceInput/Output are the shared-runner wire contract used by the
// local mini turn detector. PCM contains base64-encoded 16 kHz s16le audio so
// process IPC remains compact and language-compatible.
type EOTInferenceInput struct {
	PCM string `json:"pcm"`
}

type EOTInferenceOutput struct {
	Probability         float64 `json:"probability"`
	InferenceDurationMS float64 `json:"inferenceDurationMs"`
}

var (
	// ErrTurnDetectorClosed reports use after detector shutdown.
	ErrTurnDetectorClosed = errors.New("turn detector is closed")
	// ErrTurnPredictionSuperseded cancels local work for a replaced request.
	ErrTurnPredictionSuperseded = errors.New("turn prediction superseded")
	// ErrLocalInferenceUnavailable selects the documented positive fallback.
	ErrLocalInferenceUnavailable = errors.New("local turn inference is unavailable")
	errTurnTransportSwap         = errors.New("turn detector transport swapped")
)

// TurnDetectionEvent is emitted for one audio EOT prediction. Optional timing
// fields are pointers so an actual zero remains distinct from "not reported".
type TurnDetectionEvent struct {
	Type                   string         `json:"type"`
	EndOfTurnProbability   float64        `json:"endOfTurnProbability"`
	LastSpeakingTime       time.Time      `json:"-"`
	LastSpeakingTimeMillis int64          `json:"lastSpeakingTimeMs"`
	DetectionDelay         *time.Duration `json:"-"`
	InferenceDuration      *time.Duration `json:"-"`
	BackchannelProbability *float64       `json:"backchannelProbability,omitempty"`
}

func (event TurnDetectionEvent) MarshalJSON() ([]byte, error) {
	type wire struct {
		Type                   string   `json:"type"`
		EndOfTurnProbability   float64  `json:"endOfTurnProbability"`
		LastSpeakingTimeMillis int64    `json:"lastSpeakingTimeMs"`
		DetectionDelay         *float64 `json:"detectionDelay,omitempty"`
		InferenceDuration      *float64 `json:"inferenceDuration,omitempty"`
		BackchannelProbability *float64 `json:"backchannelProbability,omitempty"`
	}
	return json.Marshal(wire{
		Type: event.Type, EndOfTurnProbability: event.EndOfTurnProbability,
		LastSpeakingTimeMillis: event.LastSpeakingTimeMillis,
		DetectionDelay:         durationMillisecondsPointer(event.DetectionDelay),
		InferenceDuration:      durationMillisecondsPointer(event.InferenceDuration),
		BackchannelProbability: event.BackchannelProbability,
	})
}

func durationMillisecondsPointer(value *time.Duration) *float64 {
	if value == nil {
		return nil
	}
	milliseconds := float64(*value) / float64(time.Millisecond)
	return &milliseconds
}

func newTurnDetectionEvent(probability float64) TurnDetectionEvent {
	now := time.Now()
	return TurnDetectionEvent{
		Type: "eot_prediction", EndOfTurnProbability: probability,
		LastSpeakingTime: now, LastSpeakingTimeMillis: now.UnixMilli(),
	}
}

// FlushSentinel marks a hard turn boundary for transports.
type FlushSentinel struct {
	Reason string
}

// LocalEOTPredictor is the native/local model seam. Implementations should be
// safe for serialized calls; the SDK ensures at most one active prediction per
// stream and shares one lazily-created predictor across detector streams.
type LocalEOTPredictor interface {
	PredictEndOfTurn(context.Context, []int16) (float64, error)
	Close(context.Context) error
}

type LocalEOTPredictorFunc func(context.Context, []int16) (float64, error)

func (f LocalEOTPredictorFunc) PredictEndOfTurn(ctx context.Context, pcm []int16) (float64, error) {
	return f(ctx, pcm)
}
func (LocalEOTPredictorFunc) Close(context.Context) error { return nil }

// LocalEOTPredictorFactory lazily loads a shared mini-model predictor.
type LocalEOTPredictorFactory func(context.Context) (LocalEOTPredictor, error)

// StreamingTurnDetectionTransport is the extension seam used by the cloud and
// local transports. Run owns audio draining until the context is cancelled.
type StreamingTurnDetectionTransport interface {
	Attach(*TurnDetectorStream)
	Run(context.Context) error
	RunInference(context.Context, string) error
	PushFrame(context.Context, agents.AudioFrame) error
	Flush(context.Context, FlushSentinel) error
	Close() error
}

// TurnDetectorOptions maps the agents-js TurnDetector constructor while using
// typed credentials and context-aware local inference.
type TurnDetectorOptions struct {
	Version               TurnDetectorVersion
	UnlikelyThreshold     ThresholdOverride
	BackchannelThreshold  ThresholdOverride
	BaseURL               string
	Credentials           Credentials
	SampleRate            int
	ConnectOptions        agents.APIConnectOptions
	Metadata              func(context.Context) RequestMetadata
	WebSocketDialer       *websocket.Dialer
	LocalPredictor        LocalEOTPredictor
	LocalPredictorFactory LocalEOTPredictorFactory
	// Executor selects the worker-global runner registered under
	// EOTInferenceMethod. When nil, local fallback discovers an executor from
	// the stream context propagated by JobContext.
	Executor               ipc.InferenceExecutor
	KeepLocalPredictorOpen bool
	InputCapacity          int
}

type sharedLocalPredictor struct {
	ctx      context.Context
	factory  LocalEOTPredictorFactory
	provided LocalEOTPredictor
	executor ipc.InferenceExecutor
	keepOpen bool

	mu                sync.Mutex
	loading           bool
	ready             chan struct{}
	loaded            bool
	predictor         LocalEOTPredictor
	err               error
	gate              chan struct{}
	warnedUnavailable sync.Once
}

func (s *sharedLocalPredictor) get(ctx context.Context) (LocalEOTPredictor, error) {
	for {
		s.mu.Lock()
		if s.loaded {
			predictor, err := s.predictor, s.err
			s.mu.Unlock()
			return predictor, err
		}
		if s.loading {
			ready := s.ready
			s.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, context.Cause(ctx)
			case <-ready:
			}
			continue
		}
		s.loading = true
		s.ready = make(chan struct{})
		ready := s.ready
		s.mu.Unlock()

		var predictor LocalEOTPredictor
		var err error
		if s.provided != nil {
			predictor = s.provided
		} else if s.factory != nil {
			predictor, err = s.factory(s.ctx)
		} else if ipc.IsInferenceExecutor(s.executor) {
			predictor = executorEOTPredictor{executor: s.executor}
		} else if executor, ok := agents.InferenceExecutorFromContext(ctx); ok {
			predictor = executorEOTPredictor{executor: executor}
		} else {
			err = ErrLocalInferenceUnavailable
		}
		if err == nil && predictor == nil {
			err = ErrLocalInferenceUnavailable
		}
		s.mu.Lock()
		s.predictor, s.err, s.loaded, s.loading = predictor, err, true, false
		close(ready)
		s.mu.Unlock()
		return predictor, err
	}
}

func (s *sharedLocalPredictor) close(ctx context.Context) error {
	s.mu.Lock()
	if s.keepOpen {
		s.mu.Unlock()
		return nil
	}
	predictor := s.predictor
	if !s.loaded {
		// An injected predictor is owned by the adapter even when no stream ever
		// triggered lazy initialization. Factories remain lazy and are not
		// invoked solely for teardown.
		predictor = s.provided
	}
	s.predictor, s.provided = nil, nil
	s.mu.Unlock()
	if predictor == nil {
		return nil
	}
	return predictor.Close(ctx)
}

type executorEOTPredictor struct{ executor ipc.InferenceExecutor }

func (p executorEOTPredictor) PredictEndOfTurn(ctx context.Context, pcm []int16) (float64, error) {
	if !ipc.IsInferenceExecutor(p.executor) {
		return 0, ErrLocalInferenceUnavailable
	}
	bytes := make([]byte, len(pcm)*2)
	for index, sample := range pcm {
		binary.LittleEndian.PutUint16(bytes[index*2:], uint16(sample))
	}
	result, err := p.executor.DoInference(ctx, EOTInferenceMethod, EOTInferenceInput{
		PCM: base64.StdEncoding.EncodeToString(bytes),
	})
	if err != nil {
		return 0, err
	}
	switch value := result.(type) {
	case EOTInferenceOutput:
		return value.Probability, nil
	case *EOTInferenceOutput:
		if value != nil {
			return value.Probability, nil
		}
	case map[string]any:
		if probability, ok := value["probability"].(float64); ok {
			return probability, nil
		}
	}
	payload, marshalErr := json.Marshal(result)
	if marshalErr != nil {
		return 0, fmt.Errorf("decode local EOT inference output: %w", marshalErr)
	}
	var output EOTInferenceOutput
	if err := json.Unmarshal(payload, &output); err != nil {
		return 0, fmt.Errorf("decode local EOT inference output: %w", err)
	}
	return output.Probability, nil
}

func (executorEOTPredictor) Close(context.Context) error { return nil }

type resolvedTurnDetectorOptions struct {
	sampleRate    int
	thresholds    *ThresholdOptions
	cloud         *CloudTurnTransportOptions
	connect       agents.APIConnectOptions
	metadata      func(context.Context) RequestMetadata
	dialer        *websocket.Dialer
	inputCapacity int
	local         *sharedLocalPredictor
}

// TurnDetector implements the unified cloud v1 -> local v1-mini detector.
type TurnDetector struct {
	ctx    context.Context
	cancel context.CancelCauseFunc

	mu      sync.RWMutex
	model   TurnDetectorModel
	opts    resolvedTurnDetectorOptions
	streams map[*TurnDetectorStream]struct{}
	closed  bool

	metrics   agents.EventEmitter[metrics.EOTInference]
	closeOnce sync.Once
	closeDone chan struct{}
	closeErr  error
}

func NewTurnDetector(options TurnDetectorOptions) (*TurnDetector, error) {
	if options.Version != "" && options.Version != TurnDetectorV1 && options.Version != TurnDetectorV1Mini {
		return nil, fmt.Errorf("unknown turn detector version %q", options.Version)
	}
	localSelections := 0
	if options.LocalPredictor != nil {
		localSelections++
	}
	if options.LocalPredictorFactory != nil {
		localSelections++
	}
	if ipc.IsInferenceExecutor(options.Executor) {
		localSelections++
	}
	if localSelections > 1 {
		return nil, errors.New("turn detector accepts one local backend: LocalPredictor, LocalPredictorFactory, or Executor")
	}
	sampleRate := options.SampleRate
	if sampleRate == 0 {
		sampleRate = DefaultTurnDetectorSampleRate
	}
	if sampleRate <= 0 {
		return nil, errors.New("turn detector sample rate must be positive")
	}
	inputCapacity := options.InputCapacity
	if inputCapacity == 0 {
		inputCapacity = defaultTurnInputCapacity
	}
	if inputCapacity < 1 {
		return nil, errors.New("turn detector input capacity must be positive")
	}

	auto := options.Version == ""
	version := options.Version
	if version == "" {
		_, remoteEOTConfigured := os.LookupEnv("LIVEKIT_REMOTE_EOT_URL")
		if remoteEOTConfigured || os.Getenv("LIVEKIT_DEV_MODE") == "1" {
			version = TurnDetectorV1
		} else {
			version = TurnDetectorV1Mini
		}
	}
	model := TurnDetectorModelV1Mini
	if version == TurnDetectorV1 {
		model = TurnDetectorModelV1
	}
	thresholds, err := NewThresholdOptions(model, options.UnlikelyThreshold, options.BackchannelThreshold)
	if err != nil {
		return nil, err
	}

	connect := options.ConnectOptions.Resolve()
	var cloud *CloudTurnTransportOptions
	if model == TurnDetectorModelV1 {
		credentials, credentialsErr := options.Credentials.Resolve()
		if credentialsErr != nil {
			if !auto {
				return nil, fmt.Errorf("TurnDetector(version=%q): %w", TurnDetectorV1, credentialsErr)
			}
			slog.Warn("cloud turn detector credentials missing; falling back to local mini model", "error", credentialsErr)
			model = TurnDetectorModelV1Mini
			thresholds, err = NewThresholdOptions(model, options.UnlikelyThreshold, options.BackchannelThreshold)
			if err != nil {
				return nil, err
			}
		} else {
			baseURL := options.BaseURL
			if baseURL == "" {
				baseURL = DefaultURLFromEnvironment()
			}
			cloud = &CloudTurnTransportOptions{
				BaseURL: baseURL, Credentials: credentials, ConnectOptions: connect,
				Metadata: options.Metadata, WebSocketDialer: options.WebSocketDialer,
			}
		}
	}

	ctx, cancel := context.WithCancelCause(context.Background())
	detector := &TurnDetector{
		ctx: ctx, cancel: cancel, model: model, streams: make(map[*TurnDetectorStream]struct{}),
		closeDone: make(chan struct{}),
	}
	detector.opts = resolvedTurnDetectorOptions{
		sampleRate: sampleRate, thresholds: thresholds, cloud: cloud, connect: connect,
		metadata: options.Metadata, dialer: options.WebSocketDialer, inputCapacity: inputCapacity,
		local: &sharedLocalPredictor{
			ctx: ctx, factory: options.LocalPredictorFactory, provided: options.LocalPredictor,
			executor: options.Executor, keepOpen: options.KeepLocalPredictorOpen,
		},
	}
	if options.UnlikelyThreshold.set() {
		slog.Warn("a non-default turn detection threshold was provided; calibrated defaults are recommended")
	}
	if options.BackchannelThreshold.set() {
		slog.Warn("a non-default backchannel threshold was provided; calibrated defaults are recommended")
	}
	return detector, nil
}

func (d *TurnDetector) Model() string {
	d.mu.RLock()
	model := d.model
	d.mu.RUnlock()
	return string(model)
}
func (d *TurnDetector) Provider() string { return "livekit" }
func (d *TurnDetector) SampleRate() int  { return d.opts.sampleRate }

func (d *TurnDetector) Thresholds() map[string]float64 { return d.opts.thresholds.Thresholds() }
func (d *TurnDetector) UnlikelyThreshold(language agents.LanguageCode) (float64, bool) {
	return d.opts.thresholds.Lookup(language)
}
func (d *TurnDetector) BackchannelThreshold(language agents.LanguageCode) (float64, bool) {
	return d.opts.thresholds.LookupBackchannel(language)
}
func (d *TurnDetector) SupportsLanguage(language agents.LanguageCode) bool {
	return d.opts.thresholds.Supports(language)
}
func (d *TurnDetector) OnMetrics(fn func(metrics.EOTInference)) func() {
	return d.metrics.Subscribe(fn)
}

func (d *TurnDetector) setModel(model TurnDetectorModel) {
	d.mu.Lock()
	d.model = model
	d.mu.Unlock()
}

// TurnDetectorUpdateOptions is a sparse live threshold update.
type TurnDetectorUpdateOptions struct {
	UnlikelyThreshold    *ThresholdOverride
	BackchannelThreshold *ThresholdOverride
}

func (d *TurnDetector) UpdateOptions(options TurnDetectorUpdateOptions) error {
	if options.UnlikelyThreshold != nil {
		if err := d.opts.thresholds.UpdateOverrides(*options.UnlikelyThreshold); err != nil {
			return fmt.Errorf("update unlikely threshold: %w", err)
		}
	}
	if options.BackchannelThreshold != nil {
		if err := d.opts.thresholds.UpdateBackchannelOverrides(*options.BackchannelThreshold); err != nil {
			return fmt.Errorf("update backchannel threshold: %w", err)
		}
	}
	return nil
}

// TurnDetectorStreamOptions overrides per-stream transport settings.
type TurnDetectorStreamOptions struct {
	ConnectOptions agents.APIConnectOptions
	InputCapacity  int
	Transport      StreamingTurnDetectionTransport
}

func (d *TurnDetector) Stream(parent context.Context, options ...TurnDetectorStreamOptions) (*TurnDetectorStream, error) {
	if len(options) > 1 {
		return nil, errors.New("turn detector Stream accepts at most one options value")
	}
	if parent == nil {
		parent = context.Background()
	}
	var streamOptions TurnDetectorStreamOptions
	if len(options) == 1 {
		streamOptions = options[0]
	}
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil, ErrTurnDetectorClosed
	}
	model := d.model
	capacity := streamOptions.InputCapacity
	if capacity == 0 {
		capacity = d.opts.inputCapacity
	}
	if capacity < 1 {
		d.mu.Unlock()
		return nil, errors.New("turn detector stream capacity must be positive")
	}
	ctx, cancel := context.WithCancelCause(parent)
	result := &TurnDetectorStream{
		detector: d, ctx: ctx, cancel: cancel, model: model,
		input: stream.NewChannel[turnStreamInput](capacity), done: make(chan struct{}),
	}
	transport := streamOptions.Transport
	if transport == nil {
		if model == TurnDetectorModelV1 {
			cloud := *d.opts.cloud
			if streamOptions.ConnectOptions != (agents.APIConnectOptions{}) {
				cloud.ConnectOptions = streamOptions.ConnectOptions.Resolve()
			}
			transport = newCloudTurnTransport(d, &cloud)
		} else {
			transport = newLocalTurnTransport(d.opts.sampleRate, d.opts.local)
		}
	}
	result.transport = transport
	transport.Attach(result)
	d.streams[result] = struct{}{}
	d.mu.Unlock()
	go result.run()
	return result, nil
}

func (d *TurnDetector) unregister(value *TurnDetectorStream) {
	d.mu.Lock()
	delete(d.streams, value)
	d.mu.Unlock()
}

func (d *TurnDetector) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	d.closeOnce.Do(func() {
		d.mu.Lock()
		d.closed = true
		active := make([]*TurnDetectorStream, 0, len(d.streams))
		for value := range d.streams {
			active = append(active, value)
		}
		d.mu.Unlock()
		d.cancel(ErrTurnDetectorClosed)
		for _, value := range active {
			_ = value.Close()
		}
		go func() {
			for _, value := range active {
				<-value.done
			}
			d.closeErr = d.opts.local.close(context.Background())
			close(d.closeDone)
		}()
	})
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-d.closeDone:
		return d.closeErr
	}
}

func (d *TurnDetector) MarshalJSON() ([]byte, error) {
	var cloud any
	if d.opts.cloud != nil {
		cloud = struct {
			BaseURL string                   `json:"baseUrl"`
			Connect agents.APIConnectOptions `json:"connectOptions"`
		}{d.opts.cloud.BaseURL, d.opts.cloud.ConnectOptions}
	}
	return json.Marshal(struct {
		Model      TurnDetectorModel `json:"model"`
		SampleRate int               `json:"sampleRate"`
		Thresholds ThresholdSnapshot `json:"thresholds"`
		Cloud      any               `json:"cloud,omitempty"`
	}{
		Model: TurnDetectorModel(d.Model()), SampleRate: d.opts.sampleRate,
		Thresholds: d.opts.thresholds.Snapshot(), Cloud: cloud,
	})
}

type turnStreamInput struct {
	frame *agents.AudioFrame
	flush *FlushSentinel
}

type predictionResult struct {
	event TurnDetectionEvent
	err   error
}

// TurnPrediction is the Go equivalent of the SDK Future returned by predict().
// Wait is cancellation-aware and a deadline triggers the documented cloud to
// local timeout fallback.
type TurnPrediction struct {
	stream  *TurnDetectorStream
	id      string
	pending *pendingTurnPrediction
}

func (p *TurnPrediction) Wait(ctx context.Context) (TurnDetectionEvent, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-p.pending.done:
		return p.pending.result.event, p.pending.result.err
	case <-ctx.Done():
		p.stream.cancelRequest(p.id, errors.Is(context.Cause(ctx), context.DeadlineExceeded))
		return TurnDetectionEvent{}, context.Cause(ctx)
	}
}

type pendingTurnPrediction struct {
	id     string
	done   chan struct{}
	result predictionResult
}

// TurnDetectorStream is a bounded per-session audio EOT stream.
type TurnDetectorStream struct {
	detector *TurnDetector
	ctx      context.Context
	cancel   context.CancelCauseFunc
	input    *stream.Channel[turnStreamInput]

	mu              sync.Mutex
	model           TurnDetectorModel
	transport       StreamingTurnDetectionTransport
	transportCancel context.CancelCauseFunc
	pending         *pendingTurnPrediction
	fallback        bool
	closing         bool
	format          turnAudioConverter

	done      chan struct{}
	closeOnce sync.Once
}

var turnRequestSequence atomic.Uint64

func nextTurnRequestID() string {
	return "turn_request_" + strconv.FormatUint(turnRequestSequence.Add(1), 36)
}

func (s *TurnDetectorStream) Model() string {
	s.mu.Lock()
	model := s.model
	s.mu.Unlock()
	return string(model)
}
func (s *TurnDetectorStream) Provider() string { return s.detector.Provider() }
func (s *TurnDetectorStream) IsFallback() bool {
	s.mu.Lock()
	value := s.fallback
	s.mu.Unlock()
	return value
}
func (s *TurnDetectorStream) PredictionTimeout() time.Duration { return DefaultTurnPredictionTimeout }
func (s *TurnDetectorStream) UnlikelyThreshold(language agents.LanguageCode) (float64, bool) {
	return s.detector.opts.thresholds.Lookup(language)
}
func (s *TurnDetectorStream) BackchannelThreshold(language agents.LanguageCode) (float64, bool) {
	return s.detector.opts.thresholds.LookupBackchannel(language)
}
func (s *TurnDetectorStream) SupportsLanguage(language agents.LanguageCode) bool {
	return s.detector.opts.thresholds.Supports(language)
}

func (s *TurnDetectorStream) BeginPrediction(ctx context.Context) (*TurnPrediction, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	default:
	}
	s.mu.Lock()
	if s.closing || s.input.Closed() {
		s.mu.Unlock()
		event := newTurnDetectionEvent(1)
		pending := &pendingTurnPrediction{done: make(chan struct{}), result: predictionResult{event: event}}
		close(pending.done)
		return &TurnPrediction{stream: s, pending: pending}, nil
	}
	if s.pending != nil {
		s.resolvePendingLocked(newTurnDetectionEvent(0), nil)
	}
	pending := &pendingTurnPrediction{id: nextTurnRequestID(), done: make(chan struct{})}
	s.pending = pending
	transport := s.transport
	s.mu.Unlock()
	if err := transport.RunInference(ctx, pending.id); err != nil {
		s.ResolvePrediction(pending.id, 1, TurnPredictionDetails{})
		return nil, err
	}
	return &TurnPrediction{stream: s, id: pending.id, pending: pending}, nil
}

func (s *TurnDetectorStream) Predict(ctx context.Context) (TurnDetectionEvent, error) {
	prediction, err := s.BeginPrediction(ctx)
	if err != nil {
		return TurnDetectionEvent{}, err
	}
	return prediction.Wait(ctx)
}

// TurnPredictionDetails carries optional transport timing and backchannel data.
type TurnPredictionDetails struct {
	InferenceDuration      *time.Duration
	DetectionDelay         *time.Duration
	BackchannelProbability *float64
}

// ResolvePrediction accepts a transport result and ignores stale request IDs.
func (s *TurnDetectorStream) ResolvePrediction(requestID string, probability float64, details TurnPredictionDetails) {
	s.mu.Lock()
	if s.closing || s.pending == nil || s.pending.id != requestID {
		s.mu.Unlock()
		return
	}
	event := newTurnDetectionEvent(probability)
	event.InferenceDuration = cloneDurationPointer(details.InferenceDuration)
	event.DetectionDelay = cloneDurationPointer(details.DetectionDelay)
	if details.BackchannelProbability != nil {
		value := *details.BackchannelProbability
		event.BackchannelProbability = &value
	}
	s.resolvePendingLocked(event, nil)
	s.mu.Unlock()
}

func cloneDurationPointer(value *time.Duration) *time.Duration {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func (s *TurnDetectorStream) resolvePendingLocked(event TurnDetectionEvent, err error) {
	pending := s.pending
	if pending == nil {
		return
	}
	s.pending = nil
	pending.result = predictionResult{event: event, err: err}
	close(pending.done)
}

func (s *TurnDetectorStream) cancelRequest(id string, timedOut bool) {
	s.mu.Lock()
	if s.pending != nil && (id == "" || s.pending.id == id) {
		s.resolvePendingLocked(newTurnDetectionEvent(0), nil)
	}
	model := s.model
	s.mu.Unlock()
	if timedOut && model == TurnDetectorModelV1 {
		s.fallBackToLocal(agents.NewAPITimeoutError("eot prediction timed out", false, context.DeadlineExceeded))
	}
}

func (s *TurnDetectorStream) CancelInference(timedOut bool) { s.cancelRequest("", timedOut) }

func (s *TurnDetectorStream) PushAudio(ctx context.Context, frame agents.AudioFrame) error {
	if ctx == nil {
		ctx = context.Background()
	}
	frames, err := s.format.push(frame, s.detector.opts.sampleRate)
	if err != nil {
		return err
	}
	for i := range frames {
		copy := frames[i]
		if err := s.input.Send(ctx, turnStreamInput{frame: &copy}); err != nil {
			return err
		}
	}
	return nil
}

func (s *TurnDetectorStream) Flush(ctx context.Context, reason string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	frames := s.format.flush(s.detector.opts.sampleRate)
	for i := range frames {
		copy := frames[i]
		if err := s.input.Send(ctx, turnStreamInput{frame: &copy}); err != nil {
			return err
		}
	}
	sentinel := FlushSentinel{Reason: reason}
	if err := s.input.Send(ctx, turnStreamInput{flush: &sentinel}); err != nil {
		return err
	}
	s.CancelInference(false)
	return nil
}

func (s *TurnDetectorStream) EndInput(ctx context.Context) error {
	if !s.input.Closed() {
		if err := s.Flush(ctx, ""); err != nil && !errors.Is(err, stream.ErrClosed) {
			return err
		}
	}
	return s.input.Close()
}

func (s *TurnDetectorStream) drain(ctx context.Context, transport StreamingTurnDetectionTransport) error {
	for {
		input, err := s.input.Recv(ctx)
		if err != nil {
			return err
		}
		if input.flush != nil {
			if err := transport.Flush(ctx, *input.flush); err != nil {
				return err
			}
		} else if input.frame != nil {
			if err := transport.PushFrame(ctx, *input.frame); err != nil {
				return err
			}
		}
	}
}

func (s *TurnDetectorStream) run() {
	defer close(s.done)
	defer s.detector.unregister(s)
	defer func() {
		s.mu.Lock()
		transport := s.transport
		s.mu.Unlock()
		if transport != nil {
			_ = transport.Close()
		}
	}()
	for {
		s.mu.Lock()
		if s.closing {
			s.mu.Unlock()
			return
		}
		transport := s.transport
		runCtx, cancel := context.WithCancelCause(s.ctx)
		s.transportCancel = cancel
		model := s.model
		s.mu.Unlock()

		err := transport.Run(runCtx)
		cancel(context.Canceled)
		s.mu.Lock()
		if s.transport == transport {
			s.transportCancel = nil
		}
		closing := s.closing
		changed := s.transport != transport
		s.mu.Unlock()
		if closing || context.Cause(s.ctx) != nil || err == nil || errors.Is(err, io.EOF) {
			return
		}
		if changed || errors.Is(err, errTurnTransportSwap) {
			continue
		}
		if model == TurnDetectorModelV1 {
			s.fallBackToLocal(err)
			continue
		}
		s.localFailure(err)
		return
	}
}

func (s *TurnDetectorStream) fallBackToLocal(reason error) {
	s.mu.Lock()
	if s.closing || s.model != TurnDetectorModelV1 {
		s.mu.Unlock()
		return
	}
	old := s.transport
	cancel := s.transportCancel
	local := newLocalTurnTransport(s.detector.opts.sampleRate, s.detector.opts.local)
	local.Attach(s)
	s.transport, s.model, s.fallback = local, TurnDetectorModelV1Mini, true
	s.detector.opts.thresholds.ToLocalFallback()
	s.detector.setModel(TurnDetectorModelV1Mini)
	// Resolve only after every model/threshold view reflects the sticky swap.
	// Unlike JavaScript promise continuations, a Go waiter may run immediately
	// when its result channel closes.
	if s.pending != nil {
		s.resolvePendingLocked(newTurnDetectionEvent(1), nil)
	}
	s.mu.Unlock()
	slog.Warn("cloud turn detector failed; falling back to local mini model", "error", reason)
	if cancel != nil {
		cancel(errTurnTransportSwap)
	}
	_ = old.Close()
}

func (s *TurnDetectorStream) localFailure(reason error) {
	s.mu.Lock()
	if s.pending != nil {
		s.resolvePendingLocked(newTurnDetectionEvent(1), nil)
	}
	s.mu.Unlock()
	slog.Warn("local audio turn detector failed; defaulting to 1.0", "error", reason)
}

func (s *TurnDetectorStream) Close() error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closing = true
		if s.pending != nil {
			s.resolvePendingLocked(newTurnDetectionEvent(0), nil)
		}
		transport := s.transport
		cancel := s.transportCancel
		s.mu.Unlock()
		s.cancel(ErrTurnDetectorClosed)
		_ = s.input.Abort(ErrTurnDetectorClosed)
		if cancel != nil {
			cancel(ErrTurnDetectorClosed)
		}
		_ = transport.Close()
	})
	return nil
}

func (s *TurnDetectorStream) Wait(ctx context.Context) error {
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
