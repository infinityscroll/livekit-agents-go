// SPDX-License-Identifier: Apache-2.0

package inference

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"slices"
	"sync"
	"time"

	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/ipc"
	"github.com/infinityscroll/livekit-agents-go/metrics"
	vadpkg "github.com/infinityscroll/livekit-agents-go/vad"
)

// Inference VAD protocol and production defaults.
const (
	InferenceVADSampleRate      = 16000
	defaultVADWindowSamples     = 512
	defaultVADStreamCapacity    = 64
	defaultVADMinSpeech         = 50 * time.Millisecond
	defaultVADMinSilence        = 250 * time.Millisecond
	defaultVADPrefixPadding     = 500 * time.Millisecond
	defaultVADMaxBufferedSpeech = time.Minute
	defaultVADActivation        = 0.5
	defaultVADDeactivation      = 0.35
	defaultVADUpdateInterval    = 32 * time.Millisecond
	slowVADInferenceThreshold   = 200 * time.Millisecond
)

// ErrVADClosed reports an operation attempted after detector shutdown.
var ErrVADClosed = errors.New("inference VAD is closed")

// VADModel identifies a bundled local voice-activity model.
type VADModel string

// VADModelSilero selects the cross-SDK Silero model.
const VADModelSilero VADModel = "silero"

// VADPredictor is the stateful local-model seam. A factory must create an
// independent predictor per stream because Silero carries recurrent state.
// Predict must not retain the supplied window.
type VADPredictor interface {
	WindowSamples() int
	Predict(context.Context, []int16) (float64, error)
	Reset() error
	Close() error
}

// VADPredictorFactory lazily creates one stateful predictor per stream.
type VADPredictorFactory func(context.Context) (VADPredictor, error)

// VADOptions configures local inference VAD. Zero values select the pinned SDK defaults.
type VADOptions struct {
	Model                  VADModel
	MinimumSpeechDuration  time.Duration
	MinimumSilenceDuration time.Duration
	PrefixPaddingDuration  time.Duration
	MaximumBufferedSpeech  time.Duration
	ActivationThreshold    float64
	DeactivationThreshold  *float64
	PredictorFactory       VADPredictorFactory
	// Executor selects the worker-global runner registered under
	// VADInferenceMethod. When nil, each stream discovers the executor from
	// its context, matching TurnDetector and process-isolated job behavior.
	// PredictorFactory and Executor are mutually exclusive.
	Executor       ipc.InferenceExecutor
	StreamCapacity int
}

type resolvedVADOptions struct {
	model        VADModel
	minSpeech    time.Duration
	minSilence   time.Duration
	prefix       time.Duration
	maxBuffered  time.Duration
	activation   float64
	deactivation float64
	capacity     int
}

func resolveVADOptions(options VADOptions) (resolvedVADOptions, error) {
	if options.Model == "" {
		options.Model = VADModelSilero
	}
	if options.Model != VADModelSilero {
		return resolvedVADOptions{}, fmt.Errorf("unknown VAD model %q", options.Model)
	}
	if options.MinimumSpeechDuration == 0 {
		options.MinimumSpeechDuration = defaultVADMinSpeech
	}
	if options.MinimumSilenceDuration == 0 {
		options.MinimumSilenceDuration = defaultVADMinSilence
	}
	if options.PrefixPaddingDuration == 0 {
		options.PrefixPaddingDuration = defaultVADPrefixPadding
	}
	if options.MaximumBufferedSpeech == 0 {
		options.MaximumBufferedSpeech = defaultVADMaxBufferedSpeech
	}
	if options.ActivationThreshold == 0 {
		options.ActivationThreshold = defaultVADActivation
	}
	deactivation := defaultVADDeactivation
	if options.ActivationThreshold != defaultVADActivation {
		deactivation = math.Max(options.ActivationThreshold-0.15, 0.01)
	}
	if options.DeactivationThreshold != nil {
		deactivation = *options.DeactivationThreshold
	}
	if options.MinimumSpeechDuration < 0 || options.MinimumSilenceDuration < 0 || options.PrefixPaddingDuration < 0 || options.MaximumBufferedSpeech <= 0 {
		return resolvedVADOptions{}, errors.New("invalid inference VAD durations")
	}
	if options.ActivationThreshold <= 0 || options.ActivationThreshold > 1 || deactivation <= 0 || deactivation > 1 {
		return resolvedVADOptions{}, errors.New("inference VAD thresholds must be in (0, 1]")
	}
	if options.StreamCapacity == 0 {
		options.StreamCapacity = defaultVADStreamCapacity
	}
	if options.StreamCapacity < 1 {
		return resolvedVADOptions{}, errors.New("inference VAD stream capacity must be positive")
	}
	return resolvedVADOptions{
		model: options.Model, minSpeech: options.MinimumSpeechDuration,
		minSilence: options.MinimumSilenceDuration, prefix: options.PrefixPaddingDuration,
		maxBuffered: options.MaximumBufferedSpeech, activation: options.ActivationThreshold,
		deactivation: deactivation, capacity: options.StreamCapacity,
	}, nil
}

// VAD implements vad.VAD through a lazily initialized local predictor.
type VAD struct {
	mu        sync.RWMutex
	opts      resolvedVADOptions
	factory   VADPredictorFactory
	executor  ipc.InferenceExecutor
	streams   map[*InferenceVADStream]struct{}
	closed    bool
	metrics   agents.EventEmitter[metrics.VAD]
	closeOnce sync.Once
	closeDone chan struct{}
}

// NewVAD validates options without loading the native model.
func NewVAD(options VADOptions) (*VAD, error) {
	if options.PredictorFactory != nil && ipc.IsInferenceExecutor(options.Executor) {
		return nil, errors.New("inference VAD accepts either PredictorFactory or Executor, not both")
	}
	resolved, err := resolveVADOptions(options)
	if err != nil {
		return nil, err
	}
	return &VAD{
		opts: resolved, factory: options.PredictorFactory, executor: options.Executor,
		streams: make(map[*InferenceVADStream]struct{}), closeDone: make(chan struct{}),
	}, nil
}

func (v *VAD) Label() string { return "inference.VAD" }
func (v *VAD) Model() string {
	v.mu.RLock()
	model := v.opts.model
	v.mu.RUnlock()
	return string(model)
}
func (v *VAD) Provider() string { return "livekit-local-inference" }
func (v *VAD) Capabilities() vadpkg.Capabilities {
	return vadpkg.Capabilities{UpdateInterval: defaultVADUpdateInterval}
}
func (v *VAD) MinSilenceDuration() (time.Duration, bool) {
	v.mu.RLock()
	value := v.opts.minSilence
	v.mu.RUnlock()
	return value, true
}
func (v *VAD) OnMetrics(fn func(metrics.VAD)) func() { return v.metrics.Subscribe(fn) }

// VADUpdateOptions is a sparse, concurrency-safe live update.
type VADUpdateOptions struct {
	MinimumSpeechDuration  *time.Duration
	MinimumSilenceDuration *time.Duration
	PrefixPaddingDuration  *time.Duration
	MaximumBufferedSpeech  *time.Duration
	ActivationThreshold    *float64
	DeactivationThreshold  *float64
}

func (v *VAD) UpdateOptions(update VADUpdateOptions) error {
	v.mu.Lock()
	if v.closed {
		v.mu.Unlock()
		return ErrVADClosed
	}
	next := v.opts
	if update.MinimumSpeechDuration != nil {
		next.minSpeech = *update.MinimumSpeechDuration
	}
	if update.MinimumSilenceDuration != nil {
		next.minSilence = *update.MinimumSilenceDuration
	}
	if update.PrefixPaddingDuration != nil {
		next.prefix = *update.PrefixPaddingDuration
	}
	if update.MaximumBufferedSpeech != nil {
		next.maxBuffered = *update.MaximumBufferedSpeech
	}
	if update.ActivationThreshold != nil {
		next.activation = *update.ActivationThreshold
	}
	if update.DeactivationThreshold != nil {
		next.deactivation = *update.DeactivationThreshold
	}
	if next.minSpeech < 0 || next.minSilence < 0 || next.prefix < 0 || next.maxBuffered <= 0 {
		v.mu.Unlock()
		return errors.New("invalid inference VAD durations")
	}
	if next.activation <= 0 || next.activation > 1 || next.deactivation <= 0 || next.deactivation > 1 {
		v.mu.Unlock()
		return errors.New("inference VAD thresholds must be in (0, 1]")
	}
	v.opts = next
	active := make([]*InferenceVADStream, 0, len(v.streams))
	for value := range v.streams {
		active = append(active, value)
	}
	v.mu.Unlock()
	for _, value := range active {
		value.setOptions(next)
	}
	return nil
}

func (v *VAD) Stream(parent context.Context) (vadpkg.VADStream, error) {
	if parent == nil {
		parent = context.Background()
	}
	v.mu.Lock()
	if v.closed {
		v.mu.Unlock()
		return nil, ErrVADClosed
	}
	opts := v.opts
	factory := v.factory
	if factory == nil {
		executor := v.executor
		if !ipc.IsInferenceExecutor(executor) {
			executor, _ = agents.InferenceExecutorFromContext(parent)
		}
		if ipc.IsInferenceExecutor(executor) {
			factory = executorVADPredictorFactory(executor)
		}
	}
	base := vadpkg.NewBaseStream(parent, opts.capacity)
	result := &InferenceVADStream{
		BaseStream: base, parent: v, opts: opts, factory: factory,
		done: make(chan struct{}),
	}
	v.streams[result] = struct{}{}
	v.mu.Unlock()
	if factory == nil {
		slog.Warn("inference.VAD created without a local predictor; stream will be a no-op")
	}
	go result.run()
	return result, nil
}

func (v *VAD) unregister(value *InferenceVADStream) {
	v.mu.Lock()
	delete(v.streams, value)
	v.mu.Unlock()
}

func (v *VAD) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	v.closeOnce.Do(func() {
		v.mu.Lock()
		v.closed = true
		active := make([]*InferenceVADStream, 0, len(v.streams))
		for value := range v.streams {
			active = append(active, value)
		}
		v.mu.Unlock()
		for _, value := range active {
			_ = value.Close()
		}
		go func() {
			for _, value := range active {
				<-value.done
			}
			close(v.closeDone)
		}()
	})
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-v.closeDone:
		return nil
	}
}

type sampleQueue struct {
	data []int16
	head int
}

func (q *sampleQueue) len() int               { return len(q.data) - q.head }
func (q *sampleQueue) append(values []int16)  { q.data = append(q.data, values...) }
func (q *sampleQueue) peek(count int) []int16 { return q.data[q.head : q.head+count] }
func (q *sampleQueue) consumeCopy(count int) []int16 {
	count = min(count, q.len())
	result := slices.Clone(q.data[q.head : q.head+count])
	q.discard(count)
	return result
}
func (q *sampleQueue) discard(count int) {
	q.head += min(count, q.len())
	if q.head == len(q.data) {
		q.data, q.head = q.data[:0], 0
	} else if q.head > 4096 && q.head*2 > len(q.data) {
		copy(q.data, q.data[q.head:])
		q.data = q.data[:len(q.data)-q.head]
		q.head = 0
	}
}

type vadAudioState struct {
	inputRate    int
	channels     int
	resampler    *linearMonoResampler
	input        sampleQueue
	model        sampleQueue
	copyFraction float64
}

func (a *vadAudioState) push(frame agents.AudioFrame) error {
	if frame.SampleRate <= 0 || frame.Channels <= 0 || len(frame.Data)%frame.Channels != 0 {
		return agents.ErrInvalidAudioFormat
	}
	if a.inputRate == 0 {
		a.inputRate, a.channels = frame.SampleRate, frame.Channels
		if a.inputRate != InferenceVADSampleRate {
			a.resampler = newLinearMonoResampler(a.inputRate, InferenceVADSampleRate)
		}
	} else if frame.SampleRate != a.inputRate || frame.Channels != a.channels {
		return fmt.Errorf("%w: VAD input format changed", agents.ErrInvalidAudioFormat)
	}
	mono := frame.Data
	if frame.Channels != 1 {
		mono = downmixMono(frame.Data, frame.Channels)
	}
	a.input.append(mono)
	if a.resampler == nil {
		a.model.append(mono)
	} else {
		a.model.append(a.resampler.push(mono, false))
	}
	return nil
}

func (a *vadAudioState) inputForWindow(windowSamples int) []int16 {
	exact := float64(windowSamples)*float64(a.inputRate)/InferenceVADSampleRate + a.copyFraction
	count := int(exact)
	a.copyFraction = exact - float64(count)
	return a.input.consumeCopy(count)
}
func (a *vadAudioState) reset() {
	inputRate, channels := a.inputRate, a.channels
	*a = vadAudioState{inputRate: inputRate, channels: channels}
	if inputRate != 0 && inputRate != InferenceVADSampleRate {
		a.resampler = newLinearMonoResampler(inputRate, InferenceVADSampleRate)
	}
}

type vadSpeechBuffer struct {
	data           []int16
	length         int
	prefix         int
	maxReached     bool
	inputRate      int
	maxDuration    time.Duration
	prefixDuration time.Duration
}

func (b *vadSpeechBuffer) ensure(options resolvedVADOptions, inputRate int) {
	if b.inputRate == inputRate && b.maxDuration == options.maxBuffered && b.prefixDuration == options.prefix {
		return
	}
	prefix := int(options.prefix * time.Duration(inputRate) / time.Second)
	capacity := int(options.maxBuffered*time.Duration(inputRate)/time.Second) + prefix
	resized := make([]int16, capacity)
	b.length = min(b.length, capacity)
	copy(resized, b.data[:b.length])
	b.data, b.prefix, b.inputRate = resized, prefix, inputRate
	b.maxDuration, b.prefixDuration = options.maxBuffered, options.prefix
	if b.length < capacity {
		b.maxReached = false
	}
}
func (b *vadSpeechBuffer) append(values []int16) {
	available := len(b.data) - b.length
	copyCount := min(available, len(values))
	copy(b.data[b.length:], values[:copyCount])
	b.length += copyCount
	if copyCount < len(values) {
		b.maxReached = true
	}
}
func (b *vadSpeechBuffer) slidePrefix() {
	if b.length <= b.prefix {
		return
	}
	copy(b.data, b.data[b.length-b.prefix:b.length])
	b.length = b.prefix
	b.maxReached = false
}
func (b *vadSpeechBuffer) snapshot() []int16 { return slices.Clone(b.data[:b.length]) }
func (b *vadSpeechBuffer) reset()            { b.length, b.maxReached = 0, false; clear(b.data) }

// InferenceVADStream performs bounded windowing and speech-state detection.
type InferenceVADStream struct {
	*vadpkg.BaseStream
	parent  *VAD
	factory VADPredictorFactory

	optsMu sync.RWMutex
	opts   resolvedVADOptions

	done      chan struct{}
	closeOnce sync.Once
}

func (s *InferenceVADStream) setOptions(options resolvedVADOptions) {
	s.optsMu.Lock()
	s.opts = options
	s.optsMu.Unlock()
}
func (s *InferenceVADStream) options() resolvedVADOptions {
	s.optsMu.RLock()
	value := s.opts
	s.optsMu.RUnlock()
	return value
}

func (s *InferenceVADStream) run() {
	defer close(s.done)
	defer s.parent.unregister(s)
	ctx := s.Context()
	var predictor VADPredictor
	predictorInitialized := false
	windowSamples := defaultVADWindowSamples
	var audio vadAudioState
	var speech vadSpeechBuffer
	var speaking bool
	var speechDuration, silenceDuration time.Duration
	var rawSpeech, rawSilence time.Duration
	var samplesIndex int64
	var metricsDuration time.Duration
	var metricsCount int64
	lastActivity := time.Now()

	reset := func() error {
		if predictor != nil {
			if err := predictor.Reset(); err != nil {
				return err
			}
		}
		audio.reset()
		speech.reset()
		speaking = false
		speechDuration, silenceDuration, rawSpeech, rawSilence = 0, 0, 0, 0
		samplesIndex = 0
		return nil
	}
	defer func() {
		if predictor != nil {
			_ = predictor.Close()
		}
	}()

	var terminal error
	defer func() { s.Finish(terminal) }()
	for {
		input, err := s.Inputs().Recv(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				terminal = nil
			} else {
				terminal = err
			}
			return
		}
		if input.Flush {
			if err := reset(); err != nil {
				terminal = err
				return
			}
			continue
		}
		if input.Frame == nil {
			continue
		}
		if err := audio.push(*input.Frame); err != nil {
			terminal = err
			return
		}
		options := s.options()
		speech.ensure(options, audio.inputRate)
		if !predictorInitialized && s.factory != nil {
			predictorInitialized = true
			predictor, err = s.factory(ctx)
			if err != nil {
				if !errors.Is(err, ErrLocalInferenceUnavailable) {
					terminal = fmt.Errorf("initialize inference VAD: %w", err)
					return
				}
				slog.Warn("local inference VAD is unavailable; stream will be a no-op")
				predictor = nil
			}
			if predictor != nil {
				windowSamples = predictor.WindowSamples()
				if windowSamples <= 0 {
					terminal = errors.New("inference VAD predictor returned an invalid window size")
					return
				}
			}
		}

		for audio.model.len() >= windowSamples {
			window := audio.model.peek(windowSamples)
			started := time.Now()
			probability := 0.0
			if predictor != nil {
				probability, err = predictor.Predict(ctx, window)
				if err != nil {
					terminal = fmt.Errorf("inference VAD prediction: %w", err)
					return
				}
				if math.IsNaN(probability) || math.IsInf(probability, 0) || probability < 0 || probability > 1 {
					terminal = fmt.Errorf("inference VAD predictor returned invalid probability %v", probability)
					return
				}
			}
			inferenceDuration := time.Since(started)
			if inferenceDuration > slowVADInferenceThreshold {
				slog.Warn("VAD slower than realtime", "duration", inferenceDuration)
			}
			audio.model.discard(windowSamples)
			inputChunk := audio.inputForWindow(windowSamples)
			speech.append(inputChunk)
			windowDuration := time.Duration(windowSamples) * time.Second / InferenceVADSampleRate
			samplesIndex += int64(windowSamples)
			if speaking {
				speechDuration += windowDuration
			} else {
				silenceDuration += windowDuration
			}
			frame, _ := agents.NewAudioFrame(inputChunk, audio.inputRate, 1)
			now := time.Now()
			inferenceEvent := vadpkg.Event{
				Type: vadpkg.InferenceDone, SamplesIndex: samplesIndex, Timestamp: now,
				SpeechDuration: speechDuration, SilenceDuration: silenceDuration,
				Frames: []agents.AudioFrame{frame}, Probability: probability,
				InferenceDuration: inferenceDuration, Speaking: speaking,
				RawAccumulatedSilence: rawSilence, RawAccumulatedSpeech: rawSpeech,
			}
			if err := s.Emit(ctx, inferenceEvent); err != nil {
				terminal = err
				return
			}
			metricsDuration += inferenceDuration
			metricsCount++
			if metricsCount >= int64(math.Ceil(float64(time.Second)/float64(defaultVADUpdateInterval))) {
				s.parent.metrics.Emit(metrics.VAD{
					Label: s.parent.Label(), Timestamp: now, IdleTime: time.Since(lastActivity),
					InferenceDurationTotal: metricsDuration, InferenceCount: metricsCount,
					Metadata: metrics.Metadata{ModelProvider: s.parent.Provider(), ModelName: s.parent.Model()},
				})
				metricsDuration, metricsCount = 0, 0
			}

			active := probability >= options.activation || speaking && probability > options.deactivation
			if active {
				rawSpeech += windowDuration
				rawSilence = 0
				if !speaking && rawSpeech >= options.minSpeech {
					speaking, silenceDuration, speechDuration = true, 0, rawSpeech
					buffer, _ := agents.NewAudioFrame(speech.snapshot(), audio.inputRate, 1)
					event := vadpkg.Event{
						Type: vadpkg.StartOfSpeech, SamplesIndex: samplesIndex, Timestamp: now,
						SpeechDuration: speechDuration, Probability: probability,
						InferenceDuration: inferenceDuration, Frames: []agents.AudioFrame{buffer}, Speaking: true,
					}
					if err := s.Emit(ctx, event); err != nil {
						terminal = err
						return
					}
					lastActivity = now
				}
			} else {
				rawSilence += windowDuration
				rawSpeech = 0
				if !speaking {
					speech.slidePrefix()
				}
				if speaking && rawSilence >= options.minSilence {
					speaking, silenceDuration = false, rawSilence
					buffer, _ := agents.NewAudioFrame(speech.snapshot(), audio.inputRate, 1)
					event := vadpkg.Event{
						Type: vadpkg.EndOfSpeech, SamplesIndex: samplesIndex, Timestamp: now,
						SpeechDuration: max(time.Duration(0), speechDuration-rawSilence), SilenceDuration: silenceDuration,
						Probability: probability, InferenceDuration: inferenceDuration,
						Frames: []agents.AudioFrame{buffer}, Speaking: false,
					}
					if err := s.Emit(ctx, event); err != nil {
						terminal = err
						return
					}
					speechDuration = 0
					speech.slidePrefix()
					lastActivity = now
				}
			}
		}
	}
}

func (s *InferenceVADStream) Close() error {
	s.closeOnce.Do(func() {
		_ = s.BaseStream.Close()
	})
	return nil
}
func (s *InferenceVADStream) Wait(ctx context.Context) error {
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

var _ vadpkg.VAD = (*VAD)(nil)
var _ vadpkg.VADStream = (*InferenceVADStream)(nil)
