// SPDX-License-Identifier: Apache-2.0

package tts

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/metrics"
	"github.com/livekit/agents-go/stream"
	"github.com/livekit/agents-go/tokenize"
)

const (
	defaultTTSFallbackRecoveryDelay     = time.Second
	defaultTTSFallbackStreamCapacity    = 32
	defaultTTSFallbackBufferedTextBytes = 1 << 20
	ttsRecoveryText                     = "Hello world, this is a recovery test."
)

// ErrFallbackBufferLimit means streamed input could not be retained safely for
// a possible failover. The stream terminates instead of growing without bound.
var ErrFallbackBufferLimit = errors.New("TTS fallback replay buffer limit exceeded")

// AvailabilityChangedEvent reports a TTS provider entering or leaving the
// fallback rotation.
type AvailabilityChangedEvent struct {
	TTS       TTS
	Available bool
}

// FallbackStatus is an immutable provider-health snapshot.
type FallbackStatus struct {
	Available  bool
	Recovering bool
}

// FallbackOptions configures ordered TTS failover. MaxRetriesPerTTS defaults
// to two; use a negative value to disable child retries explicitly.
type FallbackOptions struct {
	TTSs []TTS
	// TTSInstances is the agents-js-compatible spelling. Set only one of
	// TTSs or TTSInstances.
	TTSInstances     []TTS
	MaxRetriesPerTTS int
	// MaxRetryPerTTS is the agents-js-compatible spelling. Non-zero values
	// override MaxRetriesPerTTS.
	MaxRetryPerTTS   int
	RecoveryDelay    time.Duration
	RetryOnChunkSent bool
	// RetryOnChunk is a concise compatibility alias for RetryOnChunkSent.
	RetryOnChunk         bool
	StreamCapacity       int
	MaxBufferedTextBytes int
	KeepProvidersOpen    bool
}

type fallbackStatus struct {
	available  bool
	recovering bool
}

// FallbackAdapter provides ordered TTS failover, background recovery,
// automatic streaming wrappers, sample-rate normalization, and dynamic model
// attribution.
type FallbackAdapter struct {
	base *Base

	tts              []TTS
	streaming        []TTS
	ownedWrappers    []*StreamAdapter
	maxRetries       int
	recoveryDelay    time.Duration
	retryOnChunkSent bool
	streamCapacity   int
	maxBufferedBytes int
	closeProviders   bool

	ctx    context.Context
	cancel context.CancelCauseFunc

	mu            sync.RWMutex
	status        []fallbackStatus
	active        int
	closed        bool
	chunks        map[*fallbackChunkedStream]struct{}
	streams       map[*fallbackSynthesizeStream]struct{}
	unsubscribers []func()
	recoveryWG    sync.WaitGroup
	availability  agents.EventEmitter[AvailabilityChangedEvent]
	closeOnce     sync.Once
	closeDone     chan struct{}
	closeErr      error
}

func NewFallbackAdapter(options FallbackOptions) (*FallbackAdapter, error) {
	if len(options.TTSs) != 0 && len(options.TTSInstances) != 0 {
		return nil, errors.New("set only one of TTSs or TTSInstances")
	}
	if len(options.TTSs) == 0 {
		options.TTSs = options.TTSInstances
	}
	if len(options.TTSs) == 0 {
		return nil, errors.New("at least one TTS instance must be provided")
	}
	if options.RecoveryDelay < 0 {
		return nil, errors.New("TTS fallback recovery delay must not be negative")
	}
	if options.TTSs[0] == nil {
		return nil, errors.New("TTS fallback provider 0 is nil")
	}
	channels := options.TTSs[0].Channels()
	sampleRate := 0
	for i, provider := range options.TTSs {
		if provider == nil {
			return nil, fmt.Errorf("TTS fallback provider %d is nil", i)
		}
		if provider.Channels() != channels {
			return nil, errors.New("all TTS instances must have the same number of channels")
		}
		if provider.SampleRate() <= 0 || provider.Channels() <= 0 {
			return nil, fmt.Errorf("TTS fallback provider %s has an invalid audio format", provider.Label())
		}
		if provider.SampleRate() > sampleRate {
			sampleRate = provider.SampleRate()
		}
	}
	if options.RecoveryDelay == 0 {
		options.RecoveryDelay = defaultTTSFallbackRecoveryDelay
	}
	if options.StreamCapacity <= 0 {
		options.StreamCapacity = defaultTTSFallbackStreamCapacity
	}
	if options.MaxBufferedTextBytes <= 0 {
		options.MaxBufferedTextBytes = defaultTTSFallbackBufferedTextBytes
	}
	maxRetries := options.MaxRetriesPerTTS
	if options.MaxRetryPerTTS != 0 {
		maxRetries = options.MaxRetryPerTTS
	}
	if maxRetries == 0 {
		maxRetries = 2
	} else if maxRetries < 0 {
		maxRetries = 0
	}

	streaming := make([]TTS, len(options.TTSs))
	var wrappers []*StreamAdapter
	for i, provider := range options.TTSs {
		streaming[i] = provider
		if provider.Capabilities().Streaming {
			continue
		}
		wrapper, err := NewStreamAdapter(
			provider,
			tokenize.NewSentenceTokenizer(tokenize.SentenceOptions{RetainFormat: true}),
			StreamAdapterOptions{StreamCapacity: options.StreamCapacity},
		)
		if err != nil {
			for _, value := range wrappers {
				_ = value.Close(context.Background())
			}
			return nil, err
		}
		streaming[i] = wrapper
		wrappers = append(wrappers, wrapper)
	}

	ctx, cancel := context.WithCancelCause(context.Background())
	a := &FallbackAdapter{
		base: NewBase("tts.FallbackAdapter", "livekit", "FallbackAdapter", sampleRate, channels,
			aggregateTTSCapabilities(options.TTSs)),
		tts: append([]TTS(nil), options.TTSs...), streaming: streaming, ownedWrappers: wrappers,
		maxRetries: maxRetries, recoveryDelay: options.RecoveryDelay,
		retryOnChunkSent: options.RetryOnChunkSent || options.RetryOnChunk, streamCapacity: options.StreamCapacity,
		maxBufferedBytes: options.MaxBufferedTextBytes, closeProviders: !options.KeepProvidersOpen,
		ctx: ctx, cancel: cancel, active: -1, status: make([]fallbackStatus, len(options.TTSs)),
		chunks: make(map[*fallbackChunkedStream]struct{}), streams: make(map[*fallbackSynthesizeStream]struct{}),
		closeDone: make(chan struct{}),
	}
	for i := range a.status {
		a.status[i].available = true
	}
	for _, provider := range a.tts {
		provider := provider
		a.unsubscribers = append(a.unsubscribers,
			provider.OnMetrics(func(metric metrics.TTS) { a.base.EmitMetrics(metric) }),
			provider.OnError(func(event ErrorEvent) { a.base.EmitError(event) }),
		)
	}
	return a, nil
}

func aggregateTTSCapabilities(providers []TTS) Capabilities {
	var caps Capabilities
	caps.AlignedTranscript = true
	for _, provider := range providers {
		child := provider.Capabilities()
		caps.Streaming = caps.Streaming || child.Streaming
		caps.AlignedTranscript = caps.AlignedTranscript && child.AlignedTranscript
	}
	return caps
}

func (a *FallbackAdapter) Label() string              { return a.base.Label() }
func (a *FallbackAdapter) SampleRate() int            { return a.base.SampleRate() }
func (a *FallbackAdapter) Channels() int              { return a.base.Channels() }
func (a *FallbackAdapter) Capabilities() Capabilities { return a.base.Capabilities() }

func (a *FallbackAdapter) Provider() string {
	a.mu.RLock()
	active := a.active
	a.mu.RUnlock()
	if active >= 0 {
		return a.tts[active].Provider()
	}
	return "livekit"
}

func (a *FallbackAdapter) Model() string {
	a.mu.RLock()
	active := a.active
	a.mu.RUnlock()
	if active >= 0 {
		return a.tts[active].Model()
	}
	return "FallbackAdapter"
}

func (a *FallbackAdapter) TTSs() []TTS                           { return append([]TTS(nil), a.tts...) }
func (a *FallbackAdapter) Instances() []TTS                      { return a.TTSs() }
func (a *FallbackAdapter) MaxRetriesPerTTS() int                 { return a.maxRetries }
func (a *FallbackAdapter) RecoveryDelay() time.Duration          { return a.recoveryDelay }
func (a *FallbackAdapter) RetryOnChunkSent() bool                { return a.retryOnChunkSent }
func (a *FallbackAdapter) OnMetrics(fn func(metrics.TTS)) func() { return a.base.OnMetrics(fn) }
func (a *FallbackAdapter) OnError(fn func(ErrorEvent)) func()    { return a.base.OnError(fn) }
func (a *FallbackAdapter) OnAvailabilityChanged(fn func(AvailabilityChangedEvent)) func() {
	return a.availability.Subscribe(fn)
}

func (a *FallbackAdapter) Status() []FallbackStatus {
	a.mu.RLock()
	result := make([]FallbackStatus, len(a.status))
	for i := range a.status {
		result[i] = FallbackStatus{Available: a.status[i].available, Recovering: a.status[i].recovering}
	}
	a.mu.RUnlock()
	return result
}

func (a *FallbackAdapter) Availability() []bool {
	status := a.Status()
	result := make([]bool, len(status))
	for i := range status {
		result[i] = status[i].Available
	}
	return result
}

func (a *FallbackAdapter) setActive(index int) {
	a.mu.Lock()
	if !a.closed {
		a.active = index
	}
	a.mu.Unlock()
}

func (a *FallbackAdapter) isAvailable(index int) bool {
	a.mu.RLock()
	available := a.status[index].available
	a.mu.RUnlock()
	return available
}

func (a *FallbackAdapter) allUnavailable() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	for i := range a.status {
		if a.status[i].available {
			return false
		}
	}
	return true
}

func (a *FallbackAdapter) markUnavailable(index int) {
	var emit bool
	a.mu.Lock()
	if !a.closed && a.status[index].available {
		a.status[index].available = false
		emit = true
	}
	provider := a.tts[index]
	a.mu.Unlock()
	if emit {
		a.availability.Emit(AvailabilityChangedEvent{TTS: provider, Available: false})
	}
	a.startRecovery(index)
}

func (a *FallbackAdapter) startRecovery(index int) {
	a.mu.Lock()
	if a.closed || a.status[index].recovering {
		a.mu.Unlock()
		return
	}
	a.status[index].recovering = true
	a.recoveryWG.Add(1)
	a.mu.Unlock()
	go a.recoveryLoop(index)
}

func (a *FallbackAdapter) recoveryLoop(index int) {
	defer a.recoveryWG.Done()
	recovered := false
	for {
		if err := a.probe(index); err == nil {
			recovered = true
			break
		}
		timer := time.NewTimer(a.recoveryDelay)
		select {
		case <-a.ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			goto done
		case <-timer.C:
		}
	}
done:
	var emit bool
	a.mu.Lock()
	a.status[index].recovering = false
	if recovered && !a.closed && !a.status[index].available {
		a.status[index].available = true
		emit = true
	}
	provider := a.tts[index]
	a.mu.Unlock()
	if emit {
		a.availability.Emit(AvailabilityChangedEvent{TTS: provider, Available: true})
	}
}

func (a *FallbackAdapter) probe(index int) error {
	ctx, cancel := context.WithTimeoutCause(a.ctx, 10*time.Second,
		agents.NewAPITimeoutError("TTS fallback recovery timed out", true, context.DeadlineExceeded))
	defer cancel()
	options := SynthesizeOptions{ConnectOptions: agents.APIConnectOptions{
		MaxRetries: -1, Timeout: 10 * time.Second, RetryInterval: time.Second,
	}}
	child, err := a.tts[index].Synthesize(ctx, ttsRecoveryText, options)
	if err == nil {
		defer child.Close()
		received := false
		for {
			audio, recvErr := child.Recv(ctx)
			if recvErr != nil {
				if errors.Is(recvErr, io.EOF) {
					if received {
						return nil
					}
					return errors.New("TTS recovery completed without audio")
				}
				return recvErr
			}
			if len(audio.Frame.Data) != 0 {
				received = true
			}
		}
	}
	if !a.streaming[index].Capabilities().Streaming {
		return err
	}
	streamed, streamErr := a.streaming[index].Stream(ctx, StreamOptions(options))
	if streamErr != nil {
		return errors.Join(err, streamErr)
	}
	defer streamed.Close()
	if streamErr = streamed.PushText(ctx, ttsRecoveryText); streamErr != nil {
		return streamErr
	}
	if streamErr = streamed.EndInput(); streamErr != nil {
		return streamErr
	}
	received := false
	for {
		audio, recvErr := streamed.Recv(ctx)
		if recvErr != nil {
			if errors.Is(recvErr, io.EOF) {
				if received {
					return nil
				}
				return errors.New("TTS streaming recovery completed without audio")
			}
			return recvErr
		}
		if len(audio.Frame.Data) != 0 {
			received = true
		}
	}
}

func (a *FallbackAdapter) childConnectOptions(input agents.APIConnectOptions) agents.APIConnectOptions {
	input.MaxRetries = a.maxRetries
	if a.maxRetries == 0 {
		input.MaxRetries = -1
	}
	return input
}

func (a *FallbackAdapter) Synthesize(parent context.Context, text string, options SynthesizeOptions) (ChunkedStream, error) {
	if parent == nil {
		parent = context.Background()
	}
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return nil, stream.ErrClosed
	}
	value := newFallbackChunkedStream(parent, a, text, options)
	a.chunks[value] = struct{}{}
	a.mu.Unlock()
	value.unregister = func() {
		a.mu.Lock()
		delete(a.chunks, value)
		a.mu.Unlock()
	}
	go value.run()
	return value, nil
}

func (a *FallbackAdapter) Stream(parent context.Context, options StreamOptions) (SynthesizeStream, error) {
	if parent == nil {
		parent = context.Background()
	}
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return nil, stream.ErrClosed
	}
	value := newFallbackSynthesizeStream(parent, a, options)
	a.streams[value] = struct{}{}
	a.mu.Unlock()
	value.unregister = func() {
		a.mu.Lock()
		delete(a.streams, value)
		a.mu.Unlock()
	}
	go value.run()
	return value, nil
}

func (a *FallbackAdapter) allFailedError(cause error) error {
	labels := make([]string, len(a.tts))
	for i := range a.tts {
		labels[i] = a.tts[i].Label()
	}
	return agents.NewAPIConnectionError(
		fmt.Sprintf("all TTS instances failed (%s)", strings.Join(labels, ", ")), false, cause,
	)
}

func (a *FallbackAdapter) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	a.closeOnce.Do(func() {
		a.mu.Lock()
		a.closed = true
		unsubscribers := a.unsubscribers
		a.unsubscribers = nil
		chunks := make([]*fallbackChunkedStream, 0, len(a.chunks))
		for value := range a.chunks {
			chunks = append(chunks, value)
		}
		streams := make([]*fallbackSynthesizeStream, 0, len(a.streams))
		for value := range a.streams {
			streams = append(streams, value)
		}
		a.mu.Unlock()
		a.cancel(stream.ErrClosed)
		for _, unsubscribe := range unsubscribers {
			unsubscribe()
		}
		for _, value := range chunks {
			_ = value.Close()
		}
		for _, value := range streams {
			_ = value.Close()
		}
		go a.finishClose(chunks, streams)
	})
	select {
	case <-a.closeDone:
		a.mu.RLock()
		err := a.closeErr
		a.mu.RUnlock()
		return err
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (a *FallbackAdapter) finishClose(chunks []*fallbackChunkedStream, streams []*fallbackSynthesizeStream) {
	for _, value := range chunks {
		<-value.done
	}
	for _, value := range streams {
		<-value.done
	}
	a.recoveryWG.Wait()
	var result error
	for _, wrapper := range a.ownedWrappers {
		result = errors.Join(result, wrapper.Close(context.Background()))
	}
	if a.closeProviders {
		for _, provider := range a.tts {
			result = errors.Join(result, provider.Close(context.Background()))
		}
	}
	a.mu.Lock()
	a.closeErr = result
	close(a.closeDone)
	a.mu.Unlock()
}

// pcm16Resampler is a stateful linear PCM16 resampler. It uses integer phase
// arithmetic, retains only samples needed by the next interpolation, and
// allocates at most one output frame per input frame.
type pcm16Resampler struct {
	inRate, outRate int64
	channels        int
	base, totalIn   int64
	nextOut         int64
	buffer          []int16
}

func newPCM16Resampler(inRate, outRate, channels int) *pcm16Resampler {
	if inRate == outRate {
		return nil
	}
	return &pcm16Resampler{inRate: int64(inRate), outRate: int64(outRate), channels: channels}
}

func (r *pcm16Resampler) push(frame agents.AudioFrame) (agents.AudioFrame, bool, error) {
	if frame.SampleRate != int(r.inRate) || frame.Channels != r.channels || len(frame.Data)%r.channels != 0 {
		return agents.AudioFrame{}, false, fmt.Errorf("%w: resampler input format", agents.ErrInvalidAudioFormat)
	}
	if len(frame.Data) == 0 {
		return agents.AudioFrame{}, false, nil
	}
	r.buffer = append(r.buffer, frame.Data...)
	r.totalIn += int64(len(frame.Data) / r.channels)
	out := r.render(false)
	if len(out) == 0 {
		return agents.AudioFrame{}, false, nil
	}
	value, _ := agents.NewAudioFrame(out, int(r.outRate), r.channels)
	value.UserData = frame.UserData
	return value, true, nil
}

func (r *pcm16Resampler) flush() (agents.AudioFrame, bool) {
	out := r.render(true)
	if len(out) == 0 {
		return agents.AudioFrame{}, false
	}
	value, _ := agents.NewAudioFrame(out, int(r.outRate), r.channels)
	return value, true
}

func (r *pcm16Resampler) render(flush bool) []int16 {
	estimated := int((r.totalIn*r.outRate)/r.inRate - r.nextOut)
	if estimated < 0 {
		estimated = 0
	}
	out := make([]int16, 0, estimated*r.channels)
	limit := int64(-1)
	if flush {
		limit = (r.totalIn*r.outRate + r.inRate/2) / r.inRate
	}
	for {
		if flush && r.nextOut >= limit {
			break
		}
		position := r.nextOut * r.inRate
		left := position / r.outRate
		fraction := position % r.outRate
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
		li := int(left-r.base) * r.channels
		ri := int(right-r.base) * r.channels
		if li < 0 || ri+r.channels > len(r.buffer) {
			break
		}
		for channel := 0; channel < r.channels; channel++ {
			leftValue := int64(r.buffer[li+channel])
			rightValue := int64(r.buffer[ri+channel])
			value := (leftValue*(r.outRate-fraction) + rightValue*fraction) / r.outRate
			out = append(out, int16(value))
		}
		r.nextOut++
	}
	needed := (r.nextOut * r.inRate) / r.outRate
	if needed > r.base {
		drop := needed - r.base
		available := int64(len(r.buffer) / r.channels)
		if drop > available {
			drop = available
		}
		copy(r.buffer, r.buffer[int(drop)*r.channels:])
		r.buffer = r.buffer[:len(r.buffer)-int(drop)*r.channels]
		r.base += drop
	}
	if flush {
		r.buffer = r.buffer[:0]
		r.base = r.totalIn
	}
	return out
}

type fallbackChunkedStream struct {
	*BaseChunkedStream
	adapter *FallbackAdapter
	text    string
	options SynthesizeOptions
	ctx     context.Context
	cancel  context.CancelCauseFunc
	stop    func() bool
	done    chan struct{}

	mu         sync.Mutex
	current    ChunkedStream
	closeOnce  sync.Once
	unregister func()
}

func newFallbackChunkedStream(parent context.Context, adapter *FallbackAdapter, text string, options SynthesizeOptions) *fallbackChunkedStream {
	ctx, cancel := context.WithCancelCause(parent)
	s := &fallbackChunkedStream{
		BaseChunkedStream: NewBaseChunkedStream(ctx, adapter.streamCapacity),
		adapter:           adapter, text: text, options: options, ctx: ctx, cancel: cancel,
		done: make(chan struct{}),
	}
	s.stop = context.AfterFunc(adapter.ctx, func() { _ = s.Close() })
	return s
}

func (s *fallbackChunkedStream) Close() error {
	s.closeOnce.Do(func() {
		s.cancel(stream.ErrClosed)
		s.mu.Lock()
		current := s.current
		s.mu.Unlock()
		if current != nil {
			_ = current.Close()
		}
		_ = s.BaseChunkedStream.Close()
	})
	return nil
}

func (s *fallbackChunkedStream) Wait(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (s *fallbackChunkedStream) run() {
	defer close(s.done)
	defer s.unregister()
	defer func() {
		if s.stop != nil {
			s.stop()
		}
	}()
	allFailed := s.adapter.allUnavailable()
	var lastErr error
	for index, provider := range s.adapter.tts {
		if cause := context.Cause(s.ctx); cause != nil {
			lastErr = cause
			break
		}
		if !allFailed && !s.adapter.isAvailable(index) {
			continue
		}
		visible, err := s.tryProvider(index, provider)
		if err == nil {
			s.BaseChunkedStream.Finish(nil)
			return
		}
		if cause := context.Cause(s.ctx); cause != nil {
			lastErr = cause
			break
		}
		lastErr = err
		s.adapter.markUnavailable(index)
		if visible && !s.adapter.retryOnChunkSent {
			s.BaseChunkedStream.Finish(err)
			return
		}
	}
	if cause := context.Cause(s.ctx); cause != nil {
		if errors.Is(cause, stream.ErrClosed) || errors.Is(cause, io.EOF) {
			s.BaseChunkedStream.Finish(nil)
		} else {
			s.BaseChunkedStream.Finish(cause)
		}
		return
	}
	if errors.Is(lastErr, stream.ErrClosed) || errors.Is(lastErr, context.Canceled) {
		s.BaseChunkedStream.Finish(nil)
		return
	}
	err := s.adapter.allFailedError(lastErr)
	s.adapter.base.EmitError(ErrorEvent{Timestamp: time.Now(), Label: s.adapter.Label(), Err: err, Recoverable: false})
	s.BaseChunkedStream.Finish(err)
}

func (s *fallbackChunkedStream) tryProvider(index int, provider TTS) (bool, error) {
	options := s.options
	options.ConnectOptions = s.adapter.childConnectOptions(options.ConnectOptions)
	child, err := provider.Synthesize(s.ctx, s.text, options)
	if err != nil {
		return false, err
	}
	s.mu.Lock()
	s.current = child
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		if s.current == child {
			s.current = nil
		}
		s.mu.Unlock()
		_ = child.Close()
	}()

	resampler := newPCM16Resampler(provider.SampleRate(), s.adapter.SampleRate(), s.adapter.Channels())
	visible, sawRaw := false, false
	var last SynthesizedAudio
	for {
		audio, recvErr := child.Recv(s.ctx)
		if errors.Is(recvErr, io.EOF) {
			if !sawRaw {
				return visible, agents.NewAPIConnectionError("TTS synthesis completed without audio", true, nil)
			}
			if resampler != nil {
				if frame, ok := resampler.flush(); ok {
					last.Frame = frame
					last.Final = true
					last.TimedTranscripts = nil
					if err := s.Emit(s.ctx, last); err != nil {
						return visible, err
					}
					visible = true
				}
			}
			s.adapter.setActive(index)
			return visible, nil
		}
		if recvErr != nil {
			return visible, recvErr
		}
		if len(audio.Frame.Data) == 0 {
			continue
		}
		sawRaw = true
		last = audio
		if resampler != nil {
			frame, ok, resampleErr := resampler.push(audio.Frame)
			if resampleErr != nil {
				return visible, resampleErr
			}
			if !ok {
				continue
			}
			audio.Frame = frame
		}
		if err := s.Emit(s.ctx, audio); err != nil {
			return visible, err
		}
		visible = true
		s.adapter.setActive(index)
	}
}

type ttsLoggedInput struct {
	value StreamInput
}

type ttsReplayLog struct {
	mu           sync.Mutex
	items        []ttsLoggedInput
	base         uint64
	next         uint64
	bytes        int
	maxBytes     int
	maxItems     int
	closed       bool
	err          error
	discard      bool
	acknowledged uint64
	changed      chan struct{}
}

func newTTSReplayLog(capacity, maxBytes int) *ttsReplayLog {
	maxItems := capacity * 32
	if maxItems < 64 {
		maxItems = 64
	}
	return &ttsReplayLog{maxBytes: maxBytes, maxItems: maxItems, changed: make(chan struct{}, 1)}
}

func (b *ttsReplayLog) notifyLocked() {
	select {
	case b.changed <- struct{}{}:
	default:
	}
}

func (b *ttsReplayLog) append(value StreamInput) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		if b.err != nil {
			return b.err
		}
		return stream.ErrClosed
	}
	if len(b.items) >= b.maxItems || len(value.Text) > b.maxBytes-b.bytes {
		return ErrFallbackBufferLimit
	}
	b.items = append(b.items, ttsLoggedInput{value: value})
	b.next++
	b.bytes += len(value.Text)
	b.notifyLocked()
	return nil
}

func (b *ttsReplayLog) firstSequence() uint64 {
	b.mu.Lock()
	value := b.base
	b.mu.Unlock()
	return value
}

func (b *ttsReplayLog) nextValue(ctx context.Context, cursor uint64) (StreamInput, uint64, error) {
	for {
		b.mu.Lock()
		if cursor < b.base {
			cursor = b.base
		}
		if cursor < b.next {
			index := int(cursor - b.base)
			if index >= 0 && index < len(b.items) {
				value := b.items[index].value
				b.mu.Unlock()
				return value, cursor + 1, nil
			}
		}
		if b.closed {
			err := b.err
			b.mu.Unlock()
			if err != nil {
				return StreamInput{}, cursor, err
			}
			return StreamInput{}, cursor, io.EOF
		}
		changed := b.changed
		b.mu.Unlock()
		select {
		case <-ctx.Done():
			return StreamInput{}, cursor, context.Cause(ctx)
		case <-changed:
		}
	}
}

func (b *ttsReplayLog) acknowledge(cursor uint64) {
	b.mu.Lock()
	if cursor > b.acknowledged {
		b.acknowledged = cursor
	}
	if b.discard {
		b.pruneLocked()
	}
	b.mu.Unlock()
}

func (b *ttsReplayLog) enableDiscard() {
	b.mu.Lock()
	b.discard = true
	b.pruneLocked()
	b.mu.Unlock()
}

func (b *ttsReplayLog) pruneLocked() {
	if b.acknowledged <= b.base {
		return
	}
	count := int(b.acknowledged - b.base)
	if count > len(b.items) {
		count = len(b.items)
	}
	for i := 0; i < count; i++ {
		b.bytes -= len(b.items[i].value.Text)
		b.items[i] = ttsLoggedInput{}
	}
	copy(b.items, b.items[count:])
	newLength := len(b.items) - count
	clear(b.items[newLength:])
	b.items = b.items[:newLength]
	b.base += uint64(count)
	b.notifyLocked()
}

func (b *ttsReplayLog) close(err error) {
	b.mu.Lock()
	if !b.closed {
		b.closed = true
		b.err = err
		b.notifyLocked()
	}
	b.mu.Unlock()
}

type fallbackSynthesizeStream struct {
	*BaseSynthesizeStream
	adapter *FallbackAdapter
	options StreamOptions
	ctx     context.Context
	cancel  context.CancelCauseFunc
	stop    func() bool
	log     *ttsReplayLog
	done    chan struct{}

	mu         sync.Mutex
	current    SynthesizeStream
	closeOnce  sync.Once
	unregister func()
}

func newFallbackSynthesizeStream(parent context.Context, adapter *FallbackAdapter, options StreamOptions) *fallbackSynthesizeStream {
	ctx, cancel := context.WithCancelCause(parent)
	s := &fallbackSynthesizeStream{
		BaseSynthesizeStream: NewBaseSynthesizeStream(ctx, adapter.streamCapacity),
		adapter:              adapter, options: options, ctx: ctx, cancel: cancel,
		log: newTTSReplayLog(adapter.streamCapacity, adapter.maxBufferedBytes), done: make(chan struct{}),
	}
	s.stop = context.AfterFunc(adapter.ctx, func() { _ = s.Close() })
	return s
}

func (s *fallbackSynthesizeStream) Close() error {
	s.closeOnce.Do(func() {
		s.cancel(stream.ErrClosed)
		s.log.close(stream.ErrClosed)
		s.mu.Lock()
		current := s.current
		s.mu.Unlock()
		if current != nil {
			_ = current.Close()
		}
		_ = s.BaseSynthesizeStream.Close()
	})
	return nil
}

func (s *fallbackSynthesizeStream) Wait(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (s *fallbackSynthesizeStream) run() {
	defer close(s.done)
	defer s.unregister()
	defer func() {
		if s.stop != nil {
			s.stop()
		}
	}()
	inputDone := make(chan error, 1)
	go func() { inputDone <- s.pumpInput() }()
	allFailed := s.adapter.allUnavailable()
	var lastErr error
	for index, provider := range s.adapter.streaming {
		if cause := context.Cause(s.ctx); cause != nil {
			lastErr = cause
			break
		}
		if !allFailed && !s.adapter.isAvailable(index) {
			continue
		}
		visible, err := s.tryProvider(index, provider)
		if err == nil {
			s.cancel(io.EOF)
			s.log.close(nil)
			<-inputDone
			s.BaseSynthesizeStream.Finish(nil)
			return
		}
		if cause := context.Cause(s.ctx); cause != nil {
			lastErr = cause
			break
		}
		lastErr = err
		s.adapter.markUnavailable(index)
		if visible && !s.adapter.retryOnChunkSent {
			s.cancel(err)
			s.log.close(err)
			<-inputDone
			s.BaseSynthesizeStream.Finish(err)
			return
		}
	}
	if cause := context.Cause(s.ctx); cause != nil {
		s.log.close(cause)
		<-inputDone
		if errors.Is(cause, stream.ErrClosed) || errors.Is(cause, io.EOF) {
			s.BaseSynthesizeStream.Finish(nil)
		} else {
			s.BaseSynthesizeStream.Finish(cause)
		}
		return
	}
	s.cancel(lastErr)
	s.log.close(lastErr)
	<-inputDone
	if errors.Is(lastErr, stream.ErrClosed) || errors.Is(lastErr, context.Canceled) {
		s.BaseSynthesizeStream.Finish(nil)
		return
	}
	err := s.adapter.allFailedError(lastErr)
	s.adapter.base.EmitError(ErrorEvent{Timestamp: time.Now(), Label: s.adapter.Label(), Err: err, Recoverable: false})
	s.BaseSynthesizeStream.Finish(err)
}

func (s *fallbackSynthesizeStream) pumpInput() error {
	for {
		input, err := s.Inputs().Recv(s.ctx)
		if errors.Is(err, io.EOF) {
			s.log.close(nil)
			return nil
		}
		if err != nil {
			s.log.close(err)
			return err
		}
		if err := s.log.append(input); err != nil {
			s.cancel(err)
			s.log.close(err)
			s.mu.Lock()
			current := s.current
			s.mu.Unlock()
			if current != nil {
				_ = current.Close()
			}
			return err
		}
	}
}

func (s *fallbackSynthesizeStream) tryProvider(index int, provider TTS) (bool, error) {
	options := s.options
	options.ConnectOptions = s.adapter.childConnectOptions(options.ConnectOptions)
	child, err := provider.Stream(s.ctx, options)
	if err != nil {
		return false, err
	}
	s.mu.Lock()
	s.current = child
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		if s.current == child {
			s.current = nil
		}
		s.mu.Unlock()
		_ = child.Close()
	}()

	attemptCtx, cancel := context.WithCancelCause(s.ctx)
	forwardDone := make(chan error, 1)
	go func() {
		err := s.forwardTo(attemptCtx, child)
		forwardDone <- err
		if err != nil && !errors.Is(err, io.EOF) && context.Cause(attemptCtx) == nil {
			_ = child.Close()
		}
	}()

	resampler := newPCM16Resampler(s.adapter.tts[index].SampleRate(), s.adapter.SampleRate(), s.adapter.Channels())
	visible, sawRaw := false, false
	var last SynthesizedAudio
	var recvErr error
	for {
		audio, err := child.Recv(attemptCtx)
		if err != nil {
			recvErr = err
			break
		}
		if len(audio.Frame.Data) == 0 {
			continue
		}
		sawRaw = true
		last = audio
		if resampler != nil {
			frame, ok, resampleErr := resampler.push(audio.Frame)
			if resampleErr != nil {
				recvErr = resampleErr
				break
			}
			if !ok {
				continue
			}
			audio.Frame = frame
		}
		if err := s.Emit(s.ctx, audio); err != nil {
			recvErr = err
			break
		}
		if !visible && !s.adapter.retryOnChunkSent {
			s.log.enableDiscard()
		}
		visible = true
		s.adapter.setActive(index)
	}
	if errors.Is(recvErr, io.EOF) && sawRaw {
		if resampler != nil {
			if frame, ok := resampler.flush(); ok {
				last.Frame = frame
				last.Final = true
				last.TimedTranscripts = nil
				if err := s.Emit(s.ctx, last); err != nil {
					recvErr = err
				} else {
					visible = true
					s.adapter.setActive(index)
				}
			}
		}
		if errors.Is(recvErr, io.EOF) {
			recvErr = nil
		}
	}
	if errors.Is(recvErr, io.EOF) && !sawRaw {
		recvErr = agents.NewAPIConnectionError("TTS stream completed without audio", true, nil)
	}
	cancel(recvErr)
	_ = child.Close()
	forwardErr := <-forwardDone
	if recvErr != nil {
		return visible, recvErr
	}
	if forwardErr != nil && !errors.Is(forwardErr, context.Canceled) && !errors.Is(forwardErr, io.EOF) {
		return visible, forwardErr
	}
	return visible, nil
}

func (s *fallbackSynthesizeStream) forwardTo(ctx context.Context, child SynthesizeStream) error {
	cursor := s.log.firstSequence()
	for {
		input, next, err := s.log.nextValue(ctx, cursor)
		if errors.Is(err, io.EOF) {
			return child.EndInput()
		}
		if err != nil {
			return err
		}
		if input.Flush {
			err = child.Flush(ctx)
		} else {
			err = child.PushText(ctx, input.Text)
		}
		if err != nil {
			return err
		}
		cursor = next
		s.log.acknowledge(cursor)
	}
}
