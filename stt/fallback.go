// SPDX-License-Identifier: Apache-2.0

package stt

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/metrics"
	"github.com/infinityscroll/livekit-agents-go/stream"
	"github.com/infinityscroll/livekit-agents-go/vad"
)

const (
	defaultFallbackAttemptTimeout = 10 * time.Second
	defaultFallbackRetryInterval  = 5 * time.Second
	defaultFallbackStreamCapacity = 32
)

// AvailabilityChangedEvent reports a provider entering or leaving the
// fallback rotation.
type AvailabilityChangedEvent struct {
	STT       STT
	Available bool
}

// FallbackStatus is an immutable status snapshot.
type FallbackStatus struct {
	Available  bool
	Recovering bool
}

// FallbackOptions configures ordered STT failover. A zero timeout, retry
// interval, or stream capacity selects the agents-js 1.7 defaults. A zero
// MaxRetriesPerSTT selects the default of one; use a negative value to disable
// child retries explicitly (mirroring APIConnectOptions).
type FallbackOptions struct {
	STTs []STT
	// STTInstances is the agents-js-compatible spelling. Set only one of
	// STTs or STTInstances.
	STTInstances     []STT
	VAD              vad.VAD
	AttemptTimeout   time.Duration
	MaxRetriesPerSTT int
	// MaxRetryPerSTT is the agents-js-compatible spelling. Non-zero values
	// override MaxRetriesPerSTT.
	MaxRetryPerSTT int
	RetryInterval  time.Duration
	StreamCapacity int

	// KeepProvidersOpen leaves the original STT instances caller-owned. By
	// default Close shuts them down after all adapter work has stopped.
	KeepProvidersOpen bool
}

type fallbackStatus struct {
	available  bool
	recovering bool
}

// FallbackAdapter provides ordered STT failover, health recovery, automatic
// VAD wrapping for batch-only providers, and dynamic provider attribution.
type FallbackAdapter struct {
	base *Base

	original       []STT
	stts           []STT
	ownedWrappers  []*StreamAdapter
	attemptTimeout time.Duration
	maxRetries     int
	retryInterval  time.Duration
	streamCapacity int
	closeProviders bool

	ctx    context.Context
	cancel context.CancelCauseFunc

	mu            sync.RWMutex
	status        []fallbackStatus
	active        int
	closed        bool
	streams       map[*fallbackSpeechStream]struct{}
	unsubscribers []func()
	recoveryWG    sync.WaitGroup
	requestWG     sync.WaitGroup
	availability  agents.EventEmitter[AvailabilityChangedEvent]
	closeOnce     sync.Once
	closeDone     chan struct{}
	closeErr      error
}

func NewFallbackAdapter(options FallbackOptions) (*FallbackAdapter, error) {
	if len(options.STTs) != 0 && len(options.STTInstances) != 0 {
		return nil, errors.New("set only one of STTs or STTInstances")
	}
	if len(options.STTs) == 0 {
		options.STTs = options.STTInstances
	}
	if len(options.STTs) == 0 {
		return nil, errors.New("at least one STT instance must be provided")
	}
	if options.AttemptTimeout < 0 {
		return nil, errors.New("STT fallback attempt timeout must not be negative")
	}
	if options.RetryInterval < 0 {
		return nil, errors.New("STT fallback retry interval must not be negative")
	}
	for i, provider := range options.STTs {
		if provider == nil {
			return nil, fmt.Errorf("STT fallback provider %d is nil", i)
		}
		if !provider.Capabilities().Streaming && options.VAD == nil {
			return nil, fmt.Errorf(
				"STTs do not support streaming: %s; provide a VAD to enable stt.StreamAdapter automatically",
				provider.Label(),
			)
		}
	}
	if options.AttemptTimeout == 0 {
		options.AttemptTimeout = defaultFallbackAttemptTimeout
	}
	if options.RetryInterval == 0 {
		options.RetryInterval = defaultFallbackRetryInterval
	}
	if options.StreamCapacity <= 0 {
		options.StreamCapacity = defaultFallbackStreamCapacity
	}
	maxRetries := options.MaxRetriesPerSTT
	if options.MaxRetryPerSTT != 0 {
		maxRetries = options.MaxRetryPerSTT
	}
	if maxRetries == 0 {
		maxRetries = 1
	} else if maxRetries < 0 {
		maxRetries = 0
	}

	wrapped := make([]STT, len(options.STTs))
	var wrappers []*StreamAdapter
	for i, provider := range options.STTs {
		wrapped[i] = provider
		if provider.Capabilities().Streaming {
			continue
		}
		adapter, err := NewStreamAdapter(provider, options.VAD, StreamAdapterOptions{StreamCapacity: options.StreamCapacity})
		if err != nil {
			for _, value := range wrappers {
				_ = value.Close(context.Background())
			}
			return nil, err
		}
		wrapped[i] = adapter
		wrappers = append(wrappers, adapter)
	}

	caps := aggregateFallbackCapabilities(wrapped)
	ctx, cancel := context.WithCancelCause(context.Background())
	a := &FallbackAdapter{
		base:     NewBase("stt.FallbackAdapter", "livekit", "FallbackAdapter", caps),
		original: append([]STT(nil), options.STTs...), stts: wrapped, ownedWrappers: wrappers,
		attemptTimeout: options.AttemptTimeout, maxRetries: maxRetries,
		retryInterval: options.RetryInterval, streamCapacity: options.StreamCapacity,
		closeProviders: !options.KeepProvidersOpen,
		ctx:            ctx, cancel: cancel, active: -1, streams: make(map[*fallbackSpeechStream]struct{}),
		status: make([]fallbackStatus, len(wrapped)), closeDone: make(chan struct{}),
	}
	for i := range a.status {
		a.status[i].available = true
	}
	for _, provider := range wrapped {
		provider := provider
		a.unsubscribers = append(a.unsubscribers,
			provider.OnMetrics(func(metric metrics.STT) { a.base.EmitMetrics(metric) }),
		)
	}
	return a, nil
}

func aggregateFallbackCapabilities(providers []STT) Capabilities {
	caps := Capabilities{Streaming: true, InterimResults: true, Diarization: true}
	allAligned := true
	for _, provider := range providers {
		child := provider.Capabilities()
		caps.InterimResults = caps.InterimResults && child.InterimResults
		caps.Diarization = caps.Diarization && child.Diarization
		caps.Keyterms = caps.Keyterms || child.Keyterms
		caps.ChatContext = caps.ChatContext || child.ChatContext
		allAligned = allAligned && child.AlignedTranscript != AlignedTranscriptNone
	}
	if allAligned {
		caps.AlignedTranscript = providers[0].Capabilities().AlignedTranscript
	}
	return caps
}

func (a *FallbackAdapter) Label() string              { return a.base.Label() }
func (a *FallbackAdapter) Capabilities() Capabilities { return a.base.Capabilities() }

func (a *FallbackAdapter) Provider() string {
	a.mu.RLock()
	active := a.active
	a.mu.RUnlock()
	if active >= 0 {
		return a.stts[active].Provider()
	}
	return "livekit"
}

func (a *FallbackAdapter) Model() string {
	a.mu.RLock()
	active := a.active
	a.mu.RUnlock()
	if active >= 0 {
		return a.stts[active].Model()
	}
	return "FallbackAdapter"
}

func (a *FallbackAdapter) STTs() []STT { return append([]STT(nil), a.stts...) }

// Instances is a Python/TypeScript-friendly alias for STTs.
func (a *FallbackAdapter) Instances() []STT { return a.STTs() }

func (a *FallbackAdapter) AttemptTimeout() time.Duration { return a.attemptTimeout }
func (a *FallbackAdapter) MaxRetriesPerSTT() int         { return a.maxRetries }
func (a *FallbackAdapter) RetryInterval() time.Duration  { return a.retryInterval }

func (a *FallbackAdapter) OnMetrics(fn func(metrics.STT)) func() { return a.base.OnMetrics(fn) }
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

func (a *FallbackAdapter) childConnectOptions() agents.APIConnectOptions {
	maxRetries := a.maxRetries
	if maxRetries == 0 {
		maxRetries = -1
	}
	return agents.APIConnectOptions{
		MaxRetries: maxRetries, RetryInterval: a.retryInterval, Timeout: a.attemptTimeout,
	}
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
	value := a.status[index].available
	a.mu.RUnlock()
	return value
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
	a.mu.Lock()
	if a.closed || !a.status[index].available {
		a.mu.Unlock()
		return
	}
	a.status[index].available = false
	provider := a.stts[index]
	a.mu.Unlock()
	a.availability.Emit(AvailabilityChangedEvent{STT: provider, Available: false})
}

func (a *FallbackAdapter) beginRecovery(index int) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed || a.status[index].recovering {
		return false
	}
	a.status[index].recovering = true
	a.recoveryWG.Add(1)
	return true
}

func (a *FallbackAdapter) finishRecovery(index int, recovered bool) {
	var emit bool
	a.mu.Lock()
	a.status[index].recovering = false
	if recovered && !a.closed && !a.status[index].available {
		a.status[index].available = true
		emit = true
	}
	provider := a.stts[index]
	a.mu.Unlock()
	if emit {
		a.availability.Emit(AvailabilityChangedEvent{STT: provider, Available: true})
	}
	a.recoveryWG.Done()
}

func (a *FallbackAdapter) Recognize(parent context.Context, frames []agents.AudioFrame, options RecognizeOptions) (SpeechEvent, error) {
	if parent == nil {
		parent = context.Background()
	}
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return SpeechEvent{}, stream.ErrClosed
	}
	a.requestWG.Add(1)
	a.mu.Unlock()
	defer a.requestWG.Done()
	requestCtx, requestCancel := context.WithCancelCause(parent)
	stopAdapter := context.AfterFunc(a.ctx, func() { requestCancel(context.Cause(a.ctx)) })
	defer func() {
		stopAdapter()
		requestCancel(context.Canceled)
	}()
	allFailed := a.allUnavailable()
	started := time.Now()
	var lastErr error
	for index, provider := range a.stts {
		if !allFailed && !a.isAvailable(index) {
			a.startRecognizeRecovery(index, frames, options)
			continue
		}
		attemptCtx, cancel := context.WithTimeoutCause(requestCtx, a.attemptTimeout,
			agents.NewAPITimeoutError(fmt.Sprintf("STT %s fallback attempt timed out", provider.Label()), true, context.DeadlineExceeded))
		childOptions := options
		childOptions.ConnectOptions = a.childConnectOptions()
		result, err := provider.Recognize(attemptCtx, frames, childOptions)
		cancel()
		if err == nil {
			a.setActive(index)
			return result, nil
		}
		if cause := context.Cause(requestCtx); cause != nil {
			return SpeechEvent{}, cause
		}
		lastErr = err
		a.markUnavailable(index)
		a.startRecognizeRecovery(index, frames, options)
	}
	err := a.allFailedError(started, lastErr)
	a.base.EmitError(ErrorEvent{Timestamp: time.Now(), Label: a.Label(), Err: err, Recoverable: false})
	return SpeechEvent{}, err
}

func (a *FallbackAdapter) startRecognizeRecovery(index int, frames []agents.AudioFrame, options RecognizeOptions) {
	if !a.beginRecovery(index) {
		return
	}
	frames = cloneAudioFrames(frames)
	go func() {
		childOptions := options
		childOptions.ConnectOptions = a.childConnectOptions()
		attemptCtx, cancel := context.WithTimeoutCause(a.ctx, a.attemptTimeout,
			agents.NewAPITimeoutError("STT fallback recovery timed out", true, context.DeadlineExceeded))
		_, err := a.stts[index].Recognize(attemptCtx, frames, childOptions)
		cancel()
		a.finishRecovery(index, err == nil)
	}()
}

func cloneAudioFrames(frames []agents.AudioFrame) []agents.AudioFrame {
	result := make([]agents.AudioFrame, len(frames))
	for i := range frames {
		result[i] = frames[i]
		result[i].Data = append([]int16(nil), frames[i].Data...)
		if frames[i].UserData != nil {
			result[i].UserData = make(map[string]any, len(frames[i].UserData))
			for key, value := range frames[i].UserData {
				result[i].UserData[key] = value
			}
		}
	}
	return result
}

func (a *FallbackAdapter) Stream(parent context.Context, options StreamOptions) (SpeechStream, error) {
	if parent == nil {
		parent = context.Background()
	}
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return nil, stream.ErrClosed
	}
	value := newFallbackSpeechStream(parent, a, options)
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

func (a *FallbackAdapter) allFailedError(started time.Time, cause error) error {
	labels := make([]string, len(a.stts))
	for i := range a.stts {
		labels[i] = a.stts[i].Label()
	}
	return agents.NewAPIConnectionError(
		fmt.Sprintf("all STTs failed (%s) after %.3fs", strings.Join(labels, ", "), time.Since(started).Seconds()),
		false, cause,
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
		active := make([]*fallbackSpeechStream, 0, len(a.streams))
		for value := range a.streams {
			active = append(active, value)
		}
		a.mu.Unlock()
		a.cancel(stream.ErrClosed)
		for _, unsubscribe := range unsubscribers {
			unsubscribe()
		}
		for _, value := range active {
			_ = value.Close()
		}
		go a.finishClose(active)
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

func (a *FallbackAdapter) finishClose(active []*fallbackSpeechStream) {
	for _, value := range active {
		<-value.done
	}
	a.requestWG.Wait()
	a.recoveryWG.Wait()
	var result error
	for _, wrapper := range a.ownedWrappers {
		result = errors.Join(result, wrapper.Close(context.Background()))
	}
	if a.closeProviders {
		for _, provider := range a.original {
			result = errors.Join(result, provider.Close(context.Background()))
		}
	}
	a.mu.Lock()
	a.closeErr = result
	close(a.closeDone)
	a.mu.Unlock()
}

type sttInputLog struct {
	mu       sync.Mutex
	items    []StreamInput
	head     int
	length   int
	capacity int
	closed   bool
	err      error
	changed  chan struct{}
}

func newSTTInputLog(capacity int) *sttInputLog {
	return &sttInputLog{
		items: make([]StreamInput, capacity), capacity: capacity,
		changed: make(chan struct{}, 1),
	}
}

func (b *sttInputLog) notifyLocked() {
	select {
	case b.changed <- struct{}{}:
	default:
	}
}

func (b *sttInputLog) append(ctx context.Context, item StreamInput) error {
	for {
		b.mu.Lock()
		if b.closed {
			err := b.err
			if err == nil {
				err = stream.ErrClosed
			}
			b.mu.Unlock()
			return err
		}
		if b.length < b.capacity {
			index := (b.head + b.length) % b.capacity
			b.items[index] = item
			b.length++
			b.notifyLocked()
			b.mu.Unlock()
			return nil
		}
		changed := b.changed
		b.mu.Unlock()
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-changed:
		}
	}
}

func (b *sttInputLog) peek(ctx context.Context) (StreamInput, error) {
	for {
		b.mu.Lock()
		if b.length != 0 {
			value := b.items[b.head]
			b.mu.Unlock()
			return value, nil
		}
		if b.closed {
			err := b.err
			b.mu.Unlock()
			if err != nil {
				return StreamInput{}, err
			}
			return StreamInput{}, io.EOF
		}
		changed := b.changed
		b.mu.Unlock()
		select {
		case <-ctx.Done():
			return StreamInput{}, context.Cause(ctx)
		case <-changed:
		}
	}
}

func (b *sttInputLog) consume() {
	b.mu.Lock()
	if b.length != 0 {
		var zero StreamInput
		b.items[b.head] = zero
		b.head = (b.head + 1) % b.capacity
		b.length--
		b.notifyLocked()
	}
	b.mu.Unlock()
}

func (b *sttInputLog) close(err error) {
	b.mu.Lock()
	if !b.closed {
		b.closed = true
		b.err = err
		b.notifyLocked()
	}
	b.mu.Unlock()
}

type sttRecoveryProbe struct {
	input *stream.Channel[StreamInput]
	close sync.Once
}

func (p *sttRecoveryProbe) finish(err error) {
	p.close.Do(func() {
		if err != nil {
			_ = p.input.Abort(err)
		} else {
			_ = p.input.Close()
		}
	})
}

type fallbackSpeechStream struct {
	*BaseStream
	adapter *FallbackAdapter
	options StreamOptions
	ctx     context.Context
	cancel  context.CancelCauseFunc
	stop    func() bool
	log     *sttInputLog
	done    chan struct{}

	mu         sync.Mutex
	current    SpeechStream
	probes     map[*sttRecoveryProbe]struct{}
	inputEnded bool
	inputErr   error
	closed     bool
	closeOnce  sync.Once
	unregister func()
}

func newFallbackSpeechStream(parent context.Context, adapter *FallbackAdapter, options StreamOptions) *fallbackSpeechStream {
	ctx, cancel := context.WithCancelCause(parent)
	s := &fallbackSpeechStream{
		BaseStream: NewBaseStream(ctx, adapter.streamCapacity), adapter: adapter,
		options: options, ctx: ctx, cancel: cancel,
		log: newSTTInputLog(adapter.streamCapacity), done: make(chan struct{}),
		probes: make(map[*sttRecoveryProbe]struct{}),
	}
	s.stop = context.AfterFunc(adapter.ctx, func() { _ = s.Close() })
	return s
}

func (s *fallbackSpeechStream) Close() error {
	s.closeOnce.Do(func() {
		s.cancel(stream.ErrClosed)
		s.log.close(stream.ErrClosed)
		s.mu.Lock()
		s.closed = true
		current := s.current
		probes := make([]*sttRecoveryProbe, 0, len(s.probes))
		for probe := range s.probes {
			probes = append(probes, probe)
		}
		s.mu.Unlock()
		if current != nil {
			_ = current.Close()
		}
		for _, probe := range probes {
			probe.finish(stream.ErrClosed)
		}
		_ = s.BaseStream.Close()
	})
	return nil
}

func (s *fallbackSpeechStream) Wait(ctx context.Context) error {
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

func (s *fallbackSpeechStream) run() {
	defer close(s.done)
	defer s.unregister()
	defer func() {
		if s.stop != nil {
			s.stop()
		}
	}()
	inputDone := make(chan error, 1)
	go func() { inputDone <- s.pumpInput() }()

	started := time.Now()
	allFailed := s.adapter.allUnavailable()
	var lastErr error
	for index, provider := range s.adapter.stts {
		if cause := context.Cause(s.ctx); cause != nil {
			lastErr = cause
			break
		}
		if !allFailed && !s.adapter.isAvailable(index) {
			s.startStreamRecovery(index)
			continue
		}
		err := s.tryProvider(index, provider)
		if err == nil {
			s.cancel(io.EOF)
			s.log.close(nil)
			<-inputDone
			s.BaseStream.Finish(nil)
			return
		}
		if cause := context.Cause(s.ctx); cause != nil {
			lastErr = cause
			break
		}
		lastErr = err
		s.adapter.markUnavailable(index)
		s.startStreamRecovery(index)
	}
	if cause := context.Cause(s.ctx); cause != nil {
		s.log.close(cause)
		<-inputDone
		if errors.Is(cause, stream.ErrClosed) || errors.Is(cause, io.EOF) {
			s.BaseStream.Finish(nil)
		} else {
			s.BaseStream.Finish(cause)
		}
		return
	}
	s.cancel(lastErr)
	s.log.close(lastErr)
	<-inputDone
	if errors.Is(lastErr, stream.ErrClosed) || errors.Is(lastErr, context.Canceled) {
		s.BaseStream.Finish(nil)
		return
	}
	err := s.adapter.allFailedError(started, lastErr)
	s.adapter.base.EmitError(ErrorEvent{Timestamp: time.Now(), Label: s.adapter.Label(), Err: err, Recoverable: false})
	s.BaseStream.Finish(err)
}

func (s *fallbackSpeechStream) pumpInput() error {
	for {
		input, err := s.Inputs().Recv(s.ctx)
		if errors.Is(err, io.EOF) {
			s.log.close(nil)
			s.finishProbes(nil)
			return nil
		}
		if err != nil {
			s.log.close(err)
			s.finishProbes(err)
			return err
		}
		if err := s.log.append(s.ctx, input); err != nil {
			s.finishProbes(err)
			return err
		}
		s.fanoutProbe(input)
	}
}

func (s *fallbackSpeechStream) fanoutProbe(input StreamInput) {
	s.mu.Lock()
	for probe := range s.probes {
		if !probe.input.TrySend(input) {
			probe.finish(errors.New("STT recovery probe input overflow"))
			delete(s.probes, probe)
		}
	}
	s.mu.Unlock()
}

func (s *fallbackSpeechStream) finishProbes(err error) {
	s.mu.Lock()
	s.inputEnded = true
	s.inputErr = err
	for probe := range s.probes {
		probe.finish(err)
		delete(s.probes, probe)
	}
	s.mu.Unlock()
}

func (s *fallbackSpeechStream) tryProvider(index int, provider STT) error {
	options := s.options
	options.ConnectOptions = s.adapter.childConnectOptions()
	child, err := provider.Stream(s.ctx, options)
	if err != nil {
		return err
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
	var recvErr error
	for {
		event, err := child.Recv(attemptCtx)
		if err != nil {
			recvErr = err
			break
		}
		s.adapter.setActive(index)
		if err := s.Emit(s.ctx, event); err != nil {
			recvErr = err
			break
		}
	}
	cancel(recvErr)
	_ = child.Close()
	forwardErr := <-forwardDone
	if errors.Is(recvErr, io.EOF) {
		return nil
	}
	if recvErr != nil {
		return recvErr
	}
	if forwardErr != nil && !errors.Is(forwardErr, context.Canceled) && !errors.Is(forwardErr, io.EOF) {
		return forwardErr
	}
	return nil
}

func (s *fallbackSpeechStream) forwardTo(ctx context.Context, child SpeechStream) error {
	for {
		input, err := s.log.peek(ctx)
		if errors.Is(err, io.EOF) {
			return child.EndInput()
		}
		if err != nil {
			return err
		}
		if input.Flush {
			err = child.Flush(ctx)
		} else if input.Frame != nil {
			err = child.Push(ctx, *input.Frame)
		}
		if err != nil {
			return err
		}
		s.log.consume()
	}
}

func (s *fallbackSpeechStream) startStreamRecovery(index int) {
	if !s.adapter.beginRecovery(index) {
		return
	}
	options := s.options
	options.ConnectOptions = agents.APIConnectOptions{
		MaxRetries: -1, Timeout: s.adapter.attemptTimeout, RetryInterval: s.adapter.retryInterval,
	}
	child, err := s.adapter.stts[index].Stream(s.ctx, options)
	if err != nil {
		s.adapter.finishRecovery(index, false)
		return
	}
	probe := &sttRecoveryProbe{input: stream.NewChannel[StreamInput](s.adapter.streamCapacity)}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		_ = child.Close()
		s.adapter.finishRecovery(index, false)
		return
	}
	s.probes[probe] = struct{}{}
	inputEnded, inputErr := s.inputEnded, s.inputErr
	s.mu.Unlock()
	if inputEnded {
		probe.finish(inputErr)
	}

	go func() {
		recovered := s.runStreamRecovery(child, probe)
		s.mu.Lock()
		delete(s.probes, probe)
		s.mu.Unlock()
		probe.finish(stream.ErrClosed)
		_ = child.Close()
		s.adapter.finishRecovery(index, recovered)
	}()
}

func (s *fallbackSpeechStream) runStreamRecovery(child SpeechStream, probe *sttRecoveryProbe) bool {
	ctx, cancel := context.WithCancelCause(s.ctx)
	defer cancel(stream.ErrClosed)
	forwardDone := make(chan struct{})
	go func() {
		defer close(forwardDone)
		for {
			input, err := probe.input.Recv(ctx)
			if errors.Is(err, io.EOF) {
				_ = child.EndInput()
				return
			}
			if err != nil {
				_ = child.Close()
				return
			}
			if input.Flush {
				err = child.Flush(ctx)
			} else if input.Frame != nil {
				err = child.Push(ctx, *input.Frame)
			}
			if err != nil {
				_ = child.Close()
				return
			}
		}
	}()
	recovered := false
	for {
		event, err := child.Recv(ctx)
		if err != nil {
			break
		}
		if event.Type == FinalTranscript && len(event.Alternatives) != 0 && event.Alternatives[0].Text != "" {
			recovered = true
			break
		}
	}
	cancel(stream.ErrClosed)
	probe.finish(stream.ErrClosed)
	_ = child.Close()
	<-forwardDone
	return recovered
}
