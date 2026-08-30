// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/inference"
	"github.com/infinityscroll/livekit-agents-go/llm"
	"github.com/infinityscroll/livekit-agents-go/stream"
	"github.com/infinityscroll/livekit-agents-go/stt"
	"github.com/infinityscroll/livekit-agents-go/vad"
)

var (
	ErrRecognitionStarted = errors.New("audio recognition is already started")
	ErrRecognitionClosed  = errors.New("audio recognition is closed")
)

const (
	DefaultRecognitionMaxBufferedFrames = 6000
	manualCommitTranscriptionDelay      = 500 * time.Millisecond
)

type RecognizedTurn struct {
	Transcript     string
	Language       agents.LanguageCode
	SpeakerID      string
	Confidence     float64
	SpeechStarted  time.Time
	SpeechEnded    time.Time
	SpeechDuration time.Duration
}

type AudioRecognitionCallbacks struct {
	OnStartOfSpeech     func(time.Time)
	OnEndOfSpeech       func(time.Time)
	OnInterimTranscript func(stt.SpeechEvent)
	OnFinalTranscript   func(stt.SpeechEvent)
	// OnCommitAudio fires once for each input epoch before transcript commit.
	// Realtime models without server turn detection use it to commit the
	// provider audio even when local STT has not produced text yet.
	OnCommitAudio       func()
	OnCommitAudioResult func() error
	OnCommit            func(RecognizedTurn)
	// OnCommitDecision is the voice-runtime hook. Returning false suppresses
	// reply generation (for example StopResponse from OnUserTurnCompleted).
	OnCommitDecision       func(RecognizedTurn) bool
	OnCommitComplete       func(bool)
	OnInterruption         func(time.Duration, int)
	OnUserTurnExceeded     func(RecognizedTurn)
	OnTranscriptionTimeout func(time.Duration, time.Time)
	OnEOTPrediction        func(inference.TurnDetectionEvent, float64, time.Duration)
	OnOverlappingSpeech    func(inference.OverlappingSpeechEvent)
	OnBackchannel          func(RecognizedTurn)
	OnActivityChanged      func()
	OnError                func(error, any)
}

type AudioRecognitionOptions struct {
	STT                  stt.STT
	VAD                  vad.VAD
	TurnDetector         *inference.TurnDetector
	InterruptionDetector *inference.AdaptiveInterruptionDetector

	STTNode func(context.Context, stream.Reader[agents.AudioFrame]) (stream.Reader[STTNodeItem], error)
	// AudioSink receives each validated frame from the single input pump. It is
	// used to feed a realtime session without teeing or duplicating goroutines.
	AudioSink func(context.Context, agents.AudioFrame) error

	TurnDetection        TurnDetectionMode
	Endpointing          Endpointing
	Interruption         InterruptionOptions
	UserTurnLimit        UserTurnLimitOptions
	ConnectOptions       agents.APIConnectOptions
	TranscriptionTimeout *time.Duration
	QueueCapacity        int
	MaxBufferedFrames    int
	Callbacks            AudioRecognitionCallbacks
}

type recognitionSignalKind uint8

const (
	recognitionFrame recognitionSignalKind = iota
	recognitionSTT
	recognitionVAD
	recognitionInputEnded
	recognitionManualCommit
	recognitionClear
	recognitionFailure
	recognitionSTTEnded
	recognitionEOT
	recognitionOverlap
	recognitionAgentStarted
	recognitionAgentEnded
	recognitionDisableAdaptive
)

type recognitionSignal struct {
	kind       recognitionSignalKind
	frame      agents.AudioFrame
	stt        stt.SpeechEvent
	vad        vad.Event
	err        error
	source     any
	epoch      uint64
	batch      bool
	done       chan error
	eot        inference.TurnDetectionEvent
	overlap    inference.OverlappingSpeechEvent
	at         time.Time
	prediction uint64
}

type batchRecognitionRequest struct {
	frames []agents.AudioFrame
	final  bool
	epoch  uint64
}

// AudioRecognition serializes VAD, STT, endpointing and manual commits through
// one bounded actor. Provider readers never mutate turn state directly.
type AudioRecognition struct {
	ctx    context.Context
	cancel context.CancelCauseFunc
	opts   AudioRecognitionOptions
	signal *stream.Channel[recognitionSignal]

	mu                 sync.Mutex
	started            bool
	closed             bool
	sttStream          stt.SpeechStream
	vadStream          vad.VADStream
	turnStream         *inference.TurnDetectorStream
	interruptionStream *inference.InterruptionStream
	sttInput           *stream.Channel[agents.AudioFrame]
	batch              *stream.Channel[batchRecognitionRequest]
	sttCancel          context.CancelCauseFunc
	vadCancel          context.CancelCauseFunc
	turnCancel         context.CancelCauseFunc
	interruptionCancel context.CancelCauseFunc
	epoch              atomic.Uint64
	lastFinal          atomic.Int64
	turnBusy           atomic.Bool
	resetMu            sync.Mutex
	wg                 sync.WaitGroup
	waitOnce           sync.Once
	done               chan struct{}
	closeOnce          sync.Once
	closeErr           error
}

func NewAudioRecognition(parent context.Context, options AudioRecognitionOptions) (*AudioRecognition, error) {
	if parent == nil {
		parent = context.Background()
	}
	if options.QueueCapacity == 0 {
		options.QueueCapacity = DefaultRecognitionQueueCapacity
	}
	if options.QueueCapacity < 1 {
		return nil, errors.New("audio recognition queue capacity must be positive")
	}
	if options.MaxBufferedFrames == 0 {
		options.MaxBufferedFrames = DefaultRecognitionMaxBufferedFrames
	}
	if options.MaxBufferedFrames < 1 {
		return nil, errors.New("audio recognition buffered-frame limit must be positive")
	}
	if options.Endpointing == nil {
		endpointing, err := NewEndpointing(DefaultEndpointingOptions)
		if err != nil {
			return nil, err
		}
		options.Endpointing = endpointing
	}
	if options.TurnDetector != nil && options.VAD == nil {
		return nil, errors.New("streaming turn detector requires a VAD model")
	}
	if options.InterruptionDetector != nil && options.VAD == nil {
		return nil, errors.New("adaptive interruption detector requires a VAD model")
	}
	if options.TurnDetection == "" {
		switch {
		case options.STT != nil || options.STTNode != nil:
			options.TurnDetection = TurnDetectionSTT
		case options.VAD != nil:
			options.TurnDetection = TurnDetectionVAD
		default:
			options.TurnDetection = TurnDetectionManual
		}
	}
	switch options.TurnDetection {
	case TurnDetectionSTT, TurnDetectionVAD, TurnDetectionManual:
	case TurnDetectionRealtimeLLM:
		if options.AudioSink == nil {
			return nil, errors.New("realtime_llm turn detection requires a realtime audio sink")
		}
	default:
		return nil, errors.New("unknown audio recognition turn detection mode")
	}
	if options.TranscriptionTimeout != nil && *options.TranscriptionTimeout < 0 {
		return nil, errors.New("audio recognition transcription timeout must not be negative")
	}
	if err := options.UserTurnLimit.Validate(); err != nil {
		return nil, fmt.Errorf("audio recognition user turn limit: %w", err)
	}
	ctx, cancel := context.WithCancelCause(parent)
	return &AudioRecognition{
		ctx: ctx, cancel: cancel, opts: options,
		signal: stream.NewChannel[recognitionSignal](options.QueueCapacity), done: make(chan struct{}),
	}, nil
}

func (r *AudioRecognition) Start(source stream.Reader[agents.AudioFrame]) error {
	if source == nil {
		return errors.New("audio recognition source must not be nil")
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return ErrRecognitionClosed
	}
	if r.started {
		r.mu.Unlock()
		return ErrRecognitionStarted
	}
	r.started = true
	// Keep the wait-group non-zero until setup is complete. Close may safely
	// begin waiting while Start is opening provider streams, and no later Add
	// can race a zero counter.
	r.wg.Add(1)
	r.mu.Unlock()
	defer r.wg.Done()

	if err := r.startSTTPipeline(r.epoch.Load()); err != nil {
		return r.startFailed(err)
	}
	if r.opts.STT != nil && !r.opts.STT.Capabilities().Streaming && r.opts.STTNode == nil {
		batch := stream.NewChannel[batchRecognitionRequest](r.opts.QueueCapacity)
		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			_ = batch.Abort(ErrRecognitionClosed)
			return ErrRecognitionClosed
		}
		r.batch = batch
		r.mu.Unlock()
		if !r.addWorkers(1) {
			_ = batch.Abort(ErrRecognitionClosed)
			return r.startFailed(ErrRecognitionClosed)
		}
		go r.batchWorker()
	}
	if err := r.startVADPipeline(r.epoch.Load()); err != nil {
		return r.startFailed(err)
	}
	if err := r.startTurnDetectorPipeline(); err != nil {
		return r.startFailed(err)
	}
	if err := r.startInterruptionPipeline(); err != nil {
		return r.startFailed(err)
	}

	actorInput := make(chan recognitionSignal)
	if !r.addWorkers(3) {
		return r.startFailed(ErrRecognitionClosed)
	}
	go r.run(actorInput)
	go r.pumpSignals(actorInput)
	go r.pumpAudio(source)
	return nil
}

func (r *AudioRecognition) startTurnDetectorPipeline() error {
	if r.opts.TurnDetector == nil {
		return nil
	}
	workerCtx, workerCancel := context.WithCancelCause(r.ctx)
	value, err := r.opts.TurnDetector.Stream(workerCtx, inference.TurnDetectorStreamOptions{
		ConnectOptions: r.opts.ConnectOptions,
		InputCapacity:  r.opts.QueueCapacity,
	})
	if err != nil {
		workerCancel(err)
		return err
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		workerCancel(ErrRecognitionClosed)
		_ = value.Close()
		return ErrRecognitionClosed
	}
	r.turnStream, r.turnCancel = value, workerCancel
	r.mu.Unlock()
	return nil
}

func (r *AudioRecognition) startInterruptionPipeline() error {
	if r.opts.InterruptionDetector == nil {
		return nil
	}
	workerCtx, workerCancel := context.WithCancelCause(r.ctx)
	value, err := r.opts.InterruptionDetector.Stream(workerCtx, inference.InterruptionStreamOptions{
		ConnectOptions: r.opts.ConnectOptions,
		InputCapacity:  r.opts.QueueCapacity,
		OutputCapacity: r.opts.QueueCapacity,
	})
	if err != nil {
		workerCancel(err)
		return err
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		workerCancel(ErrRecognitionClosed)
		_ = value.Close()
		return ErrRecognitionClosed
	}
	r.interruptionStream, r.interruptionCancel = value, workerCancel
	r.mu.Unlock()
	if !r.addWorkers(1) {
		workerCancel(ErrRecognitionClosed)
		_ = value.Close()
		return ErrRecognitionClosed
	}
	go r.readInterruptions(workerCtx, value)
	return nil
}

func (r *AudioRecognition) readInterruptions(ctx context.Context, source *inference.InterruptionStream) {
	defer r.wg.Done()
	defer r.recoverPanic("adaptive interruption reader", r.opts.InterruptionDetector)
	for {
		event, err := source.Recv(ctx)
		if err != nil {
			if !errors.Is(err, io.EOF) && context.Cause(ctx) == nil && context.Cause(r.ctx) == nil {
				r.sendFailure(err, r.opts.InterruptionDetector)
			}
			return
		}
		if err := r.signal.Send(r.ctx, recognitionSignal{kind: recognitionOverlap, overlap: event}); err != nil {
			return
		}
	}
}

func (r *AudioRecognition) startSTTPipeline(epoch uint64) error {
	if r.opts.STTNode == nil && (r.opts.STT == nil || !r.opts.STT.Capabilities().Streaming) {
		return nil
	}
	workerCtx, workerCancel := context.WithCancelCause(r.ctx)
	if r.opts.STTNode != nil {
		input := stream.NewChannel[agents.AudioFrame](r.opts.QueueCapacity)
		var output stream.Reader[STTNodeItem]
		err := guardedActivityCall("audio recognition STT node", func() (err error) {
			output, err = r.opts.STTNode(workerCtx, input)
			return err
		})
		if err != nil {
			workerCancel(err)
			_ = input.Abort(err)
			return err
		}
		if output == nil {
			err = errors.New("audio recognition STT node returned a nil stream")
			workerCancel(err)
			_ = input.Abort(err)
			return err
		}
		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			workerCancel(ErrRecognitionClosed)
			_ = input.Abort(ErrRecognitionClosed)
			return ErrRecognitionClosed
		}
		r.sttInput, r.sttCancel = input, workerCancel
		r.mu.Unlock()
		if !r.addWorkers(1) {
			workerCancel(ErrRecognitionClosed)
			_ = input.Abort(ErrRecognitionClosed)
			return ErrRecognitionClosed
		}
		go r.readSTTNode(workerCtx, output, epoch)
		return nil
	}

	var value stt.SpeechStream
	err := guardedActivityCall("audio recognition STT stream", func() (err error) {
		value, err = r.opts.STT.Stream(workerCtx, stt.StreamOptions{ConnectOptions: r.opts.ConnectOptions})
		return err
	})
	if err != nil {
		workerCancel(err)
		return err
	}
	if value == nil {
		err = errors.New("audio recognition STT returned a nil stream")
		workerCancel(err)
		return err
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		workerCancel(ErrRecognitionClosed)
		_ = guardedActivityCall("late STT stream close", value.Close)
		return ErrRecognitionClosed
	}
	r.sttStream, r.sttCancel = value, workerCancel
	r.mu.Unlock()
	if !r.addWorkers(1) {
		workerCancel(ErrRecognitionClosed)
		_ = guardedActivityCall("late STT stream close", value.Close)
		return ErrRecognitionClosed
	}
	go r.readSTT(workerCtx, value, epoch)
	return nil
}

func (r *AudioRecognition) startVADPipeline(epoch uint64) error {
	if r.opts.VAD == nil {
		return nil
	}
	workerCtx, workerCancel := context.WithCancelCause(r.ctx)
	var value vad.VADStream
	err := guardedActivityCall("audio recognition VAD stream", func() (err error) {
		value, err = r.opts.VAD.Stream(workerCtx)
		return err
	})
	if err != nil {
		workerCancel(err)
		return err
	}
	if value == nil {
		err = errors.New("audio recognition VAD returned a nil stream")
		workerCancel(err)
		return err
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		workerCancel(ErrRecognitionClosed)
		_ = guardedActivityCall("late VAD stream close", value.Close)
		return ErrRecognitionClosed
	}
	r.vadStream, r.vadCancel = value, workerCancel
	r.mu.Unlock()
	if !r.addWorkers(1) {
		workerCancel(ErrRecognitionClosed)
		_ = guardedActivityCall("late VAD stream close", value.Close)
		return ErrRecognitionClosed
	}
	go r.readVAD(workerCtx, value, epoch)
	return nil
}

func (r *AudioRecognition) addWorkers(count int) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return false
	}
	r.wg.Add(count)
	return true
}

func (r *AudioRecognition) startFailed(err error) error {
	r.cancel(err)
	r.mu.Lock()
	r.closed = true
	sttStream, vadStream, turnStream, interruptionStream, sttInput, batch := r.sttStream, r.vadStream, r.turnStream, r.interruptionStream, r.sttInput, r.batch
	sttCancel, vadCancel, turnCancel, interruptionCancel := r.sttCancel, r.vadCancel, r.turnCancel, r.interruptionCancel
	r.mu.Unlock()
	var errs []error
	if sttCancel != nil {
		sttCancel(err)
	}
	if vadCancel != nil {
		vadCancel(err)
	}
	if turnCancel != nil {
		turnCancel(err)
	}
	if interruptionCancel != nil {
		interruptionCancel(err)
	}
	if sttStream != nil {
		if closeErr := guardedActivityCall("failed STT stream close", sttStream.Close); closeErr != nil {
			errs = append(errs, closeErr)
		}
	}
	if vadStream != nil {
		if closeErr := guardedActivityCall("failed VAD stream close", vadStream.Close); closeErr != nil {
			errs = append(errs, closeErr)
		}
	}
	if turnStream != nil {
		if closeErr := turnStream.Close(); closeErr != nil {
			errs = append(errs, closeErr)
		}
	}
	if interruptionStream != nil {
		if closeErr := interruptionStream.Close(); closeErr != nil {
			errs = append(errs, closeErr)
		}
	}
	if sttInput != nil {
		_ = sttInput.Abort(err)
	}
	if batch != nil {
		_ = batch.Abort(err)
	}
	_ = r.signal.Abort(err)
	r.mu.Lock()
	r.closeErr = errors.Join(r.closeErr, errors.Join(errs...))
	r.mu.Unlock()
	return errors.Join(append([]error{err}, errs...)...)
}

func (r *AudioRecognition) pumpAudio(source stream.Reader[agents.AudioFrame]) {
	defer r.wg.Done()
	defer r.recoverPanic("audio input pump", source)
	defer func() {
		r.mu.Lock()
		sttStream, vadStream, turnStream, interruptionStream, sttInput := r.sttStream, r.vadStream, r.turnStream, r.interruptionStream, r.sttInput
		r.mu.Unlock()
		if sttStream != nil {
			_ = sttStream.EndInput()
		}
		if vadStream != nil {
			_ = vadStream.EndInput()
		}
		if sttInput != nil {
			_ = sttInput.Close()
		}
		if turnStream != nil {
			_ = turnStream.EndInput(r.ctx)
		}
		if interruptionStream != nil {
			_ = interruptionStream.EndInput(r.ctx)
		}
		_ = r.signal.Send(r.ctx, recognitionSignal{kind: recognitionInputEnded})
	}()
	batch := r.opts.STT != nil && !r.opts.STT.Capabilities().Streaming && r.opts.STTNode == nil
	for {
		frame, err := source.Recv(r.ctx)
		if err != nil {
			if !errors.Is(err, io.EOF) && context.Cause(r.ctx) == nil {
				r.sendFailure(err, source)
			}
			return
		}
		if frame.SampleRate <= 0 || frame.Channels <= 0 || frame.SamplesPerChannel <= 0 {
			r.sendFailure(agents.ErrInvalidAudioFormat, source)
			continue
		}
		if batch {
			if err := r.signal.Send(r.ctx, recognitionSignal{kind: recognitionFrame, frame: frame}); err != nil {
				return
			}
		}
		r.mu.Lock()
		sttInput, sttStream, vadStream, turnStream, interruptionStream := r.sttInput, r.sttStream, r.vadStream, r.turnStream, r.interruptionStream
		r.mu.Unlock()
		if sttInput != nil {
			if err := sttInput.Send(r.ctx, frame); err != nil {
				r.mu.Lock()
				current := r.sttInput == sttInput
				r.mu.Unlock()
				if current {
					return
				}
			}
		}
		if sttStream != nil {
			if err := sttStream.Push(r.ctx, frame); err != nil {
				r.mu.Lock()
				current := sameInterface(r.sttStream, sttStream)
				r.mu.Unlock()
				if current {
					r.sendFailure(err, r.opts.STT)
					return
				}
			}
		}
		if vadStream != nil {
			if err := vadStream.Push(r.ctx, frame); err != nil {
				r.mu.Lock()
				current := sameInterface(r.vadStream, vadStream)
				r.mu.Unlock()
				if current {
					r.sendFailure(err, r.opts.VAD)
					return
				}
			}
		}
		if turnStream != nil {
			if err := turnStream.PushAudio(r.ctx, frame); err != nil {
				r.sendFailure(err, r.opts.TurnDetector)
				return
			}
		}
		if interruptionStream != nil {
			if err := interruptionStream.PushAudio(r.ctx, frame); err != nil {
				r.sendFailure(err, r.opts.InterruptionDetector)
				return
			}
		}
		// Feed local detection first so a slow realtime transport cannot delay the
		// current frame's VAD/STT interruption decision. Backpressure remains
		// bounded by the provider streams and the single audio pump.
		if r.opts.AudioSink != nil {
			if err := r.opts.AudioSink(r.ctx, frame); err != nil {
				r.sendFailure(err, r.opts.AudioSink)
				return
			}
		}
	}
}

func (r *AudioRecognition) readSTT(ctx context.Context, source stt.SpeechStream, epoch uint64) {
	defer r.wg.Done()
	defer r.recoverPanic("STT reader", r.opts.STT)
	for {
		event, err := source.Recv(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				_ = r.signal.Send(r.ctx, recognitionSignal{kind: recognitionSTTEnded, epoch: epoch})
			}
			if !errors.Is(err, io.EOF) && context.Cause(ctx) == nil && context.Cause(r.ctx) == nil {
				r.sendFailure(err, r.opts.STT)
			}
			return
		}
		if err := r.signal.Send(r.ctx, recognitionSignal{kind: recognitionSTT, stt: event, epoch: epoch}); err != nil {
			return
		}
	}
}

func (r *AudioRecognition) readSTTNode(ctx context.Context, source stream.Reader[STTNodeItem], epoch uint64) {
	defer r.wg.Done()
	defer r.recoverPanic("STT node reader", source)
	for {
		item, err := source.Recv(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				_ = r.signal.Send(r.ctx, recognitionSignal{kind: recognitionSTTEnded, epoch: epoch})
			}
			if !errors.Is(err, io.EOF) && context.Cause(ctx) == nil && context.Cause(r.ctx) == nil {
				r.sendFailure(err, source)
			}
			return
		}
		var event stt.SpeechEvent
		if item.Event != nil {
			event = *item.Event
		} else if item.Text != "" {
			event = stt.SpeechEvent{Type: stt.FinalTranscript, Alternatives: []stt.SpeechData{{Text: item.Text}}}
		} else {
			continue
		}
		if err := r.signal.Send(r.ctx, recognitionSignal{kind: recognitionSTT, stt: event, epoch: epoch}); err != nil {
			return
		}
	}
}

func (r *AudioRecognition) readVAD(ctx context.Context, source vad.VADStream, epoch uint64) {
	defer r.wg.Done()
	defer r.recoverPanic("VAD reader", r.opts.VAD)
	for {
		event, err := source.Recv(ctx)
		if err != nil {
			if !errors.Is(err, io.EOF) && context.Cause(ctx) == nil && context.Cause(r.ctx) == nil {
				r.sendFailure(err, r.opts.VAD)
			}
			return
		}
		if err := r.signal.Send(r.ctx, recognitionSignal{kind: recognitionVAD, vad: event, epoch: epoch}); err != nil {
			return
		}
	}
}

func (r *AudioRecognition) sendFailure(err error, source any) {
	_ = r.signal.Send(r.ctx, recognitionSignal{kind: recognitionFailure, err: err, source: source})
}

func (r *AudioRecognition) recoverPanic(operation string, source any) {
	if recovered := recover(); recovered != nil {
		err := &ActivityPanicError{Operation: operation, Value: recovered, Stack: debug.Stack()}
		if callback := r.opts.Callbacks.OnError; callback != nil {
			_ = guardedActivityCall("audio recognition error callback", func() error { callback(err, source); return nil })
		}
		r.cancel(err)
	}
}

type recognitionTurnState struct {
	startedAt       time.Time
	endedAt         time.Time
	final           string
	interim         string
	language        agents.LanguageCode
	speaker         string
	confidence      float64
	frames          []agents.AudioFrame
	speaking        bool
	committed       bool
	audioCommitted  bool
	replyCommitted  bool
	interrupted     bool
	exceeded        bool
	agentSpeaking   bool
	agentStartedAt  time.Time
	overlap         bool
	adaptivePending bool
	commitPending   bool
	heldInterim     *stt.SpeechEvent
	heldFinal       []stt.SpeechEvent
	prediction      uint64
}

func (r *AudioRecognition) pumpSignals(output chan<- recognitionSignal) {
	defer r.wg.Done()
	defer close(output)
	for {
		signal, err := r.signal.Recv(r.ctx)
		if err != nil {
			return
		}
		select {
		case output <- signal:
		case <-r.ctx.Done():
			return
		}
	}
}

func (r *AudioRecognition) run(input <-chan recognitionSignal) {
	defer r.wg.Done()
	defer r.recoverPanic("recognition actor", r)
	state := recognitionTurnState{}
	epoch := r.epoch.Load()
	inputEnded := false
	var endpointTimer, transcriptTimer, limitTimer *time.Timer
	var endpointC, transcriptC, limitC <-chan time.Time
	stopTimer := func(timer **time.Timer, channel *<-chan time.Time) {
		if *timer != nil {
			if !(*timer).Stop() {
				select {
				case <-(*timer).C:
				default:
				}
			}
		}
		*channel = nil
	}
	resetTimer := func(timer **time.Timer, channel *<-chan time.Time, delay time.Duration) {
		stopTimer(timer, channel)
		if *timer == nil {
			*timer = time.NewTimer(delay)
		} else {
			(*timer).Reset(delay)
		}
		*channel = (*timer).C
	}
	defer stopTimer(&endpointTimer, &endpointC)
	defer stopTimer(&transcriptTimer, &transcriptC)
	defer stopTimer(&limitTimer, &limitC)
	setBusy := func(value bool) {
		changed := false
		if value {
			changed = r.turnBusy.CompareAndSwap(false, true)
		} else {
			changed = r.turnBusy.CompareAndSwap(true, false)
		}
		if changed {
			if callback := r.opts.Callbacks.OnActivityChanged; callback != nil {
				callback()
			}
		}
	}
	turnSnapshot := func(at time.Time) RecognizedTurn {
		if at.IsZero() {
			at = time.Now()
		}
		started := state.startedAt
		if started.IsZero() {
			started = at
		}
		return RecognizedTurn{Transcript: strings.TrimSpace(firstNonEmpty(state.final, state.interim)), Language: state.language,
			SpeakerID: state.speaker, Confidence: state.confidence, SpeechStarted: started, SpeechEnded: at, SpeechDuration: max(0, at.Sub(started))}
	}
	checkExceeded := func(at time.Time) {
		if state.exceeded {
			return
		}
		limit := r.opts.UserTurnLimit
		words := wordCount(firstNonEmpty(state.final, state.interim))
		duration := time.Duration(0)
		if !state.startedAt.IsZero() {
			duration = max(0, at.Sub(state.startedAt))
		}
		if limit.MaxWords != nil && words >= *limit.MaxWords || limit.MaxDuration != nil && duration >= *limit.MaxDuration {
			state.exceeded = true
			stopTimer(&limitTimer, &limitC)
			if callback := r.opts.Callbacks.OnUserTurnExceeded; callback != nil {
				callback(turnSnapshot(at))
			}
		}
	}
	scheduleEndpoint := func(delay time.Duration) {
		if !state.endedAt.IsZero() {
			delay = max(0, time.Until(state.endedAt.Add(delay)))
		}
		resetTimer(&endpointTimer, &endpointC, delay)
	}
	beginEOTPrediction := func() {
		r.mu.Lock()
		turnStream := r.turnStream
		r.mu.Unlock()
		if turnStream == nil {
			scheduleEndpoint(r.opts.Endpointing.MinDelay())
			return
		}
		if state.language != "" && !turnStream.SupportsLanguage(state.language) {
			scheduleEndpoint(r.opts.Endpointing.MinDelay())
			return
		}
		prediction, err := turnStream.BeginPrediction(r.ctx)
		if err != nil {
			r.sendFailure(err, r.opts.TurnDetector)
			scheduleEndpoint(r.opts.Endpointing.MinDelay())
			return
		}
		state.prediction++
		sequence := state.prediction
		if !r.addWorkers(1) {
			return
		}
		go func() {
			defer r.wg.Done()
			predictionCtx, cancel := context.WithTimeout(r.ctx, turnStream.PredictionTimeout())
			event, predictionErr := prediction.Wait(predictionCtx)
			cancel()
			_ = r.signal.Send(r.ctx, recognitionSignal{kind: recognitionEOT, eot: event, err: predictionErr, source: r.opts.TurnDetector, prediction: sequence})
		}()
	}

	commit := func() error {
		stopTimer(&endpointTimer, &endpointC)
		stopTimer(&transcriptTimer, &transcriptC)
		// Adaptive interruption owns the overlap verdict. In particular, do not
		// commit client-side realtime audio before it decides whether the input
		// was a real interruption or a backchannel.
		if state.adaptivePending {
			state.commitPending = true
			return nil
		}
		if !state.audioCommitted {
			if callback := r.opts.Callbacks.OnCommitAudioResult; callback != nil {
				if err := callback(); err != nil {
					return err
				}
			}
			if callback := r.opts.Callbacks.OnCommitAudio; callback != nil {
				callback()
			}
			state.audioCommitted = true
		}
		// A manual commit may arrive while the provider still owes a terminal
		// result. Preserve its latest interim transcript rather than generating an
		// empty turn; a later final is ignored by the committed guard.
		text := strings.TrimSpace(firstNonEmpty(state.final, state.interim))
		if state.committed {
			return nil
		}
		complete := func(shouldReply bool) {
			if state.replyCommitted {
				return
			}
			state.replyCommitted = true
			if callback := r.opts.Callbacks.OnCommitComplete; callback != nil {
				callback(shouldReply)
			}
		}
		flushBoundary := func() error {
			r.mu.Lock()
			turnStream := r.turnStream
			interruptionStream := r.interruptionStream
			r.mu.Unlock()
			var errs []error
			if turnStream != nil {
				if err := turnStream.Flush(r.ctx, "turn committed"); err != nil && !errors.Is(err, stream.ErrClosed) {
					errs = append(errs, err)
				}
			}
			if interruptionStream != nil {
				if err := interruptionStream.Flush(r.ctx); err != nil && !errors.Is(err, stream.ErrClosed) {
					errs = append(errs, err)
				}
			}
			return errors.Join(errs...)
		}
		if text == "" {
			state.committed = true
			complete(true)
			setBusy(false)
			return flushBoundary()
		}
		state.committed = true
		stopTimer(&limitTimer, &limitC)
		ended := state.endedAt
		if ended.IsZero() {
			ended = time.Now()
		}
		started := state.startedAt
		if started.IsZero() {
			started = ended
		}
		turn := RecognizedTurn{Transcript: text, Language: state.language, SpeakerID: state.speaker, Confidence: state.confidence,
			SpeechStarted: started, SpeechEnded: ended, SpeechDuration: max(0, ended.Sub(started))}
		shouldReply := true
		if callback := r.opts.Callbacks.OnCommitDecision; callback != nil {
			shouldReply = callback(turn)
		}
		if callback := r.opts.Callbacks.OnCommit; callback != nil {
			callback(turn)
		}
		complete(shouldReply)
		setBusy(false)
		return flushBoundary()
	}
	commitAndReport := func() {
		if err := commit(); err != nil {
			if callback := r.opts.Callbacks.OnError; callback != nil {
				callback(err, r.opts.AudioSink)
			}
		}
	}
	startSpeech := func(at time.Time) {
		if at.IsZero() {
			at = time.Now()
		}
		if state.speaking {
			return
		}
		if state.committed {
			agentSpeaking, agentStartedAt := state.agentSpeaking, state.agentStartedAt
			state = recognitionTurnState{agentSpeaking: agentSpeaking, agentStartedAt: agentStartedAt}
		}
		r.lastFinal.Store(0)
		setBusy(true)
		state.speaking, state.startedAt, state.endedAt = true, at, time.Time{}
		stopTimer(&endpointTimer, &endpointC)
		stopTimer(&transcriptTimer, &transcriptC)
		r.mu.Lock()
		turnStream := r.turnStream
		interruptionStream := r.interruptionStream
		r.mu.Unlock()
		if turnStream != nil {
			turnStream.CancelInference(false)
		}
		if state.agentSpeaking && interruptionStream != nil {
			state.overlap, state.adaptivePending = true, true
			if err := interruptionStream.OverlapSpeechStarted(r.ctx, 0, at); err != nil {
				r.sendFailure(err, r.opts.InterruptionDetector)
			}
		}
		if r.opts.UserTurnLimit.MaxDuration != nil {
			resetTimer(&limitTimer, &limitC, *r.opts.UserTurnLimit.MaxDuration)
		}
		if callback := r.opts.Callbacks.OnStartOfSpeech; callback != nil {
			callback(at)
		}
	}
	endSpeech := func(at time.Time) {
		if at.IsZero() {
			at = time.Now()
		}
		if !state.speaking && !state.endedAt.IsZero() {
			return
		}
		state.speaking, state.endedAt = false, at
		if callback := r.opts.Callbacks.OnEndOfSpeech; callback != nil {
			callback(at)
		}
		if r.opts.TranscriptionTimeout != nil && strings.TrimSpace(state.final) == "" {
			resetTimer(&transcriptTimer, &transcriptC, *r.opts.TranscriptionTimeout)
		}
		if state.overlap {
			r.mu.Lock()
			interruptionStream := r.interruptionStream
			r.mu.Unlock()
			if interruptionStream != nil {
				if err := interruptionStream.OverlapSpeechEnded(r.ctx, at, false); err != nil {
					r.sendFailure(err, r.opts.InterruptionDetector)
				}
			}
		}
		if r.opts.TurnDetection == TurnDetectionVAD {
			beginEOTPrediction()
		}
	}
	releaseAdaptiveTranscript := func() {
		for _, held := range state.heldFinal {
			if alternative, ok := firstAlternative(held); ok && strings.TrimSpace(alternative.Text) != "" {
				state.final = mergeTranscript(state.final, alternative.Text)
				state.language, state.speaker, state.confidence = alternative.Language, alternative.SpeakerID, alternative.Confidence
			}
			if callback := r.opts.Callbacks.OnFinalTranscript; callback != nil {
				callback(held)
			}
		}
		if state.heldInterim != nil && len(state.heldFinal) == 0 {
			if alternative, ok := firstAlternative(*state.heldInterim); ok {
				state.interim = alternative.Text
			}
			if callback := r.opts.Callbacks.OnInterimTranscript; callback != nil {
				callback(*state.heldInterim)
			}
		}
		state.heldFinal, state.heldInterim = nil, nil
	}

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-endpointC:
			endpointC = nil
			commitAndReport()
		case <-transcriptC:
			transcriptC = nil
			if strings.TrimSpace(state.final) == "" && r.opts.Callbacks.OnTranscriptionTimeout != nil {
				r.opts.Callbacks.OnTranscriptionTimeout(max(0, state.endedAt.Sub(state.startedAt)), state.startedAt)
			}
		case at := <-limitC:
			limitC = nil
			checkExceeded(at)
		case signal, ok := <-input:
			if !ok {
				return
			}
			var signalErr error
			switch signal.kind {
			case recognitionFrame:
				state.frames = append(state.frames, signal.frame)
				if len(state.frames) >= r.opts.MaxBufferedFrames {
					r.recognizeBatch(state.frames, false, epoch)
					state.frames = nil
				}
			case recognitionSTT:
				if signal.epoch != epoch {
					continue
				}
				event := signal.stt
				switch event.Type {
				case stt.StartOfSpeech:
					startSpeech(time.Now())
				case stt.InterimTranscript:
					if state.adaptivePending {
						copy := event
						state.heldInterim = &copy
						continue
					}
					if alternative, ok := firstAlternative(event); ok {
						state.interim = alternative.Text
					}
					if callback := r.opts.Callbacks.OnInterimTranscript; callback != nil {
						callback(event)
					}
					r.maybeInterrupt(&state)
					checkExceeded(time.Now())
				case stt.FinalTranscript, stt.PreflightTranscript:
					if state.committed {
						continue
					}
					if state.adaptivePending {
						state.heldFinal = append(state.heldFinal, event)
						if len(state.heldFinal) > r.opts.QueueCapacity {
							state.heldFinal = state.heldFinal[len(state.heldFinal)-r.opts.QueueCapacity:]
						}
						continue
					}
					if alternative, ok := firstAlternative(event); ok && strings.TrimSpace(alternative.Text) != "" {
						state.final = mergeTranscript(state.final, alternative.Text)
						r.lastFinal.Store(time.Now().UnixNano())
						state.language, state.speaker, state.confidence = alternative.Language, alternative.SpeakerID, alternative.Confidence
						stopTimer(&transcriptTimer, &transcriptC)
						event.Alternatives = append([]stt.SpeechData(nil), event.Alternatives...)
						event.Alternatives[0].Text = state.final
					}
					if callback := r.opts.Callbacks.OnFinalTranscript; callback != nil {
						callback(event)
					}
					if r.opts.TurnDetection == TurnDetectionVAD && !state.endedAt.IsZero() {
						beginEOTPrediction()
					}
					r.maybeInterrupt(&state)
					checkExceeded(time.Now())
				case stt.EndOfSpeech:
					endSpeech(time.Now())
					if r.opts.TurnDetection == TurnDetectionSTT || inputEnded {
						commitAndReport()
					}
				}
			case recognitionVAD:
				if signal.epoch != epoch {
					continue
				}
				event := signal.vad
				switch event.Type {
				case vad.StartOfSpeech:
					startSpeech(event.Timestamp)
				case vad.InferenceDone:
					r.maybeInterruptDuration(&state, event.SpeechDuration)
					checkExceeded(time.Now())
				case vad.EndOfSpeech:
					endSpeech(event.Timestamp)
					if r.opts.STT != nil && !r.opts.STT.Capabilities().Streaming && r.opts.STTNode == nil {
						r.recognizeBatch(state.frames, true, epoch)
						state.frames = nil
					}
				}
			case recognitionInputEnded:
				inputEnded = true
				batchSTT := r.opts.STT != nil && !r.opts.STT.Capabilities().Streaming && r.opts.STTNode == nil
				if batchSTT {
					// A zero-frame final marker still matters when the preceding
					// bounded chunk ended exactly at MaxBufferedFrames.
					r.recognizeBatch(state.frames, true, epoch)
					state.frames = nil
				}
				if !batchSTT && (r.opts.STT == nil && r.opts.STTNode == nil || strings.TrimSpace(state.final) != "") {
					commitAndReport()
				}
			case recognitionManualCommit:
				if r.opts.STT != nil && !r.opts.STT.Capabilities().Streaming && r.opts.STTNode == nil && len(state.frames) != 0 {
					r.recognizeBatch(state.frames, true, epoch)
					state.frames = nil
				} else {
					signalErr = commit()
				}
			case recognitionClear:
				agentSpeaking, agentStartedAt := state.agentSpeaking, state.agentStartedAt
				state = recognitionTurnState{agentSpeaking: agentSpeaking, agentStartedAt: agentStartedAt}
				epoch = signal.epoch
				r.lastFinal.Store(0)
				setBusy(false)
				inputEnded = false
				stopTimer(&endpointTimer, &endpointC)
				stopTimer(&transcriptTimer, &transcriptC)
				stopTimer(&limitTimer, &limitC)
			case recognitionFailure:
				if callback := r.opts.Callbacks.OnError; callback != nil {
					callback(signal.err, signal.source)
				}
			case recognitionSTTEnded:
				if signal.epoch != epoch {
					continue
				}
				if inputEnded {
					commitAndReport()
				}
			case recognitionEOT:
				if signal.prediction != state.prediction {
					continue
				}
				if signal.err != nil {
					if !errors.Is(signal.err, context.Canceled) && !errors.Is(signal.err, context.DeadlineExceeded) {
						r.sendFailure(signal.err, signal.source)
					}
					scheduleEndpoint(r.opts.Endpointing.MinDelay())
					break
				}
				r.mu.Lock()
				turnStream := r.turnStream
				r.mu.Unlock()
				threshold, supported := 0.0, false
				if turnStream != nil {
					threshold, supported = turnStream.UnlikelyThreshold(state.language)
				}
				delay := r.opts.Endpointing.MinDelay()
				if supported && signal.eot.EndOfTurnProbability < threshold {
					delay = r.opts.Endpointing.MaxDelay()
				}
				if supported {
					predictionDelay := time.Duration(0)
					if !state.endedAt.IsZero() {
						predictionDelay = max(0, time.Since(state.endedAt))
					}
					if callback := r.opts.Callbacks.OnEOTPrediction; callback != nil {
						callback(signal.eot, threshold, predictionDelay)
					}
				}
				scheduleEndpoint(delay)
			case recognitionOverlap:
				event := signal.overlap
				if callback := r.opts.Callbacks.OnOverlappingSpeech; callback != nil {
					callback(event)
				}
				interruption := event.IsInterruption
				if !interruption && r.opts.Interruption.BackchannelBoundary != nil && !state.agentStartedAt.IsZero() &&
					event.DetectedAt.Sub(state.agentStartedAt) < r.opts.Interruption.BackchannelBoundary.Start {
					interruption = true
				}
				state.adaptivePending, state.overlap = false, false
				if interruption {
					releaseAdaptiveTranscript()
					words := wordCount(firstNonEmpty(state.final, state.interim))
					if !state.interrupted && words >= r.opts.Interruption.MinWords {
						state.interrupted = true
						if callback := r.opts.Callbacks.OnInterruption; callback != nil {
							callback(max(0, event.DetectedAt.Sub(state.startedAt)), words)
						}
					}
					if state.commitPending {
						state.commitPending = false
						commitAndReport()
					}
				} else {
					turn := turnSnapshot(event.DetectedAt)
					if callback := r.opts.Callbacks.OnBackchannel; callback != nil {
						callback(turn)
					}
					agentSpeaking, agentStartedAt := state.agentSpeaking, state.agentStartedAt
					state = recognitionTurnState{agentSpeaking: agentSpeaking, agentStartedAt: agentStartedAt}
					setBusy(false)
					stopTimer(&endpointTimer, &endpointC)
					stopTimer(&transcriptTimer, &transcriptC)
					stopTimer(&limitTimer, &limitC)
				}
			case recognitionAgentStarted:
				if !state.agentSpeaking {
					state.agentSpeaking, state.agentStartedAt = true, signal.at
					r.mu.Lock()
					interruptionStream := r.interruptionStream
					r.mu.Unlock()
					if interruptionStream != nil {
						if err := interruptionStream.AgentSpeechStarted(r.ctx); err != nil {
							r.sendFailure(err, r.opts.InterruptionDetector)
						}
						if state.speaking && !state.overlap {
							state.overlap, state.adaptivePending = true, true
							if err := interruptionStream.OverlapSpeechStarted(r.ctx, 0, state.startedAt); err != nil {
								r.sendFailure(err, r.opts.InterruptionDetector)
							}
						}
					}
				}
			case recognitionAgentEnded:
				if state.agentSpeaking {
					state.agentSpeaking = false
					r.mu.Lock()
					interruptionStream := r.interruptionStream
					r.mu.Unlock()
					if interruptionStream != nil {
						if state.overlap {
							_ = interruptionStream.OverlapSpeechEnded(r.ctx, signal.at, true)
						}
						if err := interruptionStream.AgentSpeechEnded(r.ctx); err != nil {
							r.sendFailure(err, r.opts.InterruptionDetector)
						}
					}
				}
			case recognitionDisableAdaptive:
				state.adaptivePending, state.overlap = false, false
				// An infrastructure failure is not evidence that the user utterance
				// was a backchannel. Preserve already-recognized input, then fall
				// through to the normal VAD interruption/commit path.
				releaseAdaptiveTranscript()
				r.maybeInterrupt(&state)
				if state.commitPending {
					state.commitPending = false
					commitAndReport()
				}
			}
			if signal.done != nil {
				signal.done <- signalErr
				close(signal.done)
			}
		}
	}
}

func (r *AudioRecognition) recognizeBatch(frames []agents.AudioFrame, final bool, epoch uint64) {
	if len(frames) == 0 && !final || r.batch == nil {
		return
	}
	copyFrames := append([]agents.AudioFrame(nil), frames...)
	if err := r.batch.Send(r.ctx, batchRecognitionRequest{frames: copyFrames, final: final, epoch: epoch}); err != nil && context.Cause(r.ctx) == nil {
		r.sendFailure(err, r.opts.STT)
	}
}

func (r *AudioRecognition) batchWorker() {
	defer r.wg.Done()
	defer r.recoverPanic("batch STT worker", r.opts.STT)
	for {
		request, err := r.batch.Recv(r.ctx)
		if err != nil {
			return
		}
		if len(request.frames) != 0 {
			event, err := r.opts.STT.Recognize(r.ctx, request.frames, stt.RecognizeOptions{ConnectOptions: r.opts.ConnectOptions})
			if err != nil {
				r.sendFailure(err, r.opts.STT)
			} else {
				if event.Type == 0 {
					event.Type = stt.FinalTranscript
				}
				_ = r.signal.Send(r.ctx, recognitionSignal{kind: recognitionSTT, stt: event, epoch: request.epoch, batch: true})
			}
		}
		if request.final {
			_ = r.signal.Send(r.ctx, recognitionSignal{kind: recognitionSTT, stt: stt.SpeechEvent{Type: stt.EndOfSpeech}, epoch: request.epoch, batch: true})
		}
	}
}

func (r *AudioRecognition) maybeInterrupt(state *recognitionTurnState) {
	duration := time.Duration(0)
	if !state.startedAt.IsZero() {
		duration = time.Since(state.startedAt)
	}
	r.maybeInterruptDuration(state, duration)
}

// Busy reports whether a user turn is open, including pending EOT/adaptive
// decisions after VAD has returned to listening.
func (r *AudioRecognition) Busy() bool { return r.turnBusy.Load() }

func (r *AudioRecognition) maybeInterruptDuration(state *recognitionTurnState, duration time.Duration) {
	r.mu.Lock()
	adaptive := r.interruptionStream != nil
	r.mu.Unlock()
	if adaptive {
		return
	}
	if state.interrupted || !r.opts.Interruption.Enabled || duration < r.opts.Interruption.MinDuration {
		return
	}
	words := wordCount(firstNonEmpty(state.final, state.interim))
	if words < r.opts.Interruption.MinWords {
		return
	}
	state.interrupted = true
	if callback := r.opts.Callbacks.OnInterruption; callback != nil {
		callback(duration, words)
	}
}

// AgentSpeechStarted marks audible agent playout for adaptive overlap
// classification. It is serialized with recognition state and is a no-op when
// adaptive interruption is not active.
func (r *AudioRecognition) AgentSpeechStarted(ctx context.Context, at time.Time) error {
	if at.IsZero() {
		at = time.Now()
	}
	return r.sendControl(ctx, recognitionSignal{kind: recognitionAgentStarted, at: at})
}

// AgentSpeechEnded closes an adaptive overlap interval and resets the detector
// for the next audible agent segment.
func (r *AudioRecognition) AgentSpeechEnded(ctx context.Context, at time.Time) error {
	if at.IsZero() {
		at = time.Now()
	}
	return r.sendControl(ctx, recognitionSignal{kind: recognitionAgentEnded, at: at})
}

// DisableAdaptiveInterruption atomically falls back to the VAD interruption
// gate after an unrecoverable adaptive-detector error.
func (r *AudioRecognition) DisableAdaptiveInterruption(ctx context.Context) error {
	r.mu.Lock()
	value, cancel := r.interruptionStream, r.interruptionCancel
	r.interruptionStream, r.interruptionCancel = nil, nil
	r.mu.Unlock()
	if cancel != nil {
		cancel(errors.New("adaptive interruption disabled"))
	}
	if value != nil {
		_ = value.Close()
	}
	return r.sendControl(ctx, recognitionSignal{kind: recognitionDisableAdaptive})
}

func firstAlternative(event stt.SpeechEvent) (stt.SpeechData, bool) {
	if len(event.Alternatives) == 0 {
		return stt.SpeechData{}, false
	}
	return event.Alternatives[0], true
}

func mergeTranscript(current, next string) string {
	current, next = strings.TrimSpace(current), strings.TrimSpace(next)
	if current == "" || next == current {
		return next
	}
	if strings.HasPrefix(next, current) {
		return next
	}
	if strings.HasSuffix(current, next) {
		return current
	}
	return current + " " + next
}

func wordCount(text string) int {
	return len(strings.FieldsFunc(text, func(r rune) bool { return unicode.IsSpace(r) }))
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func (r *AudioRecognition) sendControl(ctx context.Context, signal recognitionSignal) error {
	if ctx == nil {
		ctx = context.Background()
	}
	signal.done = make(chan error, 1)
	if err := r.signal.Send(ctx, signal); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case err := <-signal.done:
		return err
	}
}

func (r *AudioRecognition) stopSTTPipeline(cause error) error {
	r.mu.Lock()
	value, input, cancel := r.sttStream, r.sttInput, r.sttCancel
	r.sttStream, r.sttInput, r.sttCancel = nil, nil, nil
	r.mu.Unlock()
	if cancel != nil {
		cancel(cause)
	}
	var errs []error
	if value != nil {
		if err := guardedActivityCall("reset STT stream close", value.Close); err != nil {
			errs = append(errs, err)
		}
	}
	if input != nil {
		_ = input.Abort(cause)
	}
	return errors.Join(errs...)
}

func (r *AudioRecognition) stopVADPipeline(cause error) error {
	r.mu.Lock()
	value, cancel := r.vadStream, r.vadCancel
	r.vadStream, r.vadCancel = nil, nil
	r.mu.Unlock()
	if cancel != nil {
		cancel(cause)
	}
	if value == nil {
		return nil
	}
	return guardedActivityCall("reset VAD stream close", value.Close)
}

func (r *AudioRecognition) Commit(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	r.mu.Lock()
	sttStream, sttInput, vadStream, turnStream, interruptionStream := r.sttStream, r.sttInput, r.vadStream, r.turnStream, r.interruptionStream
	r.mu.Unlock()
	var errs []error
	if sttStream != nil {
		if err := sttStream.Flush(ctx); err != nil {
			errs = append(errs, fmt.Errorf("flush STT stream: %w", err))
		}
	}
	if vadStream != nil {
		if err := vadStream.Flush(ctx); err != nil {
			errs = append(errs, fmt.Errorf("flush VAD stream: %w", err))
		}
	}
	if turnStream != nil {
		if err := turnStream.Flush(ctx, "manual commit"); err != nil {
			errs = append(errs, fmt.Errorf("flush turn detector stream: %w", err))
		}
	}
	if interruptionStream != nil {
		if err := interruptionStream.Flush(ctx); err != nil {
			errs = append(errs, fmt.Errorf("flush interruption stream: %w", err))
		}
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}
	if sttStream != nil || sttInput != nil {
		lastFinal := time.Unix(0, r.lastFinal.Load())
		remaining := manualCommitTranscriptionDelay - time.Since(lastFinal)
		if r.lastFinal.Load() == 0 || remaining > 0 {
			if remaining <= 0 || remaining > manualCommitTranscriptionDelay {
				remaining = manualCommitTranscriptionDelay
			}
			timer := time.NewTimer(remaining)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return context.Cause(ctx)
			case <-r.ctx.Done():
				return context.Cause(r.ctx)
			case <-timer.C:
			}
		}
	}
	return r.sendControl(ctx, recognitionSignal{kind: recognitionManualCommit, epoch: r.epoch.Load()})
}

func (r *AudioRecognition) Clear(ctx context.Context) error {
	r.resetMu.Lock()
	defer r.resetMu.Unlock()
	epoch := r.epoch.Add(1)
	r.mu.Lock()
	turnStream, interruptionStream := r.turnStream, r.interruptionStream
	r.mu.Unlock()
	var boundaryErrs []error
	if turnStream != nil {
		if err := turnStream.Flush(ctx, "user turn cleared"); err != nil {
			boundaryErrs = append(boundaryErrs, err)
		}
	}
	if interruptionStream != nil {
		if err := interruptionStream.Flush(ctx); err != nil {
			boundaryErrs = append(boundaryErrs, err)
		}
	}
	if err := r.sendControl(ctx, recognitionSignal{kind: recognitionClear, epoch: epoch}); err != nil {
		return errors.Join(append(boundaryErrs, err)...)
	}
	resetCause := errors.New("audio recognition user turn cleared")
	errs := boundaryErrs
	if err := r.stopSTTPipeline(resetCause); err != nil {
		errs = append(errs, err)
	}
	if err := r.stopVADPipeline(resetCause); err != nil {
		errs = append(errs, err)
	}
	r.mu.Lock()
	closed := r.closed
	r.mu.Unlock()
	if !closed {
		if err := r.startSTTPipeline(epoch); err != nil {
			errs = append(errs, fmt.Errorf("restart STT pipeline: %w", err))
		}
		if err := r.startVADPipeline(epoch); err != nil {
			errs = append(errs, fmt.Errorf("restart VAD pipeline: %w", err))
		}
	}
	return errors.Join(errs...)
}

func (r *AudioRecognition) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	r.closeOnce.Do(func() {
		r.resetMu.Lock()
		defer r.resetMu.Unlock()
		r.mu.Lock()
		r.closed = true
		sttStream, vadStream, turnStream, interruptionStream, sttInput, batch := r.sttStream, r.vadStream, r.turnStream, r.interruptionStream, r.sttInput, r.batch
		sttCancel, vadCancel, turnCancel, interruptionCancel := r.sttCancel, r.vadCancel, r.turnCancel, r.interruptionCancel
		r.sttStream, r.vadStream, r.turnStream, r.interruptionStream, r.sttInput = nil, nil, nil, nil, nil
		r.sttCancel, r.vadCancel, r.turnCancel, r.interruptionCancel = nil, nil, nil, nil
		r.mu.Unlock()
		r.cancel(ErrRecognitionClosed)
		var errs []error
		if sttCancel != nil {
			sttCancel(ErrRecognitionClosed)
		}
		if vadCancel != nil {
			vadCancel(ErrRecognitionClosed)
		}
		if turnCancel != nil {
			turnCancel(ErrRecognitionClosed)
		}
		if interruptionCancel != nil {
			interruptionCancel(ErrRecognitionClosed)
		}
		if sttStream != nil {
			if err := guardedActivityCall("STT stream close", sttStream.Close); err != nil {
				errs = append(errs, err)
			}
		}
		if vadStream != nil {
			if err := guardedActivityCall("VAD stream close", vadStream.Close); err != nil {
				errs = append(errs, err)
			}
		}
		if turnStream != nil {
			if err := turnStream.Close(); err != nil {
				errs = append(errs, err)
			}
		}
		if interruptionStream != nil {
			if err := interruptionStream.Close(); err != nil {
				errs = append(errs, err)
			}
		}
		if sttInput != nil {
			_ = sttInput.Abort(ErrRecognitionClosed)
		}
		if batch != nil {
			_ = batch.Abort(ErrRecognitionClosed)
		}
		_ = r.signal.Close()
		r.mu.Lock()
		r.closeErr = errors.Join(r.closeErr, errors.Join(errs...))
		r.mu.Unlock()
	})
	r.waitOnce.Do(func() {
		go func() {
			r.wg.Wait()
			close(r.done)
		}()
	})
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-r.done:
		r.mu.Lock()
		err := r.closeErr
		r.mu.Unlock()
		return err
	}
}

func (a *agentActivity[UserData]) attachAudioInput(source AudioInput) {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return
	}
	a.attachWG.Add(1)
	a.mu.Unlock()
	defer a.attachWG.Done()

	a.recognitionMu.Lock()
	defer a.recognitionMu.Unlock()

	a.mu.Lock()
	old := a.recognition
	a.recognition = nil
	models, turn := a.models, a.turn
	realtime, realtimeAudio := a.realtime, a.realtimeAudio
	a.realtimeAudio = false
	a.mu.Unlock()
	if old != nil {
		closeCtx, cancel := context.WithTimeout(context.Background(), a.session.opts.shutdownTimeout)
		_ = old.Close(closeCtx)
		cancel()
	}
	if realtime != nil && realtimeAudio {
		detachCtx, cancel := context.WithTimeout(a.ctx, a.session.opts.shutdownTimeout)
		if err := realtime.SetInputAudioStream(detachCtx, nil); err != nil && context.Cause(a.ctx) == nil {
			a.session.emitError(fmt.Errorf("detach realtime audio input: %w", err), realtime)
		}
		cancel()
	}
	a.mu.RLock()
	closed := a.closed
	a.mu.RUnlock()
	hasSTTHook := a.agent.hooksSnapshot().STTNode != nil
	realtimeServerTurns := models.realtime != nil && models.realtime.Capabilities().TurnDetection
	recognitionVAD := models.vad
	if models.vadDefault && realtimeServerTurns {
		recognitionVAD = nil
	}
	hasLocalRecognition := models.stt != nil || recognitionVAD != nil || hasSTTHook
	if closed || source == nil || !hasLocalRecognition && realtime == nil {
		return
	}
	if realtime != nil && !hasLocalRecognition {
		if err := realtime.SetInputAudioStream(a.ctx, source); err != nil {
			a.session.emitError(fmt.Errorf("attach realtime audio input: %w", err), realtime)
			return
		}
		a.mu.Lock()
		if !a.closed && a.realtime == realtime {
			a.realtimeAudio = true
		}
		a.mu.Unlock()
		return
	}
	mode, turnDetector, err := a.resolvedTurnDetection(models)
	if err != nil {
		a.session.emitError(err, a.agent)
		return
	}
	if realtimeServerTurns {
		turnDetector = nil
	}
	var timeout *time.Duration
	if a.session.opts.transcriptionTimeoutEnabled {
		value := a.session.opts.transcriptionTimeout
		timeout = &value
	}
	var audioSink func(context.Context, agents.AudioFrame) error
	if realtime != nil {
		audioSink = realtime.PushAudio
	}
	a.mu.RLock()
	adaptive := a.adaptiveDetector
	a.mu.RUnlock()
	recognition, err := NewAudioRecognition(a.ctx, AudioRecognitionOptions{
		STT: models.stt, VAD: recognitionVAD, TurnDetector: turnDetector, InterruptionDetector: adaptive,
		TurnDetection: mode, Endpointing: a.endpointing,
		Interruption: turn.Interruption, ConnectOptions: a.session.opts.connect.STT,
		UserTurnLimit:        turn.UserTurnLimit,
		TranscriptionTimeout: timeout, QueueCapacity: a.session.opts.recognitionQueueCapacity,
		AudioSink: audioSink,
		STTNode: func(ctx context.Context, input stream.Reader[agents.AudioFrame]) (stream.Reader[STTNodeItem], error) {
			var output stream.Reader[STTNodeItem]
			var used bool
			err := guardedActivityCall("STT node", func() (err error) {
				output, used, err = a.agent.runSTTNode(ctx, input, ModelSettings{})
				return err
			})
			if !used {
				return nil, errors.New("no custom STT node")
			}
			return output, err
		},
		Callbacks: a.recognitionCallbacks(),
	})
	if err != nil {
		a.session.emitError(err, a.agent)
		return
	}
	// Only install STTNode when the agent actually overrides it.
	if !hasSTTHook {
		recognition.opts.STTNode = nil
	}
	if err := recognition.Start(source); err != nil {
		a.session.emitError(err, a.agent)
		closeCtx, cancel := context.WithTimeout(context.Background(), a.session.opts.shutdownTimeout)
		_ = recognition.Close(closeCtx)
		cancel()
		return
	}
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		closeCtx, cancel := context.WithTimeout(context.Background(), a.session.opts.shutdownTimeout)
		_ = recognition.Close(closeCtx)
		cancel()
		return
	}
	a.recognition = recognition
	a.mu.Unlock()
	if a.session.AgentState() == AgentStateSpeaking {
		if err := recognition.AgentSpeechStarted(a.ctx, time.Now()); err != nil && context.Cause(a.ctx) == nil {
			a.session.emitError(fmt.Errorf("initialize adaptive agent speech lifecycle: %w", err), recognition)
		}
	}
}

func (a *agentActivity[UserData]) recognitionCallbacks() AudioRecognitionCallbacks {
	realtime := a.realtimeSnapshot()
	realtimeCaps := llm.RealtimeCapabilities{}
	if model := a.modelsSnapshot().realtime; model != nil {
		realtimeCaps = model.Capabilities()
	}
	return AudioRecognitionCallbacks{
		OnStartOfSpeech: func(at time.Time) {
			if realtime != nil && !realtimeCaps.TurnDetection {
				realtime.StartUserActivity()
			}
			overlap := a.session.AgentState() == AgentStateSpeaking
			a.endpointing.OnStartOfSpeech(at, overlap)
			a.session.setUserState(UserStateSpeaking)
		},
		OnEndOfSpeech: func(at time.Time) {
			a.endpointing.OnEndOfSpeech(at, false)
			a.session.setUserState(UserStateListening)
		},
		OnInterimTranscript: func(event stt.SpeechEvent) {
			if !realtimeCaps.UserTranscription {
				a.publishTranscript(event, false)
			}
		},
		OnFinalTranscript: func(event stt.SpeechEvent) {
			if !realtimeCaps.UserTranscription {
				a.publishTranscript(event, true)
			}
			if realtime == nil {
				if alternative, ok := firstAlternative(event); ok {
					a.startPreemptiveReply(a.ctx, alternative.Text, 0)
				}
			}
		},
		OnCommitAudioResult: func() error {
			if realtime != nil && !realtimeCaps.TurnDetection {
				if err := realtime.CommitAudio(a.ctx); err != nil {
					return fmt.Errorf("commit realtime audio: %w", err)
				}
			}
			return nil
		},
		OnCommitDecision: a.commitRecognizedTurn,
		OnCommitComplete: func(shouldReply bool) {
			if shouldReply && realtime != nil && !realtimeCaps.TurnDetection {
				if _, err := a.generateReply(a.ctx, GenerateReplyOptions{InputModality: InputModalityAudio}, true, true, false); err != nil && context.Cause(a.ctx) == nil {
					a.session.emitError(fmt.Errorf("generate reply for committed realtime turn: %w", err), realtime)
				}
			}
		},
		OnInterruption: func(_ time.Duration, _ int) {
			if realtimeCaps.TurnDetection {
				return
			}
			if err := a.interrupt(a.ctx, false); err != nil && !errors.Is(err, ErrNoSpeech) && !errors.Is(err, ErrInterruptionsDisabled) {
				a.session.emitError(err, a.agent)
			}
		},
		OnUserTurnExceeded: func(turn RecognizedTurn) {
			text, words, duration := turn.Transcript, wordCount(turn.Transcript), turn.SpeechDuration
			event := UserTurnExceededEvent{EventBase: newEventBase(EventType("user_turn_exceeded"), time.Now()), Transcript: text,
				AccumulatedTranscript: text, AccumulatedWordCount: words, Duration: duration}
			if err := guardedActivityCall("onUserTurnExceeded", func() error { return a.agent.runOnUserTurnExceeded(a.ctx, event) }); err != nil {
				a.session.emitError(err, a.agent)
			}
		},
		OnTranscriptionTimeout: func(duration time.Duration, started time.Time) {
			_ = a.session.events.Publish(a.ctx, UserTranscriptionTimeoutEvent{EventBase: newEventBase(EventUserTranscriptionTimeout, time.Now()), SpeechDuration: duration, VADSpeechStartedAt: started})
		},
		OnEOTPrediction: func(event inference.TurnDetectionEvent, threshold float64, delay time.Duration) {
			inferenceDuration := time.Duration(0)
			if event.InferenceDuration != nil {
				inferenceDuration = *event.InferenceDuration
			}
			_ = a.session.events.Publish(a.ctx, EOTPredictionEvent{
				EventBase: newEventBase(EventEOTPrediction, time.Now()), Probability: event.EndOfTurnProbability,
				Threshold: threshold, InferenceDuration: inferenceDuration, Delay: delay,
			})
		},
		OnOverlappingSpeech: func(event inference.OverlappingSpeechEvent) {
			probabilities := make([]float64, len(event.Probabilities))
			for index, probability := range event.Probabilities {
				probabilities[index] = float64(probability)
			}
			agentEnded := event.AgentEnded
			_ = a.session.events.Publish(a.ctx, OverlappingSpeechEvent{
				EventBase: newEventBase(EventOverlappingSpeech, time.Now()), DetectedAt: event.DetectedAt,
				Interruption: event.IsInterruption, AgentEnded: &agentEnded, TotalDuration: event.TotalDuration,
				PredictionDuration: event.PredictionDuration, DetectionDelay: event.DetectionDelay,
				OverlapStartedAt: event.OverlapStartedAt, SpeechInput: append([]int16(nil), event.SpeechInput...),
				Probabilities: probabilities, Probability: event.Probability, NumRequests: int(event.NumRequests),
			})
		},
		OnBackchannel: func(RecognizedTurn) {
			if realtime != nil && !realtimeCaps.TurnDetection {
				if err := realtime.ClearAudio(a.ctx); err != nil && context.Cause(a.ctx) == nil {
					a.session.emitError(fmt.Errorf("clear realtime backchannel audio: %w", err), realtime)
				}
			}
		},
		OnActivityChanged: a.session.notifyIdleChange,
		OnError:           func(err error, source any) { a.session.emitError(err, source) },
	}
}

func (a *agentActivity[UserData]) publishTranscript(event stt.SpeechEvent, final bool) {
	alternative, ok := firstAlternative(event)
	if !ok {
		return
	}
	var speaker *string
	if alternative.SpeakerID != "" {
		value := alternative.SpeakerID
		speaker = &value
	}
	var language *agents.LanguageCode
	if alternative.Language != "" {
		value := alternative.Language
		language = &value
	}
	_ = a.session.events.Publish(a.ctx, UserInputTranscribedEvent{
		EventBase: newEventBase(EventUserInputTranscribed, time.Now()), Transcript: alternative.Text, Final: final,
		SpeakerID: speaker, Language: language,
	})
}

func (a *agentActivity[UserData]) commitRecognizedTurn(turn RecognizedTurn) bool {
	if _, ok := a.authorizePreemptiveReply(turn.Transcript); ok {
		return false
	}
	a.cancelPreemptiveReply()
	message := llm.NewChatMessage(llm.RoleUser, turn.Transcript)
	message.TranscriptConfidence = &turn.Confidence
	chat := a.session.ChatContext()
	if err := guardedActivityCall("onUserTurnCompleted", func() error { return a.agent.runOnUserTurnCompleted(a.ctx, chat, message) }); err != nil {
		if !IsStopResponse(err) {
			a.session.emitError(err, a.agent)
		}
		return false
	}
	if a.realtimeSnapshot() != nil {
		// The provider owns realtime conversation insertion and emits its canonical
		// item ID/transcription separately when that capability is enabled. Without
		// it, retain the local STT result in the session history (but do not sync it
		// back to the provider, which already owns the audio turn).
		if model := a.modelsSnapshot().realtime; model != nil && !model.Capabilities().UserTranscription {
			a.session.commitItems(nil, message)
		}
		// Generating another reply here would race the provider response created by
		// CommitAudio; OnCommitComplete schedules it exactly once for client-side
		// turn detection.
		return true
	}
	a.session.commitItems(nil, message)
	if _, err := a.generateReply(a.ctx, GenerateReplyOptions{InputModality: InputModalityAudio}, true, true, false); err != nil {
		a.session.emitError(err, a.agent)
	}
	return false
}
