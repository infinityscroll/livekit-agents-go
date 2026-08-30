// SPDX-License-Identifier: Apache-2.0

package recorderio

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"time"

	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/voice"
)

const (
	// Defaults mirror agents-js v1.7.1 where that implementation defines a
	// value. The byte, frame, and queue limits are Go safety extensions.
	DefaultSampleRate               = 48_000
	DefaultWriteInterval            = 2500 * time.Millisecond
	DefaultClosePlayoutFlushTimeout = 2 * time.Second
	DefaultQueueCapacity            = 8
	DefaultMaxBufferedBytes         = 32 << 20
	DefaultMaxQueuedBytes           = 64 << 20
	DefaultMaxBufferedFrames        = 65_536
	DefaultMaxQueuedFrames          = 131_072
)

var (
	// Lifecycle and bounded-resource errors are stable and can be inspected
	// with errors.Is.
	ErrNotInitialized                  = errors.New("recorderio: RecordInput and RecordOutput must be called before Start")
	ErrAlreadyStarted                  = errors.New("recorderio: recorder is already started")
	ErrClosed                          = errors.New("recorderio: recorder is closed")
	ErrClosing                         = errors.New("recorderio: recorder is closing")
	ErrBufferLimit                     = errors.New("recorderio: bounded recording buffer limit reached")
	ErrPlayoutFlushTimeout             = errors.New("recorderio: final playout did not finish before the flush timeout; unflushed output was dropped")
	ErrEncoderUnavailable              = errors.New("recorderio: PCM encoder factory returned nil")
	ErrInputPaddingSkipped             = errors.New("recorderio: input silence padding exceeded the bounded buffer and was skipped")
	ErrPlaybackNotificationUnavailable = errors.New("recorderio: downstream counted a rejected segment but cannot be notified that playback finished")
)

// PCMEncoder accepts interleaved signed PCM16 stereo in little-endian byte
// order. Implementations must honor ctx and unblock Close after cancellation.
type PCMEncoder interface {
	WritePCM(context.Context, []byte) error
	Close(context.Context) error
}

// PCMEncoderFactory is primarily an integration and deterministic-testing
// seam. The default implementation launches FFmpeg and writes Ogg/Opus.
type PCMEncoderFactory func(context.Context, string, int) (PCMEncoder, error)

// RecorderOptions configures RecorderIO. Zero-valued fields use the package
// defaults unless their field documentation says otherwise.
type RecorderOptions struct {
	// AgentSession is retained for source compatibility with agents-js. The
	// pinned RecorderIO implementation stores it but does not call into it.
	AgentSession any

	// SampleRate is the Ogg/Opus output rate.
	SampleRate int
	// WriteInterval bounds how long input-only audio waits before encoding.
	WriteInterval time.Duration
	// ClosePlayoutFlushTimeout is the final bounded wait for an authoritative
	// playback-finished event. A timeout drops that unplayed agent segment.
	ClosePlayoutFlushTimeout time.Duration
	// QueueCapacity limits input/output batches waiting for the encoder.
	QueueCapacity int
	// MaxBufferedBytes and MaxBufferedFrames bound each input side and the
	// aggregate pending output. Capture applies backpressure or returns
	// ErrBufferLimit instead of growing without bound.
	MaxBufferedBytes  int
	MaxBufferedFrames int
	// MaxQueuedBytes and MaxQueuedFrames bound all batches waiting for the
	// encoder and must fit one full input/output pair.
	MaxQueuedBytes  int
	MaxQueuedFrames int

	// EncoderFactory overrides the lazy FFmpeg Ogg/Opus encoder.
	EncoderFactory PCMEncoderFactory
	// Clock overrides wall time used for playout and pause alignment.
	Clock func() time.Time
	// OnError receives asynchronous recording errors. It may run on an audio
	// or encoder goroutine and should return promptly. Panics are isolated.
	OnError func(error)
}

type resolvedOptions struct {
	sampleRate, queueCapacity, maxBufferedBytes, maxQueuedBytes int
	maxBufferedFrames, maxQueuedFrames                          int
	writeInterval, closePlayoutFlushTimeout                     time.Duration
	encoderFactory                                              PCMEncoderFactory
	clock                                                       func() time.Time
	onError                                                     func(error)
}

func resolveOptions(options RecorderOptions) (resolvedOptions, error) {
	value := resolvedOptions{
		sampleRate: options.SampleRate, writeInterval: options.WriteInterval,
		closePlayoutFlushTimeout: options.ClosePlayoutFlushTimeout,
		queueCapacity:            options.QueueCapacity, maxBufferedBytes: options.MaxBufferedBytes,
		maxQueuedBytes: options.MaxQueuedBytes, encoderFactory: options.EncoderFactory,
		maxBufferedFrames: options.MaxBufferedFrames, maxQueuedFrames: options.MaxQueuedFrames,
		clock: options.Clock, onError: options.OnError,
	}
	if value.sampleRate == 0 {
		value.sampleRate = DefaultSampleRate
	}
	if value.writeInterval == 0 {
		value.writeInterval = DefaultWriteInterval
	}
	if value.closePlayoutFlushTimeout == 0 {
		value.closePlayoutFlushTimeout = DefaultClosePlayoutFlushTimeout
	}
	if value.queueCapacity == 0 {
		value.queueCapacity = DefaultQueueCapacity
	}
	if value.maxBufferedBytes == 0 {
		value.maxBufferedBytes = DefaultMaxBufferedBytes
	}
	if value.maxQueuedBytes == 0 {
		value.maxQueuedBytes = DefaultMaxQueuedBytes
	}
	if value.maxBufferedFrames == 0 {
		value.maxBufferedFrames = DefaultMaxBufferedFrames
	}
	if value.maxQueuedFrames == 0 {
		value.maxQueuedFrames = DefaultMaxQueuedFrames
	}
	if options.QueueCapacity == 0 && value.queueCapacity > value.maxQueuedFrames {
		value.queueCapacity = value.maxQueuedFrames
	}
	if value.encoderFactory == nil {
		value.encoderFactory = newFFmpegEncoder
	}
	if value.clock == nil {
		value.clock = time.Now
	}
	if value.sampleRate <= 0 || value.writeInterval <= 0 || value.closePlayoutFlushTimeout < 0 || value.queueCapacity < 1 || value.maxBufferedBytes < 1 || value.maxQueuedBytes < 1 || value.maxBufferedFrames < 1 || value.maxQueuedFrames < 1 {
		return resolvedOptions{}, errors.New("recorderio: sample rate, intervals, capacities, and byte limits must be positive")
	}
	maxInt := int(^uint(0) >> 1)
	if value.maxBufferedBytes > maxInt/2 || value.maxQueuedBytes < value.maxBufferedBytes*2 {
		return resolvedOptions{}, errors.New("recorderio: MaxQueuedBytes must fit one full input/output pair")
	}
	if value.maxBufferedFrames > maxInt/2 || value.maxQueuedFrames < value.maxBufferedFrames*2 {
		return resolvedOptions{}, errors.New("recorderio: MaxQueuedFrames must fit one full input/output pair")
	}
	if value.queueCapacity > value.maxQueuedFrames {
		return resolvedOptions{}, errors.New("recorderio: QueueCapacity must not exceed MaxQueuedFrames")
	}
	return value, nil
}

type recorderState uint8

const (
	recorderNew recorderState = iota
	recorderRecording
	recorderClosing
	recorderClosed
)

type recordBatch struct {
	input, output []agents.AudioFrame
	bytes, frames int
}

// RecorderIO synchronizes decorated user input and agent output into the left
// and right channels, respectively, of one Ogg/Opus recording.
type RecorderIO struct {
	options resolvedOptions

	mu          sync.Mutex
	state       recorderState
	input       *RecorderAudioInput
	output      *RecorderAudioOutput
	outputPath  string
	queue       *batchQueue
	ctx         context.Context
	cancel      context.CancelCauseFunc
	forwardStop context.CancelFunc
	forwardDone chan struct{}
	encodeDone  chan struct{}
	encodeErr   error
	commitsOpen bool
	closeDone   chan struct{}
	closeErr    error

	commitMu sync.Mutex
}

// NewRecorderIO validates options without starting goroutines, resolving
// FFmpeg, or opening the output file.
func NewRecorderIO(options RecorderOptions) (*RecorderIO, error) {
	resolved, err := resolveOptions(options)
	if err != nil {
		return nil, err
	}
	return &RecorderIO{options: resolved, state: recorderNew}, nil
}

// RecordInput installs and returns the ownership-neutral input decorator.
func (r *RecorderIO) RecordInput(input voice.AudioInput) (*RecorderAudioInput, error) {
	if isNil(input) {
		return nil, errors.New("recorderio: audio input is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state != recorderNew {
		if r.state == recorderClosing {
			return nil, ErrClosing
		}
		if r.state == recorderClosed {
			return nil, ErrClosed
		}
		return nil, ErrAlreadyStarted
	}
	if r.input != nil {
		return nil, errors.New("recorderio: input is already configured")
	}
	r.input = newRecorderAudioInput(r, input)
	return r.input, nil
}

// RecordOutput installs and returns the ownership-neutral output decorator.
// The wrapped output should not also be driven independently.
func (r *RecorderIO) RecordOutput(output voice.AudioOutput) (*RecorderAudioOutput, error) {
	if isNil(output) {
		return nil, errors.New("recorderio: audio output is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state != recorderNew {
		if r.state == recorderClosing {
			return nil, ErrClosing
		}
		if r.state == recorderClosed {
			return nil, ErrClosed
		}
		return nil, ErrAlreadyStarted
	}
	if r.output != nil {
		return nil, errors.New("recorderio: output is already configured")
	}
	r.output = newRecorderAudioOutput(r, output)
	return r.output, nil
}

// Start begins bounded forwarding and encoding. FFmpeg remains lazy until the
// first non-empty PCM batch. RecordInput and RecordOutput must be called first.
func (r *RecorderIO) Start(ctx context.Context, outputPath string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if outputPath == "" {
		return errors.New("recorderio: output path is required")
	}

	r.mu.Lock()
	switch r.state {
	case recorderRecording:
		r.mu.Unlock()
		return ErrAlreadyStarted
	case recorderClosing:
		r.mu.Unlock()
		return ErrClosing
	case recorderClosed:
		r.mu.Unlock()
		return ErrClosed
	}
	if r.input == nil || r.output == nil {
		r.mu.Unlock()
		return ErrNotInitialized
	}
	r.mu.Unlock()

	directory := filepath.Dir(outputPath)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return fmt.Errorf("recorderio: create output directory: %w", err)
	}
	if err := context.Cause(ctx); err != nil {
		return err
	}

	lifetime, cancel := context.WithCancelCause(context.Background())
	forwardCtx, forwardStop := context.WithCancel(lifetime)
	queue := newBatchQueue(r.options.queueCapacity, r.options.maxQueuedBytes, r.options.maxQueuedFrames)
	r.mu.Lock()
	if r.state != recorderNew {
		state := r.state
		r.mu.Unlock()
		forwardStop()
		cancel(ErrClosed)
		switch state {
		case recorderClosing:
			return ErrClosing
		case recorderClosed:
			return ErrClosed
		default:
			return ErrAlreadyStarted
		}
	}
	r.ctx, r.cancel, r.forwardStop = lifetime, cancel, forwardStop
	r.queue, r.outputPath = queue, outputPath
	r.forwardDone, r.encodeDone = make(chan struct{}), make(chan struct{})
	r.closeDone = make(chan struct{})
	r.commitsOpen = true
	r.state = recorderRecording
	r.mu.Unlock()

	go r.runForward(forwardCtx)
	go r.runEncoder(lifetime, queue, outputPath)
	return nil
}

// Close fences new captures, waits a bounded interval for final playout,
// flushes trailing input, and joins both workers. It is concurrency-safe and
// idempotent. A caller that times out may call Close again to observe the
// eventual terminal result.
func (r *RecorderIO) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	r.mu.Lock()
	switch r.state {
	case recorderNew:
		input, output := r.input, r.output
		r.state = recorderClosed
		r.mu.Unlock()
		var errs []error
		if input != nil {
			errs = append(errs, input.close())
		}
		if output != nil {
			output.beginClose()
			output.dropPending()
			output.close()
		}
		err := errors.Join(errs...)
		r.mu.Lock()
		r.closeErr = err
		r.mu.Unlock()
		return err
	case recorderClosed:
		err := r.closeErr
		r.mu.Unlock()
		return err
	case recorderRecording:
		r.state = recorderClosing
		done := r.closeDone
		input, output := r.input, r.output
		cleanupCtx, cleanupCancel := context.WithCancelCause(context.Background())
		lifetimeCancel := r.cancel
		stop := context.AfterFunc(ctx, func() {
			cause := context.Cause(ctx)
			cleanupCancel(cause)
			if lifetimeCancel != nil {
				lifetimeCancel(cause)
			}
		})
		// Establish the capture fence before releasing the state lock. This
		// prevents a concurrent writer from entering the downstream sink after
		// Close has observably begun.
		if output != nil {
			output.beginClose()
		}
		r.mu.Unlock()
		if input != nil {
			input.wake()
		}
		go func() {
			err := r.close(cleanupCtx)
			stop()
			cleanupCancel(ErrClosed)
			r.mu.Lock()
			r.closeErr = err
			r.state = recorderClosed
			close(done)
			r.mu.Unlock()
		}()
	case recorderClosing:
		done := r.closeDone
		r.mu.Unlock()
		select {
		case <-done:
			r.mu.Lock()
			err := r.closeErr
			r.mu.Unlock()
			return err
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}

	select {
	case <-r.closeDone:
		r.mu.Lock()
		err := r.closeErr
		r.mu.Unlock()
		return err
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (r *RecorderIO) close(ctx context.Context) error {
	r.mu.Lock()
	input, output := r.input, r.output
	forwardStop, forwardDone := r.forwardStop, r.forwardDone
	queue, encodeDone, cancel := r.queue, r.encodeDone, r.cancel
	r.mu.Unlock()

	var errs []error
	if output != nil {
		output.beginClose()
		output.sealOpenSegment()
		if output.PendingPlayoutSegments() != 0 {
			flushCtx, flushCancel := withOptionalTimeout(ctx, r.options.closePlayoutFlushTimeout)
			_, err := output.WaitForPlayout(flushCtx)
			flushCancel()
			pending := output.PendingPlayoutSegments()
			if err != nil && (errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)) && pending != 0 {
				if output.HasPendingData() {
					r.reportError(ErrPlayoutFlushTimeout)
				}
				output.dropPending()
				if cause := context.Cause(ctx); cause != nil {
					errs = append(errs, cause)
				}
			} else if err != nil && (!(errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)) || pending != 0) {
				errs = append(errs, err)
			}
		}
	}

	if forwardStop != nil {
		forwardStop()
	}
	if forwardDone != nil {
		select {
		case <-forwardDone:
		case <-ctx.Done():
			errs = append(errs, context.Cause(ctx))
		}
	}

	r.commitMu.Lock()
	r.mu.Lock()
	r.commitsOpen = false
	r.mu.Unlock()
	if input != nil && queue != nil {
		frames, takeErr := input.takeBuffer(output.lastSpeechEnd())
		if takeErr != nil {
			errs = append(errs, takeErr)
		}
		if len(frames) != 0 {
			batch := recordBatch{input: frames, bytes: framesBytes(frames), frames: len(frames)}
			if err := queue.Send(ctx, batch); err != nil {
				errs = append(errs, err)
			}
		}
	}
	r.commitMu.Unlock()
	if queue != nil {
		_ = queue.Close()
	}
	if err := context.Cause(ctx); err != nil && cancel != nil {
		cancel(err)
	}
	if encodeDone != nil {
		select {
		case <-encodeDone:
		case <-ctx.Done():
			if cancel != nil {
				cancel(context.Cause(ctx))
			}
			<-encodeDone
		}
	}
	if cancel != nil {
		cancel(ErrClosed)
	}
	if input != nil {
		if err := input.close(); err != nil {
			errs = append(errs, err)
		}
	}
	if output != nil {
		output.close()
	}
	r.mu.Lock()
	if r.encodeErr != nil {
		errs = append(errs, r.encodeErr)
	}
	r.mu.Unlock()
	return errors.Join(errs...)
}

func (r *RecorderIO) runForward(ctx context.Context) {
	defer close(r.forwardDone)
	ticker := time.NewTicker(r.options.writeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !r.Recording() {
				continue
			}
			if err := r.output.commitInputIfIdle(ctx); err != nil && !errors.Is(err, ErrClosed) && !errors.Is(err, context.Canceled) {
				r.reportError(err)
			}
		}
	}
}

func (r *RecorderIO) commit(ctx context.Context, output []agents.AudioFrame, padSince time.Time) error {
	r.commitMu.Lock()
	defer r.commitMu.Unlock()
	r.mu.Lock()
	open, input, queue := r.commitsOpen, r.input, r.queue
	r.mu.Unlock()
	if !open || input == nil || queue == nil {
		return ErrClosed
	}
	inputFrames, err := input.takeBuffer(padSince)
	if len(inputFrames) == 0 && len(output) == 0 {
		return err
	}
	batch := recordBatch{input: inputFrames, output: output}
	if len(inputFrames) > int(^uint(0)>>1)-len(output) {
		batch.frames = int(^uint(0) >> 1)
	} else {
		batch.frames = len(inputFrames) + len(output)
	}
	inputBytes, outputBytes := framesBytes(inputFrames), framesBytes(output)
	if inputBytes > int(^uint(0)>>1)-outputBytes {
		batch.bytes = int(^uint(0) >> 1)
	} else {
		batch.bytes = inputBytes + outputBytes
	}
	return errors.Join(err, queue.Send(ctx, batch))
}

// Recording reports whether Start has completed and Close has not begun.
func (r *RecorderIO) Recording() bool {
	r.mu.Lock()
	value := r.state == recorderRecording
	r.mu.Unlock()
	return value
}

func (r *RecorderIO) timingOpen() bool {
	r.mu.Lock()
	value := r.commitsOpen
	r.mu.Unlock()
	return value
}

// OutputPath returns the path accepted by Start.
func (r *RecorderIO) OutputPath() (string, bool) {
	r.mu.Lock()
	path := r.outputPath
	r.mu.Unlock()
	return path, path != ""
}

// RecordingStartedAt returns the earlier first-capture wall time across the
// two decorated sides.
func (r *RecorderIO) RecordingStartedAt() (time.Time, bool) {
	r.mu.Lock()
	input, output := r.input, r.output
	r.mu.Unlock()
	inTime, inOK := input.startedAt()
	outTime, outOK := output.startedAt()
	switch {
	case !inOK:
		return outTime, outOK
	case !outOK:
		return inTime, true
	case inTime.Before(outTime):
		return inTime, true
	default:
		return outTime, true
	}
}

func (r *RecorderIO) reportError(err error) {
	if err == nil || r.options.onError == nil {
		return
	}
	func() {
		defer func() { _ = recover() }()
		r.options.onError(err)
	}()
}

func withOptionalTimeout(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return context.WithCancel(parent)
	}
	return context.WithTimeout(parent, timeout)
}

func framesBytes(frames []agents.AudioFrame) int {
	total := 0
	for i := range frames {
		if len(frames[i].Data) > (int(^uint(0)>>1)-total)/2 {
			return int(^uint(0) >> 1)
		}
		total += len(frames[i].Data) * 2
	}
	return total
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	}
	return false
}
