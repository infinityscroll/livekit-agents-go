// SPDX-License-Identifier: Apache-2.0

package backgroundaudio

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"time"

	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/stream"
)

const (
	MixerSampleRate         = 48_000
	MixerChannels           = 1
	DefaultBlockDuration    = 100 * time.Millisecond
	DefaultBufferDuration   = 400 * time.Millisecond
	DefaultStreamTimeout    = 2 * time.Second
	DefaultOperationTimeout = 2 * time.Second
	DefaultTaskTimeout      = 500 * time.Millisecond
	DefaultMaxStreams       = 32
)

var (
	ErrStreamTimeout    = errors.New("backgroundaudio: audio stream timed out")
	ErrOperationTimeout = errors.New("backgroundaudio: output operation timed out")
	ErrPlayStopped      = errors.New("backgroundaudio: playout stopped")
)

type mixerCommandKind uint8

const (
	mixerAdd mixerCommandKind = iota
	mixerRemove
)

type mixerCommand struct {
	kind   mixerCommandKind
	stream *mixStream
	id     uint64
	ack    chan error
}

type mixStream struct {
	id       uint64
	queue    *stream.Channel[agents.AudioFrame]
	handle   *PlayHandle
	cancel   context.CancelCauseFunc
	onDone   func()
	done     sync.Once
	rejected atomic.Bool
}

func (s *mixStream) complete(err error) {
	s.handle.finish(err)
	s.done.Do(func() {
		if s.onDone != nil {
			s.onDone()
		}
	})
}

type audioMixer struct {
	ctx              context.Context
	cancel           context.CancelCauseFunc
	sink             FrameSink
	commands         chan mixerCommand
	blockDuration    time.Duration
	blockSamples     int
	operationTimeout time.Duration
	maxStreams       int
	onError          func(error)
	done             chan struct{}
	accumulator      []int32
	output           []int16

	mu  sync.Mutex
	err error
}

func newAudioMixer(parent context.Context, sink FrameSink, options resolvedPlayerOptions) *audioMixer {
	ctx, cancel := context.WithCancelCause(parent)
	m := &audioMixer{
		ctx: ctx, cancel: cancel, sink: sink,
		commands:         make(chan mixerCommand, max(4, options.maxStreams*2)),
		blockDuration:    options.blockDuration,
		blockSamples:     max(1, int(time.Duration(MixerSampleRate)*options.blockDuration/time.Second)),
		operationTimeout: options.operationTimeout, maxStreams: options.maxStreams, onError: options.onError,
		done:        make(chan struct{}),
		accumulator: make([]int32, max(1, int(time.Duration(MixerSampleRate)*options.blockDuration/time.Second))),
	}
	go m.run()
	return m
}

func (m *audioMixer) add(ctx context.Context, value *mixStream) error {
	if ctx == nil {
		ctx = context.Background()
	}
	ack := make(chan error, 1)
	select {
	case m.commands <- mixerCommand{kind: mixerAdd, stream: value, ack: ack}:
	case <-ctx.Done():
		value.rejected.Store(true)
		return context.Cause(ctx)
	case <-m.done:
		if err := m.Err(); err != nil {
			return err
		}
		return ErrClosed
	}
	select {
	case err := <-ack:
		return err
	case <-ctx.Done():
		value.rejected.Store(true)
		return context.Cause(ctx)
	case <-m.done:
		value.rejected.Store(true)
		if err := m.Err(); err != nil {
			return err
		}
		return ErrClosed
	}
}

func (m *audioMixer) remove(id uint64) {
	select {
	case m.commands <- mixerCommand{kind: mixerRemove, id: id}:
	default:
	}
}

func (m *audioMixer) Close(cause error) {
	if cause == nil {
		cause = ErrClosed
	}
	m.cancel(cause)
}

func (m *audioMixer) Wait(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-m.done:
		return m.Err()
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (m *audioMixer) Err() error {
	m.mu.Lock()
	err := m.err
	m.mu.Unlock()
	return err
}

func (m *audioMixer) run() {
	defer close(m.done)
	active := make(map[uint64]*mixStream)
	var ticker *time.Ticker
	var tick <-chan time.Time
	stopTicker := func() {
		if ticker != nil {
			ticker.Stop()
			ticker = nil
			tick = nil
		}
	}
	defer stopTicker()
	defer func() {
		cause := context.Cause(m.ctx)
		for _, value := range active {
			value.cancel(cause)
			_ = value.queue.Abort(cause)
			value.complete(normalizePlayError(cause))
		}
	}()

	for {
		select {
		case <-m.ctx.Done():
			m.setErr(normalizeMixerError(context.Cause(m.ctx)))
			return
		case command := <-m.commands:
			switch command.kind {
			case mixerAdd:
				if command.stream.rejected.Load() {
					command.ack <- context.Canceled
					command.stream.complete(context.Canceled)
					continue
				}
				if len(active) >= m.maxStreams {
					command.ack <- errors.New("backgroundaudio: mixer stream capacity exceeded")
					continue
				}
				active[command.stream.id] = command.stream
				if ticker == nil {
					ticker = time.NewTicker(m.blockDuration)
					tick = ticker.C
				}
				command.ack <- nil
			case mixerRemove:
				if value := active[command.id]; value != nil {
					delete(active, command.id)
					value.cancel(ErrPlayStopped)
					_ = value.queue.Abort(ErrPlayStopped)
					value.complete(nil)
				}
				if len(active) == 0 {
					stopTicker()
				}
			}
		case <-tick:
			if err := m.mixOne(active); err != nil {
				m.setErr(err)
				m.reportError(err)
				m.cancel(err)
				return
			}
			if len(active) == 0 {
				stopTicker()
			}
		}
	}
}

func (m *audioMixer) mixOne(active map[uint64]*mixStream) error {
	accumulator := m.accumulator
	if len(accumulator) != m.blockSamples {
		accumulator = make([]int32, m.blockSamples)
		m.accumulator = accumulator
	} else {
		clear(accumulator)
	}
	haveAudio := false
	for id, value := range active {
		frame, ok, err := value.queue.TryRecv()
		if !ok {
			if err != nil {
				delete(active, id)
				value.cancel(err)
				playErr := normalizePlayError(err)
				value.complete(playErr)
				if playErr != nil {
					m.reportError(playErr)
				}
			}
			continue
		}
		if frame.SampleRate != MixerSampleRate || frame.Channels != MixerChannels || frame.SamplesPerChannel != len(frame.Data) {
			return fmt.Errorf("%w: mixer received rate=%d channels=%d samples=%d data=%d", agents.ErrInvalidAudioFormat, frame.SampleRate, frame.Channels, frame.SamplesPerChannel, len(frame.Data))
		}
		haveAudio = true
		for index, sample := range frame.Data {
			if index >= len(accumulator) {
				break
			}
			accumulator[index] += int32(sample)
		}
	}
	if !haveAudio {
		return nil
	}
	_, immediate := m.sink.(immediateFrameSink)
	var data []int16
	if immediate {
		if len(m.output) != len(accumulator) {
			m.output = make([]int16, len(accumulator))
		}
		data = m.output
	} else {
		data = make([]int16, len(accumulator))
	}
	for index, sample := range accumulator {
		switch {
		case sample > math.MaxInt16:
			data[index] = math.MaxInt16
		case sample < math.MinInt16:
			data[index] = math.MinInt16
		default:
			data[index] = int16(sample)
		}
	}
	frame, _ := agents.NewAudioFrame(data, MixerSampleRate, MixerChannels)
	if immediate {
		return m.sink.CaptureFrame(m.ctx, frame)
	}
	ctx, cancel := context.WithTimeoutCause(m.ctx, m.operationTimeout, ErrOperationTimeout)
	err := m.sink.CaptureFrame(ctx, frame)
	cause := context.Cause(ctx)
	cancel()
	if err != nil {
		if cause != nil {
			return cause
		}
		return err
	}
	return nil
}

func (m *audioMixer) setErr(err error) {
	m.mu.Lock()
	if m.err == nil {
		m.err = err
	}
	m.mu.Unlock()
}

func (m *audioMixer) reportError(err error) {
	if err == nil || m.onError == nil {
		return
	}
	func() {
		defer func() { _ = recover() }()
		m.onError(err)
	}()
}

func normalizePlayError(err error) error {
	if err == nil || errors.Is(err, io.EOF) || errors.Is(err, ErrPlayStopped) || errors.Is(err, ErrClosed) || errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func normalizeMixerError(err error) error {
	if err == nil || errors.Is(err, ErrClosed) || errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

type frameConverter struct {
	blockSamples int
	volume       float64
	rate         int
	channels     int
	resampler    *linearMonoResampler
	pending      []int16
}

func newFrameConverter(blockSamples int, volume float64) *frameConverter {
	return &frameConverter{blockSamples: blockSamples, volume: volume, pending: make([]int16, 0, blockSamples*2)}
}

func (c *frameConverter) push(frame agents.AudioFrame) ([]agents.AudioFrame, error) {
	if frame.SampleRate <= 0 || frame.Channels <= 0 || frame.SamplesPerChannel < 0 || len(frame.Data) != frame.SamplesPerChannel*frame.Channels {
		return nil, fmt.Errorf("%w: rate=%d channels=%d samples=%d data=%d", agents.ErrInvalidAudioFormat, frame.SampleRate, frame.Channels, frame.SamplesPerChannel, len(frame.Data))
	}
	if len(frame.Data) == 0 {
		return nil, fmt.Errorf("%w: empty frames are not accepted", agents.ErrInvalidAudioFormat)
	}
	var output []agents.AudioFrame
	if c.rate != 0 && (c.rate != frame.SampleRate || c.channels != frame.Channels) {
		output = append(output, c.flush()...)
		c.rate, c.channels, c.resampler = 0, 0, nil
	}
	if c.rate == 0 {
		c.rate, c.channels = frame.SampleRate, frame.Channels
		if c.rate != MixerSampleRate {
			c.resampler = newLinearMonoResampler(c.rate, MixerSampleRate)
		}
	}
	if c.resampler == nil && frame.Channels == 1 && len(c.pending) == 0 && len(frame.Data) == c.blockSamples {
		if c.volume == 1 {
			return append(output, frame), nil
		}
		data := make([]int16, len(frame.Data))
		applyVolume(data, frame.Data, c.volume)
		value, _ := agents.NewAudioFrame(data, MixerSampleRate, MixerChannels)
		return append(output, value), nil
	}
	mono := downmix(frame.Data, frame.Channels)
	if c.resampler != nil {
		mono = c.resampler.push(mono, false)
	}
	c.pending = append(c.pending, mono...)
	return append(output, c.emit(false)...), nil
}

func (c *frameConverter) flush() []agents.AudioFrame {
	if c.resampler != nil {
		c.pending = append(c.pending, c.resampler.push(nil, true)...)
	}
	return c.emit(true)
}

func (c *frameConverter) emit(flush bool) []agents.AudioFrame {
	count := len(c.pending) / c.blockSamples
	if flush && len(c.pending)%c.blockSamples != 0 {
		count++
	}
	if count == 0 {
		return nil
	}
	frames := make([]agents.AudioFrame, 0, count)
	offset := 0
	for len(c.pending)-offset >= c.blockSamples || flush && len(c.pending)-offset != 0 {
		n := min(c.blockSamples, len(c.pending)-offset)
		data := make([]int16, n)
		applyVolume(data, c.pending[offset:offset+n], c.volume)
		frame, _ := agents.NewAudioFrame(data, MixerSampleRate, MixerChannels)
		frames = append(frames, frame)
		offset += n
	}
	if offset != 0 {
		copy(c.pending, c.pending[offset:])
		c.pending = c.pending[:len(c.pending)-offset]
	}
	return frames
}

func applyVolume(destination, source []int16, volume float64) {
	if volume == 1 {
		copy(destination, source)
		return
	}
	for index, sample := range source {
		value := float64(sample) * volume
		if value > math.MaxInt16 {
			value = math.MaxInt16
		} else if value < math.MinInt16 {
			value = math.MinInt16
		}
		// Math.round in JavaScript is floor(x+0.5), including negative ties.
		destination[index] = int16(math.Floor(value + .5))
	}
}

func downmix(input []int16, channels int) []int16 {
	if channels == 1 {
		return input
	}
	output := make([]int16, len(input)/channels)
	for sample := range output {
		var sum int64
		base := sample * channels
		for channel := 0; channel < channels; channel++ {
			sum += int64(input[base+channel])
		}
		output[sample] = int16(sum / int64(channels))
	}
	return output
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
	output := make([]int16, 0, estimated)
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
		output = append(output, int16(value))
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
	return output
}

type workerTracker struct {
	mu    sync.Mutex
	count int
	done  chan struct{}
}

func newWorkerTracker() *workerTracker {
	done := make(chan struct{})
	close(done)
	return &workerTracker{done: done}
}

func (w *workerTracker) Add() {
	w.mu.Lock()
	if w.count == 0 {
		w.done = make(chan struct{})
	}
	w.count++
	w.mu.Unlock()
}

func (w *workerTracker) Done() {
	w.mu.Lock()
	w.count--
	if w.count == 0 {
		close(w.done)
	}
	w.mu.Unlock()
}

func (w *workerTracker) Wait(ctx context.Context) error {
	w.mu.Lock()
	done := w.done
	w.mu.Unlock()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

var nextStreamID atomic.Uint64
