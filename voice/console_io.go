// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/stream"
	agentpb "github.com/livekit/protocol/livekit/agent"
)

const (
	ConsoleWireSampleRate          = 48_000
	ConsoleAgentSampleRate         = 24_000
	DefaultConsoleAudioQueueSize   = 64
	DefaultConsoleMaxFrameDuration = 2 * time.Second
	DefaultConsoleOperationTimeout = 5 * time.Second
	DefaultConsoleSessionDirectory = "console-recordings"
)

var (
	ErrConsoleIOClosed          = errors.New("voice console IO is closed")
	ErrConsoleIOAlreadyAcquired = errors.New("voice console IO was already acquired by another session")
)

// streamingPCMConverter keeps interpolation state across protobuf frame
// boundaries. It downmixes interleaved PCM to mono before resampling.
type streamingPCMConverter struct {
	targetRate int
	rate       int
	channels   int
	resampler  *linearPCMResampler
}

func newStreamingPCMConverter(targetRate int) *streamingPCMConverter {
	return &streamingPCMConverter{targetRate: targetRate}
}

func (c *streamingPCMConverter) Push(frame agents.AudioFrame) ([]agents.AudioFrame, error) {
	if err := validateConsoleAudioFrame(frame); err != nil {
		return nil, err
	}
	var result []agents.AudioFrame
	if c.rate != 0 && (c.rate != frame.SampleRate || c.channels != frame.Channels) {
		result = append(result, c.Flush()...)
		c.Reset()
	}
	if c.rate == 0 {
		c.rate, c.channels = frame.SampleRate, frame.Channels
		c.resampler = newLinearPCMResampler(frame.SampleRate, c.targetRate)
	}
	mono := downmixConsolePCM(frame.Data, frame.Channels)
	data := c.resampler.Push(mono)
	if len(data) != 0 {
		converted, _ := agents.NewAudioFrame(data, c.targetRate, 1)
		converted.UserData = frame.UserData
		result = append(result, converted)
	}
	return result, nil
}

func (c *streamingPCMConverter) Flush() []agents.AudioFrame {
	if c.resampler == nil {
		return nil
	}
	data := c.resampler.Flush()
	if len(data) == 0 {
		return nil
	}
	frame, _ := agents.NewAudioFrame(data, c.targetRate, 1)
	return []agents.AudioFrame{frame}
}

func (c *streamingPCMConverter) Reset() {
	c.rate, c.channels, c.resampler = 0, 0, nil
}

// linearPCMResampler holds the trailing source sample needed to interpolate
// the first output sample of the next frame. The phase is measured in source
// samples, avoiding a discontinuity or sample-count drift at frame boundaries.
type linearPCMResampler struct {
	inRate, outRate int
	step            float64
	position        float64
	pending         []int16
}

func newLinearPCMResampler(inRate, outRate int) *linearPCMResampler {
	return &linearPCMResampler{inRate: inRate, outRate: outRate, step: float64(inRate) / float64(outRate)}
}

func (r *linearPCMResampler) Push(input []int16) []int16 {
	if len(input) != 0 {
		r.pending = append(r.pending, input...)
	}
	return r.emit(false)
}

func (r *linearPCMResampler) Flush() []int16 {
	output := r.emit(true)
	r.pending = nil
	r.position = 0
	return output
}

func (r *linearPCMResampler) emit(flush bool) []int16 {
	if len(r.pending) == 0 {
		return nil
	}
	estimate := int(math.Ceil((float64(len(r.pending))-r.position)/r.step)) + 1
	if estimate < 0 {
		estimate = 0
	}
	output := make([]int16, 0, estimate)
	for r.position < float64(len(r.pending)) {
		index := int(r.position)
		fraction := r.position - float64(index)
		if index >= len(r.pending) {
			break
		}
		if index+1 >= len(r.pending) && fraction != 0 && !flush {
			break
		}
		left := float64(r.pending[index])
		right := left
		if index+1 < len(r.pending) {
			right = float64(r.pending[index+1])
		}
		value := left + (right-left)*fraction
		value = min(math.MaxInt16, max(math.MinInt16, value))
		output = append(output, int16(math.Round(value)))
		r.position += r.step
	}
	drop := int(r.position)
	if drop > 0 {
		if drop >= len(r.pending) {
			r.position -= float64(len(r.pending))
			r.pending = r.pending[:0]
		} else {
			copy(r.pending, r.pending[drop:])
			r.pending = r.pending[:len(r.pending)-drop]
			r.position -= float64(drop)
		}
	}
	return output
}

func downmixConsolePCM(data []int16, channels int) []int16 {
	if channels == 1 {
		return data
	}
	mono := make([]int16, len(data)/channels)
	for sample := range mono {
		var total int64
		for channel := 0; channel < channels; channel++ {
			total += int64(data[sample*channels+channel])
		}
		mono[sample] = int16(total / int64(channels))
	}
	return mono
}

func validateConsoleAudioFrame(frame agents.AudioFrame) error {
	if frame.SampleRate < 8_000 || frame.SampleRate > 192_000 || frame.Channels < 1 || frame.Channels > 8 ||
		frame.SamplesPerChannel <= 0 || len(frame.Data) != frame.SamplesPerChannel*frame.Channels {
		return fmt.Errorf("%w: rate=%d channels=%d samples=%d data=%d", agents.ErrInvalidAudioFormat,
			frame.SampleRate, frame.Channels, frame.SamplesPerChannel, len(frame.Data))
	}
	if frame.Duration() > DefaultConsoleMaxFrameDuration {
		return fmt.Errorf("%w: console frame duration %s exceeds %s", agents.ErrInvalidAudioFormat, frame.Duration(), DefaultConsoleMaxFrameDuration)
	}
	return nil
}

func consoleProtoToAudio(frame *agentpb.AgentSessionMessage_ConsoleIO_AudioFrame) (agents.AudioFrame, error) {
	if frame == nil || len(frame.Data) == 0 || len(frame.Data)%2 != 0 {
		return agents.AudioFrame{}, fmt.Errorf("%w: invalid PCM16 byte length", agents.ErrInvalidAudioFormat)
	}
	if uint64(frame.SamplesPerChannel)*uint64(frame.NumChannels)*2 != uint64(len(frame.Data)) {
		return agents.AudioFrame{}, fmt.Errorf("%w: protobuf audio dimensions do not match data", agents.ErrInvalidAudioFormat)
	}
	data := make([]int16, len(frame.Data)/2)
	for index := range data {
		data[index] = int16(binary.LittleEndian.Uint16(frame.Data[index*2:]))
	}
	value := agents.AudioFrame{Data: data, SampleRate: int(frame.SampleRate), Channels: int(frame.NumChannels), SamplesPerChannel: int(frame.SamplesPerChannel)}
	if err := validateConsoleAudioFrame(value); err != nil {
		return agents.AudioFrame{}, err
	}
	return value, nil
}

func audioToConsoleProto(frame agents.AudioFrame) (*agentpb.AgentSessionMessage_ConsoleIO_AudioFrame, error) {
	if err := validateConsoleAudioFrame(frame); err != nil {
		return nil, err
	}
	data := make([]byte, len(frame.Data)*2)
	for index, sample := range frame.Data {
		binary.LittleEndian.PutUint16(data[index*2:], uint16(sample))
	}
	return &agentpb.AgentSessionMessage_ConsoleIO_AudioFrame{
		Data: data, SampleRate: uint32(frame.SampleRate), NumChannels: uint32(frame.Channels), SamplesPerChannel: uint32(frame.SamplesPerChannel),
	}, nil
}

// TCPAudioInput is a bounded 48 kHz broker-to-24 kHz agent audio bridge.
type TCPAudioInput struct {
	frames    *stream.Channel[agents.AudioFrame]
	converter *streamingPCMConverter
	mu        sync.Mutex
	attached  atomic.Bool
	closeOnce sync.Once
}

// TcpAudioInput is the mechanical-migration spelling used by agents-js and
// Python. New Go code should use TCPAudioInput.
type TcpAudioInput = TCPAudioInput

func NewTCPAudioInput(capacity ...int) (*TCPAudioInput, error) {
	queueSize := DefaultConsoleAudioQueueSize
	if len(capacity) > 1 {
		return nil, errors.New("voice TCP audio input accepts at most one capacity")
	}
	if len(capacity) == 1 {
		queueSize = capacity[0]
	}
	if queueSize < 1 {
		return nil, errors.New("voice TCP audio input capacity must be positive")
	}
	return &TCPAudioInput{frames: stream.NewChannel[agents.AudioFrame](queueSize), converter: newStreamingPCMConverter(ConsoleAgentSampleRate)}, nil
}

// NewTcpAudioInput preserves the agents-js spelling.
func NewTcpAudioInput(capacity ...int) (*TCPAudioInput, error) { return NewTCPAudioInput(capacity...) }

func (i *TCPAudioInput) PushFrame(ctx context.Context, value *agentpb.AgentSessionMessage_ConsoleIO_AudioFrame) error {
	frame, err := consoleProtoToAudio(value)
	if err != nil {
		return err
	}
	i.mu.Lock()
	converted, err := i.converter.Push(frame)
	i.mu.Unlock()
	if err != nil {
		return err
	}
	for _, output := range converted {
		if err := i.frames.Send(ctx, output); err != nil {
			return err
		}
	}
	return nil
}

func (i *TCPAudioInput) TryPushFrame(value *agentpb.AgentSessionMessage_ConsoleIO_AudioFrame) error {
	frame, err := consoleProtoToAudio(value)
	if err != nil {
		return err
	}
	i.mu.Lock()
	converted, err := i.converter.Push(frame)
	i.mu.Unlock()
	if err != nil {
		return err
	}
	for _, output := range converted {
		if !i.frames.TrySend(output) {
			return ErrSessionQueueFull
		}
	}
	return nil
}

func (i *TCPAudioInput) Recv(ctx context.Context) (agents.AudioFrame, error) {
	return i.frames.Recv(ctx)
}
func (i *TCPAudioInput) SetAttached(value bool) { i.attached.Store(value) }
func (i *TCPAudioInput) OnAttached()            { i.attached.Store(true) }
func (i *TCPAudioInput) OnDetached()            { i.attached.Store(false) }
func (i *TCPAudioInput) Close() error {
	var err error
	i.closeOnce.Do(func() { err = i.frames.Close() })
	return err
}

type consolePlayoutSegment struct {
	started time.Time
	pushed  time.Duration
	done    chan struct{}
	once    sync.Once
}

// TCPAudioOutput is a 24 kHz agent-to-48 kHz broker bridge with one bounded
// playout handshake in flight. A new segment cannot overtake the previous
// flush, and ClearBuffer deterministically releases every waiter.
type TCPAudioOutput struct {
	transport SessionTransport
	converter *streamingPCMConverter
	state     *audioOutputState
	op        chan struct{}

	mu           sync.Mutex
	current      *consolePlayoutSegment
	flushing     *consolePlayoutSegment
	closed       bool
	attached     atomic.Bool
	pauseMu      sync.Mutex
	paused       bool
	pauseChanged chan struct{}

	closeOnce sync.Once
	closeDone chan struct{}
	closeErr  error
}

// TcpAudioOutput is the mechanical-migration spelling used by agents-js and
// Python. New Go code should use TCPAudioOutput.
type TcpAudioOutput = TCPAudioOutput

func NewTCPAudioOutput(transport SessionTransport) (*TCPAudioOutput, error) {
	if transport == nil {
		return nil, errors.New("voice TCP audio output requires a session transport")
	}
	output := &TCPAudioOutput{
		transport: transport, converter: newStreamingPCMConverter(ConsoleWireSampleRate),
		state: newAudioOutputState(nil), op: make(chan struct{}, 1), pauseChanged: make(chan struct{}), closeDone: make(chan struct{}),
	}
	output.op <- struct{}{}
	return output, nil
}

func NewTcpAudioOutput(transport SessionTransport) (*TCPAudioOutput, error) {
	return NewTCPAudioOutput(transport)
}

func (o *TCPAudioOutput) lock(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-o.op:
		return nil
	}
}
func (o *TCPAudioOutput) unlock() { o.op <- struct{}{} }

func (o *TCPAudioOutput) CaptureFrame(ctx context.Context, frame agents.AudioFrame) error {
	if err := o.waitUntilResumed(ctx); err != nil {
		return err
	}
	for {
		if err := o.lock(ctx); err != nil {
			return err
		}
		o.mu.Lock()
		if o.closed {
			o.mu.Unlock()
			o.unlock()
			return ErrConsoleIOClosed
		}
		previous := o.flushing
		o.mu.Unlock()
		if previous == nil {
			break
		}
		o.unlock()
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-previous.done:
		}
	}
	defer o.unlock()
	opened := o.state.beginFrame()
	converted, err := o.converter.Push(frame)
	if err != nil {
		if opened {
			o.state.rollbackFrame(true)
		}
		return err
	}
	var pushed time.Duration
	for _, output := range converted {
		wire, frameErr := audioToConsoleProto(output)
		if frameErr != nil {
			o.failPartialCapture(opened, pushed)
			return frameErr
		}
		message := &agentpb.AgentSessionMessage{Message: &agentpb.AgentSessionMessage_AudioOutput{AudioOutput: wire}}
		if sendErr := o.transport.SendMessage(ctx, message); sendErr != nil {
			o.failPartialCapture(opened, pushed)
			return sendErr
		}
		pushed += output.Duration()
	}
	o.mu.Lock()
	if o.current == nil {
		o.current = &consolePlayoutSegment{started: time.Now(), done: make(chan struct{})}
		o.state.notifyStarted(PlaybackStartedEvent{CreatedAt: o.current.started})
	}
	o.current.pushed += pushed
	o.mu.Unlock()
	return nil
}

func (o *TCPAudioOutput) failPartialCapture(opened bool, pushed time.Duration) {
	if pushed == 0 {
		if opened {
			o.state.rollbackFrame(true)
		}
		return
	}
	o.converter.Reset()
	o.mu.Lock()
	segment := o.current
	if segment == nil {
		segment = &consolePlayoutSegment{started: time.Now(), done: make(chan struct{})}
		o.state.notifyStarted(PlaybackStartedEvent{CreatedAt: segment.started})
	}
	segment.pushed += pushed
	o.current = nil
	o.mu.Unlock()
	o.state.flush()
	o.finishSegment(segment, true)
}

func (o *TCPAudioOutput) Flush(ctx context.Context) error {
	if err := o.lock(ctx); err != nil {
		return err
	}
	defer o.unlock()
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		return ErrConsoleIOClosed
	}
	if o.flushing != nil {
		o.mu.Unlock()
		return errors.New("voice TCP audio output flush is already in progress")
	}
	segment := o.current
	o.mu.Unlock()
	for _, output := range o.converter.Flush() {
		wire, err := audioToConsoleProto(output)
		if err != nil {
			return err
		}
		if err := o.transport.SendMessage(ctx, &agentpb.AgentSessionMessage{Message: &agentpb.AgentSessionMessage_AudioOutput{AudioOutput: wire}}); err != nil {
			return err
		}
		if segment != nil {
			segment.pushed += output.Duration()
		}
	}
	o.converter.Reset()
	o.mu.Lock()
	if segment != nil {
		o.current = nil
		o.flushing = segment
	}
	o.mu.Unlock()
	o.state.flush()
	message := &agentpb.AgentSessionMessage{Message: &agentpb.AgentSessionMessage_AudioPlaybackFlush{AudioPlaybackFlush: &agentpb.AgentSessionMessage_ConsoleIO_AudioPlaybackFlush{}}}
	if err := o.transport.SendMessage(ctx, message); err != nil {
		if segment != nil {
			o.mu.Lock()
			if o.flushing == segment {
				o.flushing = nil
			}
			o.mu.Unlock()
			o.finishSegment(segment, true)
		}
		return err
	}
	return nil
}

func (o *TCPAudioOutput) ClearBuffer(ctx context.Context) error {
	if err := o.lock(ctx); err != nil {
		return err
	}
	defer o.unlock()
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		return ErrConsoleIOClosed
	}
	segment := o.flushing
	if segment == nil {
		segment = o.current
	}
	o.current, o.flushing = nil, nil
	o.converter.Reset()
	o.mu.Unlock()
	o.state.abandon()
	message := &agentpb.AgentSessionMessage{Message: &agentpb.AgentSessionMessage_AudioPlaybackClear{AudioPlaybackClear: &agentpb.AgentSessionMessage_ConsoleIO_AudioPlaybackClear{}}}
	err := o.transport.SendMessage(ctx, message)
	if segment != nil {
		o.finishSegment(segment, true)
	}
	return err
}

func (o *TCPAudioOutput) NotifyPlayoutFinished() error {
	o.mu.Lock()
	segment := o.flushing
	if segment != nil {
		o.flushing = nil
	}
	o.mu.Unlock()
	if segment == nil {
		return ErrUnexpectedPlaybackFinished
	}
	o.finishSegment(segment, false)
	return nil
}

func (o *TCPAudioOutput) finishSegment(segment *consolePlayoutSegment, interrupted bool) {
	if segment == nil {
		return
	}
	segment.once.Do(func() {
		played := segment.pushed
		if interrupted {
			played = min(segment.pushed, max(time.Duration(0), time.Since(segment.started)))
		}
		_ = o.state.notifyFinished(PlaybackFinishedEvent{PlaybackPosition: played, Interrupted: interrupted})
		close(segment.done)
	})
}

func (o *TCPAudioOutput) WaitForPlayout(ctx context.Context) (PlaybackFinishedEvent, error) {
	return o.state.wait(ctx)
}
func (o *TCPAudioOutput) Pause(ctx context.Context) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return context.Cause(ctx)
		}
	}
	o.pauseMu.Lock()
	defer o.pauseMu.Unlock()
	o.mu.Lock()
	closed := o.closed
	o.mu.Unlock()
	if closed {
		return ErrConsoleIOClosed
	}
	if !o.paused {
		o.paused = true
		close(o.pauseChanged)
		o.pauseChanged = make(chan struct{})
	}
	return nil
}
func (o *TCPAudioOutput) Resume(ctx context.Context) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return context.Cause(ctx)
		}
	}
	o.pauseMu.Lock()
	defer o.pauseMu.Unlock()
	o.mu.Lock()
	closed := o.closed
	o.mu.Unlock()
	if closed {
		return ErrConsoleIOClosed
	}
	if o.paused {
		o.paused = false
		close(o.pauseChanged)
		o.pauseChanged = make(chan struct{})
	}
	return nil
}
func (o *TCPAudioOutput) waitUntilResumed(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		o.pauseMu.Lock()
		paused, changed := o.paused, o.pauseChanged
		o.pauseMu.Unlock()
		if !paused {
			return nil
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-changed:
		}
	}
}
func (o *TCPAudioOutput) CanPause() bool         { return true }
func (o *TCPAudioOutput) SampleRate() int        { return ConsoleAgentSampleRate }
func (o *TCPAudioOutput) SetAttached(value bool) { o.attached.Store(value) }
func (o *TCPAudioOutput) OnAttached()            { o.attached.Store(true) }
func (o *TCPAudioOutput) OnDetached()            { o.attached.Store(false) }
func (o *TCPAudioOutput) OnPlaybackStarted(fn func(PlaybackStartedEvent)) func() {
	return o.state.started.Subscribe(fn)
}
func (o *TCPAudioOutput) OnPlaybackFinished(fn func(PlaybackFinishedEvent)) func() {
	return o.state.completed.Subscribe(fn)
}
func (o *TCPAudioOutput) PendingPlayoutSegments() uint64 {
	captured, finished := o.state.counts()
	return captured - finished
}
func (o *TCPAudioOutput) CapturedPlayoutSegments() uint64 {
	captured, _ := o.state.counts()
	return captured
}

func (o *TCPAudioOutput) Close(ctx context.Context) error {
	o.closeOnce.Do(func() { go o.closeWorker() })
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-o.closeDone:
		return o.closeErr
	}
}

func (o *TCPAudioOutput) closeWorker() {
	defer close(o.closeDone)
	ctx, cancel := context.WithTimeout(context.Background(), DefaultConsoleOperationTimeout)
	defer cancel()
	if err := o.lock(ctx); err != nil {
		o.closeErr = err
		return
	}
	o.mu.Lock()
	o.closed = true
	segment := o.flushing
	if segment == nil {
		segment = o.current
	}
	o.current, o.flushing = nil, nil
	o.converter.Reset()
	o.mu.Unlock()
	o.pauseMu.Lock()
	if o.paused {
		o.paused = false
		close(o.pauseChanged)
		o.pauseChanged = make(chan struct{})
	}
	o.pauseMu.Unlock()
	o.state.abandon()
	if segment != nil {
		o.finishSegment(segment, true)
		o.closeErr = errors.Join(o.closeErr, o.transport.SendMessage(ctx, &agentpb.AgentSessionMessage{Message: &agentpb.AgentSessionMessage_AudioPlaybackClear{AudioPlaybackClear: &agentpb.AgentSessionMessage_ConsoleIO_AudioPlaybackClear{}}}))
	}
	o.unlock()
}

// AgentsConsole carries console transport/IO from the CLI runner into the
// AgentSession constructed by the user's entrypoint. Initialization is lazy;
// normal workers pay only one atomic load in AgentSession.Start.
type AgentsConsole struct {
	mu sync.Mutex

	enabled          atomic.Bool
	record           bool
	transport        SessionTransport
	input            *TCPAudioInput
	output           *TCPAudioOutput
	sessionInput     AudioInput
	sessionOutput    AudioOutput
	recording        ConsoleRecording
	directory        string
	recordingOnce    sync.Once
	recordingDone    chan struct{}
	recordingErr     error
	recordingClosing bool

	acquired      any
	host          consoleHostCloser
	job           any
	simulationEnd any
	closing       bool

	closeOnce sync.Once
	closeDone chan struct{}
	closeErr  error
}

type consoleHostCloser interface{ Close(context.Context) error }

// ConsoleRecording owns optional audio decorators installed for --record.
// RecorderIO satisfies this interface without creating a voice import cycle.
type ConsoleRecording interface{ Close(context.Context) error }

type AgentsConsoleOptions struct {
	Enabled          bool
	Record           bool
	Transport        SessionTransport
	AudioInput       *TCPAudioInput
	AudioOutput      *TCPAudioOutput
	SessionDirectory string
}

func NewAgentsConsole(options AgentsConsoleOptions) (*AgentsConsole, error) {
	if options.Enabled && options.Transport == nil {
		return nil, errors.New("voice enabled console requires a session transport")
	}
	if options.SessionDirectory == "" {
		now := time.Now()
		options.SessionDirectory = filepath.Join(DefaultConsoleSessionDirectory, fmt.Sprintf("session-%02d-%02d-%02d%02d%02d",
			int(now.Month()), now.Day(), now.Hour(), now.Minute(), now.Second()))
	}
	console := &AgentsConsole{
		record: options.Record, transport: options.Transport, input: options.AudioInput,
		output: options.AudioOutput, directory: options.SessionDirectory,
		recordingDone: make(chan struct{}), closeDone: make(chan struct{}),
	}
	if options.AudioInput != nil {
		console.sessionInput = options.AudioInput
	}
	if options.AudioOutput != nil {
		console.sessionOutput = options.AudioOutput
	}
	console.enabled.Store(options.Enabled)
	return console, nil
}

func (c *AgentsConsole) Enabled() bool { return c != nil && c.enabled.Load() }
func (c *AgentsConsole) Record() bool  { c.mu.Lock(); value := c.record; c.mu.Unlock(); return value }
func (c *AgentsConsole) SessionDirectory() string {
	c.mu.Lock()
	value := c.directory
	c.mu.Unlock()
	return value
}
func (c *AgentsConsole) IOAcquired() bool {
	c.mu.Lock()
	value := c.acquired != nil
	c.mu.Unlock()
	return value
}

// SetRecordingIO installs ownership-neutral decorators around the TCP audio
// bridges. It must be called before an AgentSession acquires the console.
func (c *AgentsConsole) SetRecordingIO(input AudioInput, output AudioOutput, recording ConsoleRecording) error {
	if c == nil || isNilInterface(input) || isNilInterface(output) || isNilInterface(recording) {
		return errors.New("voice console recording requires input, output, and an owner")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.acquired != nil || c.host != nil {
		return ErrConsoleIOAlreadyAcquired
	}
	if c.closing || c.recordingClosing {
		return ErrConsoleIOClosed
	}
	if c.recording != nil {
		return errors.New("voice console recording IO is already configured")
	}
	c.sessionInput, c.sessionOutput, c.recording = input, output, recording
	return nil
}

// ConsoleRecordingInfo is a best-effort metadata snapshot exposed by a console
// recorder. Custom ConsoleRecording implementations may omit either field.
type ConsoleRecordingInfo struct {
	OutputPath string
	StartedAt  time.Time
	HasStarted bool
}

// RecordingInfo returns recorder metadata without transferring ownership.
func (c *AgentsConsole) RecordingInfo() ConsoleRecordingInfo {
	if c == nil {
		return ConsoleRecordingInfo{}
	}
	c.mu.Lock()
	recording := c.recording
	c.mu.Unlock()
	var result ConsoleRecordingInfo
	if value, ok := recording.(interface{ OutputPath() (string, bool) }); ok {
		result.OutputPath, _ = value.OutputPath()
	}
	if value, ok := recording.(interface{ RecordingStartedAt() (time.Time, bool) }); ok {
		result.StartedAt, result.HasStarted = value.RecordingStartedAt()
	}
	return result
}

// CloseRecording finalizes the optional console recorder without closing the
// console transport. It is concurrent, idempotent, and uses an internal
// cleanup deadline so a caller timeout cannot strand the recorder workers.
func (c *AgentsConsole) CloseRecording(ctx context.Context) error {
	if c == nil {
		return nil
	}
	c.recordingOnce.Do(func() {
		c.mu.Lock()
		c.recordingClosing = true
		recording := c.recording
		c.mu.Unlock()
		go func() {
			defer close(c.recordingDone)
			if recording == nil {
				return
			}
			closeCtx, cancel := context.WithTimeout(context.Background(), DefaultConsoleOperationTimeout)
			defer cancel()
			err := recording.Close(closeCtx)
			c.mu.Lock()
			c.recordingErr = err
			c.mu.Unlock()
		}()
	})
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-c.recordingDone:
		c.mu.Lock()
		err := c.recordingErr
		c.mu.Unlock()
		return err
	}
}

var defaultAgentsConsole atomic.Pointer[AgentsConsole]

func DefaultAgentsConsole() *AgentsConsole { return defaultAgentsConsole.Load() }

// SetDefaultAgentsConsole installs process-local console state and returns an
// idempotent restore function. It performs no IO and is safe for nested tests.
func SetDefaultAgentsConsole(console *AgentsConsole) func() {
	previous := defaultAgentsConsole.Swap(console)
	var once sync.Once
	return func() { once.Do(func() { defaultAgentsConsole.CompareAndSwap(console, previous) }) }
}

// BindAgentsConsoleJob supplies the fake console job and simulation callback
// to the session host without making AgentsConsole itself generic.
func BindAgentsConsoleJob[UserData any](console *AgentsConsole, job *agents.JobContext[UserData], onSimulationEnd agents.SimulationEndFunc[UserData]) error {
	if console == nil {
		return errors.New("voice cannot bind a nil AgentsConsole")
	}
	console.mu.Lock()
	defer console.mu.Unlock()
	if console.acquired != nil {
		return errors.New("voice cannot bind console job after IO acquisition")
	}
	console.job, console.simulationEnd = job, onSimulationEnd
	return nil
}

func (c *AgentsConsole) Close(ctx context.Context) error {
	if c == nil {
		return nil
	}
	c.enabled.Store(false)
	c.mu.Lock()
	c.closing = true
	c.mu.Unlock()
	c.closeOnce.Do(func() { go c.closeWorker() })
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-c.closeDone:
		return c.closeErr
	}
}

func (c *AgentsConsole) closeWorker() {
	defer close(c.closeDone)
	c.mu.Lock()
	host, input, output, transport := c.host, c.input, c.output, c.transport
	c.host, c.acquired = nil, nil
	c.mu.Unlock()
	closeCtx, cancel := context.WithTimeout(context.Background(), DefaultConsoleOperationTimeout)
	defer cancel()
	if host != nil {
		c.closeErr = errors.Join(c.closeErr, host.Close(closeCtx))
	}
	c.closeErr = errors.Join(c.closeErr, c.CloseRecording(closeCtx))
	if output != nil {
		c.closeErr = errors.Join(c.closeErr, output.Close(closeCtx))
	}
	if input != nil {
		c.closeErr = errors.Join(c.closeErr, input.Close())
	}
	if transport != nil {
		c.closeErr = errors.Join(c.closeErr, transport.Close(closeCtx))
	}
}

var _ AudioInput = (*TCPAudioInput)(nil)
var _ AudioOutput = (*TCPAudioOutput)(nil)
