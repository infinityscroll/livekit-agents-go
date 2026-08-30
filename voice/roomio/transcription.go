// SPDX-License-Identifier: Apache-2.0

package roomio

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/stream"
	"github.com/infinityscroll/livekit-agents-go/tts"
	"github.com/infinityscroll/livekit-agents-go/voice"
	"github.com/livekit/protocol/livekit"
	agentpb "github.com/livekit/protocol/livekit/agent"
	lksdk "github.com/livekit/server-sdk-go/v2"
	"google.golang.org/protobuf/encoding/protojson"
)

var ErrParticipantTranscriptionClosed = errors.New("roomio: participant transcription output is closed")

type textWriter interface {
	Write(context.Context, string) error
	Close()
}

type textPublisher interface {
	Connected() bool
	Stream(lksdk.StreamTextOptions) textWriter
	MicrophoneTrackID(string) string
	LocalIdentity() string
}

type roomTextPublisher struct{ room *lksdk.Room }

func (p roomTextPublisher) Connected() bool {
	return p.room != nil && p.room.ConnectionState() == lksdk.ConnectionStateConnected
}
func (p roomTextPublisher) Stream(options lksdk.StreamTextOptions) textWriter {
	return &sdkTextWriter{writer: p.room.LocalParticipant.StreamText(options)}
}
func (p roomTextPublisher) LocalIdentity() string {
	if p.room == nil || p.room.LocalParticipant == nil {
		return ""
	}
	return p.room.LocalParticipant.Identity()
}
func (p roomTextPublisher) MicrophoneTrackID(identity string) string {
	if p.room == nil {
		return ""
	}
	var participant lksdk.Participant
	if p.room.LocalParticipant != nil && p.room.LocalParticipant.Identity() == identity {
		participant = p.room.LocalParticipant
	} else {
		participant = p.room.GetParticipantByIdentity(identity)
	}
	if participant == nil {
		return ""
	}
	publication := participant.GetTrackPublication(livekit.TrackSource_MICROPHONE)
	if publication == nil {
		return ""
	}
	return publication.SID()
}

type sdkTextWriter struct{ writer *lksdk.TextStreamWriter }

// Write cannot make the SDK's initial Write call context-aware: v2.18.1 sends
// into an internal unbuffered queue before exposing completion, and Close can
// block behind its buffer-status lock. The surrounding actor still bounds
// callers and honors their deadlines; Capabilities reports the transport gap.
func (w *sdkTextWriter) Write(ctx context.Context, text string) error {
	if w == nil || w.writer == nil {
		return ErrParticipantTranscriptionClosed
	}
	done := make(chan struct{})
	callback := func() { close(done) }
	w.writer.Write(text, &callback)
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}
func (w *sdkTextWriter) Close() {
	if w != nil && w.writer != nil {
		w.writer.Close()
	}
}

type textCommandKind uint8

const (
	textCapture textCommandKind = iota
	textFlush
	textSetParticipant
)

type textCommand struct {
	kind        textCommandKind
	ctx         context.Context
	text        agents.TimedString
	participant string
	done        chan error
}

// ParticipantTranscriptionOutput publishes the lk.transcription data-stream
// contract. Delta streams reuse one writer per segment; non-delta streams emit
// interim snapshots and a final snapshot on flush.
type ParticipantTranscriptionOutput struct {
	ctx              context.Context
	cancel           context.CancelCauseFunc
	pub              textPublisher
	delta            bool
	json             bool
	expressive       func() bool
	operationTimeout time.Duration

	commands *stream.Channel[textCommand]
	attached atomic.Bool
	closed   atomic.Bool

	mu           sync.Mutex
	activeWriter textWriter
	participant  string
	done         chan struct{}
	closeOnce    sync.Once

	// Actor-owned state below.
	capturing bool
	segmentID string
	latest    string
	writer    textWriter
	stripper  tts.TranscriptMarkupStripper
	tags      []tts.ExpressiveTag
}

func NewParticipantTranscriptionOutput(parent context.Context, room *lksdk.Room, isDeltaStream bool, participant string, options RoomOutputOptions) (*ParticipantTranscriptionOutput, error) {
	if room == nil || room.LocalParticipant == nil {
		return nil, errors.New("roomio: connected room with local participant is required for transcription")
	}
	resolved, err := resolveOutputOptions(&options)
	if err != nil {
		return nil, err
	}
	return newParticipantTranscriptionOutput(parent, roomTextPublisher{room: room}, isDeltaStream, participant, resolved), nil
}

func newParticipantTranscriptionOutput(parent context.Context, publisher textPublisher, isDeltaStream bool, participant string, options RoomOutputOptions) *ParticipantTranscriptionOutput {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancelCause(parent)
	expressive := options.ExpressiveEnabled
	if expressive == nil {
		expressive = func() bool { return false }
	}
	o := &ParticipantTranscriptionOutput{
		ctx: ctx, cancel: cancel, pub: publisher, delta: isDeltaStream, json: options.JSONFormat,
		expressive: expressive, operationTimeout: options.TextOperationTimeout,
		commands:    stream.NewChannel[textCommand](options.TextCapacity),
		participant: participant, done: make(chan struct{}),
	}
	o.resetSegment()
	go o.run()
	return o
}

func (o *ParticipantTranscriptionOutput) SetAttached(attached bool) { o.attached.Store(attached) }
func (o *ParticipantTranscriptionOutput) OnAttached()               { o.attached.Store(true) }
func (o *ParticipantTranscriptionOutput) OnDetached()               { o.attached.Store(false) }

func (o *ParticipantTranscriptionOutput) CaptureText(ctx context.Context, text agents.TimedString) error {
	if !o.attached.Load() {
		return nil
	}
	return o.send(ctx, textCommand{kind: textCapture, text: text})
}

func (o *ParticipantTranscriptionOutput) Flush(ctx context.Context) error {
	return o.send(ctx, textCommand{kind: textFlush})
}

func (o *ParticipantTranscriptionOutput) SetParticipant(ctx context.Context, identity string) error {
	return o.send(ctx, textCommand{kind: textSetParticipant, participant: identity})
}

func (o *ParticipantTranscriptionOutput) ParticipantIdentity() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.participant
}

func (o *ParticipantTranscriptionOutput) send(ctx context.Context, command textCommand) error {
	if o.closed.Load() {
		return ErrParticipantTranscriptionClosed
	}
	command.done = make(chan error, 1)
	if ctx == nil {
		ctx = context.Background()
	}
	command.ctx = ctx
	if err := o.commands.Send(ctx, command); err != nil {
		return err
	}
	select {
	case err := <-command.done:
		return err
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-o.ctx.Done():
		return ErrParticipantTranscriptionClosed
	}
}

func (o *ParticipantTranscriptionOutput) run() {
	defer close(o.done)
	for {
		command, err := o.commands.Recv(o.ctx)
		if err != nil {
			o.closeWriter()
			return
		}
		operationParent := command.ctx
		deadlineCancel := func() {}
		if _, hasDeadline := operationParent.Deadline(); !hasDeadline {
			operationParent, deadlineCancel = context.WithTimeout(operationParent, o.operationTimeout)
		}
		operationCtx, cancel := context.WithCancelCause(operationParent)
		stop := context.AfterFunc(o.ctx, func() { cancel(context.Cause(o.ctx)) })
		select {
		case <-operationCtx.Done():
			err = context.Cause(operationCtx)
		default:
			switch command.kind {
			case textCapture:
				err = o.capture(operationCtx, command.text)
			case textFlush:
				err = o.flush(operationCtx)
			case textSetParticipant:
				err = o.flush(operationCtx)
				o.mu.Lock()
				o.participant = command.participant
				o.mu.Unlock()
				o.resetSegment()
			}
		}
		stop()
		cancel(nil)
		deadlineCancel()
		command.done <- err
	}
}

func (o *ParticipantTranscriptionOutput) capture(ctx context.Context, text agents.TimedString) error {
	o.mu.Lock()
	participant := o.participant
	o.mu.Unlock()
	if participant == "" || !o.pub.Connected() {
		return nil
	}
	if !o.capturing {
		o.resetSegment()
		o.capturing = true
	}
	raw := text.Text
	clean := raw
	if o.expressive() {
		if o.delta {
			clean = o.stripper.Push(raw)
		} else {
			clean, o.tags = tts.SplitAllMarkup(raw)
			clean = strings.TrimLeft(clean, " \t\r\n")
		}
	}
	if clean == "" {
		return nil
	}
	payload, err := o.encode(clean, text)
	if err != nil {
		return err
	}
	o.latest = payload
	if o.delta {
		if o.writer == nil {
			o.writer = o.newWriter(false, o.stripper.Tags())
		}
		return o.write(ctx, o.writer, payload)
	}
	writer := o.newWriter(false, o.tags)
	if err := o.write(ctx, writer, payload); err != nil {
		writer.Close()
		return err
	}
	writer.Close()
	return nil
}

func (o *ParticipantTranscriptionOutput) flush(ctx context.Context) error {
	if !o.capturing {
		return nil
	}
	o.capturing = false
	if !o.pub.Connected() {
		o.closeWriter()
		o.resetSegment()
		return nil
	}
	if o.delta {
		pending := ""
		if o.expressive() {
			pending = o.stripper.Flush()
		}
		writer := o.writer
		o.writer = nil
		if writer == nil && pending != "" {
			writer = o.newWriter(true, o.stripper.Tags())
		}
		if writer != nil {
			if pending != "" {
				payload, err := o.encode(pending, agents.TimedString{Text: pending})
				if err != nil {
					writer.Close()
					return err
				}
				if err := o.write(ctx, writer, payload); err != nil {
					writer.Close()
					return err
				}
			}
			writer.Close()
		}
	} else if o.latest != "" {
		writer := o.newWriter(true, o.tags)
		if err := o.write(ctx, writer, o.latest); err != nil {
			writer.Close()
			return err
		}
		writer.Close()
	}
	o.resetSegment()
	return nil
}

func (o *ParticipantTranscriptionOutput) resetSegment() {
	o.segmentID = "SG_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	o.capturing = false
	o.latest = ""
	o.stripper = tts.TranscriptMarkupStripper{}
	o.tags = nil
}

func (o *ParticipantTranscriptionOutput) encode(clean string, timing agents.TimedString) (string, error) {
	if !o.json {
		return clean, nil
	}
	message := &agentpb.TimedString{Text: clean, Confidence: timing.Confidence, SpeakerId: timing.SpeakerID}
	if timing.StartTime != nil {
		seconds := timing.StartTime.Seconds()
		message.StartTime = &seconds
	}
	if timing.EndTime != nil {
		seconds := timing.EndTime.Seconds()
		message.EndTime = &seconds
	}
	if timing.StartTimeOffset != nil {
		seconds := timing.StartTimeOffset.Seconds()
		message.StartTimeOffset = &seconds
	}
	encoded, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(message)
	if err != nil {
		return "", err
	}
	return string(encoded) + "\n", nil
}

func (o *ParticipantTranscriptionOutput) newWriter(final bool, tags []tts.ExpressiveTag) textWriter {
	o.mu.Lock()
	participant := o.participant
	o.mu.Unlock()
	attributes := map[string]string{
		AttributeTranscriptionFinal:     fmt.Sprintf("%t", final),
		AttributeTranscriptionSegmentID: o.segmentID,
	}
	if trackID := o.pub.MicrophoneTrackID(participant); trackID != "" {
		attributes[AttributeTranscriptionTrackID] = trackID
	}
	for key, value := range tts.ExpressionAttribute(tags) {
		attributes[key] = value
	}
	// server-sdk-go v2.18.1 cannot override sender identity on text streams.
	// Preserve attribution as an explicit header attribute for Go consumers.
	if participant != "" && participant != o.pub.LocalIdentity() {
		attributes[AttributeTranscribedParticipant] = participant
	}
	return o.pub.Stream(lksdk.StreamTextOptions{Topic: TopicTranscription, Attributes: attributes})
}

func (o *ParticipantTranscriptionOutput) write(ctx context.Context, writer textWriter, text string) error {
	o.mu.Lock()
	o.activeWriter = writer
	o.mu.Unlock()
	err := writer.Write(ctx, text)
	o.mu.Lock()
	if o.activeWriter == writer {
		o.activeWriter = nil
	}
	o.mu.Unlock()
	return err
}

func (o *ParticipantTranscriptionOutput) closeWriter() {
	if o.writer != nil {
		o.writer.Close()
		o.writer = nil
	}
	o.mu.Lock()
	if o.activeWriter != nil {
		o.activeWriter.Close()
		o.activeWriter = nil
	}
	o.mu.Unlock()
}

func (o *ParticipantTranscriptionOutput) Close(ctx context.Context) error {
	if o == nil {
		return nil
	}
	o.closeOnce.Do(func() {
		o.closed.Store(true)
		o.cancel(ErrParticipantTranscriptionClosed)
		_ = o.commands.Abort(ErrParticipantTranscriptionClosed)
	})
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-o.done:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// ParallelTextOutput fans out deterministically without spawning per-chunk
// goroutines. It is useful for RoomIO plus a caller-provided external sink.
type ParallelTextOutput struct{ outputs []voice.TextOutput }

func NewParallelTextOutput(outputs ...voice.TextOutput) *ParallelTextOutput {
	filtered := make([]voice.TextOutput, 0, len(outputs))
	for _, output := range outputs {
		if output != nil {
			filtered = append(filtered, output)
		}
	}
	return &ParallelTextOutput{outputs: filtered}
}
func (o *ParallelTextOutput) CaptureText(ctx context.Context, text agents.TimedString) error {
	for _, output := range o.outputs {
		if err := output.CaptureText(ctx, text); err != nil {
			return err
		}
	}
	return nil
}
func (o *ParallelTextOutput) Flush(ctx context.Context) error {
	for _, output := range o.outputs {
		if err := output.Flush(ctx); err != nil {
			return err
		}
	}
	return nil
}
func (o *ParallelTextOutput) SetAttached(attached bool) {
	for _, output := range o.outputs {
		output.SetAttached(attached)
	}
}
func (o *ParallelTextOutput) OnAttached() {
	for _, output := range o.outputs {
		output.OnAttached()
	}
}
func (o *ParallelTextOutput) OnDetached() {
	for _, output := range o.outputs {
		output.OnDetached()
	}
}

var _ voice.TextOutput = (*ParticipantTranscriptionOutput)(nil)
var _ voice.TextOutput = (*ParallelTextOutput)(nil)
