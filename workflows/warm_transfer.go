// SPDX-License-Identifier: Apache-2.0

package workflows

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"strings"
	"sync"
	"time"

	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/llm"
	"github.com/infinityscroll/livekit-agents-go/stt"
	"github.com/infinityscroll/livekit-agents-go/tts"
	"github.com/infinityscroll/livekit-agents-go/vad"
	"github.com/infinityscroll/livekit-agents-go/voice"
	"github.com/livekit/protocol/livekit"
	"google.golang.org/protobuf/proto"
)

const (
	HumanAgentIdentity                  = "human-agent-sip"
	DefaultCallerHangupNoticeTimeout    = 30 * time.Second
	DefaultCallerHangupCleanupTimeout   = 10 * time.Second
	DefaultWarmTransferCleanupTimeout   = 10 * time.Second
	DefaultWarmTransferMergeTimeout     = 30 * time.Second
	DefaultWarmTransferDialDrainTimeout = 10 * time.Second
	BuiltinHoldMusic                    = "hold_music"
)

var (
	ErrWarmTransferAlreadyStarted = errors.New("workflows: warm transfer has already started")
	ErrWarmTransferNotStarted     = errors.New("workflows: warm transfer has not started")
	ErrWarmTransferAlreadyDone    = errors.New("workflows: warm transfer is already complete")
	ErrWarmTransferNotReady       = errors.New("workflows: warm transfer is not ready to merge")
	ErrWarmTransferCallerGone     = errors.New("workflows: caller hung up before the transfer completed")
	ErrWarmTransferVoicemail      = errors.New("workflows: voicemail detected")
	ErrWarmTransferDeclined       = errors.New("workflows: human agent declined to connect")
	ErrWarmTransferRoomClosed     = errors.New("workflows: human agent room closed")
)

const CallerHangupInstruction = `The caller has hung up before the transfer could be completed.
Briefly inform the human agent that the caller has left and that you are ending the call now.`

const WarmTransferPersona = `# Identity

You are an agent that is reaching out to a human agent for help. There has been a previous conversation
between you and a caller, the conversation history is included below.

# Goal

Your main goal is to give the human agent sufficient context about why the caller had called in,
so that the human agent could gain sufficient knowledge to help the caller directly.`

const WarmTransferInstructionsTemplate = `{persona}

# Context

In the conversation, user refers to the human agent, caller refers to the person who's transcript is included.
Remember, you are not speaking to the caller right now, you are speaking to the human agent.

## Conversation history with caller
{_conversation_history}
## End of conversation history with caller

Once the human agent has confirmed, you should call the tool ` + "`connect_to_caller`" + ` to connect them to the caller.

You are talking to the human agent now, start by giving them a summary of the conversation so far, and answer any questions they might have.

{extra}
`

type WarmTransferResult struct {
	HumanAgentIdentity string `json:"humanAgentIdentity"`
}

// WarmTransferSpeech is exact text or a callback that starts speech in the
// consultation session. Its zero value means no speech.
type WarmTransferSpeech struct {
	Text  *string
	Start func(context.Context, WarmTransferSession) (*voice.SpeechHandle, error)
}

func TextSpeech(text string) WarmTransferSpeech { return WarmTransferSpeech{Text: &text} }

func SpeechFunc(start func(context.Context, WarmTransferSession) (*voice.SpeechHandle, error)) WarmTransferSpeech {
	return WarmTransferSpeech{Start: start}
}

func (s WarmTransferSpeech) validate(name string) error {
	if s.Text != nil && s.Start != nil {
		return fmt.Errorf("workflows: %s accepts text or callback, not both", name)
	}
	return nil
}

// WarmTransferInstructionConfig models the string | InstructionParts union.
// Full replaces the prompt; Parts retains the template. Both nil selects the
// built-in prompt.
type WarmTransferInstructionConfig struct {
	Full  *string
	Parts *InstructionParts
}

func FullWarmTransferInstructions(value string) WarmTransferInstructionConfig {
	return WarmTransferInstructionConfig{Full: &value}
}

func PartialWarmTransferInstructions(value InstructionParts) WarmTransferInstructionConfig {
	return WarmTransferInstructionConfig{Parts: &value}
}

func (i WarmTransferInstructionConfig) validate() error {
	if i.Full != nil && i.Parts != nil {
		return errors.New("workflows: warm-transfer instructions accept a full string or parts, not both")
	}
	return nil
}

type WarmTransferHoldAudio struct {
	Source string
	Volume float64
}

type WarmTransferIOState struct {
	AudioInput          bool
	AudioOutput         bool
	TranscriptionOutput bool
}

type WarmTransferParticipantEvent struct {
	Identity string
	Kind     livekit.ParticipantInfo_Kind
}

// WarmTransferHold is owned by the backend and stopped exactly once.
type WarmTransferHold interface {
	Stop(context.Context) error
}

// WarmTransferSession is the consultation surface used by the workflow. Every
// *voice.AgentSession[T] satisfies it.
type WarmTransferSession interface {
	Say(context.Context, string, voice.SayOptions) (*voice.SpeechHandle, error)
	GenerateReply(context.Context, voice.GenerateReplyOptions) (*voice.SpeechHandle, error)
	Interrupt(context.Context, bool) error
	Close(context.Context, ...voice.CloseOptions) error
}

// WarmTransferConsultation owns the private human-agent room and its session.
// Disconnected must return a stable, close-only/read-only channel; a nil value
// is rejected so room failures cannot be silently missed.
type WarmTransferConsultation interface {
	RoomName() string
	Session() WarmTransferSession
	Disconnected() <-chan error
	Close(context.Context) error
}

type WarmTransferDialRequest struct {
	CallerRoomName     string
	CallerIdentity     string
	HumanRoomName      string
	HumanIdentity      string
	SIPCallTo          string
	SIPTrunkID         string
	SIPConnection      *livekit.SIPOutboundConfig
	SIPNumber          string
	SIPHeaders         map[string]string
	DTMF               *string
	RingingTimeout     *time.Duration
	Instructions       llm.Instructions
	ChatContext        *llm.ChatContext
	Tools              *llm.Context
	AllowInterruptions *bool
	STT                agents.Override[stt.STT]
	VAD                agents.Override[vad.VAD]
	LLM                agents.Override[llm.LLM]
	TTS                agents.Override[tts.TTS]
	TurnHandling       *voice.TurnHandlingOptions
}

func (r WarmTransferDialRequest) Clone() WarmTransferDialRequest {
	r.SIPHeaders = cloneStringMap(r.SIPHeaders)
	if r.SIPConnection != nil {
		r.SIPConnection = proto.Clone(r.SIPConnection).(*livekit.SIPOutboundConfig)
	}
	if r.DTMF != nil {
		value := *r.DTMF
		r.DTMF = &value
	}
	if r.RingingTimeout != nil {
		value := *r.RingingTimeout
		r.RingingTimeout = &value
	}
	if r.ChatContext != nil {
		r.ChatContext = r.ChatContext.Copy(llm.CopyOptions{})
	}
	if r.Tools != nil {
		r.Tools = r.Tools.Copy()
	}
	if r.AllowInterruptions != nil {
		value := *r.AllowInterruptions
		r.AllowInterruptions = &value
	}
	r.TurnHandling = cloneTurnHandlingOptions(r.TurnHandling)
	return r
}

// WarmTransferBackend isolates RTC/SIP ownership from the deterministic
// workflow state machine. Methods that may block are context-first. The caller
// event stream must be bounded by the implementation and must never be closed
// while Run is active without first carrying the terminal disconnect event.
type WarmTransferBackend interface {
	CallerRoomName(context.Context) (string, error)
	CallerLocalIdentity(context.Context) (string, error)
	CallerPresent(context.Context) (bool, error)
	CallerDisconnected() <-chan WarmTransferParticipantEvent

	CaptureCallerIO(context.Context) (WarmTransferIOState, error)
	SetCallerIO(context.Context, WarmTransferIOState) error
	StartHold(context.Context, WarmTransferHoldAudio) (WarmTransferHold, error)

	Dial(context.Context, WarmTransferDialRequest) (WarmTransferConsultation, error)
	MoveParticipant(context.Context, string, string, string) error
	RemoveParticipant(context.Context, string, string) error
	DeleteRoom(context.Context, string) error
}

// WarmTransferPostMergeBackend optionally installs the caller-room cleanup
// listener after a successful merge. Ownership moves to the backend because the
// foreground task is about to return.
type WarmTransferPostMergeBackend interface {
	DeleteCallerRoomOnDisconnect(context.Context, string) error
}

// WarmTransferBackendFuncs is a zero-allocation adapter for applications that
// already own RTC callbacks, RoomIO, or a custom consultation session.
type WarmTransferBackendFuncs struct {
	CallerRoomNameFunc      func(context.Context) (string, error)
	CallerLocalIdentityFunc func(context.Context) (string, error)
	CallerPresentFunc       func(context.Context) (bool, error)
	CallerDisconnectedChan  <-chan WarmTransferParticipantEvent
	CaptureCallerIOFunc     func(context.Context) (WarmTransferIOState, error)
	SetCallerIOFunc         func(context.Context, WarmTransferIOState) error
	StartHoldFunc           func(context.Context, WarmTransferHoldAudio) (WarmTransferHold, error)
	DialFunc                func(context.Context, WarmTransferDialRequest) (WarmTransferConsultation, error)
	MoveParticipantFunc     func(context.Context, string, string, string) error
	RemoveParticipantFunc   func(context.Context, string, string) error
	DeleteRoomFunc          func(context.Context, string) error
	PostMergeCleanupFunc    func(context.Context, string) error
}

func (b WarmTransferBackendFuncs) CallerRoomName(ctx context.Context) (string, error) {
	if b.CallerRoomNameFunc == nil {
		return "", errors.New("workflows: CallerRoomName backend function is required")
	}
	return b.CallerRoomNameFunc(ctx)
}
func (b WarmTransferBackendFuncs) CallerLocalIdentity(ctx context.Context) (string, error) {
	if b.CallerLocalIdentityFunc == nil {
		return "", errors.New("workflows: CallerLocalIdentity backend function is required")
	}
	return b.CallerLocalIdentityFunc(ctx)
}
func (b WarmTransferBackendFuncs) CallerPresent(ctx context.Context) (bool, error) {
	if b.CallerPresentFunc == nil {
		return false, errors.New("workflows: CallerPresent backend function is required")
	}
	return b.CallerPresentFunc(ctx)
}
func (b WarmTransferBackendFuncs) CallerDisconnected() <-chan WarmTransferParticipantEvent {
	return b.CallerDisconnectedChan
}
func (b WarmTransferBackendFuncs) CaptureCallerIO(ctx context.Context) (WarmTransferIOState, error) {
	if b.CaptureCallerIOFunc == nil {
		return WarmTransferIOState{}, errors.New("workflows: CaptureCallerIO backend function is required")
	}
	return b.CaptureCallerIOFunc(ctx)
}
func (b WarmTransferBackendFuncs) SetCallerIO(ctx context.Context, state WarmTransferIOState) error {
	if b.SetCallerIOFunc == nil {
		return errors.New("workflows: SetCallerIO backend function is required")
	}
	return b.SetCallerIOFunc(ctx, state)
}
func (b WarmTransferBackendFuncs) StartHold(ctx context.Context, audio WarmTransferHoldAudio) (WarmTransferHold, error) {
	if b.StartHoldFunc == nil {
		return nil, errors.New("workflows: StartHold backend function is required unless hold audio is disabled")
	}
	return b.StartHoldFunc(ctx, audio)
}
func (b WarmTransferBackendFuncs) Dial(ctx context.Context, request WarmTransferDialRequest) (WarmTransferConsultation, error) {
	if b.DialFunc == nil {
		return nil, errors.New("workflows: Dial backend function is required")
	}
	return b.DialFunc(ctx, request.Clone())
}
func (b WarmTransferBackendFuncs) MoveParticipant(ctx context.Context, from, identity, to string) error {
	if b.MoveParticipantFunc == nil {
		return errors.New("workflows: MoveParticipant backend function is required")
	}
	return b.MoveParticipantFunc(ctx, from, identity, to)
}
func (b WarmTransferBackendFuncs) RemoveParticipant(ctx context.Context, room, identity string) error {
	if b.RemoveParticipantFunc == nil {
		return errors.New("workflows: RemoveParticipant backend function is required")
	}
	return b.RemoveParticipantFunc(ctx, room, identity)
}
func (b WarmTransferBackendFuncs) DeleteRoom(ctx context.Context, room string) error {
	if b.DeleteRoomFunc == nil {
		return errors.New("workflows: DeleteRoom backend function is required")
	}
	return b.DeleteRoomFunc(ctx, room)
}
func (b WarmTransferBackendFuncs) DeleteCallerRoomOnDisconnect(ctx context.Context, room string) error {
	if b.PostMergeCleanupFunc == nil {
		return nil
	}
	return b.PostMergeCleanupFunc(ctx, room)
}

type WarmTransferTaskOptions[UserData any] struct {
	AbortContext context.Context
	Backend      WarmTransferBackend

	SIPCallTo      string
	SIPTrunkID     agents.Override[string]
	SIPConnection  *livekit.SIPOutboundConfig
	SIPNumber      string
	SIPHeaders     map[string]string
	DTMF           *string
	RingingTimeout *time.Duration
	RoomName       *string

	// Zero inherits the agents-js default hold music at volume 0.8. Use
	// agents.Disable[WarmTransferHoldAudio]() for holdAudio: null.
	HoldAudio agents.Override[WarmTransferHoldAudio]

	GreetingSpeech          WarmTransferSpeech
	CallerHangupSpeech      WarmTransferSpeech
	CallerHangupInstruction *string // Deprecated: prefer CallerHangupSpeech.
	Instructions            WarmTransferInstructionConfig
	ChatCtx                 *llm.ChatContext
	Tools                   *llm.Context
	STT                     agents.Override[stt.STT]
	VAD                     agents.Override[vad.VAD]
	LLM                     agents.Override[llm.LLM]
	TTS                     agents.Override[tts.TTS]
	TurnHandling            *voice.TurnHandlingOptions
	AllowInterruptions      *bool

	CallerHangupNoticeTimeout  time.Duration
	CallerHangupCleanupTimeout time.Duration
	CleanupTimeout             time.Duration
	MergeTimeout               time.Duration
	DialDrainTimeout           time.Duration
	OnError                    func(error)
}

type transferOutcome struct {
	result WarmTransferResult
	err    error
}

type WarmTransferTask[UserData any] struct {
	options      WarmTransferTaskOptions[UserData]
	trunkID      string
	holdAudio    WarmTransferHoldAudio
	holdEnabled  bool
	instructions llm.Instructions
	tools        *llm.Context
	voiceTask    *voice.AgentTask[WarmTransferResult, UserData]

	mu           sync.Mutex
	started      bool
	done         bool
	merging      bool
	callerRoom   string
	humanRoom    string
	consultation WarmTransferConsultation
	runCtx       context.Context
	decision     chan transferOutcome
	mergeDone    chan struct{}
	decisionOnce sync.Once
	ready        chan struct{}
	readyOnce    sync.Once
}

func NewWarmTransferTask[UserData any](options WarmTransferTaskOptions[UserData]) (*WarmTransferTask[UserData], error) {
	if options.Backend == nil {
		return nil, errors.New("workflows: warm-transfer Backend is required")
	}
	if options.SIPCallTo == "" {
		return nil, errors.New("`sipCallTo` must be set")
	}
	if options.RoomName != nil && *options.RoomName == "" {
		return nil, errors.New("`roomName` must not be empty")
	}
	if err := options.GreetingSpeech.validate("GreetingSpeech"); err != nil {
		return nil, err
	}
	if err := options.CallerHangupSpeech.validate("CallerHangupSpeech"); err != nil {
		return nil, err
	}
	if err := options.Instructions.validate(); err != nil {
		return nil, err
	}

	trunkID := ""
	switch {
	case options.SIPTrunkID.IsDisabled():
	case func() bool {
		value, ok := options.SIPTrunkID.Value()
		if ok {
			trunkID = value
		}
		return ok
	}():
	case options.SIPConnection != nil:
	default:
		trunkID = os.Getenv("LIVEKIT_SIP_OUTBOUND_TRUNK")
	}
	if trunkID == "" && options.SIPConnection == nil {
		return nil, errors.New("`LIVEKIT_SIP_OUTBOUND_TRUNK` environment variable, `sipTrunkId`, or `sipConnection` must be set")
	}
	if options.SIPNumber == "" {
		options.SIPNumber = os.Getenv("LIVEKIT_SIP_NUMBER")
	}
	if options.CallerHangupNoticeTimeout == 0 {
		options.CallerHangupNoticeTimeout = DefaultCallerHangupNoticeTimeout
	}
	if options.CallerHangupCleanupTimeout == 0 {
		options.CallerHangupCleanupTimeout = DefaultCallerHangupCleanupTimeout
	}
	if options.CleanupTimeout == 0 {
		options.CleanupTimeout = DefaultWarmTransferCleanupTimeout
	}
	if options.MergeTimeout == 0 {
		options.MergeTimeout = DefaultWarmTransferMergeTimeout
	}
	if options.DialDrainTimeout == 0 {
		options.DialDrainTimeout = DefaultWarmTransferDialDrainTimeout
	}
	if options.RingingTimeout != nil && *options.RingingTimeout < 0 {
		return nil, errors.New("workflows: RingingTimeout must not be negative")
	}
	if options.CallerHangupNoticeTimeout < 0 || options.CallerHangupCleanupTimeout < 0 || options.CleanupTimeout < 0 || options.MergeTimeout <= 0 || options.DialDrainTimeout < 0 {
		return nil, errors.New("workflows: warm-transfer timeouts must not be negative and MergeTimeout must be positive")
	}
	hold, enabled := options.HoldAudio.Resolve(WarmTransferHoldAudio{Source: BuiltinHoldMusic, Volume: 0.8}, true)
	if enabled {
		if hold.Source == "" {
			return nil, errors.New("workflows: hold audio source must not be empty")
		}
		if math.IsNaN(hold.Volume) || math.IsInf(hold.Volume, 0) || hold.Volume < 0 {
			return nil, errors.New("workflows: hold audio volume must be finite and non-negative")
		}
	}
	instructions := ResolveWarmTransferInstructions(options.Instructions, options.ChatCtx)
	t := &WarmTransferTask[UserData]{
		options: options, trunkID: trunkID, holdAudio: hold, holdEnabled: enabled,
		instructions: llm.NewInstructions(instructions, ""), decision: make(chan transferOutcome, 1), mergeDone: make(chan struct{}), ready: make(chan struct{}),
	}
	tools, err := t.buildTools(options.Tools)
	if err != nil {
		return nil, err
	}
	t.tools = tools
	voiceTask, err := voice.NewAgentTask[WarmTransferResult](voice.AgentTaskOptions[UserData]{
		AgentOptions: voice.AgentOptions[UserData]{
			ID: "warm_transfer", Instructions: t.instructions, ChatContext: options.ChatCtx, Tools: tools,
			STT: options.STT, VAD: options.VAD, LLM: options.LLM, TTS: options.TTS, TurnHandling: options.TurnHandling,
			Hooks: voice.AgentHooks[UserData]{OnEnter: func(ctx context.Context, _ *voice.AgentContext[UserData]) error {
				result, runErr := t.Run(ctx)
				if runErr != nil {
					_ = t.voiceTask.Fail(runErr)
				} else {
					_ = t.voiceTask.Complete(result)
				}
				return nil
			}},
		},
	})
	if err != nil {
		return nil, err
	}
	t.voiceTask = voiceTask
	return t, nil
}

// CreateWarmTransferTask is the functional constructor name used by
// agents-js. Go returns validation errors instead of throwing.
func CreateWarmTransferTask[UserData any](options WarmTransferTaskOptions[UserData]) (*WarmTransferTask[UserData], error) {
	return NewWarmTransferTask(options)
}

func MustWarmTransferTask[UserData any](options WarmTransferTaskOptions[UserData]) *WarmTransferTask[UserData] {
	task, err := NewWarmTransferTask(options)
	if err != nil {
		panic(err)
	}
	return task
}

func (t *WarmTransferTask[UserData]) VoiceTask() *voice.AgentTask[WarmTransferResult, UserData] {
	return t.voiceTask
}
func (t *WarmTransferTask[UserData]) Instructions() llm.Instructions { return t.instructions }
func (t *WarmTransferTask[UserData]) ToolContext() *llm.Context      { return t.tools.Copy() }

// WaitReady waits until the outbound call has answered and the consultation
// tools may be invoked.
func (t *WarmTransferTask[UserData]) WaitReady(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-t.ready:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (t *WarmTransferTask[UserData]) Run(ctx context.Context) (result WarmTransferResult, runErr error) {
	if ctx == nil {
		ctx = context.Background()
	}
	t.mu.Lock()
	if t.started {
		t.mu.Unlock()
		return WarmTransferResult{}, ErrWarmTransferAlreadyStarted
	}
	t.started = true
	ctx, cancel := context.WithCancelCause(ctx)
	t.runCtx = ctx
	t.mu.Unlock()
	defer cancel(nil)
	if t.options.AbortContext != nil {
		stop := context.AfterFunc(t.options.AbortContext, func() { cancel(context.Cause(t.options.AbortContext)) })
		defer stop()
		if err := context.Cause(t.options.AbortContext); err != nil {
			cancel(err)
		}
	}

	callerEvents := t.options.Backend.CallerDisconnected()
	if callerEvents == nil {
		return WarmTransferResult{}, errors.New("workflows: warm-transfer backend must provide caller disconnect events")
	}
	callerRoom, err := t.options.Backend.CallerRoomName(ctx)
	if err != nil {
		return WarmTransferResult{}, fmt.Errorf("workflows: get caller room: %w", err)
	}
	humanRoom, err := ResolveHumanAgentRoomName(callerRoom, t.options.RoomName)
	if err != nil {
		return WarmTransferResult{}, err
	}
	callerIdentity, err := t.options.Backend.CallerLocalIdentity(ctx)
	if err != nil {
		return WarmTransferResult{}, fmt.Errorf("workflows: get caller local identity: %w", err)
	}
	if callerIdentity == "" {
		return WarmTransferResult{}, errors.New("caller room local participant is not available")
	}
	t.mu.Lock()
	t.callerRoom, t.humanRoom = callerRoom, humanRoom
	t.mu.Unlock()

	present, err := t.options.Backend.CallerPresent(ctx)
	if err != nil {
		return WarmTransferResult{}, fmt.Errorf("workflows: check caller presence: %w", err)
	}
	if !present {
		return WarmTransferResult{}, &llm.ToolError{Message: ErrWarmTransferCallerGone.Error()}
	}

	originalIO, err := t.options.Backend.CaptureCallerIO(ctx)
	if err != nil {
		return WarmTransferResult{}, fmt.Errorf("workflows: capture caller I/O state: %w", err)
	}
	ioChanged := false
	var hold WarmTransferHold
	var consultation WarmTransferConsultation
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), t.options.CleanupTimeout)
		defer cleanupCancel()
		if consultation != nil {
			t.safeCleanup("close consultation", func() error { return consultation.Close(cleanupCtx) })
		}
		if humanRoom != "" {
			t.safeCleanup("delete human-agent room", func() error { return t.options.Backend.DeleteRoom(cleanupCtx, humanRoom) })
		}
		if hold != nil {
			t.safeCleanup("stop hold audio", func() error { return hold.Stop(cleanupCtx) })
		}
		if ioChanged {
			t.safeCleanup("restore caller I/O", func() error { return t.options.Backend.SetCallerIO(cleanupCtx, originalIO) })
		}
		t.mu.Lock()
		t.done = true
		t.consultation = nil
		t.mu.Unlock()
	}()

	if t.holdEnabled {
		hold, err = t.options.Backend.StartHold(ctx, t.holdAudio)
		if err != nil {
			return WarmTransferResult{}, fmt.Errorf("workflows: start hold audio: %w", err)
		}
		if hold == nil {
			return WarmTransferResult{}, errors.New("workflows: hold backend returned nil handle")
		}
	}
	// Treat a failed setter as potentially partial and always restore the
	// captured state during rollback.
	ioChanged = true
	if err = t.options.Backend.SetCallerIO(ctx, WarmTransferIOState{}); err != nil {
		return WarmTransferResult{}, fmt.Errorf("workflows: disable caller I/O: %w", err)
	}

	request := WarmTransferDialRequest{
		CallerRoomName: callerRoom, CallerIdentity: callerIdentity, HumanRoomName: humanRoom,
		HumanIdentity: HumanAgentIdentity, SIPCallTo: t.options.SIPCallTo, SIPTrunkID: t.trunkID,
		SIPConnection: cloneSIPConnection(t.options.SIPConnection), SIPNumber: t.options.SIPNumber,
		SIPHeaders: cloneStringMap(t.options.SIPHeaders), DTMF: cloneStringPtr(t.options.DTMF),
		RingingTimeout: roundedRingingTimeout(t.options.RingingTimeout), Instructions: t.instructions,
		ChatContext: cloneChat(t.options.ChatCtx), Tools: t.tools.Copy(), AllowInterruptions: cloneBoolPtr(t.options.AllowInterruptions),
		STT: t.options.STT, VAD: t.options.VAD, LLM: t.options.LLM, TTS: t.options.TTS, TurnHandling: cloneTurnHandlingOptions(t.options.TurnHandling),
	}
	dialCtx, cancelDial := context.WithCancelCause(ctx)
	dialResult := make(chan struct {
		consultation WarmTransferConsultation
		err          error
	}, 1)
	go func() {
		value, dialErr := t.options.Backend.Dial(dialCtx, request.Clone())
		dialResult <- struct {
			consultation WarmTransferConsultation
			err          error
		}{value, dialErr}
	}()
	select {
	case dialed := <-dialResult:
		cancelDial(nil)
		if dialed.err != nil {
			if cause := context.Cause(ctx); cause != nil {
				return WarmTransferResult{}, cause
			}
			return WarmTransferResult{}, &llm.ToolError{Message: "could not dial human agent"}
		}
		consultation = dialed.consultation
	case event, ok := <-callerEvents:
		cancelDial(ErrWarmTransferCallerGone)
		t.drainLateDial(dialResult)
		if !ok || isCallerKind(event.Kind) {
			return WarmTransferResult{}, &llm.ToolError{Message: "caller hung up before the transfer completed"}
		}
		return WarmTransferResult{}, &llm.ToolError{Message: "caller hung up before the transfer completed"}
	case <-ctx.Done():
		cancelDial(context.Cause(ctx))
		t.drainLateDial(dialResult)
		return WarmTransferResult{}, context.Cause(ctx)
	}
	if consultation == nil || consultation.Session() == nil || consultation.RoomName() == "" || consultation.Disconnected() == nil {
		return WarmTransferResult{}, errors.New("workflows: dial backend returned an incomplete consultation")
	}
	t.mu.Lock()
	t.consultation = consultation
	t.humanRoom = consultation.RoomName()
	t.mu.Unlock()
	t.readyOnce.Do(func() { close(t.ready) })
	humanRoom = consultation.RoomName()
	if _, speechErr := CreateWarmTransferSpeech(ctx, consultation.Session(), t.options.GreetingSpeech, voice.SayOptions{}); speechErr != nil {
		t.report(fmt.Errorf("workflows: greet human agent: %w", speechErr))
	}

	for {
		select {
		case outcome := <-t.decision:
			if outcome.err != nil {
				return WarmTransferResult{}, outcome.err
			}
			if post, ok := t.options.Backend.(WarmTransferPostMergeBackend); ok {
				postCtx, postCancel := context.WithTimeout(context.Background(), t.options.CleanupTimeout)
				err := post.DeleteCallerRoomOnDisconnect(postCtx, callerRoom)
				postCancel()
				if err != nil {
					t.report(fmt.Errorf("workflows: install post-merge caller cleanup: %w", err))
				}
			}
			return outcome.result, nil
		case event, ok := <-callerEvents:
			if ok && !isCallerKind(event.Kind) {
				continue
			}
			if outcome, completed := t.takeOutcome(); completed {
				if outcome.err != nil {
					return WarmTransferResult{}, outcome.err
				}
				return outcome.result, nil
			}
			if t.isMerging() {
				if outcome, merged := t.waitForMerge(ctx); merged {
					if outcome.err != nil {
						return WarmTransferResult{}, outcome.err
					}
					return outcome.result, nil
				}
			}
			t.notifyCallerHangup(consultation)
			return WarmTransferResult{}, &llm.ToolError{Message: "caller hung up before the transfer completed"}
		case roomErr, ok := <-consultation.Disconnected():
			if outcome, completed := t.takeOutcome(); completed {
				if outcome.err != nil {
					return WarmTransferResult{}, outcome.err
				}
				return outcome.result, nil
			}
			if t.isMerging() {
				if outcome, merged := t.waitForMerge(ctx); merged {
					if outcome.err != nil {
						return WarmTransferResult{}, outcome.err
					}
					return outcome.result, nil
				}
			}
			if !ok {
				roomErr = ErrWarmTransferRoomClosed
			}
			if roomErr == nil {
				roomErr = ErrWarmTransferRoomClosed
			}
			return WarmTransferResult{}, &llm.ToolError{Message: "room closed: " + roomErr.Error()}
		case <-ctx.Done():
			if outcome, completed := t.takeOutcome(); completed {
				if outcome.err != nil {
					return WarmTransferResult{}, outcome.err
				}
				return outcome.result, nil
			}
			if t.isMerging() {
				if outcome, merged := t.waitForMerge(ctx); merged {
					if outcome.err != nil {
						return WarmTransferResult{}, outcome.err
					}
					return outcome.result, nil
				}
			}
			return WarmTransferResult{}, context.Cause(ctx)
		}
	}
}

func (t *WarmTransferTask[UserData]) buildTools(existing *llm.Context) (*llm.Context, error) {
	entries := make([]any, 0, 3)
	if existing != nil {
		for _, tool := range existing.Flatten() {
			entries = append(entries, tool)
		}
	}
	connect, err := llm.NewTool(llm.FunctionToolOptions[struct{}, any]{
		Name: "connect_to_caller", Description: "Called when the human agent wants to connect to the caller.", Flags: llm.ToolFlagIgnoreOnEnter,
		Execute: func(ctx context.Context, _ struct{}, _ llm.ToolOptions) (any, error) { return nil, t.connect(ctx) },
	})
	if err != nil {
		return nil, err
	}
	type declineInput struct {
		Reason string `json:"reason"`
	}
	declineSchema := json.RawMessage(`{"type":"object","properties":{"reason":{"type":"string","description":"A short explanation of why the human agent declined to connect to the caller"}},"required":["reason"],"additionalProperties":false}`)
	decline, err := llm.NewTool(llm.FunctionToolOptions[declineInput, any]{
		Name: "decline_transfer", Description: "Handles the case when the human agent explicitly declines to connect to the caller.", Parameters: declineSchema, Flags: llm.ToolFlagIgnoreOnEnter,
		Validate: func(input *declineInput) error {
			if strings.TrimSpace(input.Reason) == "" {
				return errors.New("reason must not be empty")
			}
			return nil
		},
		Execute: func(_ context.Context, input declineInput, _ llm.ToolOptions) (any, error) {
			err := &llm.ToolError{Message: "human agent declined to connect: " + input.Reason}
			return nil, t.requestCompletion(transferOutcome{err: err})
		},
	})
	if err != nil {
		return nil, err
	}
	voicemail, err := llm.NewTool(llm.FunctionToolOptions[struct{}, any]{
		Name: "voicemail_detected", Description: "Called when the call reaches voicemail. Use this tool AFTER you hear the voicemail greeting", Flags: llm.ToolFlagIgnoreOnEnter,
		Execute: func(_ context.Context, _ struct{}, _ llm.ToolOptions) (any, error) {
			return nil, t.requestCompletion(transferOutcome{err: &llm.ToolError{Message: "voicemail detected"}})
		},
	})
	if err != nil {
		return nil, err
	}
	entries = append(entries, connect, decline, voicemail)
	return llm.NewToolContext(entries...)
}

func (t *WarmTransferTask[UserData]) connect(_ context.Context) error {
	t.mu.Lock()
	if !t.started {
		t.mu.Unlock()
		return &llm.ToolError{Message: ErrWarmTransferNotStarted.Error()}
	}
	if t.done {
		t.mu.Unlock()
		return &llm.ToolError{Message: "the transfer was already cancelled"}
	}
	if t.consultation == nil || t.callerRoom == "" || t.humanRoom == "" {
		t.mu.Unlock()
		return &llm.ToolError{Message: ErrWarmTransferNotReady.Error()}
	}
	if t.merging {
		t.mu.Unlock()
		return &llm.ToolError{Message: "the transfer is already being connected"}
	}
	t.merging = true
	callerRoom, humanRoom, runCtx := t.callerRoom, t.humanRoom, t.runCtx
	t.mu.Unlock()
	mergeCtx, cancel := context.WithTimeout(context.WithoutCancel(runCtx), t.options.MergeTimeout)
	err := t.options.Backend.MoveParticipant(mergeCtx, humanRoom, HumanAgentIdentity, callerRoom)
	cancel()
	if err == nil {
		t.complete(transferOutcome{result: WarmTransferResult{HumanAgentIdentity: HumanAgentIdentity}})
	} else if cause := context.Cause(runCtx); cause != nil {
		t.complete(transferOutcome{err: cause})
	}
	t.mu.Lock()
	t.merging = false
	close(t.mergeDone)
	t.mergeDone = make(chan struct{})
	t.mu.Unlock()
	if err != nil {
		return err
	}
	return nil
}

func (t *WarmTransferTask[UserData]) complete(outcome transferOutcome) {
	t.decisionOnce.Do(func() {
		t.mu.Lock()
		t.done = true
		t.mu.Unlock()
		t.decision <- outcome
	})
}

func (t *WarmTransferTask[UserData]) requestCompletion(outcome transferOutcome) error {
	t.mu.Lock()
	started, done := t.started, t.done
	t.mu.Unlock()
	if !started {
		return &llm.ToolError{Message: ErrWarmTransferNotStarted.Error()}
	}
	if done {
		return &llm.ToolError{Message: "the transfer was already cancelled"}
	}
	t.complete(outcome)
	return nil
}

func (t *WarmTransferTask[UserData]) takeOutcome() (transferOutcome, bool) {
	select {
	case outcome := <-t.decision:
		return outcome, true
	default:
		return transferOutcome{}, false
	}
}

func (t *WarmTransferTask[UserData]) isMerging() bool {
	t.mu.Lock()
	value := t.merging
	t.mu.Unlock()
	return value
}

func (t *WarmTransferTask[UserData]) waitForMerge(ctx context.Context) (transferOutcome, bool) {
	t.mu.Lock()
	done := t.mergeDone
	t.mu.Unlock()
	timer := time.NewTimer(t.options.MergeTimeout)
	defer timer.Stop()
	select {
	case outcome := <-t.decision:
		return outcome, true
	case <-done:
		select {
		case outcome := <-t.decision:
			return outcome, true
		default:
			return transferOutcome{}, false
		}
	case <-timer.C:
		return transferOutcome{err: context.DeadlineExceeded}, true
	case <-ctx.Done():
		// The merge uses a bounded context without caller cancellation so success
		// can win a late abort. Keep waiting on the bounded merge timer.
		select {
		case outcome := <-t.decision:
			return outcome, true
		case <-done:
			select {
			case outcome := <-t.decision:
				return outcome, true
			default:
				return transferOutcome{}, false
			}
		case <-timer.C:
			return transferOutcome{err: context.Cause(ctx)}, true
		}
	}
}

func (t *WarmTransferTask[UserData]) notifyCallerHangup(consultation WarmTransferConsultation) {
	session := consultation.Session()
	noticeCtx, cancel := context.WithTimeout(context.Background(), t.options.CallerHangupNoticeTimeout)
	_ = session.Interrupt(noticeCtx, true)
	handle, err := CreateCallerHangupSpeech(noticeCtx, session, t.options.CallerHangupSpeech, t.options.CallerHangupInstruction)
	if err == nil && handle != nil {
		err = handle.WaitForPlayout(noticeCtx)
	}
	cancel()
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.report(fmt.Errorf("workflows: notify human agent of caller hangup: %w", err))
	}
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), t.options.CallerHangupCleanupTimeout)
	if err := t.options.Backend.RemoveParticipant(cleanupCtx, consultation.RoomName(), HumanAgentIdentity); err != nil {
		t.report(fmt.Errorf("workflows: remove human agent after caller hangup: %w", err))
	}
	cleanupCancel()
}

func (t *WarmTransferTask[UserData]) drainLateDial(dialResult <-chan struct {
	consultation WarmTransferConsultation
	err          error
}) {
	timeout := t.options.DialDrainTimeout
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case result := <-dialResult:
		if result.consultation != nil {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), t.options.CleanupTimeout)
			_ = result.consultation.Close(cleanupCtx)
			cancel()
		}
	case <-timer.C:
		t.report(errors.New("workflows: timed out waiting for cancelled dial to return; backend violated context contract"))
	}
}

func (t *WarmTransferTask[UserData]) safeCleanup(operation string, fn func() error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			t.report(fmt.Errorf("workflows: %s panicked: %v", operation, recovered))
		}
	}()
	if err := fn(); err != nil {
		t.report(fmt.Errorf("workflows: %s: %w", operation, err))
	}
}

func (t *WarmTransferTask[UserData]) report(err error) {
	if err == nil || t.options.OnError == nil {
		return
	}
	defer func() { _ = recover() }()
	t.options.OnError(err)
}

func isCallerKind(kind livekit.ParticipantInfo_Kind) bool {
	return kind == livekit.ParticipantInfo_STANDARD || kind == livekit.ParticipantInfo_SIP || kind == livekit.ParticipantInfo_CONNECTOR
}

// ResolveWarmTransferInstructions renders the pinned agents-js prompt. A full
// string bypasses templating; unset parts preserve defaults and explicit empty
// parts remove their section.
func ResolveWarmTransferInstructions(config WarmTransferInstructionConfig, chat *llm.ChatContext) string {
	if config.Full != nil {
		return *config.Full
	}
	persona, extra := WarmTransferPersona, ""
	if config.Parts != nil {
		if config.Parts.Persona != nil {
			persona = config.Parts.Persona.Value()
		}
		if config.Parts.Extra != nil {
			extra = config.Parts.Extra.Value()
		}
	}
	replacer := strings.NewReplacer(
		"{persona}", persona,
		"{_conversation_history}", FormatWarmTransferConversation(chat),
		"{extra}", extra,
	)
	return replacer.Replace(WarmTransferInstructionsTemplate)
}

func FormatWarmTransferConversation(chat *llm.ChatContext) string {
	if chat == nil {
		return ""
	}
	var result strings.Builder
	for _, item := range chat.Items() {
		message, ok := item.(*llm.ChatMessage)
		if !ok || (message.Role != llm.RoleUser && message.Role != llm.RoleAssistant) {
			continue
		}
		text, ok := message.TextContent()
		if !ok || text == "" {
			continue
		}
		role := "Caller"
		if message.Role == llm.RoleAssistant {
			role = "Assistant"
		}
		fmt.Fprintf(&result, "%s: %s\n", role, text)
	}
	return result.String()
}

func ResolveHumanAgentRoomName(callerRoomName string, override *string) (string, error) {
	if override == nil {
		return callerRoomName + "-human-agent", nil
	}
	if *override == callerRoomName {
		return "", errors.New("`roomName` must differ from the caller room name")
	}
	return *override, nil
}

func CreateWarmTransferSpeech(ctx context.Context, session WarmTransferSession, speech WarmTransferSpeech, options voice.SayOptions) (*voice.SpeechHandle, error) {
	if session == nil {
		return nil, errors.New("workflows: warm-transfer session is required")
	}
	if speech.Start != nil {
		return speech.Start(ctx, session)
	}
	if speech.Text != nil {
		return session.Say(ctx, *speech.Text, options)
	}
	return nil, nil
}

func CreateCallerHangupSpeech(ctx context.Context, session WarmTransferSession, speech WarmTransferSpeech, instruction *string) (*voice.SpeechHandle, error) {
	allow, add := false, false
	handle, err := CreateWarmTransferSpeech(ctx, session, speech, voice.SayOptions{AllowInterruptions: &allow, AddToChatContext: &add})
	if err != nil || handle != nil {
		return handle, err
	}
	value := CallerHangupInstruction
	if instruction != nil {
		value = *instruction
	}
	instructions := llm.NewInstructions(value, "")
	return session.GenerateReply(ctx, voice.GenerateReplyOptions{
		Instructions: &instructions, AllowInterruptions: &allow,
		ToolChoice: llm.ToolChoice{Kind: llm.ToolChoiceNone},
	})
}

func roundedRingingTimeout(value *time.Duration) *time.Duration {
	if value == nil {
		return nil
	}
	rounded := time.Duration(math.Round(float64(*value)/float64(time.Second))) * time.Second
	return &rounded
}
func cloneStringMap(source map[string]string) map[string]string {
	if source == nil {
		return map[string]string{}
	}
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}
func cloneStringPtr(source *string) *string {
	if source == nil {
		return nil
	}
	value := *source
	return &value
}
func cloneBoolPtr(source *bool) *bool {
	if source == nil {
		return nil
	}
	value := *source
	return &value
}
func cloneChat(source *llm.ChatContext) *llm.ChatContext {
	if source == nil {
		return nil
	}
	return source.Copy(llm.CopyOptions{})
}
func cloneSIPConnection(source *livekit.SIPOutboundConfig) *livekit.SIPOutboundConfig {
	if source == nil {
		return nil
	}
	return proto.Clone(source).(*livekit.SIPOutboundConfig)
}

func cloneTurnHandlingOptions(source *voice.TurnHandlingOptions) *voice.TurnHandlingOptions {
	if source == nil {
		return nil
	}
	copy := *source
	if source.Interruption.FalseInterruptionTimeout != nil {
		value := *source.Interruption.FalseInterruptionTimeout
		copy.Interruption.FalseInterruptionTimeout = &value
	}
	if source.Interruption.BackchannelBoundary != nil {
		value := *source.Interruption.BackchannelBoundary
		copy.Interruption.BackchannelBoundary = &value
	}
	if source.UserTurnLimit.MaxWords != nil {
		value := *source.UserTurnLimit.MaxWords
		copy.UserTurnLimit.MaxWords = &value
	}
	if source.UserTurnLimit.MaxDuration != nil {
		value := *source.UserTurnLimit.MaxDuration
		copy.UserTurnLimit.MaxDuration = &value
	}
	return &copy
}
