// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/llm"
	"github.com/infinityscroll/livekit-agents-go/stream"
	"github.com/infinityscroll/livekit-agents-go/stt"
	"github.com/infinityscroll/livekit-agents-go/tts"
	"github.com/infinityscroll/livekit-agents-go/vad"
)

var (
	ErrAgentAlreadyRunning  = errors.New("voice agent already has a running activity")
	ErrAgentNotRunning      = errors.New("voice agent is not running")
	ErrAgentActivityChanged = errors.New("voice agent activity changed during update")
)

type StopResponse struct{}

func (StopResponse) Error() string { return "stop response" }

func IsStopResponse(err error) bool {
	var stopped StopResponse
	if errors.As(err, &stopped) {
		return true
	}
	var stoppedPointer *StopResponse
	return errors.As(err, &stoppedPointer)
}

type ModelSettings struct {
	ToolChoice llm.ToolChoice
}

type ExpressiveOptions struct {
	SpeechSteering          *tts.SpeechSteeringOptions
	TTSInstructionsTemplate *llm.Instructions
	TTSInstructionsAppend   string
}

type AsyncToolOptions = llm.AsyncToolOptions

type ToolHandlingOptions struct {
	Async *AsyncToolOptions
}

type AgentOptions[UserData any] struct {
	ID           string
	Instructions llm.Instructions
	ChatContext  *llm.ChatContext
	Tools        *llm.Context
	STT          agents.Override[stt.STT]
	VAD          agents.Override[vad.VAD]
	LLM          agents.Override[llm.LLM]
	Realtime     agents.Override[llm.RealtimeModel]
	TTS          agents.Override[tts.TTS]
	Expressive   agents.Override[ExpressiveOptions]
	TurnHandling *TurnHandlingOptions
	ToolHandling ToolHandlingOptions

	MinConsecutiveSpeechDelay *time.Duration
	UseTTSAlignedTranscript   *bool
	Hooks                     AgentHooks[UserData]
}

type AgentUpdateOptions struct {
	STT        *agents.Override[stt.STT]
	VAD        *agents.Override[vad.VAD]
	LLM        *agents.Override[llm.LLM]
	Realtime   *agents.Override[llm.RealtimeModel]
	TTS        *agents.Override[tts.TTS]
	Expressive *agents.Override[ExpressiveOptions]
}

// AgentSessionAccess is the hook-safe subset of AgentSession. Context is first
// on every operation that can block, and text streams have a separate typed
// method instead of a dynamic string/stream union.
type AgentSessionAccess[UserData any] interface {
	UserData() *UserData
	ChatContext() *llm.ChatContext
	Say(context.Context, string, SayOptions) (*SpeechHandle, error)
	SayStream(context.Context, stream.Reader[string], SayOptions) (*SpeechHandle, error)
	GenerateReply(context.Context, GenerateReplyOptions) (*SpeechHandle, error)
	Interrupt(context.Context, bool) error
}

type SayOptions struct {
	Audio              stream.Reader[agents.AudioFrame]
	AllowInterruptions *bool
	AddToChatContext   *bool
}

type GenerateReplyOptions struct {
	UserInput          string
	UserMessage        *llm.ChatMessage
	ChatContext        *llm.ChatContext
	Instructions       *llm.Instructions
	ToolChoice         llm.ToolChoice
	AllowInterruptions *bool
	InputModality      InputModality
}

func (o GenerateReplyOptions) Validate() error {
	if o.UserInput != "" && o.UserMessage != nil {
		return errors.New("generate reply accepts either UserInput or UserMessage, not both")
	}
	if o.InputModality != "" && o.InputModality != InputModalityAudio && o.InputModality != InputModalityText {
		return errors.New("unknown generate reply input modality")
	}
	return nil
}

type STTNodeItem struct {
	Event *stt.SpeechEvent
	Text  string
}

func STTEventItem(event stt.SpeechEvent) STTNodeItem { return STTNodeItem{Event: &event} }
func STTTextItem(text string) STTNodeItem            { return STTNodeItem{Text: text} }

type LLMNodeItem struct {
	Chunk *llm.ChatChunk
	Text  string
	Flush bool
}

func LLMChunkItem(chunk llm.ChatChunk) LLMNodeItem { return LLMNodeItem{Chunk: &chunk} }
func LLMTextItem(text string) LLMNodeItem          { return LLMNodeItem{Text: text} }
func LLMFlushItem() LLMNodeItem                    { return LLMNodeItem{Flush: true} }

type TranscriptionNodeItem struct {
	Text  string
	Timed *agents.TimedString
}

func TranscriptionTextItem(text string) TranscriptionNodeItem {
	return TranscriptionNodeItem{Text: text}
}
func TranscriptionTimedItem(value agents.TimedString) TranscriptionNodeItem {
	return TranscriptionNodeItem{Timed: &value}
}

type AgentHooks[UserData any] struct {
	OnEnter                 func(context.Context, *AgentContext[UserData]) error
	OnExit                  func(context.Context, *AgentContext[UserData]) error
	OnUserTurnCompleted     func(context.Context, *AgentContext[UserData], *llm.ChatContext, *llm.ChatMessage) error
	OnUserTurnExceeded      func(context.Context, *AgentContext[UserData], UserTurnExceededEvent) error
	STTNode                 func(context.Context, *AgentContext[UserData], stream.Reader[agents.AudioFrame], ModelSettings) (stream.Reader[STTNodeItem], error)
	LLMNode                 func(context.Context, *AgentContext[UserData], *llm.ChatContext, *llm.Context, ModelSettings) (stream.Reader[LLMNodeItem], error)
	TTSNode                 func(context.Context, *AgentContext[UserData], stream.Reader[string], ModelSettings) (stream.Reader[agents.AudioFrame], error)
	RealtimeAudioOutputNode func(context.Context, *AgentContext[UserData], stream.Reader[agents.AudioFrame], ModelSettings) (stream.Reader[agents.AudioFrame], error)
	TranscriptionNode       func(context.Context, *AgentContext[UserData], stream.Reader[TranscriptionNodeItem], ModelSettings) (stream.Reader[TranscriptionNodeItem], error)
}

type agentRuntime[UserData any] interface {
	Session() AgentSessionAccess[UserData]
	UpdateChatContext(context.Context, *llm.ChatContext) error
	UpdateInstructions(context.Context, llm.Instructions) error
	UpdateModels(context.Context, AgentUpdateOptions) error
	UpdateTools(context.Context, *llm.Context) error
}

type Agent[UserData any] struct {
	mu sync.RWMutex

	id                        string
	instructions              llm.Instructions
	chatContext               *llm.ChatContext
	tools                     *llm.Context
	stt                       agents.Override[stt.STT]
	vad                       agents.Override[vad.VAD]
	llm                       agents.Override[llm.LLM]
	realtime                  agents.Override[llm.RealtimeModel]
	tts                       agents.Override[tts.TTS]
	expressive                agents.Override[ExpressiveOptions]
	turnHandling              *TurnHandlingOptions
	toolHandling              ToolHandlingOptions
	minConsecutiveSpeechDelay *time.Duration
	useTTSAlignedTranscript   *bool
	hooks                     AgentHooks[UserData]
	runtime                   agentRuntime[UserData]
	taskResult                func() (value any, complete bool, err error)
}

func NewAgent[UserData any](options AgentOptions[UserData]) (*Agent[UserData], error) {
	if options.ID == "" {
		options.ID = "default_agent"
	}
	if err := validateAgentID(options.ID); err != nil {
		return nil, err
	}
	if options.TurnHandling != nil {
		if err := options.TurnHandling.Validate(); err != nil {
			return nil, fmt.Errorf("agent turn handling: %w", err)
		}
	}
	if options.MinConsecutiveSpeechDelay != nil && *options.MinConsecutiveSpeechDelay < 0 {
		return nil, errors.New("minimum consecutive speech delay must not be negative")
	}
	if err := validateModelOverrides(options.STT, options.VAD, options.LLM, options.Realtime, options.TTS); err != nil {
		return nil, err
	}
	if !options.LLM.IsInherited() && !options.Realtime.IsInherited() {
		return nil, errors.New("agent accepts one LLM selection: Chat LLM or RealtimeModel")
	}
	chat := options.ChatContext
	if chat == nil {
		chat = llm.EmptyChatContext()
	} else {
		chat = chat.Copy(llm.CopyOptions{})
	}
	tools := options.Tools
	if tools == nil {
		tools = llm.EmptyToolContext()
	} else {
		tools = tools.Copy()
	}
	return &Agent[UserData]{
		id: options.ID, instructions: options.Instructions, chatContext: chat, tools: tools,
		stt: options.STT, vad: options.VAD, llm: options.LLM, realtime: options.Realtime, tts: options.TTS,
		expressive: cloneExpressiveOverride(options.Expressive), turnHandling: cloneTurnHandling(options.TurnHandling),
		toolHandling:              cloneToolHandling(options.ToolHandling),
		minConsecutiveSpeechDelay: cloneDuration(options.MinConsecutiveSpeechDelay),
		useTTSAlignedTranscript:   cloneBool(options.UseTTSAlignedTranscript), hooks: options.Hooks,
	}, nil
}

func MustAgent[UserData any](options AgentOptions[UserData]) *Agent[UserData] {
	agent, err := NewAgent(options)
	if err != nil {
		panic(err)
	}
	return agent
}

func validateAgentID(id string) error {
	for index, r := range id {
		valid := r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_'
		if !valid || index == 0 && r >= '0' && r <= '9' {
			return errors.New("agent id must use lower_snake_case and start with a letter or underscore")
		}
	}
	return nil
}

func validateModelOverrides(values ...any) error {
	for _, value := range values {
		var model any
		switch override := value.(type) {
		case agents.Override[stt.STT]:
			model, _ = override.Value()
		case agents.Override[vad.VAD]:
			model, _ = override.Value()
		case agents.Override[llm.LLM]:
			model, _ = override.Value()
		case agents.Override[llm.RealtimeModel]:
			model, _ = override.Value()
		case agents.Override[tts.TTS]:
			model, _ = override.Value()
		}
		if model != nil {
			ref := reflect.ValueOf(model)
			if (ref.Kind() == reflect.Pointer || ref.Kind() == reflect.Interface || ref.Kind() == reflect.Map || ref.Kind() == reflect.Slice || ref.Kind() == reflect.Func) && ref.IsNil() {
				return errors.New("agent model override must not contain a typed nil; use agents.Disable instead")
			}
		}
	}
	return nil
}

func (a *Agent[UserData]) ID() string { a.mu.RLock(); value := a.id; a.mu.RUnlock(); return value }
func (a *Agent[UserData]) Instructions() llm.Instructions {
	a.mu.RLock()
	value := a.instructions
	a.mu.RUnlock()
	return value
}
func (a *Agent[UserData]) ChatContext() *llm.ChatContext {
	a.mu.RLock()
	value := a.chatContext.Copy(llm.CopyOptions{})
	a.mu.RUnlock()
	return value
}
func (a *Agent[UserData]) ToolContext() *llm.Context {
	a.mu.RLock()
	value := a.tools.Copy()
	a.mu.RUnlock()
	return value
}
func (a *Agent[UserData]) STTOverride() agents.Override[stt.STT] {
	a.mu.RLock()
	value := a.stt
	a.mu.RUnlock()
	return value
}
func (a *Agent[UserData]) VADOverride() agents.Override[vad.VAD] {
	a.mu.RLock()
	value := a.vad
	a.mu.RUnlock()
	return value
}
func (a *Agent[UserData]) LLMOverride() agents.Override[llm.LLM] {
	a.mu.RLock()
	value := a.llm
	a.mu.RUnlock()
	return value
}
func (a *Agent[UserData]) RealtimeOverride() agents.Override[llm.RealtimeModel] {
	a.mu.RLock()
	value := a.realtime
	a.mu.RUnlock()
	return value
}
func (a *Agent[UserData]) TTSOverride() agents.Override[tts.TTS] {
	a.mu.RLock()
	value := a.tts
	a.mu.RUnlock()
	return value
}
func (a *Agent[UserData]) ExpressiveOverride() agents.Override[ExpressiveOptions] {
	a.mu.RLock()
	value := cloneExpressiveOverride(a.expressive)
	a.mu.RUnlock()
	return value
}
func (a *Agent[UserData]) TurnHandling() *TurnHandlingOptions {
	a.mu.RLock()
	value := cloneTurnHandling(a.turnHandling)
	a.mu.RUnlock()
	return value
}
func (a *Agent[UserData]) MinConsecutiveSpeechDelay() *time.Duration {
	a.mu.RLock()
	value := cloneDuration(a.minConsecutiveSpeechDelay)
	a.mu.RUnlock()
	return value
}
func (a *Agent[UserData]) UseTTSAlignedTranscript() *bool {
	a.mu.RLock()
	value := cloneBool(a.useTTSAlignedTranscript)
	a.mu.RUnlock()
	return value
}

func (a *Agent[UserData]) Session() (AgentSessionAccess[UserData], error) {
	a.mu.RLock()
	runtime := a.runtime
	a.mu.RUnlock()
	if runtime == nil {
		return nil, ErrAgentNotRunning
	}
	return runtime.Session(), nil
}

func (a *Agent[UserData]) UpdateChatContext(ctx context.Context, chat *llm.ChatContext) error {
	if chat == nil {
		return errors.New("agent chat context must not be nil")
	}
	copy := chat.Copy(llm.CopyOptions{})
	a.mu.RLock()
	runtime := a.runtime
	a.mu.RUnlock()
	if runtime != nil {
		if err := runtime.UpdateChatContext(ctx, copy.Copy(llm.CopyOptions{})); err != nil {
			return err
		}
	}
	a.mu.Lock()
	if a.runtime != runtime {
		a.mu.Unlock()
		return ErrAgentActivityChanged
	}
	a.chatContext = copy
	a.mu.Unlock()
	return nil
}

func (a *Agent[UserData]) UpdateInstructions(ctx context.Context, instructions llm.Instructions) error {
	a.mu.RLock()
	runtime := a.runtime
	a.mu.RUnlock()
	if runtime != nil {
		if err := runtime.UpdateInstructions(ctx, instructions); err != nil {
			return err
		}
	}
	a.mu.Lock()
	if a.runtime != runtime {
		a.mu.Unlock()
		return ErrAgentActivityChanged
	}
	a.instructions = instructions
	a.mu.Unlock()
	return nil
}

func (a *Agent[UserData]) UpdateOptions(ctx context.Context, update AgentUpdateOptions) error {
	if err := validateAgentUpdate(update); err != nil {
		return err
	}
	a.mu.RLock()
	runtime := a.runtime
	nextLLM, nextRealtime := a.llm, a.realtime
	a.mu.RUnlock()
	if update.LLM != nil {
		nextLLM = *update.LLM
	}
	if update.Realtime != nil {
		nextRealtime = *update.Realtime
	}
	if !nextLLM.IsInherited() && !nextRealtime.IsInherited() {
		return errors.New("agent update accepts one LLM selection: Chat LLM or RealtimeModel")
	}
	if runtime != nil {
		if err := runtime.UpdateModels(ctx, update); err != nil {
			return err
		}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.runtime != runtime {
		return ErrAgentActivityChanged
	}
	if update.STT != nil {
		a.stt = *update.STT
	}
	if update.VAD != nil {
		a.vad = *update.VAD
	}
	if update.LLM != nil {
		a.llm = *update.LLM
	}
	if update.Realtime != nil {
		a.realtime = *update.Realtime
	}
	if update.TTS != nil {
		a.tts = *update.TTS
	}
	if update.Expressive != nil {
		a.expressive = cloneExpressiveOverride(*update.Expressive)
	}
	return nil
}

func validateAgentUpdate(update AgentUpdateOptions) error {
	values := make([]any, 0, 5)
	if update.STT != nil {
		values = append(values, *update.STT)
	}
	if update.VAD != nil {
		values = append(values, *update.VAD)
	}
	if update.LLM != nil {
		values = append(values, *update.LLM)
	}
	if update.Realtime != nil {
		values = append(values, *update.Realtime)
	}
	if update.TTS != nil {
		values = append(values, *update.TTS)
	}
	return validateModelOverrides(values...)
}

func (a *Agent[UserData]) UpdateTools(ctx context.Context, tools *llm.Context) error {
	if tools == nil {
		return errors.New("agent tool context must not be nil")
	}
	copy := tools.Copy()
	a.mu.RLock()
	runtime := a.runtime
	a.mu.RUnlock()
	if runtime != nil {
		if err := runtime.UpdateTools(ctx, copy.Copy()); err != nil {
			return err
		}
	}
	a.mu.Lock()
	if a.runtime != runtime {
		a.mu.Unlock()
		return ErrAgentActivityChanged
	}
	a.tools = copy
	a.mu.Unlock()
	return nil
}

func (a *Agent[UserData]) bindRuntime(runtime agentRuntime[UserData]) error {
	if runtime == nil {
		return errors.New("agent runtime must not be nil")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.runtime != nil && a.runtime != runtime {
		return ErrAgentAlreadyRunning
	}
	a.runtime = runtime
	return nil
}

func (a *Agent[UserData]) unbindRuntime(runtime agentRuntime[UserData]) {
	a.mu.Lock()
	if a.runtime == runtime {
		a.runtime = nil
	}
	a.mu.Unlock()
}

func (a *Agent[UserData]) setTaskResultProvider(provider func() (any, bool, error)) {
	a.mu.Lock()
	a.taskResult = provider
	a.mu.Unlock()
}

func (a *Agent[UserData]) taskOutput() (any, bool, error) {
	if a == nil {
		return nil, false, nil
	}
	a.mu.RLock()
	provider := a.taskResult
	a.mu.RUnlock()
	if provider == nil {
		return nil, false, nil
	}
	return provider()
}

type AgentContext[UserData any] struct{ agent *Agent[UserData] }

func newAgentContext[UserData any](agent *Agent[UserData]) *AgentContext[UserData] {
	return &AgentContext[UserData]{agent: agent}
}
func (c *AgentContext[UserData]) Agent() *Agent[UserData] { return c.agent }
func (c *AgentContext[UserData]) Session() (AgentSessionAccess[UserData], error) {
	return c.agent.Session()
}
func (c *AgentContext[UserData]) ID() string                     { return c.agent.ID() }
func (c *AgentContext[UserData]) Instructions() llm.Instructions { return c.agent.Instructions() }
func (c *AgentContext[UserData]) ChatContext() *llm.ChatContext  { return c.agent.ChatContext() }
func (c *AgentContext[UserData]) ToolContext() *llm.Context      { return c.agent.ToolContext() }

func (a *Agent[UserData]) runOnEnter(ctx context.Context) error {
	if a.hooks.OnEnter == nil {
		return nil
	}
	return a.hooks.OnEnter(ctx, newAgentContext(a))
}
func (a *Agent[UserData]) runOnExit(ctx context.Context) error {
	if a.hooks.OnExit == nil {
		return nil
	}
	return a.hooks.OnExit(ctx, newAgentContext(a))
}
func (a *Agent[UserData]) runOnUserTurnCompleted(ctx context.Context, chat *llm.ChatContext, message *llm.ChatMessage) error {
	if a.hooks.OnUserTurnCompleted == nil {
		return nil
	}
	if chat == nil || message == nil {
		return errors.New("completed user turn requires chat context and message")
	}
	return a.hooks.OnUserTurnCompleted(ctx, newAgentContext(a), chat.Copy(llm.CopyOptions{}), message.Clone())
}
func (a *Agent[UserData]) runOnUserTurnExceeded(ctx context.Context, event UserTurnExceededEvent) error {
	if a.hooks.OnUserTurnExceeded != nil {
		return a.hooks.OnUserTurnExceeded(ctx, newAgentContext(a), event)
	}
	session, err := a.Session()
	if err != nil {
		return err
	}
	allow := false
	instructions := llm.NewInstructions("The user has been speaking too long without giving a chance to reply. Politely cut in with a short reply or notice. Keep it short since the user cannot interrupt it.", "")
	_, err = session.GenerateReply(ctx, GenerateReplyOptions{UserInput: event.Transcript, Instructions: &instructions, AllowInterruptions: &allow, ToolChoice: llm.ToolChoice{Kind: llm.ToolChoiceNone}, InputModality: InputModalityAudio})
	return err
}

func (a *Agent[UserData]) hooksSnapshot() AgentHooks[UserData] {
	a.mu.RLock()
	hooks := a.hooks
	a.mu.RUnlock()
	return hooks
}

func (a *Agent[UserData]) runSTTNode(ctx context.Context, audio stream.Reader[agents.AudioFrame], settings ModelSettings) (stream.Reader[STTNodeItem], bool, error) {
	hook := a.hooksSnapshot().STTNode
	if hook == nil {
		return nil, false, nil
	}
	output, err := hook(ctx, newAgentContext(a), audio, settings)
	if err == nil && output == nil {
		err = errors.New("agent STT node returned a nil stream")
	}
	return output, true, err
}
func (a *Agent[UserData]) runLLMNode(ctx context.Context, chat *llm.ChatContext, tools *llm.Context, settings ModelSettings) (stream.Reader[LLMNodeItem], bool, error) {
	hook := a.hooksSnapshot().LLMNode
	if hook == nil {
		return nil, false, nil
	}
	output, err := hook(ctx, newAgentContext(a), chat, tools, settings)
	if err == nil && output == nil {
		err = errors.New("agent LLM node returned a nil stream")
	}
	return output, true, err
}
func (a *Agent[UserData]) runTTSNode(ctx context.Context, text stream.Reader[string], settings ModelSettings) (stream.Reader[agents.AudioFrame], bool, error) {
	hook := a.hooksSnapshot().TTSNode
	if hook == nil {
		return nil, false, nil
	}
	output, err := hook(ctx, newAgentContext(a), text, settings)
	if err == nil && output == nil {
		err = errors.New("agent TTS node returned a nil stream")
	}
	return output, true, err
}
func (a *Agent[UserData]) runRealtimeAudioOutputNode(ctx context.Context, audio stream.Reader[agents.AudioFrame], settings ModelSettings) (stream.Reader[agents.AudioFrame], bool, error) {
	hook := a.hooksSnapshot().RealtimeAudioOutputNode
	if hook == nil {
		return nil, false, nil
	}
	output, err := hook(ctx, newAgentContext(a), audio, settings)
	if err == nil && output == nil {
		err = errors.New("agent realtime audio output node returned a nil stream")
	}
	return output, true, err
}
func (a *Agent[UserData]) runTranscriptionNode(ctx context.Context, input stream.Reader[TranscriptionNodeItem], settings ModelSettings) (stream.Reader[TranscriptionNodeItem], bool, error) {
	hook := a.hooksSnapshot().TranscriptionNode
	if hook == nil {
		return input, false, nil
	}
	output, err := hook(ctx, newAgentContext(a), input, settings)
	if err == nil && output == nil {
		err = errors.New("agent transcription node returned a nil stream")
	}
	return output, true, err
}

func cloneTurnHandling(input *TurnHandlingOptions) *TurnHandlingOptions {
	if input == nil {
		return nil
	}
	copy := *input
	copy.Interruption.FalseInterruptionTimeout = cloneDuration(input.Interruption.FalseInterruptionTimeout)
	if input.Interruption.BackchannelBoundary != nil {
		value := *input.Interruption.BackchannelBoundary
		copy.Interruption.BackchannelBoundary = &value
	}
	if input.UserTurnLimit.MaxWords != nil {
		value := *input.UserTurnLimit.MaxWords
		copy.UserTurnLimit.MaxWords = &value
	}
	copy.UserTurnLimit.MaxDuration = cloneDuration(input.UserTurnLimit.MaxDuration)
	return &copy
}
func cloneToolHandling(input ToolHandlingOptions) ToolHandlingOptions {
	if input.Async == nil {
		return ToolHandlingOptions{}
	}
	copy := *input.Async
	return ToolHandlingOptions{Async: &copy}
}
func cloneExpressiveOverride(input agents.Override[ExpressiveOptions]) agents.Override[ExpressiveOptions] {
	if input.IsDisabled() {
		return agents.Disable[ExpressiveOptions]()
	}
	value, ok := input.Value()
	if !ok {
		return agents.Override[ExpressiveOptions]{}
	}
	if value.SpeechSteering != nil {
		copy := *value.SpeechSteering
		value.SpeechSteering = &copy
	}
	if value.TTSInstructionsTemplate != nil {
		copy := *value.TTSInstructionsTemplate
		value.TTSInstructionsTemplate = &copy
	}
	return agents.Use(value)
}
func cloneDuration(input *time.Duration) *time.Duration {
	if input == nil {
		return nil
	}
	value := *input
	return &value
}
func cloneBool(input *bool) *bool {
	if input == nil {
		return nil
	}
	value := *input
	return &value
}
