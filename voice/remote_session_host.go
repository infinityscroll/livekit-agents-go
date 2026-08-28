// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/stream"
	agentpb "github.com/livekit/protocol/livekit/agent"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	DefaultSessionHostQueueCapacity        = 128
	DefaultSessionHostRequestConcurrency   = 8
	DefaultSessionHostShutdownDrainTimeout = 5 * time.Second
)

var (
	ErrSessionHostClosed       = errors.New("voice session host is closed")
	ErrSessionHostNotStarted   = errors.New("voice session host is not started")
	ErrSessionHostQueueFull    = errors.New("voice session host outgoing queue is full")
	ErrSessionHostRequestLimit = errors.New("voice session host request concurrency limit reached")
	ErrSessionHostAlreadyBound = errors.New("voice session host is already bound to another session")
)

type SessionHostOptions[UserData any] struct {
	ParentContext         context.Context
	AudioInput            *TCPAudioInput
	AudioOutput           *TCPAudioOutput
	JobContext            *agents.JobContext[UserData]
	OnSimulationEnd       agents.SimulationEndFunc[UserData]
	QueueCapacity         int
	MaxConcurrentRequests int
	RequestTimeout        time.Duration
	ShutdownDrainTimeout  time.Duration
	OnError               func(error)
	OnTransportClosed     func(error)
	// ExternalTransport leaves final transport shutdown to the caller. Console
	// mode uses this so a failed AgentSession.Start can retry one connection.
	ExternalTransport bool
}

type sessionHostOutbound struct {
	message *agentpb.AgentSessionMessage
	ack     chan error
}

// SessionHost exposes an AgentSession over the LiveKit AgentSession protobuf.
// It uses one ordered bounded writer and a fixed request semaphore, so event
// bursts and remote callers cannot create an unbounded number of goroutines.
type SessionHost[UserData any] struct {
	transport  SessionTransport
	opts       SessionHostOptions[UserData]
	ctx        context.Context
	cancel     context.CancelCauseFunc
	recvCtx    context.Context
	cancelRecv context.CancelCauseFunc

	startMu   sync.Mutex
	mu        sync.RWMutex
	started   bool
	closed    bool
	session   *AgentSession[UserData]
	startedAt time.Time
	sub       *EventSubscription

	outgoing       *stream.Channel[sessionHostOutbound]
	requestSlots   chan struct{}
	requestMu      sync.Mutex
	accepting      bool
	requestCount   int
	requestChanged chan struct{}

	writerDone chan struct{}
	recvDone   chan struct{}
	eventDone  chan struct{}
	closeOnce  sync.Once
	closeDone  chan struct{}
	closeErr   error
}

func NewSessionHost[UserData any](transport SessionTransport, options ...SessionHostOptions[UserData]) (*SessionHost[UserData], error) {
	if transport == nil {
		return nil, errors.New("voice session host requires a transport")
	}
	if len(options) > 1 {
		return nil, errors.New("voice session host accepts at most one options value")
	}
	var resolved SessionHostOptions[UserData]
	if len(options) == 1 {
		resolved = options[0]
	}
	if resolved.ParentContext == nil {
		resolved.ParentContext = context.Background()
	}
	if resolved.QueueCapacity == 0 {
		resolved.QueueCapacity = DefaultSessionHostQueueCapacity
	}
	if resolved.QueueCapacity < 1 {
		return nil, errors.New("voice session host queue capacity must be positive")
	}
	if resolved.MaxConcurrentRequests == 0 {
		resolved.MaxConcurrentRequests = DefaultSessionHostRequestConcurrency
	}
	if resolved.MaxConcurrentRequests < 1 {
		return nil, errors.New("voice session host request concurrency must be positive")
	}
	if resolved.RequestTimeout == 0 {
		resolved.RequestTimeout = DefaultRemoteRequestTimeout
	}
	if resolved.RequestTimeout < 0 {
		return nil, errors.New("voice session host request timeout must not be negative")
	}
	if resolved.ShutdownDrainTimeout == 0 {
		resolved.ShutdownDrainTimeout = DefaultSessionHostShutdownDrainTimeout
	}
	if resolved.ShutdownDrainTimeout < 0 {
		return nil, errors.New("voice session host shutdown drain timeout must not be negative")
	}
	ctx, cancel := context.WithCancelCause(resolved.ParentContext)
	recvCtx, cancelRecv := context.WithCancelCause(resolved.ParentContext)
	return &SessionHost[UserData]{
		transport: transport, opts: resolved, ctx: ctx, cancel: cancel,
		recvCtx: recvCtx, cancelRecv: cancelRecv,
		outgoing:     stream.NewChannel[sessionHostOutbound](resolved.QueueCapacity),
		requestSlots: make(chan struct{}, resolved.MaxConcurrentRequests), accepting: true,
		requestChanged: make(chan struct{}),
		writerDone:     make(chan struct{}), recvDone: make(chan struct{}), eventDone: make(chan struct{}), closeDone: make(chan struct{}),
	}, nil
}

// RegisterSession binds the host exactly once. Registering the same session is
// idempotent; replacing a bound session is rejected to preserve event order.
func (h *SessionHost[UserData]) RegisterSession(session *AgentSession[UserData]) error {
	if session == nil {
		return errors.New("voice session host cannot register a nil session")
	}
	h.startMu.Lock()
	defer h.startMu.Unlock()
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return ErrSessionHostClosed
	}
	if h.session == session {
		h.mu.Unlock()
		return nil
	}
	if h.session != nil || h.started {
		h.mu.Unlock()
		return ErrSessionHostAlreadyBound
	}
	h.mu.Unlock()
	subscription, err := session.Subscribe(EventSubscriptionOptions{Capacity: h.opts.QueueCapacity})
	if err != nil {
		return fmt.Errorf("voice session host subscribe: %w", err)
	}
	h.mu.Lock()
	h.session, h.sub = session, subscription
	h.mu.Unlock()
	return nil
}

func (h *SessionHost[UserData]) Start(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	h.startMu.Lock()
	defer h.startMu.Unlock()
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return ErrSessionHostClosed
	}
	if h.started {
		h.mu.Unlock()
		return nil
	}
	if h.session == nil {
		h.mu.Unlock()
		return errors.New("voice session host requires RegisterSession before Start")
	}
	h.mu.Unlock()
	if cause := context.Cause(h.ctx); cause != nil {
		return cause
	}
	if err := h.transport.Start(ctx); err != nil {
		return err
	}
	cause := context.Cause(h.ctx)
	if cause == nil {
		cause = context.Cause(ctx)
	}
	if cause != nil {
		if !h.opts.ExternalTransport {
			closeCtx, cancel := context.WithTimeout(context.Background(), h.opts.ShutdownDrainTimeout)
			_ = h.transport.Close(closeCtx)
			cancel()
		}
		return cause
	}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		if !h.opts.ExternalTransport {
			_ = h.transport.Close(context.Background())
		}
		return ErrSessionHostClosed
	}
	h.started, h.startedAt = true, time.Now()
	h.mu.Unlock()
	go h.writeLoop()
	go h.eventLoop()
	go h.recvLoop()
	go func() {
		<-h.ctx.Done()
		_ = h.Close(context.Background())
	}()
	return nil
}

func (h *SessionHost[UserData]) Started() bool {
	h.mu.RLock()
	started := h.started && !h.closed
	h.mu.RUnlock()
	return started
}

func (h *SessionHost[UserData]) report(err error) {
	if err == nil {
		return
	}
	if callback := h.opts.OnError; callback != nil {
		func() {
			defer func() { _ = recover() }()
			callback(err)
		}()
		return
	}
	slog.Warn("voice session host error", "error", err)
}

func (h *SessionHost[UserData]) writeLoop() {
	defer close(h.writerDone)
	for {
		outbound, err := h.outgoing.Recv(h.ctx)
		if err != nil {
			return
		}
		sendErr := h.transport.SendMessage(h.ctx, outbound.message)
		if outbound.ack != nil {
			outbound.ack <- sendErr
		} else if sendErr != nil && context.Cause(h.ctx) == nil {
			h.report(fmt.Errorf("voice session host send event: %w", sendErr))
		}
	}
}

func (h *SessionHost[UserData]) eventLoop() {
	defer close(h.eventDone)
	h.mu.RLock()
	subscription := h.sub
	h.mu.RUnlock()
	if subscription == nil {
		return
	}
	for {
		event, err := subscription.Recv(h.ctx)
		if err != nil {
			return
		}
		wire, encodeErr := h.encodeEvent(event)
		if encodeErr != nil {
			h.report(encodeErr)
			continue
		}
		if wire == nil {
			continue
		}
		if err := h.outgoing.Send(h.ctx, sessionHostOutbound{message: &agentpb.AgentSessionMessage{Message: &agentpb.AgentSessionMessage_Event{Event: wire}}}); err != nil {
			return
		}
	}
}

func (h *SessionHost[UserData]) recvLoop() {
	defer close(h.recvDone)
	for {
		message, err := h.transport.Recv(h.recvCtx)
		if err != nil {
			if context.Cause(h.recvCtx) == nil {
				terminal := err
				if errors.Is(err, io.EOF) || errors.Is(err, ErrSessionTransportClosed) {
					terminal = ErrSessionHostClosed
				}
				if callback := h.opts.OnTransportClosed; callback != nil {
					func() { defer func() { _ = recover() }(); callback(terminal) }()
				}
				if !errors.Is(terminal, ErrSessionHostClosed) {
					h.report(fmt.Errorf("voice session host receive: %w", terminal))
				}
				h.cancel(terminal)
			}
			return
		}
		switch value := message.Message.(type) {
		case *agentpb.AgentSessionMessage_Request:
			h.startRequest(value.Request)
		case *agentpb.AgentSessionMessage_AudioInput:
			if h.opts.AudioInput != nil {
				if pushErr := h.opts.AudioInput.TryPushFrame(value.AudioInput); pushErr != nil {
					h.report(fmt.Errorf("voice session host audio input: %w", pushErr))
				}
			}
		case *agentpb.AgentSessionMessage_AudioPlaybackFinished:
			if h.opts.AudioOutput != nil {
				if finishErr := h.opts.AudioOutput.NotifyPlayoutFinished(); finishErr != nil {
					h.report(fmt.Errorf("voice session host playout finished: %w", finishErr))
				}
			}
		}
	}
}

func (h *SessionHost[UserData]) startRequest(request *agentpb.SessionRequest) {
	if request == nil {
		return
	}
	h.requestMu.Lock()
	if !h.accepting {
		h.requestMu.Unlock()
		return
	}
	select {
	case h.requestSlots <- struct{}{}:
		h.requestCount++
		h.requestMu.Unlock()
		go func() {
			defer func() {
				<-h.requestSlots
				h.requestMu.Lock()
				h.requestCount--
				close(h.requestChanged)
				h.requestChanged = make(chan struct{})
				h.requestMu.Unlock()
			}()
			h.handleRequestSafe(request)
		}()
	default:
		h.requestMu.Unlock()
		message := h.errorResponse(request.RequestId, ErrSessionHostRequestLimit.Error())
		if !h.outgoing.TrySend(sessionHostOutbound{message: message}) {
			h.report(ErrSessionHostQueueFull)
		}
	}
}

func (h *SessionHost[UserData]) handleRequestSafe(request *agentpb.SessionRequest) {
	defer func() {
		if recovered := recover(); recovered != nil {
			h.report(fmt.Errorf("voice session host request panicked: %v\n%s", recovered, debug.Stack()))
			ctx, cancel := context.WithTimeout(h.ctx, h.opts.RequestTimeout)
			defer cancel()
			_ = h.send(ctx, h.errorResponse(request.RequestId, "internal error"))
		}
	}()
	ctx, cancel := context.WithTimeout(h.ctx, h.opts.RequestTimeout)
	defer cancel()
	if err := h.handleRequest(ctx, request); err != nil && context.Cause(h.ctx) == nil {
		h.report(fmt.Errorf("voice session host request %q: %w", request.RequestId, err))
		errorCtx, errorCancel := context.WithTimeout(h.ctx, min(h.opts.RequestTimeout, DefaultConsoleOperationTimeout))
		_ = h.send(errorCtx, h.errorResponse(request.RequestId, "internal error"))
		errorCancel()
	}
}

func (h *SessionHost[UserData]) send(ctx context.Context, message *agentpb.AgentSessionMessage) error {
	ack := make(chan error, 1)
	if err := h.outgoing.Send(ctx, sessionHostOutbound{message: message, ack: ack}); err != nil {
		return err
	}
	select {
	case err := <-ack:
		return err
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (h *SessionHost[UserData]) response(requestID string, response *agentpb.SessionResponse) *agentpb.AgentSessionMessage {
	response.RequestId = requestID
	return &agentpb.AgentSessionMessage{Message: &agentpb.AgentSessionMessage_Response{Response: response}}
}

func (h *SessionHost[UserData]) errorResponse(requestID, message string) *agentpb.AgentSessionMessage {
	return h.response(requestID, &agentpb.SessionResponse{Error: &message})
}

func (h *SessionHost[UserData]) handleRequest(ctx context.Context, request *agentpb.SessionRequest) error {
	h.mu.RLock()
	session, startedAt := h.session, h.startedAt
	h.mu.RUnlock()
	if session == nil {
		return h.send(ctx, h.errorResponse(request.RequestId, "session is not registered"))
	}
	response := &agentpb.SessionResponse{}
	var responseError string
	switch value := request.Request.(type) {
	case *agentpb.SessionRequest_Ping_:
		response.Response = &agentpb.SessionResponse_Pong_{Pong: &agentpb.SessionResponse_Pong{}}
	case *agentpb.SessionRequest_GetChatHistory_:
		items, err := encodeRemoteChatItems(session.ChatContext().Items(), true)
		if err != nil {
			return err
		}
		response.Response = &agentpb.SessionResponse_GetChatHistory{GetChatHistory: &agentpb.SessionResponse_GetChatHistoryResponse{Items: items}}
	case *agentpb.SessionRequest_GetAgentInfo_:
		agent := session.Agent()
		if agent == nil {
			responseError = "session has no current agent"
			response.Response = &agentpb.SessionResponse_GetAgentInfo{GetAgentInfo: &agentpb.SessionResponse_GetAgentInfoResponse{}}
			break
		}
		instructions := agent.Instructions().Value()
		chat, err := encodeRemoteChatItems(agent.ChatContext().Items(), true)
		if err != nil {
			return err
		}
		response.Response = &agentpb.SessionResponse_GetAgentInfo{GetAgentInfo: &agentpb.SessionResponse_GetAgentInfoResponse{
			Id: agent.ID(), Instructions: &instructions, Tools: agent.ToolContext().SortedToolNames(), ChatCtx: chat,
		}}
	case *agentpb.SessionRequest_RunInput_:
		result := &agentpb.SessionResponse_RunInputResponse{}
		if value.RunInput == nil || value.RunInput.Text == "" {
			responseError = "empty run_input text"
		} else {
			_ = session.Interrupt(ctx, true)
			run, err := session.Run(ctx, RunOptions{UserInput: value.RunInput.Text})
			if err != nil {
				responseError = err.Error()
			} else {
				_, waitErr := run.Wait(ctx)
				if waitErr != nil {
					responseError = waitErr.Error()
				}
				for _, event := range run.Events() {
					encoded, encodeErr := encodeRemoteChatItem(event.Item)
					if encodeErr != nil {
						return encodeErr
					}
					result.Items = append(result.Items, encoded)
				}
			}
		}
		response.Response = &agentpb.SessionResponse_RunInput{RunInput: result}
	case *agentpb.SessionRequest_GetSessionState_:
		agent := session.Agent()
		agentID := ""
		if agent != nil {
			agentID = agent.ID()
		}
		response.Response = &agentpb.SessionResponse_GetSessionState{GetSessionState: &agentpb.SessionResponse_GetSessionStateResponse{
			AgentState: encodeRemoteAgentState(session.AgentState()), UserState: encodeRemoteUserState(session.UserState()), AgentId: agentID,
			Options: encodeRemoteSessionOptions(session.opts), CreatedAt: remoteTimestamp(startedAt),
		}}
	case *agentpb.SessionRequest_GetRtcStats:
		// server-sdk-go v2.18.1 does not expose the JS/Python RTC stats getter.
		response.Response = &agentpb.SessionResponse_GetRtcStats{GetRtcStats: &agentpb.SessionResponse_GetRTCStatsResponse{}}
	case *agentpb.SessionRequest_GetSessionUsage_:
		response.Response = &agentpb.SessionResponse_GetSessionUsage{GetSessionUsage: &agentpb.SessionResponse_GetSessionUsageResponse{Usage: encodeRemoteUsage(session.Usage()), CreatedAt: remoteTimestamp(time.Now())}}
	case *agentpb.SessionRequest_GetFrameworkInfo_:
		response.Response = &agentpb.SessionResponse_GetFrameworkInfo{GetFrameworkInfo: &agentpb.SessionResponse_GetFrameworkInfoResponse{Sdk: "go", SdkVersion: agents.Version}}
	case *agentpb.SessionRequest_UpdateIo:
		if value.UpdateIo != nil {
			if input := value.UpdateIo.Input; input != nil && input.AudioEnabled != nil {
				session.Input().SetAudioEnabled(*input.AudioEnabled)
			}
			if output := value.UpdateIo.Output; output != nil {
				if output.AudioEnabled != nil {
					session.Output().SetAudioEnabled(*output.AudioEnabled)
				}
				if output.TranscriptionEnabled != nil {
					session.Output().SetTranscriptionEnabled(*output.TranscriptionEnabled)
				}
			}
		}
		response.Response = &agentpb.SessionResponse_UpdateIo{UpdateIo: &agentpb.SessionResponse_UpdateIOResponse{}}
	case *agentpb.SessionRequest_FinalizeSimulation_:
		finalized := &agentpb.SessionResponse_FinalizeSimulationResponse{}
		if sim, ok := h.opts.JobContext.SimulationContext(); ok {
			sim.BeginFinalize(agents.SimulationVerdict{Success: value.FinalizeSimulation.GetProvisionalSuccess(), Reason: value.FinalizeSimulation.GetProvisionalReason()}, &agents.SimulationRun{Id: sim.RunID()})
			if h.opts.OnSimulationEnd != nil {
				if err := callSimulationEnd(ctx, h.opts.OnSimulationEnd, sim); err != nil {
					responseError = err.Error()
				}
			}
			if verdict, ok := sim.UserVerdict(); ok {
				finalized.UserVerdict = &agentpb.SessionResponse_FinalizeSimulationResponse_SimulationVerdict{Success: verdict.Success, Reason: verdict.Reason}
			}
		}
		response.Response = &agentpb.SessionResponse_FinalizeSimulation{FinalizeSimulation: finalized}
	default:
		return h.send(ctx, h.errorResponse(request.RequestId, "unsupported session request"))
	}
	if responseError != "" {
		response.Error = &responseError
	}
	return h.send(ctx, h.response(request.RequestId, response))
}

func (h *SessionHost[UserData]) encodeEvent(event Event) (*agentpb.AgentSessionEvent, error) {
	if event == nil {
		return nil, nil
	}
	wire := &agentpb.AgentSessionEvent{CreatedAt: remoteTimestamp(event.Time())}
	switch value := event.(type) {
	case AgentStateChangedEvent:
		wire.Event = &agentpb.AgentSessionEvent_AgentStateChanged_{AgentStateChanged: &agentpb.AgentSessionEvent_AgentStateChanged{OldState: encodeRemoteAgentState(value.OldState), NewState: encodeRemoteAgentState(value.NewState)}}
	case UserStateChangedEvent:
		wire.Event = &agentpb.AgentSessionEvent_UserStateChanged_{UserStateChanged: &agentpb.AgentSessionEvent_UserStateChanged{OldState: encodeRemoteUserState(value.OldState), NewState: encodeRemoteUserState(value.NewState)}}
	case UserInputTranscribedEvent:
		transcribed := &agentpb.AgentSessionEvent_UserInputTranscribed{Transcript: value.Transcript, IsFinal: value.Final}
		if value.Language != nil {
			language := string(*value.Language)
			transcribed.Language = &language
		}
		wire.Event = &agentpb.AgentSessionEvent_UserInputTranscribed_{UserInputTranscribed: transcribed}
	case ConversationItemAddedEvent:
		item, err := encodeRemoteChatItem(value.Item)
		if err != nil {
			return nil, err
		}
		wire.Event = &agentpb.AgentSessionEvent_ConversationItemAdded_{ConversationItemAdded: &agentpb.AgentSessionEvent_ConversationItemAdded{Item: item}}
	case FunctionToolsExecutedEvent:
		calls := make([]*agentpb.FunctionCall, 0, len(value.FunctionCalls))
		for _, call := range value.FunctionCalls {
			if call != nil {
				calls = append(calls, &agentpb.FunctionCall{Id: call.ID, CallId: call.CallID, Name: call.Name, Arguments: call.Arguments})
			}
		}
		outputs := make([]*agentpb.FunctionCallOutput, 0, len(value.FunctionCallOutputs))
		for _, output := range value.FunctionCallOutputs {
			if output != nil {
				outputs = append(outputs, &agentpb.FunctionCallOutput{Id: output.ID, CallId: output.CallID, Name: output.Name, Output: output.Output, IsError: output.IsError})
			}
		}
		wire.Event = &agentpb.AgentSessionEvent_FunctionToolsExecuted_{FunctionToolsExecuted: &agentpb.AgentSessionEvent_FunctionToolsExecuted{FunctionCalls: calls, FunctionCallOutputs: outputs}}
	case MetricsCollectedEvent:
		h.mu.RLock()
		session := h.session
		h.mu.RUnlock()
		if session == nil {
			return nil, nil
		}
		wire.Event = &agentpb.AgentSessionEvent_SessionUsageUpdated_{SessionUsageUpdated: &agentpb.AgentSessionEvent_SessionUsageUpdated{Usage: encodeRemoteUsage(session.Usage())}}
	case SessionUsageUpdatedEvent:
		wire.Event = &agentpb.AgentSessionEvent_SessionUsageUpdated_{SessionUsageUpdated: &agentpb.AgentSessionEvent_SessionUsageUpdated{Usage: encodeRemoteUsage(value.Usage)}}
	case OverlappingSpeechEvent:
		overlap := &agentpb.AgentSessionEvent_OverlappingSpeech{IsInterruption: value.Interruption, DetectionDelay: value.DetectionDelay.Seconds(), DetectedAt: remoteTimestamp(value.DetectedAt)}
		if value.OverlapStartedAt != nil {
			overlap.OverlapStartedAt = remoteTimestamp(*value.OverlapStartedAt)
		}
		wire.Event = &agentpb.AgentSessionEvent_OverlappingSpeech_{OverlappingSpeech: overlap}
	case AgentFalseInterruptionEvent:
		wire.Event = &agentpb.AgentSessionEvent_AgentFalseInterruption_{AgentFalseInterruption: &agentpb.AgentSessionEvent_AgentFalseInterruption{Resumed: value.Resumed}}
	case AMDPredictionEvent:
		wire.Event = &agentpb.AgentSessionEvent_AmdPrediction_{AmdPrediction: &agentpb.AgentSessionEvent_AmdPrediction{SpeechDuration: remoteDuration(value.SpeechDuration()), Delay: remoteDuration(value.Delay()), Category: encodeRemoteAMDCategory(value.Category), Reason: value.Reason, Transcript: value.Transcript}}
	case EOTPredictionEvent:
		wire.Event = &agentpb.AgentSessionEvent_EotPrediction_{EotPrediction: &agentpb.AgentSessionEvent_EotPrediction{Probability: float32(value.Probability), Threshold: float32(value.Threshold), InferenceDuration: remoteDuration(value.InferenceDuration), Delay: remoteDuration(value.Delay)}}
	case ErrorEvent:
		message := "Unknown error"
		if value.Err != nil {
			message = value.Err.Error()
		}
		wire.Event = &agentpb.AgentSessionEvent_Error_{Error: &agentpb.AgentSessionEvent_Error{Message: message}}
	case DebugMessageEvent:
		payload, err := structpb.NewStruct(value.Payload)
		if err != nil {
			return nil, err
		}
		wire.Event = &agentpb.AgentSessionEvent_DebugMessage{DebugMessage: &agentpb.DebugMessage{Payload: payload}}
	default:
		return nil, nil
	}
	return wire, nil
}

func (h *SessionHost[UserData]) Close(ctx context.Context) error {
	h.closeOnce.Do(func() { go h.closeWorker() })
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-h.closeDone:
		return h.closeErr
	}
}

func (h *SessionHost[UserData]) AClose(ctx context.Context) error { return h.Close(ctx) }

// rollbackUnstarted releases the pre-start event subscription without closing
// the reusable transport. It exists solely to keep AgentSession.Start retry-safe
// after a dial failure.
func (h *SessionHost[UserData]) rollbackUnstarted() {
	h.startMu.Lock()
	h.mu.Lock()
	if h.started {
		h.mu.Unlock()
		h.startMu.Unlock()
		return
	}
	h.closed = true
	sub := h.sub
	h.mu.Unlock()
	if sub != nil {
		_ = sub.Close()
	}
	h.cancelRecv(ErrSessionHostClosed)
	h.cancel(ErrSessionHostClosed)
	_ = h.outgoing.Close()
	h.startMu.Unlock()
}

func (h *SessionHost[UserData]) closeWorker() {
	defer close(h.closeDone)
	h.startMu.Lock()
	h.mu.Lock()
	started := h.started
	h.started, h.closed = false, true
	sub := h.sub
	h.mu.Unlock()
	h.requestMu.Lock()
	h.accepting = false
	h.requestMu.Unlock()
	h.cancelRecv(ErrSessionHostClosed)
	if sub != nil {
		_ = sub.Close()
	}
	h.startMu.Unlock()
	if !started {
		h.cancel(ErrSessionHostClosed)
		_ = h.outgoing.Close()
		if !h.opts.ExternalTransport {
			closeCtx, cancel := context.WithTimeout(context.Background(), h.opts.ShutdownDrainTimeout)
			h.closeErr = h.transport.Close(closeCtx)
			cancel()
		}
		return
	}
	deadlineCtx, cancel := context.WithTimeout(context.Background(), h.opts.ShutdownDrainTimeout)
	defer cancel()
	for {
		h.requestMu.Lock()
		remaining, changed := h.requestCount, h.requestChanged
		h.requestMu.Unlock()
		if remaining == 0 {
			break
		}
		select {
		case <-changed:
		case <-deadlineCtx.Done():
			h.closeErr = errors.Join(h.closeErr, errors.New("voice session host request drain timed out"))
			h.cancel(ErrSessionHostClosed)
			goto requestsDrained
		}
	}
requestsDrained:
	select {
	case <-h.eventDone:
	case <-deadlineCtx.Done():
		h.closeErr = errors.Join(h.closeErr, errors.New("voice session host event drain timed out"))
		h.cancel(ErrSessionHostClosed)
	}
	_ = h.outgoing.Close()
	select {
	case <-h.writerDone:
	case <-deadlineCtx.Done():
		h.closeErr = errors.Join(h.closeErr, errors.New("voice session host writer drain timed out"))
	}
	h.cancel(ErrSessionHostClosed)
	if !h.opts.ExternalTransport {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), h.opts.ShutdownDrainTimeout)
		h.closeErr = errors.Join(h.closeErr, h.transport.Close(closeCtx))
		closeCancel()
	}
	select {
	case <-h.recvDone:
	case <-deadlineCtx.Done():
		h.closeErr = errors.Join(h.closeErr, errors.New("voice session host receive loop did not stop"))
	}
}

func callSimulationEnd[UserData any](ctx context.Context, callback agents.SimulationEndFunc[UserData], simulation *agents.SimulationContext[UserData]) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("simulation end callback panicked: %v", recovered)
		}
	}()
	return callback(ctx, simulation)
}
