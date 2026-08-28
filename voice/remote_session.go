// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/stream"
	agentpb "github.com/livekit/protocol/livekit/agent"
	"google.golang.org/protobuf/proto"
)

const (
	DefaultRemoteRequestTimeout  = 60 * time.Second
	DefaultRemoteReadyTimeout    = 5 * time.Second
	DefaultRemoteRetryInterval   = 500 * time.Millisecond
	DefaultRemotePendingRequests = 128
	DefaultRemoteEventCapacity   = 64
)

var (
	ErrRemoteSessionClosed      = errors.New("voice remote session is closed")
	ErrRemoteSessionNotStarted  = errors.New("voice remote session is not started")
	ErrRemoteRequestTimeout     = errors.New("voice remote session request timed out")
	ErrRemoteRequestLimit       = errors.New("voice remote session pending request limit reached")
	ErrRemoteEventOverflow      = errors.New("voice remote session event subscriber overflow")
	ErrUnexpectedRemoteResponse = errors.New("voice remote session received an unexpected response type")
)

type RemoteSessionEventType string

const (
	RemoteEventAgentFalseInterruption RemoteSessionEventType = "agent_false_interruption"
	RemoteEventAgentStateChanged      RemoteSessionEventType = "agent_state_changed"
	RemoteEventUserStateChanged       RemoteSessionEventType = "user_state_changed"
	RemoteEventConversationItemAdded  RemoteSessionEventType = "conversation_item_added"
	RemoteEventUserInputTranscribed   RemoteSessionEventType = "user_input_transcribed"
	RemoteEventFunctionToolsStarted   RemoteSessionEventType = "function_tools_started"
	RemoteEventFunctionToolsExecuted  RemoteSessionEventType = "function_tools_executed"
	RemoteEventToolExecutionUpdated   RemoteSessionEventType = "tool_execution_updated"
	RemoteEventOverlappingSpeech      RemoteSessionEventType = "overlapping_speech"
	RemoteEventAMDPrediction          RemoteSessionEventType = "amd_prediction"
	RemoteEventEOTPrediction          RemoteSessionEventType = "eot_prediction"
	RemoteEventSessionUsage           RemoteSessionEventType = "session_usage"
	RemoteEventDebugMessage           RemoteSessionEventType = "debug_message"
	RemoteEventError                  RemoteSessionEventType = "error"
)

type RemoteSessionEvent struct {
	Type      RemoteSessionEventType
	CreatedAt time.Time
	Event     *agentpb.AgentSessionEvent
	Value     proto.Message
}

type RemoteSessionOptions struct {
	ParentContext      context.Context
	RequestTimeout     time.Duration
	MaxPendingRequests int
	EventCapacity      int
}

type remotePendingRequest struct {
	result chan remoteRequestResult
}

type remoteRequestResult struct {
	response *agentpb.SessionResponse
	err      error
}

type RemoteEventSubscription struct {
	owner *RemoteSession
	id    uint64
	out   *stream.Channel[RemoteSessionEvent]
	once  sync.Once
}

func (s *RemoteEventSubscription) Recv(ctx context.Context) (RemoteSessionEvent, error) {
	if s == nil || s.out == nil {
		return RemoteSessionEvent{}, ErrRemoteSessionClosed
	}
	return s.out.Recv(ctx)
}
func (s *RemoteEventSubscription) Close() error {
	if s == nil {
		return nil
	}
	s.once.Do(func() { s.owner.removeEventSubscription(s.id) })
	return nil
}

// RemoteSession is the context-first Go client for the LiveKit AgentSession
// protobuf. Pending RPCs and event subscribers are explicitly bounded.
type RemoteSession struct {
	transport SessionTransport
	opts      RemoteSessionOptions
	ctx       context.Context
	cancel    context.CancelCauseFunc

	startMu sync.Mutex
	mu      sync.Mutex
	started bool
	closed  bool
	pending map[string]*remotePendingRequest

	eventMu   sync.RWMutex
	eventSubs map[uint64]*RemoteEventSubscription
	nextSubID uint64

	recvDone  chan struct{}
	recvStart bool
	closeOnce sync.Once
	closeDone chan struct{}
	closeErr  error
}

func NewRemoteSession(transport SessionTransport, options ...RemoteSessionOptions) (*RemoteSession, error) {
	if transport == nil {
		return nil, errors.New("voice remote session requires a transport")
	}
	if len(options) > 1 {
		return nil, errors.New("voice remote session accepts at most one options value")
	}
	var resolved RemoteSessionOptions
	if len(options) == 1 {
		resolved = options[0]
	}
	if resolved.ParentContext == nil {
		resolved.ParentContext = context.Background()
	}
	if resolved.RequestTimeout == 0 {
		resolved.RequestTimeout = DefaultRemoteRequestTimeout
	}
	if resolved.RequestTimeout < 0 {
		return nil, errors.New("voice remote session request timeout must not be negative")
	}
	if resolved.MaxPendingRequests == 0 {
		resolved.MaxPendingRequests = DefaultRemotePendingRequests
	}
	if resolved.MaxPendingRequests < 1 {
		return nil, errors.New("voice remote session pending request limit must be positive")
	}
	if resolved.EventCapacity == 0 {
		resolved.EventCapacity = DefaultRemoteEventCapacity
	}
	if resolved.EventCapacity < 1 {
		return nil, errors.New("voice remote session event capacity must be positive")
	}
	ctx, cancel := context.WithCancelCause(resolved.ParentContext)
	return &RemoteSession{
		transport: transport, opts: resolved, ctx: ctx, cancel: cancel,
		pending: make(map[string]*remotePendingRequest), eventSubs: make(map[uint64]*RemoteEventSubscription),
		recvDone: make(chan struct{}), closeDone: make(chan struct{}),
	}, nil
}

func (s *RemoteSession) Start(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.startMu.Lock()
	defer s.startMu.Unlock()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrRemoteSessionClosed
	}
	if s.started {
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()
	if cause := context.Cause(s.ctx); cause != nil {
		return cause
	}
	if err := s.transport.Start(ctx); err != nil {
		return err
	}
	cause := context.Cause(s.ctx)
	if cause == nil {
		cause = context.Cause(ctx)
	}
	if cause != nil {
		closeCtx, cancel := context.WithTimeout(context.Background(), DefaultConsoleOperationTimeout)
		_ = s.transport.Close(closeCtx)
		cancel()
		return cause
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		_ = s.transport.Close(context.Background())
		return ErrRemoteSessionClosed
	}
	s.started = true
	s.mu.Unlock()
	go s.recvLoop()
	go func() {
		<-s.ctx.Done()
		_ = s.Close(context.Background())
	}()
	s.mu.Lock()
	s.recvStart = true
	s.mu.Unlock()
	return nil
}

func (s *RemoteSession) Started() bool {
	s.mu.Lock()
	started := s.started && !s.closed
	s.mu.Unlock()
	return started
}

func (s *RemoteSession) SubscribeEvents(capacity ...int) (*RemoteEventSubscription, error) {
	value := s.opts.EventCapacity
	if len(capacity) > 1 {
		return nil, errors.New("voice remote event subscription accepts at most one capacity")
	}
	if len(capacity) == 1 {
		value = capacity[0]
	}
	if value < 1 {
		return nil, errors.New("voice remote event capacity must be positive")
	}
	s.eventMu.Lock()
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		s.eventMu.Unlock()
		return nil, ErrRemoteSessionClosed
	}
	id := s.nextSubID
	s.nextSubID++
	subscription := &RemoteEventSubscription{owner: s, id: id, out: stream.NewChannel[RemoteSessionEvent](value)}
	s.eventSubs[id] = subscription
	s.eventMu.Unlock()
	return subscription, nil
}

func (s *RemoteSession) OnEvent(callback func(RemoteSessionEvent), capacity ...int) (func(), error) {
	if callback == nil {
		return nil, errors.New("voice remote event callback must not be nil")
	}
	subscription, err := s.SubscribeEvents(capacity...)
	if err != nil {
		return nil, err
	}
	go func() {
		defer subscription.Close()
		for {
			event, recvErr := subscription.Recv(s.ctx)
			if recvErr != nil {
				return
			}
			func() {
				defer func() { _ = recover() }()
				callback(event)
			}()
		}
	}()
	return func() { _ = subscription.Close() }, nil
}

func (s *RemoteSession) removeEventSubscription(id uint64) {
	s.eventMu.Lock()
	subscription := s.eventSubs[id]
	delete(s.eventSubs, id)
	s.eventMu.Unlock()
	if subscription != nil {
		_ = subscription.out.Close()
	}
}

func (s *RemoteSession) recvLoop() {
	defer close(s.recvDone)
	for {
		message, err := s.transport.Recv(s.ctx)
		if err != nil {
			if context.Cause(s.ctx) == nil {
				terminal := err
				if errors.Is(err, ErrSessionTransportClosed) || errors.Is(err, io.EOF) {
					terminal = ErrRemoteSessionClosed
				}
				s.mu.Lock()
				s.closed, s.started = true, false
				s.mu.Unlock()
				s.cancel(terminal)
				s.failPending(terminal)
			}
			return
		}
		switch value := message.Message.(type) {
		case *agentpb.AgentSessionMessage_Response:
			s.dispatchResponse(value.Response)
		case *agentpb.AgentSessionMessage_Event:
			s.dispatchEvent(value.Event)
		}
	}
}

func (s *RemoteSession) dispatchResponse(response *agentpb.SessionResponse) {
	if response == nil {
		return
	}
	s.mu.Lock()
	pending := s.pending[response.RequestId]
	delete(s.pending, response.RequestId)
	s.mu.Unlock()
	if pending != nil {
		pending.result <- remoteRequestResult{response: response}
	}
}

func (s *RemoteSession) dispatchEvent(event *agentpb.AgentSessionEvent) {
	converted, ok := remoteEventFromProto(event)
	if !ok {
		return
	}
	s.eventMu.RLock()
	subscriptions := make([]*RemoteEventSubscription, 0, len(s.eventSubs))
	for _, subscription := range s.eventSubs {
		subscriptions = append(subscriptions, subscription)
	}
	s.eventMu.RUnlock()
	for _, subscription := range subscriptions {
		if !subscription.out.TrySend(converted) {
			_ = subscription.out.Abort(ErrRemoteEventOverflow)
			s.removeEventSubscription(subscription.id)
		}
	}
}

func remoteEventFromProto(event *agentpb.AgentSessionEvent) (RemoteSessionEvent, bool) {
	if event == nil {
		return RemoteSessionEvent{}, false
	}
	cloned := proto.Clone(event).(*agentpb.AgentSessionEvent)
	result := RemoteSessionEvent{Event: cloned}
	if cloned.CreatedAt != nil && cloned.CreatedAt.IsValid() {
		result.CreatedAt = cloned.CreatedAt.AsTime()
	}
	switch value := cloned.Event.(type) {
	case *agentpb.AgentSessionEvent_AgentFalseInterruption_:
		result.Type, result.Value = RemoteEventAgentFalseInterruption, value.AgentFalseInterruption
	case *agentpb.AgentSessionEvent_AgentStateChanged_:
		result.Type, result.Value = RemoteEventAgentStateChanged, value.AgentStateChanged
	case *agentpb.AgentSessionEvent_UserStateChanged_:
		result.Type, result.Value = RemoteEventUserStateChanged, value.UserStateChanged
	case *agentpb.AgentSessionEvent_ConversationItemAdded_:
		result.Type, result.Value = RemoteEventConversationItemAdded, value.ConversationItemAdded
	case *agentpb.AgentSessionEvent_UserInputTranscribed_:
		result.Type, result.Value = RemoteEventUserInputTranscribed, value.UserInputTranscribed
	case *agentpb.AgentSessionEvent_FunctionToolsStarted_:
		result.Type, result.Value = RemoteEventFunctionToolsStarted, value.FunctionToolsStarted
	case *agentpb.AgentSessionEvent_FunctionToolsExecuted_:
		result.Type, result.Value = RemoteEventFunctionToolsExecuted, value.FunctionToolsExecuted
	case *agentpb.AgentSessionEvent_ToolExecutionUpdated_:
		result.Type, result.Value = RemoteEventToolExecutionUpdated, value.ToolExecutionUpdated
	case *agentpb.AgentSessionEvent_OverlappingSpeech_:
		result.Type, result.Value = RemoteEventOverlappingSpeech, value.OverlappingSpeech
	case *agentpb.AgentSessionEvent_AmdPrediction_:
		result.Type, result.Value = RemoteEventAMDPrediction, value.AmdPrediction
	case *agentpb.AgentSessionEvent_EotPrediction_:
		result.Type, result.Value = RemoteEventEOTPrediction, value.EotPrediction
	case *agentpb.AgentSessionEvent_SessionUsageUpdated_:
		result.Type, result.Value = RemoteEventSessionUsage, value.SessionUsageUpdated
	case *agentpb.AgentSessionEvent_DebugMessage:
		result.Type, result.Value = RemoteEventDebugMessage, value.DebugMessage
	case *agentpb.AgentSessionEvent_Error_:
		result.Type, result.Value = RemoteEventError, value.Error
	default:
		return RemoteSessionEvent{}, false
	}
	return result, true
}

func (s *RemoteSession) sendRequest(ctx context.Context, request *agentpb.SessionRequest, raw bool) (*agentpb.SessionResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ErrRemoteSessionClosed
	}
	if !s.started {
		s.mu.Unlock()
		return nil, ErrRemoteSessionNotStarted
	}
	if len(s.pending) >= s.opts.MaxPendingRequests {
		s.mu.Unlock()
		return nil, ErrRemoteRequestLimit
	}
	request.RequestId = agents.ShortUUID("req_")
	pending := &remotePendingRequest{result: make(chan remoteRequestResult, 1)}
	s.pending[request.RequestId] = pending
	s.mu.Unlock()
	remove := func() {
		s.mu.Lock()
		if s.pending[request.RequestId] == pending {
			delete(s.pending, request.RequestId)
		}
		s.mu.Unlock()
	}
	if err := s.transport.SendMessage(ctx, &agentpb.AgentSessionMessage{Message: &agentpb.AgentSessionMessage_Request{Request: request}}); err != nil {
		remove()
		return nil, err
	}
	select {
	case result := <-pending.result:
		if result.err != nil {
			return nil, result.err
		}
		response := result.response
		if response == nil {
			return nil, ErrRemoteSessionClosed
		}
		if !raw && response.Error != nil {
			return nil, &RemoteSessionRequestError{RequestID: request.RequestId, Err: errors.New(response.GetError())}
		}
		return response, nil
	case <-ctx.Done():
		remove()
		if errors.Is(context.Cause(ctx), context.DeadlineExceeded) {
			return nil, fmt.Errorf("%w: request %s", ErrRemoteRequestTimeout, request.RequestId)
		}
		return nil, context.Cause(ctx)
	case <-s.ctx.Done():
		remove()
		return nil, ErrRemoteSessionClosed
	}
}

type RemoteSessionRequestError struct {
	RequestID string
	Err       error
}

func (e *RemoteSessionRequestError) Error() string {
	if e == nil {
		return "voice remote session request failed"
	}
	return fmt.Sprintf("voice remote session request %s failed: %v", e.RequestID, e.Err)
}
func (e *RemoteSessionRequestError) Unwrap() error { return e.Err }

func (s *RemoteSession) requestContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if timeout == 0 {
		timeout = s.opts.RequestTimeout
	}
	return context.WithTimeout(ctx, timeout)
}

func (s *RemoteSession) Ping(ctx context.Context) error {
	requestCtx, cancel := s.requestContext(ctx, 0)
	defer cancel()
	response, err := s.sendRequest(requestCtx, &agentpb.SessionRequest{Request: &agentpb.SessionRequest_Ping_{Ping: &agentpb.SessionRequest_Ping{}}}, false)
	if err != nil {
		return err
	}
	if response.GetPong() == nil {
		return ErrUnexpectedRemoteResponse
	}
	return nil
}

func (s *RemoteSession) WaitForReady(ctx context.Context, retryInterval time.Duration) error {
	if ctx == nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), DefaultRemoteReadyTimeout)
		defer cancel()
	}
	if retryInterval <= 0 {
		retryInterval = DefaultRemoteRetryInterval
	}
	var last error
	for {
		attemptCtx, cancel := context.WithTimeout(ctx, retryInterval)
		err := s.Ping(attemptCtx)
		cancel()
		if err == nil {
			return nil
		}
		last = err
		timer := time.NewTimer(retryInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			if last != nil && !errors.Is(last, ErrRemoteRequestTimeout) && !errors.Is(last, context.DeadlineExceeded) {
				return last
			}
			return context.Cause(ctx)
		case <-timer.C:
		}
	}
}

func (s *RemoteSession) FetchSessionState(ctx context.Context) (*agentpb.SessionResponse_GetSessionStateResponse, error) {
	requestCtx, cancel := s.requestContext(ctx, 0)
	defer cancel()
	response, err := s.sendRequest(requestCtx, &agentpb.SessionRequest{Request: &agentpb.SessionRequest_GetSessionState_{GetSessionState: &agentpb.SessionRequest_GetSessionState{}}}, false)
	if err != nil {
		return nil, err
	}
	if value := response.GetGetSessionState(); value != nil {
		return value, nil
	}
	return nil, ErrUnexpectedRemoteResponse
}
func (s *RemoteSession) GetSessionState(ctx context.Context) (*agentpb.SessionResponse_GetSessionStateResponse, error) {
	return s.FetchSessionState(ctx)
}

func (s *RemoteSession) FetchChatHistory(ctx context.Context) (*agentpb.SessionResponse_GetChatHistoryResponse, error) {
	requestCtx, cancel := s.requestContext(ctx, 0)
	defer cancel()
	response, err := s.sendRequest(requestCtx, &agentpb.SessionRequest{Request: &agentpb.SessionRequest_GetChatHistory_{GetChatHistory: &agentpb.SessionRequest_GetChatHistory{}}}, false)
	if err != nil {
		return nil, err
	}
	if value := response.GetGetChatHistory(); value != nil {
		return value, nil
	}
	return nil, ErrUnexpectedRemoteResponse
}
func (s *RemoteSession) GetChatHistory(ctx context.Context) (*agentpb.SessionResponse_GetChatHistoryResponse, error) {
	return s.FetchChatHistory(ctx)
}

func (s *RemoteSession) FetchAgentInfo(ctx context.Context) (*agentpb.SessionResponse_GetAgentInfoResponse, error) {
	requestCtx, cancel := s.requestContext(ctx, 0)
	defer cancel()
	response, err := s.sendRequest(requestCtx, &agentpb.SessionRequest{Request: &agentpb.SessionRequest_GetAgentInfo_{GetAgentInfo: &agentpb.SessionRequest_GetAgentInfo{}}}, false)
	if err != nil {
		return nil, err
	}
	if value := response.GetGetAgentInfo(); value != nil {
		return value, nil
	}
	return nil, ErrUnexpectedRemoteResponse
}
func (s *RemoteSession) GetAgentInfo(ctx context.Context) (*agentpb.SessionResponse_GetAgentInfoResponse, error) {
	return s.FetchAgentInfo(ctx)
}

func (s *RemoteSession) FetchFrameworkInfo(ctx context.Context) (*agentpb.SessionResponse_GetFrameworkInfoResponse, error) {
	requestCtx, cancel := s.requestContext(ctx, 0)
	defer cancel()
	response, err := s.sendRequest(requestCtx, &agentpb.SessionRequest{Request: &agentpb.SessionRequest_GetFrameworkInfo_{GetFrameworkInfo: &agentpb.SessionRequest_GetFrameworkInfo{}}}, false)
	if err != nil {
		return nil, err
	}
	if value := response.GetGetFrameworkInfo(); value != nil {
		return value, nil
	}
	return nil, ErrUnexpectedRemoteResponse
}
func (s *RemoteSession) GetFrameworkInfo(ctx context.Context) (*agentpb.SessionResponse_GetFrameworkInfoResponse, error) {
	return s.FetchFrameworkInfo(ctx)
}

func (s *RemoteSession) SendMessage(ctx context.Context, text string, timeout ...time.Duration) (*agentpb.SessionResponse_RunInputResponse, error) {
	requestTimeout := time.Duration(0)
	if len(timeout) > 1 {
		return nil, errors.New("voice remote send message accepts at most one timeout")
	}
	if len(timeout) == 1 {
		requestTimeout = timeout[0]
	}
	if requestTimeout < 0 {
		return nil, errors.New("voice remote send message timeout must not be negative")
	}
	requestCtx, cancel := s.requestContext(ctx, requestTimeout)
	defer cancel()
	response, err := s.sendRequest(requestCtx, &agentpb.SessionRequest{Request: &agentpb.SessionRequest_RunInput_{RunInput: &agentpb.SessionRequest_RunInput{Text: text}}}, false)
	if err != nil {
		return nil, err
	}
	if value := response.GetRunInput(); value != nil {
		return value, nil
	}
	return nil, ErrUnexpectedRemoteResponse
}
func (s *RemoteSession) Run(ctx context.Context, text string, timeout ...time.Duration) (*agentpb.SessionResponse_RunInputResponse, error) {
	return s.SendMessage(ctx, text, timeout...)
}

func (s *RemoteSession) FetchRTCStats(ctx context.Context) (*agentpb.SessionResponse_GetRTCStatsResponse, error) {
	requestCtx, cancel := s.requestContext(ctx, 0)
	defer cancel()
	response, err := s.sendRequest(requestCtx, &agentpb.SessionRequest{Request: &agentpb.SessionRequest_GetRtcStats{GetRtcStats: &agentpb.SessionRequest_GetRTCStats{}}}, false)
	if err != nil {
		return nil, err
	}
	if value := response.GetGetRtcStats(); value != nil {
		return value, nil
	}
	return nil, ErrUnexpectedRemoteResponse
}
func (s *RemoteSession) GetRTCStats(ctx context.Context) (*agentpb.SessionResponse_GetRTCStatsResponse, error) {
	return s.FetchRTCStats(ctx)
}

func (s *RemoteSession) FetchSessionUsage(ctx context.Context) (*agentpb.SessionResponse_GetSessionUsageResponse, error) {
	requestCtx, cancel := s.requestContext(ctx, 0)
	defer cancel()
	response, err := s.sendRequest(requestCtx, &agentpb.SessionRequest{Request: &agentpb.SessionRequest_GetSessionUsage_{GetSessionUsage: &agentpb.SessionRequest_GetSessionUsage{}}}, false)
	if err != nil {
		return nil, err
	}
	if value := response.GetGetSessionUsage(); value != nil {
		return value, nil
	}
	return nil, ErrUnexpectedRemoteResponse
}
func (s *RemoteSession) GetSessionUsage(ctx context.Context) (*agentpb.SessionResponse_GetSessionUsageResponse, error) {
	return s.FetchSessionUsage(ctx)
}

type RemoteUpdateIOOptions struct {
	InputAudioEnabled          *bool
	InputVideoEnabled          *bool
	OutputAudioEnabled         *bool
	OutputVideoEnabled         *bool
	OutputTranscriptionEnabled *bool
	Timeout                    time.Duration
}

func (s *RemoteSession) UpdateIO(ctx context.Context, options RemoteUpdateIOOptions) (*agentpb.SessionResponse_UpdateIOResponse, error) {
	if options.Timeout < 0 {
		return nil, errors.New("voice remote update IO timeout must not be negative")
	}
	input := &agentpb.SessionRequest_UpdateIO_Input{AudioEnabled: options.InputAudioEnabled, VideoEnabled: options.InputVideoEnabled}
	output := &agentpb.SessionRequest_UpdateIO_Output{AudioEnabled: options.OutputAudioEnabled, VideoEnabled: options.OutputVideoEnabled, TranscriptionEnabled: options.OutputTranscriptionEnabled}
	update := &agentpb.SessionRequest_UpdateIO{}
	if input.AudioEnabled != nil || input.VideoEnabled != nil {
		update.Input = input
	}
	if output.AudioEnabled != nil || output.VideoEnabled != nil || output.TranscriptionEnabled != nil {
		update.Output = output
	}
	requestCtx, cancel := s.requestContext(ctx, options.Timeout)
	defer cancel()
	response, err := s.sendRequest(requestCtx, &agentpb.SessionRequest{Request: &agentpb.SessionRequest_UpdateIo{UpdateIo: update}}, false)
	if err != nil {
		return nil, err
	}
	if value := response.GetUpdateIo(); value != nil {
		return value, nil
	}
	return nil, ErrUnexpectedRemoteResponse
}

type FinalizeSimulationOptions struct {
	ProvisionalSuccess bool
	ProvisionalReason  string
	Timeout            time.Duration
}

type FinalizeSimulationError struct {
	Message     string
	UserVerdict *agentpb.SessionResponse_FinalizeSimulationResponse_SimulationVerdict
}

func (e *FinalizeSimulationError) Error() string {
	if e == nil {
		return "voice finalize simulation failed"
	}
	return e.Message
}

func (s *RemoteSession) FinalizeSimulation(ctx context.Context, options FinalizeSimulationOptions) (*agentpb.SessionResponse_FinalizeSimulationResponse, error) {
	if options.Timeout < 0 {
		return nil, errors.New("voice remote finalize simulation timeout must not be negative")
	}
	requestCtx, cancel := s.requestContext(ctx, options.Timeout)
	defer cancel()
	request := &agentpb.SessionRequest{Request: &agentpb.SessionRequest_FinalizeSimulation_{FinalizeSimulation: &agentpb.SessionRequest_FinalizeSimulation{
		ProvisionalSuccess: options.ProvisionalSuccess, ProvisionalReason: options.ProvisionalReason,
	}}}
	response, err := s.sendRequest(requestCtx, request, true)
	if err != nil {
		return nil, err
	}
	value := response.GetFinalizeSimulation()
	if response.Error != nil {
		return nil, &FinalizeSimulationError{Message: response.GetError(), UserVerdict: value.GetUserVerdict()}
	}
	if value == nil {
		return nil, ErrUnexpectedRemoteResponse
	}
	return value, nil
}

func (s *RemoteSession) failPending(err error) {
	if err == nil {
		err = ErrRemoteSessionClosed
	}
	s.mu.Lock()
	pending := s.pending
	s.pending = make(map[string]*remotePendingRequest)
	s.mu.Unlock()
	for _, request := range pending {
		request.result <- remoteRequestResult{err: err}
	}
}

func (s *RemoteSession) Close(ctx context.Context) error {
	s.closeOnce.Do(func() { go s.closeWorker() })
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-s.closeDone:
		return s.closeErr
	}
}

// AClose is the migration alias for Python/agents-js aclose().
func (s *RemoteSession) AClose(ctx context.Context) error { return s.Close(ctx) }

func (s *RemoteSession) closeWorker() {
	defer close(s.closeDone)
	s.startMu.Lock()
	s.mu.Lock()
	s.closed, s.started = true, false
	recvStarted := s.recvStart
	s.mu.Unlock()
	s.cancel(ErrRemoteSessionClosed)
	s.failPending(ErrRemoteSessionClosed)
	closeCtx, cancel := context.WithTimeout(context.Background(), DefaultConsoleOperationTimeout)
	s.closeErr = s.transport.Close(closeCtx)
	cancel()
	s.startMu.Unlock()
	if recvStarted {
		timer := time.NewTimer(DefaultConsoleOperationTimeout)
		select {
		case <-s.recvDone:
			if !timer.Stop() {
				<-timer.C
			}
		case <-timer.C:
			s.closeErr = errors.Join(s.closeErr, errors.New("voice remote session receive loop did not stop"))
		}
	}
	s.eventMu.Lock()
	subscriptions := make([]*RemoteEventSubscription, 0, len(s.eventSubs))
	for _, subscription := range s.eventSubs {
		subscriptions = append(subscriptions, subscription)
	}
	clear(s.eventSubs)
	s.eventMu.Unlock()
	for _, subscription := range subscriptions {
		_ = subscription.out.Close()
	}
}
