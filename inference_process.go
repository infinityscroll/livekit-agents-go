// SPDX-License-Identifier: Apache-2.0

package agents

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/livekit/agents-go/internal/workerprotocol"
	"github.com/livekit/agents-go/ipc"
)

const (
	inferenceChildEnvironment = "LIVEKIT_AGENTS_GO_INFERENCE_CHILD"
	inferenceChildAddressEnv  = "LIVEKIT_AGENTS_GO_INFERENCE_IPC_ADDRESS"
	inferenceChildTokenEnv    = "LIVEKIT_AGENTS_GO_INFERENCE_IPC_TOKEN"
	inferenceChildTestRunEnv  = "LIVEKIT_AGENTS_GO_TEST_INFERENCE_CHILD_RUN"

	inferenceProcessConcurrency = 64
	inferenceControlTimeout     = 5 * time.Second
)

const (
	inferenceErrorUnknownMethod        = "unknown_method"
	inferenceErrorCanceled             = "canceled"
	inferenceErrorDeadlineExceeded     = "deadline_exceeded"
	inferenceErrorExecutorClosed       = "executor_closed"
	inferenceErrorProcessUnavailable   = "process_unavailable"
	inferenceErrorProcessCrashed       = "process_crashed"
	inferenceErrorConcurrencyLimit     = "concurrency_limit"
	inferenceErrorRegistrationMismatch = "registration_mismatch"
	inferenceErrorRunnerPanic          = "runner_panic"
	inferenceErrorInvalidRequest       = "invalid_request"
	inferenceErrorInternal             = "internal"
)

var (
	// ErrInferenceProcessUnavailable reports that the supervised runner child
	// cannot accept a request. It is safe to use with errors.Is.
	ErrInferenceProcessUnavailable = errors.New("agents: inference process is unavailable")
	// ErrInferenceProcessCrashed reports an unexpected runner-child exit.
	ErrInferenceProcessCrashed = errors.New("agents: inference process crashed")
	// ErrInferenceConcurrencyLimit reports that the fixed-size subprocess
	// request budget is exhausted. Requests are not placed on an unbounded queue.
	ErrInferenceConcurrencyLimit = errors.New("agents: inference concurrency limit reached")
	// ErrInferenceRegistrationMismatch means the parent and same-binary child
	// did not execute the same startup registration path.
	ErrInferenceRegistrationMismatch = errors.New("agents: inference runner registrations differ in child process")
)

// InferenceProcessError is a stable error category returned by a supervised
// inference subprocess. Arbitrary native errors cannot preserve their concrete
// Go type across an OS process boundary, but their message and category do.
type InferenceProcessError struct {
	Code    string
	Message string
}

func (e *InferenceProcessError) Error() string {
	if e == nil {
		return "agents: inference process error"
	}
	if e.Message != "" {
		return e.Message
	}
	return "agents: inference process error: " + e.Code
}

func (e *InferenceProcessError) Is(target error) bool {
	if e == nil {
		return false
	}
	switch e.Code {
	case inferenceErrorCanceled:
		return target == context.Canceled
	case inferenceErrorDeadlineExceeded:
		return target == context.DeadlineExceeded
	case inferenceErrorExecutorClosed:
		return target == ErrInferenceExecutorClosed || target == ipc.ErrInferenceExecutorClosed
	case inferenceErrorProcessUnavailable:
		return target == ErrInferenceProcessUnavailable
	case inferenceErrorProcessCrashed:
		return target == ErrInferenceProcessCrashed || target == ErrInferenceProcessUnavailable
	case inferenceErrorConcurrencyLimit:
		return target == ErrInferenceConcurrencyLimit
	case inferenceErrorRegistrationMismatch:
		return target == ErrInferenceRegistrationMismatch
	default:
		return false
	}
}

type managedInferenceExecutor interface {
	ipc.InferenceExecutor
	Healthy() error
	Close(context.Context) error
}

type inferenceProcessReply struct {
	data json.RawMessage
	err  error
}

type inferenceProcessPending struct {
	method string
	reply  chan inferenceProcessReply
}

// inferenceProcessExecutor is the worker-parent proxy for one supervised,
// same-binary child. The process is created only when the registry snapshot at
// pool Start is non-empty. All process-isolated jobs share this one proxy.
type inferenceProcessExecutor struct {
	cmd             *exec.Cmd
	conn            *workerprotocol.FramedConn
	pid             int
	methods         []string
	shutdownTimeout time.Duration
	next            atomic.Uint64

	ctx    context.Context
	cancel context.CancelCauseFunc

	mu          sync.Mutex
	pending     map[string]*inferenceProcessPending
	accepting   bool
	initialized bool
	closing     bool
	graceful    bool
	terminalErr error
	exitErr     error

	slots        chan struct{}
	protocolDone chan workerprotocol.IPCMessage
	readDone     chan struct{}
	exitDone     chan struct{}
	closeOnce    sync.Once
	closeDone    chan struct{}
	closeErr     error
}

func newInferenceProcessExecutor(ctx context.Context, methods []string, shutdownTimeout time.Duration, childEnvironment ...string) (_ *inferenceProcessExecutor, startErr error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	methods = append([]string(nil), methods...)
	sort.Strings(methods)
	if len(methods) == 0 {
		return nil, errors.New("agents: cannot start an inference process without registered runners")
	}
	if shutdownTimeout <= 0 {
		shutdownTimeout = DefaultShutdownProcessTimeout
	}

	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("agents: listen for inference child: %w", err)
	}
	defer listener.Close()
	token := ShortUUID("inference_ipc_") + ShortUUID("")
	executable, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("agents: locate executable for inference child: %w", err)
	}
	arguments := append([]string(nil), os.Args[1:]...)
	if testRun := os.Getenv(inferenceChildTestRunEnv); testRun != "" {
		arguments = append(arguments, "-test.run="+testRun)
	}
	command := exec.Command(executable, arguments...)
	configureJobCommand(command)
	replacements := []string{
		inferenceChildEnvironment + "=1",
		jobChildEnvironment + "=0",
		inferenceChildAddressEnv + "=" + listener.Addr().String(),
		inferenceChildTokenEnv + "=" + token,
	}
	replacements = append(replacements, childEnvironment...)
	command.Env = replaceProcessEnvironment(os.Environ(), replacements...)
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr

	runCtx, cancel := context.WithCancelCause(context.Background())
	executor := &inferenceProcessExecutor{
		cmd: command, methods: methods, shutdownTimeout: shutdownTimeout,
		ctx: runCtx, cancel: cancel, pending: make(map[string]*inferenceProcessPending),
		slots:        make(chan struct{}, inferenceProcessConcurrency),
		protocolDone: make(chan workerprotocol.IPCMessage, 1), readDone: make(chan struct{}),
		exitDone: make(chan struct{}), closeDone: make(chan struct{}),
	}
	if err := command.Start(); err != nil {
		cancel(err)
		return nil, fmt.Errorf("agents: start inference child: %w", err)
	}
	executor.pid = command.Process.Pid
	go func() {
		exitErr := command.Wait()
		executor.mu.Lock()
		executor.exitErr = exitErr
		executor.mu.Unlock()
		close(executor.exitDone)
	}()
	started := false
	defer func() {
		if started {
			return
		}
		rollbackCtx, rollbackCancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer rollbackCancel()
		startErr = errors.Join(startErr, executor.rollbackStart(rollbackCtx))
	}()

	framed, err := acceptAuthenticatedChild(ctx, listener, token, executor.exitDone, "inference child")
	if err != nil {
		return nil, err
	}
	executor.conn = framed
	if err := framed.Write(ctx, workerprotocol.IPCMessage{
		Type: workerprotocol.IPCTypeInitialize, Methods: methods,
	}); err != nil {
		return nil, fmt.Errorf("agents: initialize inference child: %w", err)
	}
	ready, err := framed.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("agents: initialize inference child: %w", err)
	}
	if ready.Type != workerprotocol.IPCTypeReady {
		return nil, fmt.Errorf("agents: expected ready from inference child, got %q", ready.Type)
	}
	if ready.Error != "" {
		return nil, decodeInferenceError(ready.ErrorCode, ready.Error, ready.Method)
	}
	actual := append([]string(nil), ready.Methods...)
	sort.Strings(actual)
	if !slices.Equal(actual, methods) {
		return nil, &InferenceProcessError{Code: inferenceErrorRegistrationMismatch, Message: fmt.Sprintf(
			"%v: parent=%v child=%v", ErrInferenceRegistrationMismatch, methods, actual,
		)}
	}
	executor.mu.Lock()
	executor.initialized = true
	executor.accepting = true
	executor.mu.Unlock()
	go executor.readLoop()
	started = true
	return executor, nil
}

func (e *inferenceProcessExecutor) DoInference(ctx context.Context, method string, data any) (any, error) {
	if e == nil {
		return nil, ErrInferenceProcessUnavailable
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if method == "" {
		return nil, errors.New("agents: inference method must not be empty")
	}
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	if _, registered := slices.BinarySearch(e.methods, method); !registered {
		return nil, &ipc.UnknownMethodError{Method: method}
	}
	payload, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("agents: encode inference input for %s: %w", method, err)
	}
	select {
	case e.slots <- struct{}{}:
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	default:
		return nil, ErrInferenceConcurrencyLimit
	}

	requestID := fmt.Sprintf("inference_process_req_%x", e.next.Add(1))
	reply := make(chan inferenceProcessReply, 1)
	e.mu.Lock()
	if !e.accepting {
		terminalErr := e.terminalErr
		if terminalErr == nil {
			terminalErr = ipc.ErrInferenceExecutorClosed
		}
		e.mu.Unlock()
		<-e.slots
		return nil, terminalErr
	}
	e.pending[requestID] = &inferenceProcessPending{method: method, reply: reply}
	e.mu.Unlock()

	if err := e.conn.Write(ctx, workerprotocol.IPCMessage{
		Type: workerprotocol.IPCTypeInferenceRequest, RequestID: requestID,
		Method: method, Data: payload, DeadlineUnixNano: inferenceDeadlineUnixNano(ctx),
	}); err != nil {
		e.removePending(requestID)
		return nil, fmt.Errorf("agents: send inference request %s: %w", method, err)
	}

	select {
	case response := <-reply:
		if response.err != nil {
			return nil, response.err
		}
		if len(response.data) == 0 || string(response.data) == "null" {
			return nil, nil
		}
		var result any
		if err := json.Unmarshal(response.data, &result); err != nil {
			return nil, fmt.Errorf("agents: decode inference response for %s: %w", method, err)
		}
		return result, nil
	case <-ctx.Done():
		e.removePending(requestID)
		cancelCtx, cancel := context.WithTimeout(context.Background(), inferenceControlTimeout)
		_ = e.conn.Write(cancelCtx, workerprotocol.IPCMessage{
			Type: workerprotocol.IPCTypeInferenceCancel, RequestID: requestID,
		})
		cancel()
		return nil, context.Cause(ctx)
	}
}

func (e *inferenceProcessExecutor) removePending(requestID string) *inferenceProcessPending {
	e.mu.Lock()
	pending := e.pending[requestID]
	if pending != nil {
		delete(e.pending, requestID)
	}
	e.mu.Unlock()
	if pending != nil {
		<-e.slots
	}
	return pending
}

func (e *inferenceProcessExecutor) readLoop() {
	defer close(e.readDone)
	for {
		message, err := e.conn.Read(e.ctx)
		if err != nil {
			if context.Cause(e.ctx) != nil {
				return
			}
			e.fail(&InferenceProcessError{Code: inferenceErrorProcessCrashed, Message: fmt.Sprintf(
				"%v: inference child IPC failed: %v", ErrInferenceProcessCrashed, err,
			)})
			return
		}
		switch message.Type {
		case workerprotocol.IPCTypeInferenceResponse:
			pending := e.removePending(message.RequestID)
			if pending == nil {
				continue
			}
			var responseErr error
			if message.Error != "" {
				responseErr = decodeInferenceError(message.ErrorCode, message.Error, firstNonEmpty(message.Method, pending.method))
			}
			pending.reply <- inferenceProcessReply{data: message.Data, err: responseErr}
		case workerprotocol.IPCTypeDone:
			e.mu.Lock()
			e.graceful = true
			e.mu.Unlock()
			select {
			case e.protocolDone <- message:
			default:
			}
			return
		default:
			e.fail(&InferenceProcessError{Code: inferenceErrorProcessUnavailable, Message: fmt.Sprintf(
				"%v: unexpected inference child message %q", ErrInferenceProcessUnavailable, message.Type,
			)})
			return
		}
	}
}

func (e *inferenceProcessExecutor) fail(failure error) {
	if failure == nil {
		failure = ErrInferenceProcessUnavailable
	}
	e.mu.Lock()
	if e.terminalErr == nil {
		e.terminalErr = failure
	}
	e.accepting = false
	pending := e.pending
	e.pending = make(map[string]*inferenceProcessPending)
	e.mu.Unlock()
	e.cancel(failure)
	for _, request := range pending {
		<-e.slots
		request.reply <- inferenceProcessReply{err: failure}
	}
}

func (e *inferenceProcessExecutor) Healthy() error {
	if e == nil {
		return ErrInferenceProcessUnavailable
	}
	e.mu.Lock()
	initialized := e.initialized
	closing := e.closing
	terminalErr := e.terminalErr
	exitErr := e.exitErr
	e.mu.Unlock()
	if terminalErr != nil {
		return terminalErr
	}
	if !initialized {
		return &InferenceProcessError{Code: inferenceErrorProcessUnavailable, Message: "agents: inference process is not initialized"}
	}
	if closing {
		return ipc.ErrInferenceExecutorClosed
	}
	select {
	case <-e.exitDone:
		return &InferenceProcessError{Code: inferenceErrorProcessCrashed, Message: fmt.Sprintf("%v: %v", ErrInferenceProcessCrashed, exitErr)}
	default:
		return nil
	}
}

func (e *inferenceProcessExecutor) Close(ctx context.Context) error {
	if e == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	e.closeOnce.Do(func() { go e.closeAsync() })
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-e.closeDone:
		return e.closeErr
	}
}

func (e *inferenceProcessExecutor) closeAsync() {
	defer close(e.closeDone)
	e.mu.Lock()
	e.closing = true
	e.accepting = false
	terminalErr := e.terminalErr
	e.mu.Unlock()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), e.shutdownTimeout)
	defer cancel()
	if terminalErr == nil {
		if err := e.conn.Write(shutdownCtx, workerprotocol.IPCMessage{Type: workerprotocol.IPCTypeStop, Reason: "worker inference executor closed"}); err != nil {
			e.closeErr = errors.Join(e.closeErr, fmt.Errorf("agents: stop inference child: %w", err))
		}
	}

	var done workerprotocol.IPCMessage
	gotDone := false
	select {
	case done = <-e.protocolDone:
		gotDone = true
	case <-e.exitDone:
		select {
		case done = <-e.protocolDone:
			gotDone = true
		default:
		}
	case <-shutdownCtx.Done():
		e.closeErr = errors.Join(e.closeErr, context.DeadlineExceeded)
	}
	if gotDone && done.Error != "" {
		e.closeErr = errors.Join(e.closeErr, decodeInferenceError(done.ErrorCode, done.Error, done.Method))
	}
	_ = e.conn.Close()
	e.cancel(ipc.ErrInferenceExecutorClosed)

	select {
	case <-e.exitDone:
	case <-shutdownCtx.Done():
		e.closeErr = errors.Join(e.closeErr, killJobProcess(e.cmd))
		forceTimer := time.NewTimer(inferenceControlTimeout)
		select {
		case <-e.exitDone:
			if !forceTimer.Stop() {
				<-forceTimer.C
			}
		case <-forceTimer.C:
			e.closeErr = errors.Join(e.closeErr, errors.New("agents: inference child could not be reaped after forced termination"))
		}
	}

	e.mu.Lock()
	terminalErr = e.terminalErr
	graceful := e.graceful
	exitErr := e.exitErr
	e.mu.Unlock()
	if terminalErr != nil {
		e.closeErr = errors.Join(e.closeErr, terminalErr)
	}
	if !gotDone && !graceful && terminalErr == nil {
		terminalErr = &InferenceProcessError{Code: inferenceErrorProcessCrashed, Message: fmt.Sprintf("%v: %v", ErrInferenceProcessCrashed, exitErr)}
		e.closeErr = errors.Join(e.closeErr, terminalErr)
	}
	if terminalErr == nil {
		terminalErr = ipc.ErrInferenceExecutorClosed
	}
	e.fail(terminalErr)
	readTimer := time.NewTimer(inferenceControlTimeout)
	select {
	case <-e.readDone:
		if !readTimer.Stop() {
			<-readTimer.C
		}
	case <-readTimer.C:
		e.closeErr = errors.Join(e.closeErr, errors.New("agents: inference IPC reader did not stop"))
	}
}

func (e *inferenceProcessExecutor) rollbackStart(ctx context.Context) error {
	if e == nil || e.cmd == nil {
		return nil
	}
	if e.conn != nil {
		_ = e.conn.Close()
	}
	e.cancel(ErrInferenceProcessUnavailable)
	killErr := killJobProcess(e.cmd)
	select {
	case <-e.exitDone:
		return killErr
	case <-ctx.Done():
		return errors.Join(killErr, context.Cause(ctx))
	}
}

func (e *inferenceProcessExecutor) PID() int {
	if e == nil {
		return 0
	}
	return e.pid
}

func isInferenceChildProcess() bool { return os.Getenv(inferenceChildEnvironment) == "1" }

// runInferenceChildProcess owns every native runner in process-isolated worker
// mode. Factories must be linked and registered along the same startup path in
// this re-executed binary; parent-only closures cannot be serialized.
func runInferenceChildProcess(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	address := os.Getenv(inferenceChildAddressEnv)
	token := os.Getenv(inferenceChildTokenEnv)
	if address == "" || token == "" {
		return errors.New("agents: inference child IPC environment is incomplete")
	}
	connectCtx, cancelConnect := context.WithTimeout(ctx, DefaultInitializeTimeout)
	raw, err := (&net.Dialer{}).DialContext(connectCtx, "tcp", address)
	cancelConnect()
	if err != nil {
		return fmt.Errorf("agents: connect inference child to worker parent: %w", err)
	}
	conn := workerprotocol.NewFramedConn(raw)
	defer conn.Close()
	helloCtx, cancelHello := context.WithTimeout(ctx, inferenceControlTimeout)
	err = conn.Write(helloCtx, workerprotocol.IPCMessage{Type: workerprotocol.IPCTypeHello, Token: token})
	cancelHello()
	if err != nil {
		return fmt.Errorf("agents: authenticate inference child: %w", err)
	}
	initializeRequestCtx, cancelInitializeRequest := context.WithTimeout(ctx, DefaultInferenceInitializeTimeout)
	initialize, err := conn.Read(initializeRequestCtx)
	cancelInitializeRequest()
	if err != nil {
		return fmt.Errorf("agents: read inference initialization request: %w", err)
	}
	if initialize.Type != workerprotocol.IPCTypeInitialize {
		return fmt.Errorf("agents: expected inference initialize, got %q", initialize.Type)
	}
	factories := RegisteredInferenceRunners()
	methods := sortedInferenceMethods(factories)
	expected := append([]string(nil), initialize.Methods...)
	sort.Strings(expected)
	if !slices.Equal(methods, expected) {
		mismatch := &InferenceProcessError{Code: inferenceErrorRegistrationMismatch, Message: fmt.Sprintf(
			"%v: parent=%v child=%v; register factories before pool Start and on the child startup path",
			ErrInferenceRegistrationMismatch, expected, methods,
		)}
		code, message, method := encodeInferenceError(mismatch)
		_ = writeInferenceReady(conn, workerprotocol.IPCMessage{
			Type: workerprotocol.IPCTypeReady, ErrorCode: code, Error: message, Method: method, Methods: methods,
		})
		return mismatch
	}
	executor, err := NewLocalInferenceExecutor(factories)
	if err != nil {
		code, message, method := encodeInferenceError(err)
		_ = writeInferenceReady(conn, workerprotocol.IPCMessage{Type: workerprotocol.IPCTypeReady, ErrorCode: code, Error: message, Method: method, Methods: methods})
		return err
	}
	initializeCtx, cancelInitialize := context.WithTimeout(ctx, DefaultInferenceInitializeTimeout)
	err = executor.Initialize(initializeCtx)
	cancelInitialize()
	if err != nil {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), DefaultShutdownProcessTimeout)
		closeErr := executor.Close(closeCtx)
		closeCancel()
		code, message, method := encodeInferenceError(err)
		_ = writeInferenceReady(conn, workerprotocol.IPCMessage{Type: workerprotocol.IPCTypeReady, ErrorCode: code, Error: message, Method: method, Methods: methods})
		return errors.Join(err, closeErr)
	}
	if err := writeInferenceReady(conn, workerprotocol.IPCMessage{Type: workerprotocol.IPCTypeReady, Methods: methods}); err != nil {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), DefaultShutdownProcessTimeout)
		closeErr := executor.Close(closeCtx)
		closeCancel()
		return errors.Join(err, closeErr)
	}
	return (&inferenceChildServer{
		conn: conn, executor: executor, slots: make(chan struct{}, inferenceProcessConcurrency),
		cancels: make(map[string]context.CancelCauseFunc),
	}).serve(ctx)
}

func writeInferenceReady(conn *workerprotocol.FramedConn, message workerprotocol.IPCMessage) error {
	writeCtx, cancel := context.WithTimeout(context.Background(), inferenceControlTimeout)
	defer cancel()
	return conn.Write(writeCtx, message)
}

type inferenceChildServer struct {
	conn     *workerprotocol.FramedConn
	executor *LocalInferenceExecutor
	slots    chan struct{}

	mu       sync.Mutex
	cancels  map[string]context.CancelCauseFunc
	requests sync.WaitGroup
}

func (s *inferenceChildServer) serve(parent context.Context) error {
	runCtx, cancelRun := context.WithCancelCause(parent)
	defer cancelRun(nil)
	for {
		message, err := s.conn.Read(runCtx)
		if err != nil {
			cancelRun(err)
			s.cancelAll(err)
			closeCtx, cancelClose := context.WithTimeout(context.Background(), DefaultShutdownProcessTimeout)
			closeErr := s.executor.Close(closeCtx)
			cancelClose()
			return errors.Join(fmt.Errorf("agents: inference parent IPC disconnected: %w", err), closeErr)
		}
		switch message.Type {
		case workerprotocol.IPCTypeInferenceRequest:
			s.startRequest(runCtx, message)
		case workerprotocol.IPCTypeInferenceCancel:
			s.cancelRequest(message.RequestID)
		case workerprotocol.IPCTypeStop:
			cancelRun(ipc.ErrInferenceExecutorClosed)
			s.cancelAll(ipc.ErrInferenceExecutorClosed)
			closeCtx, cancelClose := context.WithTimeout(context.Background(), DefaultShutdownProcessTimeout)
			closeErr := s.executor.Close(closeCtx)
			waitDone := make(chan struct{})
			go func() { s.requests.Wait(); close(waitDone) }()
			select {
			case <-waitDone:
			case <-closeCtx.Done():
				closeErr = errors.Join(closeErr, context.Cause(closeCtx))
			}
			cancelClose()
			done := workerprotocol.IPCMessage{Type: workerprotocol.IPCTypeDone, Success: closeErr == nil, Reason: message.Reason}
			if closeErr != nil {
				done.ErrorCode, done.Error, done.Method = encodeInferenceError(closeErr)
			}
			writeCtx, cancelWrite := context.WithTimeout(context.Background(), inferenceControlTimeout)
			writeErr := s.conn.Write(writeCtx, done)
			cancelWrite()
			return errors.Join(closeErr, writeErr)
		default:
			return fmt.Errorf("agents: unexpected inference parent message %q", message.Type)
		}
	}
}

func (s *inferenceChildServer) startRequest(parent context.Context, message workerprotocol.IPCMessage) {
	if message.RequestID == "" || message.Method == "" {
		s.writeError(message.RequestID, message.Method, &InferenceProcessError{Code: inferenceErrorInvalidRequest, Message: "agents: malformed inference request"})
		return
	}
	select {
	case s.slots <- struct{}{}:
	default:
		s.writeError(message.RequestID, message.Method, ErrInferenceConcurrencyLimit)
		return
	}
	requestCtx, cancel, cancelDeadline := inferenceRequestContext(parent, message.DeadlineUnixNano)
	s.mu.Lock()
	if _, duplicate := s.cancels[message.RequestID]; duplicate {
		s.mu.Unlock()
		cancel(errors.New("agents: duplicate inference request ID"))
		cancelDeadline()
		<-s.slots
		s.writeError(message.RequestID, message.Method, &InferenceProcessError{Code: inferenceErrorInvalidRequest, Message: "agents: duplicate inference request ID"})
		return
	}
	s.cancels[message.RequestID] = cancel
	s.mu.Unlock()
	s.requests.Add(1)
	go func() {
		defer s.requests.Done()
		defer func() {
			s.mu.Lock()
			delete(s.cancels, message.RequestID)
			s.mu.Unlock()
			cancel(nil)
			cancelDeadline()
			<-s.slots
		}()
		defer func() {
			if recovered := recover(); recovered != nil {
				s.writeError(message.RequestID, message.Method, &InferenceProcessError{Code: inferenceErrorRunnerPanic, Message: fmt.Sprintf("agents: inference request panicked: %v", recovered)})
			}
		}()
		var data any
		if len(message.Data) != 0 && string(message.Data) != "null" {
			if err := json.Unmarshal(message.Data, &data); err != nil {
				s.writeError(message.RequestID, message.Method, &InferenceProcessError{Code: inferenceErrorInvalidRequest, Message: fmt.Sprintf("agents: decode inference request: %v", err)})
				return
			}
		}
		result, err := s.executor.DoInference(requestCtx, message.Method, data)
		if err != nil {
			s.writeError(message.RequestID, message.Method, err)
			return
		}
		payload, err := json.Marshal(result)
		if err != nil {
			s.writeError(message.RequestID, message.Method, fmt.Errorf("agents: encode inference response: %w", err))
			return
		}
		writeCtx, cancelWrite := context.WithTimeout(context.Background(), inferenceControlTimeout)
		defer cancelWrite()
		_ = s.conn.Write(writeCtx, workerprotocol.IPCMessage{
			Type: workerprotocol.IPCTypeInferenceResponse, RequestID: message.RequestID,
			Method: message.Method, Data: payload,
		})
	}()
}

func (s *inferenceChildServer) writeError(requestID, method string, inferenceErr error) {
	if requestID == "" {
		return
	}
	code, message, errorMethod := encodeInferenceError(inferenceErr)
	writeCtx, cancel := context.WithTimeout(context.Background(), inferenceControlTimeout)
	defer cancel()
	_ = s.conn.Write(writeCtx, workerprotocol.IPCMessage{
		Type: workerprotocol.IPCTypeInferenceResponse, RequestID: requestID,
		Method: firstNonEmpty(errorMethod, method), ErrorCode: code, Error: message,
	})
}

func (s *inferenceChildServer) cancelRequest(requestID string) {
	if requestID == "" {
		return
	}
	s.mu.Lock()
	cancel := s.cancels[requestID]
	s.mu.Unlock()
	if cancel != nil {
		cancel(context.Canceled)
	}
}

func (s *inferenceChildServer) cancelAll(cause error) {
	if cause == nil {
		cause = context.Canceled
	}
	s.mu.Lock()
	cancels := make([]context.CancelCauseFunc, 0, len(s.cancels))
	for _, cancel := range s.cancels {
		cancels = append(cancels, cancel)
	}
	s.mu.Unlock()
	for _, cancel := range cancels {
		cancel(cause)
	}
}

func sortedInferenceMethods(factories map[string]InferenceRunnerFactory) []string {
	if len(factories) == 0 {
		return nil
	}
	methods := make([]string, 0, len(factories))
	for method := range factories {
		methods = append(methods, method)
	}
	sort.Strings(methods)
	return methods
}

func inferenceDeadlineUnixNano(ctx context.Context) int64 {
	if ctx == nil {
		return 0
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return 0
	}
	return deadline.UnixNano()
}

func inferenceRequestContext(parent context.Context, deadlineUnixNano int64) (context.Context, context.CancelCauseFunc, context.CancelFunc) {
	requestCtx, cancel := context.WithCancelCause(parent)
	if deadlineUnixNano == 0 {
		return requestCtx, cancel, func() {}
	}
	deadlineCtx, cancelDeadline := context.WithDeadline(requestCtx, time.Unix(0, deadlineUnixNano))
	return deadlineCtx, cancel, cancelDeadline
}

func encodeInferenceError(err error) (code, message, method string) {
	if err == nil {
		return "", "", ""
	}
	message = err.Error()
	var unknown *ipc.UnknownMethodError
	var processErr *InferenceProcessError
	switch {
	case errors.As(err, &unknown):
		return inferenceErrorUnknownMethod, message, unknown.Method
	case errors.Is(err, context.DeadlineExceeded):
		return inferenceErrorDeadlineExceeded, message, ""
	case errors.Is(err, context.Canceled):
		return inferenceErrorCanceled, message, ""
	case errors.Is(err, ErrInferenceExecutorClosed), errors.Is(err, ipc.ErrInferenceExecutorClosed):
		return inferenceErrorExecutorClosed, message, ""
	case errors.Is(err, ErrInferenceProcessCrashed):
		return inferenceErrorProcessCrashed, message, ""
	case errors.Is(err, ErrInferenceProcessUnavailable):
		return inferenceErrorProcessUnavailable, message, ""
	case errors.Is(err, ErrInferenceConcurrencyLimit):
		return inferenceErrorConcurrencyLimit, message, ""
	case errors.Is(err, ErrInferenceRegistrationMismatch):
		return inferenceErrorRegistrationMismatch, message, ""
	case errors.As(err, &processErr):
		return processErr.Code, message, ""
	case strings.Contains(message, "panicked"):
		return inferenceErrorRunnerPanic, message, ""
	default:
		return inferenceErrorInternal, message, ""
	}
}

func decodeInferenceError(code, message, method string) error {
	if message == "" {
		message = "agents: remote inference failed"
	}
	switch code {
	case inferenceErrorUnknownMethod:
		return &ipc.UnknownMethodError{Method: method}
	case inferenceErrorCanceled:
		return &InferenceProcessError{Code: code, Message: message}
	case inferenceErrorDeadlineExceeded:
		return &InferenceProcessError{Code: code, Message: message}
	case "":
		// Older peers did not send a code. Preserve their diagnostic while
		// making the loss of concrete type explicit.
		return &InferenceProcessError{Code: inferenceErrorInternal, Message: message}
	default:
		return &InferenceProcessError{Code: code, Message: message}
	}
}

func acceptAuthenticatedChild(ctx context.Context, listener net.Listener, token string, childExited <-chan struct{}, role string) (*workerprotocol.FramedConn, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if role == "" {
		role = "child"
	}
	stopClosing := context.AfterFunc(ctx, func() { _ = listener.Close() })
	defer stopClosing()
	authenticated := make(chan struct{})
	defer close(authenticated)
	go func() {
		select {
		case <-childExited:
			_ = listener.Close()
		case <-authenticated:
		}
	}()
	for {
		raw, err := listener.Accept()
		if err != nil {
			if context.Cause(ctx) != nil {
				return nil, context.Cause(ctx)
			}
			select {
			case <-childExited:
				return nil, fmt.Errorf("agents: %s exited before IPC handshake", role)
			default:
			}
			return nil, fmt.Errorf("agents: accept %s: %w", role, err)
		}
		framed := workerprotocol.NewFramedConn(raw)
		handshakeCtx, cancel := context.WithTimeout(ctx, time.Second)
		hello, readErr := framed.Read(handshakeCtx)
		cancel()
		if readErr == nil && hello.Type == workerprotocol.IPCTypeHello && subtle.ConstantTimeCompare([]byte(hello.Token), []byte(token)) == 1 {
			return framed, nil
		}
		_ = framed.Close()
		if context.Cause(ctx) != nil {
			return nil, context.Cause(ctx)
		}
		select {
		case <-childExited:
			return nil, fmt.Errorf("agents: %s exited during IPC authentication", role)
		default:
		}
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

var _ managedInferenceExecutor = (*inferenceProcessExecutor)(nil)
