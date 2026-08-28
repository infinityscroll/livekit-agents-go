// SPDX-License-Identifier: Apache-2.0

package agents

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/livekit/agents-go/internal/workerprotocol"
	"github.com/livekit/agents-go/ipc"
)

const jobInferenceConcurrency = 64

type jobInferenceResponse struct {
	data json.RawMessage
	err  error
}

type jobInferencePending struct {
	method string
	reply  chan jobInferenceResponse
}

// jobIPCInferenceClient is installed in a process-isolated JobContext. One
// existing authenticated connection carries both control and inference frames;
// a single external read loop delivers responses through dispatchResponse.
type jobIPCInferenceClient struct {
	conn *workerprotocol.FramedConn
	next atomic.Uint64

	mu      sync.Mutex
	pending map[string]*jobInferencePending
	closed  bool
	err     error
}

func newJobIPCInferenceClient(conn *workerprotocol.FramedConn) *jobIPCInferenceClient {
	return &jobIPCInferenceClient{conn: conn, pending: make(map[string]*jobInferencePending)}
}

func (c *jobIPCInferenceClient) DoInference(ctx context.Context, method string, data any) (any, error) {
	if c == nil || c.conn == nil {
		return nil, ipc.ErrInferenceExecutorClosed
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
	payload, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("agents: encode inference input for %s: %w", method, err)
	}
	requestID := fmt.Sprintf("inference_req_%x", c.next.Add(1))
	reply := make(chan jobInferenceResponse, 1)
	c.mu.Lock()
	if c.closed {
		closeErr := c.err
		if closeErr == nil {
			closeErr = ipc.ErrInferenceExecutorClosed
		}
		c.mu.Unlock()
		return nil, closeErr
	}
	c.pending[requestID] = &jobInferencePending{method: method, reply: reply}
	c.mu.Unlock()

	if err := c.conn.Write(ctx, workerprotocol.IPCMessage{
		Type: workerprotocol.IPCTypeInferenceRequest, RequestID: requestID,
		Method: method, Data: payload, DeadlineUnixNano: inferenceDeadlineUnixNano(ctx),
	}); err != nil {
		c.removePending(requestID)
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
		c.removePending(requestID)
		cancelCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		_ = c.conn.Write(cancelCtx, workerprotocol.IPCMessage{
			Type: workerprotocol.IPCTypeInferenceCancel, RequestID: requestID,
		})
		cancel()
		return nil, context.Cause(ctx)
	}
}

func (c *jobIPCInferenceClient) removePending(requestID string) {
	c.mu.Lock()
	delete(c.pending, requestID)
	c.mu.Unlock()
}

func (c *jobIPCInferenceClient) dispatchResponse(message workerprotocol.IPCMessage) {
	if c == nil || message.RequestID == "" {
		return
	}
	c.mu.Lock()
	pending := c.pending[message.RequestID]
	delete(c.pending, message.RequestID)
	c.mu.Unlock()
	if pending == nil {
		return
	}
	var err error
	if message.Error != "" {
		err = decodeInferenceError(message.ErrorCode, message.Error, firstNonEmpty(message.Method, pending.method))
	}
	pending.reply <- jobInferenceResponse{data: message.Data, err: err}
}

func (c *jobIPCInferenceClient) close(err error) {
	if c == nil {
		return
	}
	if err == nil {
		err = ipc.ErrInferenceExecutorClosed
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed, c.err = true, err
	pending := c.pending
	c.pending = nil
	c.mu.Unlock()
	for _, request := range pending {
		request.reply <- jobInferenceResponse{err: err}
	}
}

var _ ipc.InferenceExecutor = (*jobIPCInferenceClient)(nil)

func (e *processExecutor[T]) startInference(ctx context.Context, message workerprotocol.IPCMessage) {
	if message.RequestID == "" || message.Method == "" {
		e.writeInferenceError(message.RequestID, &InferenceProcessError{Code: inferenceErrorInvalidRequest, Message: "agents: malformed inference request"})
		return
	}
	select {
	case e.inferenceSlots <- struct{}{}:
	default:
		e.writeInferenceError(message.RequestID, ErrInferenceConcurrencyLimit)
		return
	}
	requestCtx, cancel, cancelDeadline := inferenceRequestContext(ctx, message.DeadlineUnixNano)
	e.inferenceMu.Lock()
	if _, duplicate := e.inferenceCancel[message.RequestID]; duplicate {
		e.inferenceMu.Unlock()
		cancel(errors.New("duplicate inference request ID"))
		cancelDeadline()
		<-e.inferenceSlots
		e.writeInferenceError(message.RequestID, &InferenceProcessError{Code: inferenceErrorInvalidRequest, Message: "agents: duplicate inference request ID"})
		return
	}
	e.inferenceCancel[message.RequestID] = cancel
	e.inferenceMu.Unlock()

	go func() {
		defer func() {
			e.inferenceMu.Lock()
			delete(e.inferenceCancel, message.RequestID)
			e.inferenceMu.Unlock()
			cancel(nil)
			cancelDeadline()
			<-e.inferenceSlots
		}()
		var data any
		if len(message.Data) != 0 && string(message.Data) != "null" {
			if err := json.Unmarshal(message.Data, &data); err != nil {
				e.writeInferenceError(message.RequestID, fmt.Errorf("agents: decode inference request: %w", err))
				return
			}
		}
		if isNilInferenceExecutor(e.inference) {
			e.writeInferenceError(message.RequestID, &ipc.UnknownMethodError{Method: message.Method})
			return
		}
		result, err := e.inference.DoInference(requestCtx, message.Method, data)
		if err != nil {
			e.writeInferenceError(message.RequestID, err)
			return
		}
		payload, err := json.Marshal(result)
		if err != nil {
			e.writeInferenceError(message.RequestID, fmt.Errorf("agents: encode inference response: %w", err))
			return
		}
		writeCtx, writeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer writeCancel()
		if err := e.conn.Write(writeCtx, workerprotocol.IPCMessage{
			Type: workerprotocol.IPCTypeInferenceResponse, RequestID: message.RequestID, Method: message.Method, Data: payload,
		}); err != nil && ctx.Err() == nil {
			e.logger.Warn("failed to send inference response to job", "request_id", message.RequestID, "error", err)
		}
	}()
}

func (e *processExecutor[T]) writeInferenceError(requestID string, inferenceErr error) {
	if e == nil || e.conn == nil || requestID == "" {
		return
	}
	code, message, method := encodeInferenceError(inferenceErr)
	if message == "" {
		message = "agents: inference failed"
	}
	writeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := e.conn.Write(writeCtx, workerprotocol.IPCMessage{
		Type: workerprotocol.IPCTypeInferenceResponse, RequestID: requestID,
		Method: method, ErrorCode: code, Error: message,
	}); err != nil {
		e.logger.Debug("failed to send inference error to job", "request_id", requestID, "error", err)
	}
}

func (e *processExecutor[T]) cancelInference(requestID string) {
	if e == nil || requestID == "" {
		return
	}
	e.inferenceMu.Lock()
	cancel := e.inferenceCancel[requestID]
	e.inferenceMu.Unlock()
	if cancel != nil {
		cancel(context.Canceled)
	}
}

func (e *processExecutor[T]) cancelAllInference(cause error) {
	if e == nil {
		return
	}
	if cause == nil {
		cause = context.Canceled
	}
	e.inferenceMu.Lock()
	cancels := make([]context.CancelCauseFunc, 0, len(e.inferenceCancel))
	for _, cancel := range e.inferenceCancel {
		cancels = append(cancels, cancel)
	}
	e.inferenceMu.Unlock()
	for _, cancel := range cancels {
		cancel(cause)
	}
}
