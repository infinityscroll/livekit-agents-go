// SPDX-License-Identifier: Apache-2.0

package elevenlabs

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	agents "github.com/infinityscroll/livekit-agents-go"
)

type ttsCommandKind uint8

const (
	ttsContentCommand ttsCommandKind = iota
	ttsCloseContextCommand
	ttsCloseSocketCommand
)

type ttsCommand struct {
	kind  ttsCommandKind
	state *ttsContextState
	text  string
	flush bool
	init  bool
	ack   chan error
}

type ttsProviderResult struct {
	event ttsProviderEvent
	err   error
}

type ttsContextState struct {
	id          string
	ctx         context.Context
	events      chan ttsProviderResult
	initialized atomic.Bool
	failOnce    sync.Once
	failureMu   sync.Mutex
	failure     error
	failed      chan struct{}
}

func (s *ttsContextState) fail(err error) {
	if err == nil {
		err = agents.NewAPIConnectionError("ElevenLabs TTS context failed", true, nil)
	}
	s.failOnce.Do(func() {
		s.failureMu.Lock()
		s.failure = err
		s.failureMu.Unlock()
		close(s.failed)
	})
}

func (s *ttsContextState) failureError() error {
	s.failureMu.Lock()
	err := s.failure
	s.failureMu.Unlock()
	return err
}

type ttsConnection struct {
	owner *TTS
	opts  resolvedTTSOptions
	ws    *websocket.Conn

	ctx    context.Context
	cancel context.CancelCauseFunc
	done   chan struct{}

	commands chan ttsCommand
	stop     chan struct{}
	stopOnce sync.Once
	retireCh chan struct{}

	mu       sync.Mutex
	contexts map[string]*ttsContextState
	retired  bool
	closed   bool
	slots    chan struct{}

	workers sync.WaitGroup
}

func newTTSConnection(ctx context.Context, owner *TTS, opts resolvedTTSOptions) (*ttsConnection, error) {
	u := multiContextTTSURL(opts)
	header := make(http.Header)
	header.Set(AuthorizationHeader, opts.apiKey)
	if opts.userAgent != "" {
		header.Set("User-Agent", opts.userAgent)
	}
	ws, response, err := dialWebSocket(ctx, opts.wsDialer, u, header)
	if err != nil {
		if response != nil {
			defer response.Body.Close()
			body, readErr := readBounded(response.Body, opts.maxResponseBytes)
			if readErr == nil {
				return nil, apiStatusError(response, body)
			}
		}
		return nil, normalizeNetworkError(err)
	}
	connectionCtx, cancel := context.WithCancelCause(context.Background())
	c := &ttsConnection{
		owner: owner, opts: cloneResolvedTTSOptions(opts), ws: ws,
		ctx: connectionCtx, cancel: cancel, done: make(chan struct{}),
		commands: make(chan ttsCommand, opts.writerCapacity), stop: make(chan struct{}), retireCh: make(chan struct{}, 1),
		contexts: make(map[string]*ttsContextState), slots: make(chan struct{}, opts.maxActiveContexts),
	}
	ws.SetReadLimit(opts.maxWSMessageBytes)
	c.workers.Add(2)
	workerErrors := make(chan error, 2)
	go func() {
		defer c.workers.Done()
		workerErrors <- c.writeLoop()
	}()
	go func() {
		defer c.workers.Done()
		workerErrors <- c.readLoop()
	}()
	go c.supervise(workerErrors)
	return c, nil
}

func (t *TTS) acquireContext(ctx context.Context, state *ttsContextState, opts resolvedTTSOptions, connect agents.APIConnectOptions) (*ttsConnection, error) {
	var lastErr error
	for attempt := 0; attempt <= connect.MaxRetries; attempt++ {
		if err := t.lockGate(ctx); err != nil {
			return nil, err
		}
		t.optsMu.RLock()
		currentGeneration := t.opts.generation
		t.optsMu.RUnlock()
		if t.closed.Load() {
			t.unlockGate()
			return nil, io.ErrClosedPipe
		}

		t.connMu.Lock()
		connection := t.current
		if connection != nil && (connection.opts.generation != opts.generation || !connection.accepting()) {
			connection = nil
		}
		t.connMu.Unlock()
		newConnection := false
		if connection == nil {
			attemptCtx, cancel := withAttemptTimeout(ctx, connect.Timeout)
			connection, lastErr = newTTSConnection(attemptCtx, t, opts)
			cancel()
			if lastErr == nil {
				newConnection = true
				t.connMu.Lock()
				t.connections[connection] = struct{}{}
				if opts.generation == currentGeneration {
					t.current = connection
				}
				t.connMu.Unlock()
			}
		}
		if lastErr == nil {
			lastErr = connection.register(ctx, state)
		}
		if newConnection && opts.generation != currentGeneration {
			connection.markRetired()
		}
		t.unlockGate()
		if lastErr == nil {
			return connection, nil
		}
		if newConnection {
			connection.markRetired()
		}
		if ctx.Err() != nil {
			return nil, context.Cause(ctx)
		}
		if !isRetryable(lastErr) || attempt == connect.MaxRetries {
			return nil, lastErr
		}
		if err := waitRetry(ctx, retryDelay(connect, attempt)); err != nil {
			return nil, err
		}
	}
	return nil, lastErr
}

func (c *ttsConnection) accepting() bool {
	c.mu.Lock()
	accepting := !c.closed && !c.retired && c.ctx.Err() == nil
	c.mu.Unlock()
	return accepting
}

func (c *ttsConnection) register(ctx context.Context, state *ttsContextState) error {
	if state.failed == nil {
		state.failed = make(chan struct{})
	}
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-c.ctx.Done():
		return normalizeNetworkError(context.Cause(c.ctx))
	case c.slots <- struct{}{}:
	}
	c.mu.Lock()
	if c.closed || c.retired {
		c.mu.Unlock()
		<-c.slots
		return agents.NewAPIConnectionError("ElevenLabs TTS connection is not accepting contexts", true, nil)
	}
	if _, exists := c.contexts[state.id]; exists {
		c.mu.Unlock()
		<-c.slots
		return &ProtocolError{Message: "duplicate TTS context ID"}
	}
	c.contexts[state.id] = state
	c.mu.Unlock()
	return nil
}

func (c *ttsConnection) sendContent(ctx context.Context, state *ttsContextState, text string, flush bool) error {
	command := ttsCommand{
		kind: ttsContentCommand, state: state, text: text, flush: flush,
		init: !state.initialized.Swap(true), ack: make(chan error, 1),
	}
	err := c.sendCommand(ctx, command)
	if err != nil && command.init {
		state.initialized.Store(false)
	}
	return err
}

func (c *ttsConnection) sendCloseContext(ctx context.Context, state *ttsContextState) error {
	return c.sendCommand(ctx, ttsCommand{kind: ttsCloseContextCommand, state: state, ack: make(chan error, 1)})
}

func (c *ttsConnection) sendCommand(ctx context.Context, command ttsCommand) error {
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-c.done:
		return agents.NewAPIConnectionError("ElevenLabs TTS connection closed", true, context.Cause(c.ctx))
	case c.commands <- command:
	}
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-c.done:
		return agents.NewAPIConnectionError("ElevenLabs TTS connection closed", true, context.Cause(c.ctx))
	case err := <-command.ack:
		return err
	}
}

func (c *ttsConnection) releaseContext(state *ttsContextState, sendClose bool) {
	if state == nil {
		return
	}
	if sendClose {
		closeCtx, cancel := context.WithTimeout(context.Background(), min(c.opts.readWriteTimeout, 500*time.Millisecond))
		_ = c.sendCloseContext(closeCtx, state)
		cancel()
	}
	c.mu.Lock()
	_, exists := c.contexts[state.id]
	if exists {
		delete(c.contexts, state.id)
	}
	retire := c.retired && len(c.contexts) == 0
	c.mu.Unlock()
	if exists {
		<-c.slots
	}
	if retire {
		select {
		case c.retireCh <- struct{}{}:
		default:
		}
	}
}

func (c *ttsConnection) markRetired() {
	c.mu.Lock()
	c.retired = true
	ready := len(c.contexts) == 0
	c.mu.Unlock()
	if ready {
		select {
		case c.retireCh <- struct{}{}:
		default:
		}
	}
}

func (c *ttsConnection) requestClose() {
	c.stopOnce.Do(func() { close(c.stop) })
}

func (c *ttsConnection) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-c.done:
		return nil
	}
}

func (c *ttsConnection) writeLoop() error {
	for {
		select {
		case <-c.ctx.Done():
			return nil
		case command := <-c.commands:
			err := c.writeCommand(command)
			command.ack <- err
			if err != nil {
				return err
			}
		}
	}
}

func (c *ttsConnection) writeCommand(command ttsCommand) error {
	switch command.kind {
	case ttsContentCommand:
		if command.init {
			settings := any(map[string]any{})
			if c.opts.voiceSettings != nil {
				settings = c.opts.voiceSettings
			}
			packet := map[string]any{
				"text": " ", "voice_settings": settings, "context_id": command.state.id,
			}
			if len(c.opts.chunkLengthSchedule) != 0 {
				packet["generation_config"] = map[string]any{"chunk_length_schedule": c.opts.chunkLengthSchedule}
			}
			if len(c.opts.pronunciationDictionaries) != 0 {
				packet["pronunciation_dictionary_locators"] = c.opts.pronunciationDictionaries
			}
			if err := writeJSON(c.ctx, c.ws, c.opts.readWriteTimeout, packet); err != nil {
				return err
			}
		}
		packet := map[string]any{"text": command.text, "context_id": command.state.id}
		if command.flush {
			packet["flush"] = true
		}
		return writeJSON(c.ctx, c.ws, c.opts.readWriteTimeout, packet)
	case ttsCloseContextCommand:
		return writeJSON(c.ctx, c.ws, c.opts.readWriteTimeout, map[string]any{
			"context_id": command.state.id, "close_context": true,
		})
	case ttsCloseSocketCommand:
		return writeJSON(c.ctx, c.ws, c.opts.readWriteTimeout, map[string]any{"close_socket": true})
	default:
		return &ProtocolError{Message: "unknown TTS writer command"}
	}
}

func (c *ttsConnection) readLoop() error {
	for {
		messageType, payload, err := c.ws.ReadMessage()
		if err != nil {
			if c.ctx.Err() != nil {
				return nil
			}
			return websocketReadError(err, "ElevenLabs TTS websocket closed")
		}
		if messageType != websocket.TextMessage {
			return &ProtocolError{Message: "TTS returned a non-text websocket frame"}
		}
		event, err := decodeTTSProviderEvent(payload)
		if err != nil {
			return err
		}
		if event.ProviderError != nil && event.ContextID == "" {
			return event.ProviderError
		}
		c.mu.Lock()
		state := c.contexts[event.ContextID]
		c.mu.Unlock()
		if state == nil {
			if event.Type == "flush_done" {
				continue
			}
			// Late final/error frames are legal after client-side interruption.
			continue
		}
		if event.ProviderError != nil {
			state.fail(event.ProviderError)
			continue
		}
		select {
		case <-c.ctx.Done():
			return nil
		case <-state.ctx.Done():
			continue
		case state.events <- ttsProviderResult{event: event}:
		}
	}
}

func (c *ttsConnection) supervise(workerErrors <-chan error) {
	var cause error
	graceful := false
	select {
	case cause = <-workerErrors:
	case <-c.stop:
		graceful = true
		cause = io.ErrClosedPipe
	case <-c.retireCh:
		graceful = true
		cause = io.EOF
	}
	if graceful {
		closeCtx, cancel := context.WithTimeout(context.Background(), min(c.opts.readWriteTimeout, time.Second))
		_ = c.sendCommand(closeCtx, ttsCommand{kind: ttsCloseSocketCommand, ack: make(chan error, 1)})
		cancel()
	}
	if cause == nil {
		cause = agents.NewAPIConnectionError("ElevenLabs TTS worker stopped", true, nil)
	}
	c.cancel(cause)
	_ = c.ws.Close()
	c.workers.Wait()
	c.failAll(cause)
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	close(c.done)
	c.owner.removeConnection(c)
}

func (c *ttsConnection) failAll(err error) {
	c.mu.Lock()
	states := make([]*ttsContextState, 0, len(c.contexts))
	for _, state := range c.contexts {
		states = append(states, state)
	}
	count := len(states)
	clear(c.contexts)
	c.mu.Unlock()
	for range count {
		<-c.slots
	}
	for _, state := range states {
		state.fail(err)
	}
}

func multiContextTTSURL(opts resolvedTTSOptions) string {
	u := websocketURL(opts.baseURL, "text-to-speech", opts.voiceID, "multi-stream-input")
	query := u.Query()
	query.Set("model_id", string(opts.model))
	query.Set("output_format", string(opts.encoding))
	if opts.language != "" {
		query.Set("language_code", agents.BaseLanguage(string(opts.language)))
	}
	query.Set("enable_ssml_parsing", boolString(opts.enableSSMLParsing))
	query.Set("enable_logging", boolString(opts.enableLogging))
	query.Set("inactivity_timeout", fmt.Sprintf("%g", opts.inactivityTimeout.Seconds()))
	query.Set("apply_text_normalization", string(opts.applyTextNormalization))
	if opts.applyLanguageTextNormalization != nil {
		query.Set("apply_language_text_normalization", boolString(*opts.applyLanguageTextNormalization))
	}
	if opts.syncAlignment {
		query.Set("sync_alignment", "true")
	}
	query.Set("auto_mode", boolString(opts.autoMode))
	u.RawQuery = query.Encode()
	return u.String()
}
