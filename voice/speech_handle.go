// SPDX-License-Identifier: Apache-2.0

// Package voice implements LiveKit's agent-session state machine and media
// pipeline.
package voice

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/llm"
)

const (
	SpeechPriorityLow    = 0
	SpeechPriorityNormal = 5
	SpeechPriorityHigh   = 10

	ReplyTaskCancelTimeout = 2 * time.Second
	InterruptionTimeout    = 5 * time.Second
)

var (
	ErrSpeechNotDone          = errors.New("speech handle is not done")
	ErrInterruptionsDisabled  = errors.New("speech handle does not allow interruptions")
	ErrNoActiveGeneration     = errors.New("speech handle has no active generation")
	ErrInvalidGenerationIndex = errors.New("speech generation index is invalid")
)

type InputModality string

const (
	InputModalityAudio InputModality = "audio"
	InputModalityText  InputModality = "text"
)

type InputDetails struct {
	Modality InputModality
}

var DefaultInputDetails = InputDetails{Modality: InputModalityAudio}

type SpeechHandleCircularWaitError struct {
	FunctionName string
}

func (e *SpeechHandleCircularWaitError) Error() string {
	return fmt.Sprintf("cannot wait for speech playout from blocking tool %q that owns the same speech handle: this would create a circular wait", e.FunctionName)
}

type SpeechHandleOptions struct {
	AllowInterruptions bool
	// AllowInterruptionsSet distinguishes an explicit false from the default true.
	AllowInterruptionsSet bool
	StepIndex             int
	InputDetails          InputDetails
	Parent                *SpeechHandle
	// InterruptTimeout exists for deterministic tests and specialized runtimes.
	// Zero uses the cross-SDK five-second watchdog.
	InterruptTimeout time.Duration
}

type SpeechHandle struct {
	id      string
	parent  *SpeechHandle
	input   InputDetails
	step    int
	timeout time.Duration

	mu                   sync.RWMutex
	allowInterruptions   bool
	interruptionHolds    int
	interruptionsRestore bool
	interrupted          bool
	scheduled            bool
	done                 bool
	err                  error
	numSteps             int
	chatItems            []llm.ChatItem
	generations          []chan struct{}
	authorized           bool
	authorizationChanged chan struct{}
	scheduledChanged     chan struct{}
	interrupt            chan struct{}
	completion           chan struct{}
	watchdog             *time.Timer
	nextOwnedID          uint64
	cancelOwned          map[uint64]context.CancelCauseFunc
	nextCallbackID       uint64
	doneCallbacks        map[uint64]func(*SpeechHandle)
	itemCallbacks        map[uint64]func(llm.ChatItem)
}

func NewSpeechHandle(options SpeechHandleOptions) *SpeechHandle {
	allow := true
	if options.AllowInterruptionsSet {
		allow = options.AllowInterruptions
	}
	input := options.InputDetails
	if input.Modality == "" {
		input = DefaultInputDetails
	}
	timeout := options.InterruptTimeout
	if timeout <= 0 {
		timeout = InterruptionTimeout
	}
	return &SpeechHandle{
		id: agents.ShortUUID("speech_"), parent: options.Parent, input: input,
		step: options.StepIndex, timeout: timeout, allowInterruptions: allow,
		interruptionsRestore: allow, numSteps: 1,
		authorizationChanged: make(chan struct{}), scheduledChanged: make(chan struct{}),
		interrupt: make(chan struct{}), completion: make(chan struct{}),
		doneCallbacks: make(map[uint64]func(*SpeechHandle)), itemCallbacks: make(map[uint64]func(llm.ChatItem)),
		cancelOwned: make(map[uint64]context.CancelCauseFunc),
	}
}

func (h *SpeechHandle) ID() string                       { return h.id }
func (h *SpeechHandle) Parent() *SpeechHandle            { return h.parent }
func (h *SpeechHandle) InputDetails() InputDetails       { return h.input }
func (h *SpeechHandle) StepIndex() int                   { return h.step }
func (h *SpeechHandle) InterruptSignal() <-chan struct{} { return h.interrupt }

func (h *SpeechHandle) Interrupted() bool {
	h.mu.RLock()
	value := h.interrupted
	h.mu.RUnlock()
	return value
}
func (h *SpeechHandle) Scheduled() bool {
	h.mu.RLock()
	value := h.scheduled
	h.mu.RUnlock()
	return value
}
func (h *SpeechHandle) Done() bool { h.mu.RLock(); value := h.done; h.mu.RUnlock(); return value }
func (h *SpeechHandle) NumSteps() int {
	h.mu.RLock()
	value := h.numSteps
	h.mu.RUnlock()
	return value
}

func (h *SpeechHandle) IncrementSteps() int {
	h.mu.Lock()
	h.numSteps++
	steps := h.numSteps
	h.mu.Unlock()
	return steps
}

func (h *SpeechHandle) AllowInterruptions() bool {
	h.mu.RLock()
	value := h.allowInterruptions
	h.mu.RUnlock()
	return value
}

func (h *SpeechHandle) SetAllowInterruptions(allow bool) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.interrupted && !allow {
		return errors.New("cannot disable interruptions after speech was interrupted")
	}
	h.allowInterruptions = allow
	return nil
}

// HoldInterruptions temporarily protects this handle. The returned release
// function is idempotent and supports nested holds.
func (h *SpeechHandle) HoldInterruptions() func() {
	h.mu.Lock()
	if h.interruptionHolds == 0 {
		h.interruptionsRestore = h.allowInterruptions
		h.allowInterruptions = false
	}
	h.interruptionHolds++
	h.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			h.mu.Lock()
			if h.interruptionHolds > 0 {
				h.interruptionHolds--
			}
			if h.interruptionHolds == 0 && !h.interrupted {
				h.allowInterruptions = h.interruptionsRestore
			}
			h.mu.Unlock()
		})
	}
}

func (h *SpeechHandle) Error() error {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if !h.done {
		return ErrSpeechNotDone
	}
	return h.err
}

func (h *SpeechHandle) ChatItems() []llm.ChatItem {
	h.mu.RLock()
	items := make([]llm.ChatItem, len(h.chatItems))
	copy(items, h.chatItems)
	h.mu.RUnlock()
	return items
}

func (h *SpeechHandle) Interrupt(force bool) error {
	h.mu.Lock()
	if h.interrupted || h.done {
		h.mu.Unlock()
		return nil
	}
	if !force && !h.allowInterruptions {
		h.mu.Unlock()
		return ErrInterruptionsDisabled
	}
	h.interrupted = true
	close(h.interrupt)
	timeout := h.timeout
	h.watchdog = time.AfterFunc(timeout, h.interruptionWatchdog)
	h.mu.Unlock()
	return nil
}

func (h *SpeechHandle) interruptionWatchdog() {
	h.mu.Lock()
	if h.done {
		h.mu.Unlock()
		return
	}
	cancel := make([]context.CancelCauseFunc, 0, len(h.cancelOwned))
	for _, fn := range h.cancelOwned {
		cancel = append(cancel, fn)
	}
	h.mu.Unlock()
	for _, fn := range cancel {
		fn(context.DeadlineExceeded)
	}
	h.MarkDone(nil)
}

// OwnContext registers a child operation which the interruption watchdog can
// force-cancel. The unregister function should be deferred by its owner.
func (h *SpeechHandle) OwnContext(parent context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancelCause(parent)
	h.mu.Lock()
	if h.done {
		h.mu.Unlock()
		cancel(ErrSpeechNotDone)
		return ctx, func() {}
	}
	id := h.nextOwnedID
	h.nextOwnedID++
	h.cancelOwned[id] = cancel
	h.mu.Unlock()
	var once sync.Once
	return ctx, func() {
		once.Do(func() {
			h.mu.Lock()
			delete(h.cancelOwned, id)
			h.mu.Unlock()
		})
	}
}

type functionCallWaitContext struct {
	handle      *SpeechHandle
	name        string
	nonBlocking bool
}

type functionCallWaitContextKey struct{}

// WithFunctionCallContext marks a blocking tool execution for circular-wait
// detection. Tool runners should wrap user callbacks with this context.
func WithFunctionCallContext(ctx context.Context, handle *SpeechHandle, name string, nonBlocking bool) context.Context {
	return context.WithValue(ctx, functionCallWaitContextKey{}, functionCallWaitContext{handle: handle, name: name, nonBlocking: nonBlocking})
}

func (h *SpeechHandle) Wait(ctx context.Context) error {
	if call, ok := ctx.Value(functionCallWaitContextKey{}).(functionCallWaitContext); ok && call.handle == h && !call.nonBlocking {
		return &SpeechHandleCircularWaitError{FunctionName: call.name}
	}
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-h.completion:
		return h.Error()
	}
}

func (h *SpeechHandle) WaitForPlayout(ctx context.Context) error { return h.Wait(ctx) }

func (h *SpeechHandle) AddDoneCallback(callback func(*SpeechHandle)) func() {
	if callback == nil {
		return func() {}
	}
	h.mu.Lock()
	if h.done {
		h.mu.Unlock()
		go invokeSpeechDoneCallback(callback, h)
		return func() {}
	}
	id := h.nextCallbackID
	h.nextCallbackID++
	h.doneCallbacks[id] = callback
	h.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() { h.mu.Lock(); delete(h.doneCallbacks, id); h.mu.Unlock() })
	}
}

func (h *SpeechHandle) AddItemCallback(callback func(llm.ChatItem)) func() {
	if callback == nil {
		return func() {}
	}
	h.mu.Lock()
	id := h.nextCallbackID
	h.nextCallbackID++
	h.itemCallbacks[id] = callback
	h.mu.Unlock()
	var once sync.Once
	return func() { once.Do(func() { h.mu.Lock(); delete(h.itemCallbacks, id); h.mu.Unlock() }) }
}

func (h *SpeechHandle) AddChatItems(items ...llm.ChatItem) {
	filtered := make([]llm.ChatItem, 0, len(items))
	for _, item := range items {
		if item != nil {
			filtered = append(filtered, item)
		}
	}
	if len(filtered) == 0 {
		return
	}
	h.mu.Lock()
	h.chatItems = append(h.chatItems, filtered...)
	callbacks := orderedItemCallbacks(h.itemCallbacks)
	h.mu.Unlock()
	for _, item := range filtered {
		for _, callback := range callbacks {
			invokeSpeechItemCallback(callback, item)
		}
	}
}

func (h *SpeechHandle) MarkScheduled() {
	h.mu.Lock()
	if !h.scheduled {
		h.scheduled = true
		close(h.scheduledChanged)
	}
	h.mu.Unlock()
}

func (h *SpeechHandle) WaitForScheduled(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	h.mu.RLock()
	if h.scheduled {
		h.mu.RUnlock()
		return nil
	}
	changed := h.scheduledChanged
	h.mu.RUnlock()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-changed:
		return nil
	}
}

func (h *SpeechHandle) AuthorizeGeneration() int {
	h.mu.Lock()
	generation := make(chan struct{})
	h.generations = append(h.generations, generation)
	index := len(h.generations) - 1
	if !h.authorized {
		h.authorized = true
		close(h.authorizationChanged)
	}
	h.mu.Unlock()
	return index
}

func (h *SpeechHandle) ClearAuthorization() {
	h.mu.Lock()
	if h.authorized {
		h.authorized = false
		h.authorizationChanged = make(chan struct{})
	}
	h.mu.Unlock()
}

func (h *SpeechHandle) WaitForAuthorization(ctx context.Context) error {
	for {
		h.mu.RLock()
		if h.authorized {
			h.mu.RUnlock()
			return nil
		}
		changed := h.authorizationChanged
		h.mu.RUnlock()
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-changed:
		}
	}
}

func (h *SpeechHandle) WaitForGeneration(ctx context.Context, index int) error {
	h.mu.RLock()
	if len(h.generations) == 0 {
		h.mu.RUnlock()
		return ErrNoActiveGeneration
	}
	if index < 0 {
		index = len(h.generations) - 1
	}
	if index >= len(h.generations) {
		h.mu.RUnlock()
		return ErrInvalidGenerationIndex
	}
	done := h.generations[index]
	h.mu.RUnlock()
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-done:
		return nil
	}
}

func (h *SpeechHandle) MarkGenerationDone() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.generations) == 0 {
		return ErrNoActiveGeneration
	}
	done := h.generations[len(h.generations)-1]
	select {
	case <-done:
	default:
		close(done)
	}
	return nil
}

func (h *SpeechHandle) MarkDone(err error) {
	h.mu.Lock()
	if !h.done {
		h.done = true
		h.err = err
		close(h.completion)
	}
	if len(h.generations) != 0 {
		done := h.generations[len(h.generations)-1]
		select {
		case <-done:
		default:
			close(done)
		}
	}
	if h.watchdog != nil {
		h.watchdog.Stop()
		h.watchdog = nil
	}
	callbacks := orderedDoneCallbacks(h.doneCallbacks)
	clear(h.doneCallbacks)
	h.mu.Unlock()
	if len(callbacks) != 0 {
		go func() {
			for _, callback := range callbacks {
				invokeSpeechDoneCallback(callback, h)
			}
		}()
	}
}

func invokeSpeechDoneCallback(callback func(*SpeechHandle), handle *SpeechHandle) {
	defer func() {
		if recovered := recover(); recovered != nil {
			slog.Error("speech completion callback panicked", "panic", recovered, "speech_id", handle.ID())
		}
	}()
	callback(handle)
}

func invokeSpeechItemCallback(callback func(llm.ChatItem), item llm.ChatItem) {
	defer func() {
		if recovered := recover(); recovered != nil {
			slog.Error("speech item callback panicked", "panic", recovered)
		}
	}()
	callback(item)
}

func orderedDoneCallbacks(input map[uint64]func(*SpeechHandle)) []func(*SpeechHandle) {
	ids := make([]uint64, 0, len(input))
	for id := range input {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	result := make([]func(*SpeechHandle), 0, len(ids))
	for _, id := range ids {
		result = append(result, input[id])
	}
	return result
}

func orderedItemCallbacks(input map[uint64]func(llm.ChatItem)) []func(llm.ChatItem) {
	ids := make([]uint64, 0, len(input))
	for id := range input {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	result := make([]func(llm.ChatItem), 0, len(ids))
	for _, id := range ids {
		result = append(result, input[id])
	}
	return result
}
