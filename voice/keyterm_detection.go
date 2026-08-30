// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/inference"
	"github.com/infinityscroll/livekit-agents-go/llm"
	"github.com/infinityscroll/livekit-agents-go/metrics"
	"github.com/infinityscroll/livekit-agents-go/stt"
)

const (
	DefaultKeytermDetectionTimeout = 10 * time.Second
	DefaultKeytermDetectionModel   = "google/gemma-4-31b-it"
	KeytermPendingTTL              = 3
	MaxKeytermTranscriptMessages   = 12
)

var (
	ErrKeytermDetectorClosed = errors.New("voice keyterm detector is closed")
	keytermToolOnce          sync.Once
	keytermToolContext       *llm.Context
	keytermToolErr           error
)

// KeytermsOptions configures static STT biasing and optional background
// extraction. Its zero value leaves both features disabled.
type KeytermsOptions struct {
	Keyterms         []string
	KeytermDetection KeytermDetectionOptions
}

// KeytermDetectionOptions mirrors the TypeScript/Python keyterm detector while
// keeping model selection unambiguous in Go. LLM and LLMModel are mutually
// exclusive. A zero Timeout selects DefaultKeytermDetectionTimeout.
type KeytermDetectionOptions struct {
	Enabled      bool
	LLM          llm.LLM
	LLMModel     string
	TurnInterval int
	MaxKeyterms  *int
	Instructions string
	Timeout      time.Duration
}

type ResolvedKeytermDetectionOptions struct {
	Enabled      bool
	LLM          llm.LLM
	LLMModel     string
	TurnInterval int
	MaxKeyterms  *int
	Instructions string
	Timeout      time.Duration
}

type ResolvedKeytermsOptions struct {
	Keyterms         []string
	KeytermDetection ResolvedKeytermDetectionOptions
}

// ResolveKeytermDetectionOptions validates and defaults detection options.
func ResolveKeytermDetectionOptions(options KeytermDetectionOptions) (ResolvedKeytermDetectionOptions, error) {
	if options.LLM != nil && options.LLMModel != "" {
		return ResolvedKeytermDetectionOptions{}, errors.New("keyterm detection accepts either LLM or LLMModel, not both")
	}
	if options.TurnInterval <= 0 {
		options.TurnInterval = 1
	}
	if options.Timeout == 0 {
		options.Timeout = DefaultKeytermDetectionTimeout
	}
	if options.Timeout < 0 {
		return ResolvedKeytermDetectionOptions{}, errors.New("keyterm detection timeout must not be negative")
	}
	if options.MaxKeyterms != nil && *options.MaxKeyterms < 0 {
		return ResolvedKeytermDetectionOptions{}, errors.New("keyterm detection max keyterms must not be negative")
	}
	model := options.LLMModel
	if model == "" {
		model = DefaultKeytermDetectionModel
	}
	return ResolvedKeytermDetectionOptions{
		Enabled: options.Enabled, LLM: options.LLM, LLMModel: model,
		TurnInterval: options.TurnInterval, MaxKeyterms: cloneInt(options.MaxKeyterms),
		Instructions: options.Instructions, Timeout: options.Timeout,
	}, nil
}

func ResolveKeytermsOptions(options KeytermsOptions) (ResolvedKeytermsOptions, error) {
	detection, err := ResolveKeytermDetectionOptions(options.KeytermDetection)
	if err != nil {
		return ResolvedKeytermsOptions{}, err
	}
	return ResolvedKeytermsOptions{Keyterms: dedupeKeyterms(options.Keyterms), KeytermDetection: detection}, nil
}

func cloneInt(value *int) *int {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

// KeytermState is supplied to the extraction prompt. Applied entries bias the
// recognizer; candidates remain pending until a later pass confirms them.
type KeytermState struct {
	Term    string
	Applied bool
}

// KeytermDetectionResult contains changes requested by record_keyterms.
type KeytermDetectionResult struct {
	Pending []string
	Confirm []string
	Remove  []string
}

// KeytermDetectorSession is intentionally small so AgentSession and test
// harnesses can both drive the detector without adapters.
type KeytermDetectorSession interface {
	ChatContext() *llm.ChatContext
	OnEvent(func(Event), EventSubscriptionOptions) (func(), error)
}

type keytermActivity struct {
	ctx          context.Context
	cancel       context.CancelCauseFunc
	trigger      chan *llm.ChatContext
	done         chan struct{}
	busy         atomic.Bool
	unsubscribe  func()
	unsubMetrics func()
	turns        atomic.Uint64
}

// KeytermDetector owns keyterm state for a whole session. Start and Pause bind
// individual agent activities while confirmed state survives handoffs.
type KeytermDetector struct {
	options ResolvedKeytermDetectionOptions

	mu       sync.RWMutex
	static   []string
	detected []string
	pending  map[string]int
	tick     int
	boundSTT stt.STT
	model    llm.LLM
	ownedLLM bool
	active   *keytermActivity
	closed   bool

	runMu   sync.Mutex
	metrics agents.EventEmitter[metrics.LLM]
}

func NewKeytermDetector(options KeytermsOptions) (*KeytermDetector, error) {
	resolved, err := ResolveKeytermsOptions(options)
	if err != nil {
		return nil, err
	}
	return &KeytermDetector{
		options: resolved.KeytermDetection, static: resolved.Keyterms,
		pending: make(map[string]int), model: resolved.KeytermDetection.LLM,
	}, nil
}

func (d *KeytermDetector) OnMetrics(fn func(metrics.LLM)) func() {
	return d.metrics.Subscribe(fn)
}

func (d *KeytermDetector) Keyterms() []string {
	d.mu.RLock()
	result := mergeKeyterms(d.static, d.detected)
	d.mu.RUnlock()
	return result
}

func (d *KeytermDetector) StaticKeyterms() []string {
	d.mu.RLock()
	result := slices.Clone(d.static)
	d.mu.RUnlock()
	return result
}

func (d *KeytermDetector) DetectedKeyterms() []string {
	d.mu.RLock()
	result := slices.Clone(d.detected)
	d.mu.RUnlock()
	return result
}

func (d *KeytermDetector) PendingKeyterms() []string {
	d.mu.RLock()
	result := make([]string, 0, len(d.pending))
	for term := range d.pending {
		result = append(result, term)
	}
	d.mu.RUnlock()
	slices.Sort(result)
	return result
}

func (d *KeytermDetector) SetStaticKeyterms(terms []string) error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return ErrKeytermDetectorClosed
	}
	d.static = dedupeKeyterms(terms)
	speech := d.boundSTT
	effective := mergeKeyterms(d.static, d.detected)
	d.mu.Unlock()
	return pushSessionKeyterms(speech, effective)
}

// SwapSTT binds the current effective set to a recognizer. Rebinding the same
// instance is a no-op; a new keyterm-capable instance receives an empty set too,
// clearing state left by a prior session.
func (d *KeytermDetector) SwapSTT(speech stt.STT) error {
	// Interfaces whose dynamic value is a nil pointer are not equal to nil and
	// invoking Capabilities on one would panic. Normalize them before identity
	// comparison so custom STTs can safely use pointer receivers.
	if isNilInterface(speech) {
		speech = nil
	}
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return ErrKeytermDetectorClosed
	}
	if sameInterface(speech, d.boundSTT) {
		d.mu.Unlock()
		return nil
	}
	d.boundSTT = speech
	effective := mergeKeyterms(d.static, d.detected)
	d.mu.Unlock()
	if speech == nil || len(effective) == 0 && !speech.Capabilities().Keyterms {
		return nil
	}
	return pushSessionKeyterms(speech, effective)
}

// Start binds the current activity. Only one activity can be active; callers
// must Pause the previous activity first. Detection is skipped when disabled or
// when the recognizer cannot consume keyterms.
func (d *KeytermDetector) Start(parent context.Context, session KeytermDetectorSession, speech stt.STT) error {
	if parent == nil {
		parent = context.Background()
	}
	if session == nil || speech == nil {
		return errors.New("keyterm detector start requires a session and STT")
	}
	d.mu.RLock()
	active, closed := d.active != nil, d.closed
	d.mu.RUnlock()
	if closed {
		return ErrKeytermDetectorClosed
	}
	if active {
		return errors.New("keyterm detector activity is already started")
	}
	if err := d.SwapSTT(speech); err != nil {
		return err
	}
	if !d.options.Enabled {
		return nil
	}
	if !speech.Capabilities().Keyterms {
		slog.Warn("keyterm detection is enabled but the STT does not support keyterms; skipping", "stt", speech.Label())
		return nil
	}
	model, err := d.detectionLLM()
	if err != nil {
		return fmt.Errorf("resolve keyterm detection LLM: %w", err)
	}
	ctx, cancel := context.WithCancelCause(parent)
	activity := &keytermActivity{ctx: ctx, cancel: cancel, trigger: make(chan *llm.ChatContext, 1), done: make(chan struct{})}
	activity.unsubMetrics = model.OnMetrics(func(value metrics.LLM) { d.metrics.Emit(value) })
	unsubscribe, err := session.OnEvent(func(event Event) {
		added, ok := event.(ConversationItemAddedEvent)
		if !ok {
			if pointer, pointerOK := event.(*ConversationItemAddedEvent); pointerOK && pointer != nil {
				added = *pointer
				ok = true
			}
		}
		if !ok {
			return
		}
		message, ok := added.Item.(*llm.ChatMessage)
		if !ok || message.Role != llm.RoleUser {
			return
		}
		text, hasText := message.TextContent()
		if !hasText || strings.TrimSpace(text) == "" {
			return
		}
		turn := activity.turns.Add(1)
		if turn%uint64(d.options.TurnInterval) != 0 || !activity.busy.CompareAndSwap(false, true) {
			return
		}
		snapshot := session.ChatContext().Copy(llm.CopyOptions{
			ExcludeConfigUpdate: true, ExcludeFunctionCall: true,
			ExcludeHandoff: true, ExcludeEmptyMessage: true,
		})
		select {
		case activity.trigger <- snapshot:
		case <-activity.ctx.Done():
			activity.busy.Store(false)
		default:
			activity.busy.Store(false)
		}
	}, EventSubscriptionOptions{Capacity: 8})
	if err != nil {
		activity.unsubMetrics()
		cancel(err)
		return fmt.Errorf("subscribe keyterm detector: %w", err)
	}
	activity.unsubscribe = unsubscribe
	d.mu.Lock()
	if d.closed || d.active != nil {
		d.mu.Unlock()
		unsubscribe()
		activity.unsubMetrics()
		cancel(ErrKeytermDetectorClosed)
		if d.closed {
			return ErrKeytermDetectorClosed
		}
		return errors.New("keyterm detector activity is already started")
	}
	d.active = activity
	d.mu.Unlock()
	go d.runActivity(activity)
	return nil
}

func (d *KeytermDetector) runActivity(activity *keytermActivity) {
	defer close(activity.done)
	for {
		select {
		case <-activity.ctx.Done():
			return
		case snapshot := <-activity.trigger:
			err := d.RunOnce(activity.ctx, snapshot)
			activity.busy.Store(false)
			if err != nil && context.Cause(activity.ctx) == nil {
				slog.Error("keyterm detection pass failed", "error", err)
			}
		}
	}
}

func (d *KeytermDetector) detectionLLM() (llm.LLM, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.model != nil {
		return d.model, nil
	}
	model, err := inference.LLMFromModelString(d.options.LLMModel)
	if err != nil {
		return nil, err
	}
	d.model, d.ownedLLM = model, true
	return model, nil
}

// Pause detaches the current activity without discarding detected state.
func (d *KeytermDetector) Pause(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	d.mu.Lock()
	activity := d.active
	d.active = nil
	d.mu.Unlock()
	if activity == nil {
		return nil
	}
	activity.unsubscribe()
	activity.unsubMetrics()
	activity.cancel(context.Canceled)
	select {
	case <-activity.done:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// Close permanently stops detection and closes a lazily-created inference LLM.
// Caller-provided LLM instances remain caller-owned.
func (d *KeytermDetector) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	d.closed = true
	activity := d.active
	d.active = nil
	model, owned := d.model, d.ownedLLM
	d.mu.Unlock()
	var errs []error
	if activity != nil {
		activity.unsubscribe()
		activity.unsubMetrics()
		activity.cancel(context.Canceled)
		select {
		case <-activity.done:
		case <-ctx.Done():
			errs = append(errs, context.Cause(ctx))
		}
	}
	if owned && model != nil {
		if err := model.Close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("close keyterm detection LLM: %w", err))
		}
	}
	return errors.Join(errs...)
}

// RunOnce executes one extraction pass and applies confirmed state atomically.
func (d *KeytermDetector) RunOnce(ctx context.Context, chat *llm.ChatContext) error {
	d.runMu.Lock()
	defer d.runMu.Unlock()
	if ctx == nil {
		ctx = context.Background()
	}
	if err := context.Cause(ctx); err != nil {
		return nil
	}
	d.mu.RLock()
	if d.closed {
		d.mu.RUnlock()
		return ErrKeytermDetectorClosed
	}
	model := d.model
	current := make([]KeytermState, 0, len(d.static)+len(d.detected)+len(d.pending))
	for _, term := range d.static {
		current = append(current, KeytermState{Term: term, Applied: true})
	}
	for _, term := range d.detected {
		current = append(current, KeytermState{Term: term, Applied: true})
	}
	for term := range d.pending {
		current = append(current, KeytermState{Term: term})
	}
	instructions, timeout := d.options.Instructions, d.options.Timeout
	d.mu.RUnlock()
	if model == nil {
		var err error
		model, err = d.detectionLLM()
		if err != nil {
			return err
		}
	}
	passCtx, cancel := context.WithTimeout(ctx, timeout)
	result, err := DetectKeyterms(passCtx, model, chat, DetectKeytermsOptions{Instructions: instructions, CurrentKeyterms: current})
	cancel()
	if err != nil {
		if context.Cause(ctx) != nil || errors.Is(err, context.DeadlineExceeded) {
			return nil
		}
		return err
	}
	if context.Cause(ctx) != nil {
		return nil
	}

	d.mu.Lock()
	if d.closed || context.Cause(ctx) != nil {
		d.mu.Unlock()
		return nil
	}
	before := mergeKeyterms(d.static, d.detected)
	d.tick++
	for _, term := range result.Remove {
		delete(d.pending, term)
		if index := slices.Index(d.detected, term); index >= 0 {
			d.detected = slices.Delete(d.detected, index, index+1)
		}
	}
	for _, term := range result.Pending {
		if term == "" || slices.Contains(d.static, term) || slices.Contains(d.detected, term) {
			continue
		}
		if _, exists := d.pending[term]; !exists {
			d.pending[term] = d.tick
		}
	}
	for _, term := range result.Confirm {
		if term == "" || slices.Contains(d.static, term) {
			continue
		}
		delete(d.pending, term)
		if !slices.Contains(d.detected, term) {
			d.detected = append(d.detected, term)
		}
	}
	for term, addedAt := range d.pending {
		if d.tick-addedAt >= KeytermPendingTTL {
			delete(d.pending, term)
		}
	}
	if d.options.MaxKeyterms != nil && len(d.detected) > *d.options.MaxKeyterms {
		d.detected = slices.Clone(d.detected[len(d.detected)-*d.options.MaxKeyterms:])
	}
	after := mergeKeyterms(d.static, d.detected)
	speech := d.boundSTT
	d.mu.Unlock()
	if slices.Equal(before, after) {
		return nil
	}
	return pushSessionKeyterms(speech, after)
}

type DetectKeytermsOptions struct {
	Instructions    string
	CurrentKeyterms []KeytermState
}

// DetectKeyterms forces one record_keyterms tool call and parses its changes.
func DetectKeyterms(ctx context.Context, model llm.LLM, chat *llm.ChatContext, options DetectKeytermsOptions) (KeytermDetectionResult, error) {
	if model == nil {
		return KeytermDetectionResult{}, errors.New("keyterm detection LLM is required")
	}
	input, ok := FormatKeytermDetectionInput(chat, options.CurrentKeyterms)
	if !ok {
		return KeytermDetectionResult{}, nil
	}
	instructions := options.Instructions
	if instructions == "" {
		instructions = DefaultKeytermDetectionInstructions
	}
	request := llm.EmptyChatContext()
	if _, err := request.AddMessage(llm.RoleSystem, instructions); err != nil {
		return KeytermDetectionResult{}, err
	}
	if _, err := request.AddMessage(llm.RoleUser, input); err != nil {
		return KeytermDetectionResult{}, err
	}
	toolContext, err := recordKeytermsToolContext()
	if err != nil {
		return KeytermDetectionResult{}, err
	}
	output, err := model.Chat(ctx, llm.ChatOptions{
		ChatContext: request, ToolContext: toolContext,
		ToolChoice: llm.ToolChoice{Kind: llm.ToolChoiceRequired},
	})
	if err != nil {
		return KeytermDetectionResult{}, err
	}
	defer output.Close()
	collected, err := output.Collect(ctx)
	if err != nil {
		return KeytermDetectionResult{}, err
	}
	return ParseKeytermToolCalls(collected.ToolCalls), nil
}

func recordKeytermsToolContext() (*llm.Context, error) {
	keytermToolOnce.Do(func() {
		type input struct {
			Pending []string `json:"pending"`
			Confirm []string `json:"confirm"`
			Remove  []string `json:"remove"`
		}
		tool, err := llm.NewTool(llm.FunctionToolOptions[input, struct{}]{
			Name: "record_keyterms", Description: "Update the STT keyterms based on the latest transcript.",
			Parameters: json.RawMessage(`{"type":"object","properties":{"pending":{"type":"array","items":{"type":"string"}},"confirm":{"type":"array","items":{"type":"string"}},"remove":{"type":"array","items":{"type":"string"}}},"required":["pending","confirm","remove"],"additionalProperties":false}`),
			Execute:    func(context.Context, input, llm.ToolOptions) (struct{}, error) { return struct{}{}, nil },
		})
		if err != nil {
			keytermToolErr = err
			return
		}
		keytermToolContext, keytermToolErr = llm.NewToolContext(tool)
	})
	return keytermToolContext, keytermToolErr
}

func ParseKeytermToolCalls(calls []*llm.FunctionCall) KeytermDetectionResult {
	for _, call := range calls {
		if call == nil || call.Name != "record_keyterms" {
			continue
		}
		var wire struct {
			Pending []any `json:"pending"`
			Confirm []any `json:"confirm"`
			Remove  []any `json:"remove"`
		}
		if err := json.Unmarshal([]byte(call.Arguments), &wire); err != nil {
			return KeytermDetectionResult{}
		}
		return KeytermDetectionResult{Pending: cleanTerms(wire.Pending), Confirm: cleanTerms(wire.Confirm), Remove: cleanTerms(wire.Remove)}
	}
	return KeytermDetectionResult{}
}

func cleanTerms(values []any) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		term, ok := value.(string)
		if ok && strings.TrimSpace(term) != "" {
			result = append(result, term)
		}
	}
	return result
}

// FormatKeytermDetectionInput renders at most the newest twelve transcript
// messages, then restores chronological order. It excludes blank lines inside
// turns so blank lines unambiguously delimit turns.
func FormatKeytermDetectionInput(chat *llm.ChatContext, current []KeytermState) (string, bool) {
	if chat == nil {
		return "", false
	}
	items := chat.Items()
	turns := make([]string, 0, min(MaxKeytermTranscriptMessages, len(items)))
	for index := len(items) - 1; index >= 0 && len(turns) < MaxKeytermTranscriptMessages; index-- {
		message, ok := items[index].(*llm.ChatMessage)
		if !ok || message.Role != llm.RoleUser && message.Role != llm.RoleAssistant {
			continue
		}
		text, hasText := message.TextContent()
		if !hasText || strings.TrimSpace(text) == "" {
			continue
		}
		lines := strings.Split(text, "\n")
		lines = slices.DeleteFunc(lines, func(line string) bool { return strings.TrimSpace(line) == "" })
		turns = append(turns, strings.ToUpper(string(message.Role))+": "+strings.Join(lines, "\n"))
	}
	if len(turns) == 0 {
		return "", false
	}
	slices.Reverse(turns)
	applied := make([]string, 0, len(current))
	candidates := make([]string, 0, len(current))
	for _, entry := range current {
		if entry.Applied {
			applied = append(applied, entry.Term)
		} else {
			candidates = append(candidates, entry.Term)
		}
	}
	if len(applied) == 0 {
		applied = append(applied, "(none)")
	}
	if len(candidates) == 0 {
		candidates = append(candidates, "(none)")
	}
	return "## Transcript (USER = raw STT, may be wrong; ASSISTANT = correct spelling)\n" + strings.Join(turns, "\n\n") +
		"\n\n## Applied keyterms (biasing the recognizer now)\n" + strings.Join(applied, ", ") +
		"\n\n## Candidate keyterms (seen, not yet applied)\n" + strings.Join(candidates, ", ") +
		"\n\nUpdate the keyterms from the latest turns, then call `record_keyterms` once.", true
}

func pushSessionKeyterms(speech stt.STT, terms []string) error {
	if speech == nil {
		return nil
	}
	updater, ok := speech.(stt.SessionKeytermUpdater)
	if !ok {
		if len(terms) != 0 || speech.Capabilities().Keyterms {
			slog.Warn("STT cannot accept session keyterm updates; skipping", "stt", speech.Label())
		}
		return nil
	}
	return updater.UpdateSessionKeyterms(slices.Clone(terms))
}

func dedupeKeyterms(terms []string) []string {
	result := make([]string, 0, len(terms))
	seen := make(map[string]struct{}, len(terms))
	for _, term := range terms {
		if _, exists := seen[term]; exists {
			continue
		}
		seen[term] = struct{}{}
		result = append(result, term)
	}
	return result
}

func mergeKeyterms(static, detected []string) []string {
	result := make([]string, 0, len(static)+len(detected))
	result = append(result, static...)
	seen := make(map[string]struct{}, len(static)+len(detected))
	for _, term := range static {
		seen[term] = struct{}{}
	}
	for _, term := range detected {
		if _, exists := seen[term]; !exists {
			seen[term] = struct{}{}
			result = append(result, term)
		}
	}
	return result
}

const DefaultKeytermDetectionInstructions = `You maintain STT keyterms that bias a recognizer toward the correct spelling of distinctive words (names, places, companies, products, technical terms). Each turn, adjust them with one record_keyterms call.

A WRONG spelling biases the recognizer for the rest of the call with no recovery, so precision beats coverage: apply only a spelling you can CORROBORATE, and when unsure change nothing.

USER lines are raw STT — often wrong, and the same error recurs, so repetition is NOT proof a spelling is right. ASSISTANT lines are the agent's own writing: trust the agent's confident use of its OWN names (brands, staff, locations) and confirm those promptly — but an assistant merely echoing the user's sounds, or hedging about a spelling, does NOT corroborate.

CONFIRM a pending term only when corroborated by one of:
  1. a letter-by-letter spell-out the assistant then accepts WITHOUT reservation — confirm exactly those letters, appending nothing;
  2. the assistant's own confident use of that exact distinctive spelling;
  3. an explicit user correction ("no, not X — it's Y").
Recurrence alone never confirms.

HEDGE RULE: if after a spell-out or name read-back the assistant signals the letters may be off ("for now", "with that caveat", "may have that slightly off", "did I catch that?", "to be confirmed", "I don't want to guess", "double-check"), the spelling is unreliable — keep the term PENDING and never confirm it, EVEN IF the user replies "yes". Only a cleanly accepted spell-out confirms.

Never apply: a user-line word that sounds like a known term (it's that term misheard); a distinctive name glued to an ordinary word ("Blue Haven Hotel" — keep the bare name pending); an odd phrase only the user says and the assistant never adopts; a fragment left by an interruption; ordinary words or fillers.

Report only CHANGES; never re-list an applied term.
  - ` + "`pending`" + `: a distinctive term seen but not yet corroborated;
  - ` + "`confirm`" + `: a pending term that just met the bar above;
  - ` + "`remove`" + `: only a spelling the user just corrected away. Applied terms are otherwise sticky.
If nothing meets the bar this turn, change nothing.`
