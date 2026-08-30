// SPDX-License-Identifier: Apache-2.0

package inference

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	agents "github.com/infinityscroll/livekit-agents-go"
)

const (
	DefaultAvatarSampleRate = 16000
	maxAvatarResponseBytes  = 1 << 20
)

var ErrAvatarSessionAlreadyStarted = errors.New("inference avatar session may only be started once")

// LemonSliceOptions contains the gateway's first-class LemonSlice fields.
// Additional provider options belong in AvatarSessionOptions.ExtraKwargs.
type LemonSliceOptions struct {
	ImageURL    string
	Prompt      string
	IdlePrompt  string
	IdleTimeout *time.Duration
}

// AvatarSessionOptions configures the low-level inference gateway lifecycle.
// Media routing is intentionally supplied by voice/avatar.InferenceSession to
// avoid a Go package cycle between inference and voice.
type AvatarSessionOptions struct {
	Model                     string
	AvatarParticipantIdentity string
	AvatarParticipantName     string
	LemonSlice                LemonSliceOptions
	ExtraKwargs               map[string]any
	BaseURL                   string
	Credentials               Credentials
	HTTPClient                *http.Client
	ConnectOptions            agents.APIConnectOptions
	Metadata                  RequestMetadata
	IdempotencyKey            string
}

// AvatarSessionStartOptions are the room values used by the gateway to mint
// the avatar worker token. All values are required.
type AvatarSessionStartOptions struct {
	LiveKitURL    string
	RoomName      string
	RoomSID       string
	AgentIdentity string
}

// AvatarSessionInfo is the authoritative gateway response retained by a
// successfully created AvatarSession.
type AvatarSessionInfo struct {
	SessionID         string `json:"session_id,omitempty"`
	ProviderSessionID string `json:"provider_session_id,omitempty"`
	AvatarIdentity    string `json:"avatar_identity,omitempty"`
	SampleRate        int    `json:"sample_rate,omitempty"`
}

type avatarCreateResponse struct {
	AvatarSessionInfo
	TerminateToken string `json:"terminate_token,omitempty"`
}

type avatarCloseOperation struct {
	done chan struct{}
	err  error
}

// AvatarSession provisions and terminates one paid provider session through
// the LiveKit inference gateway. It is safe for concurrent inspection and
// Close calls. A failed create may be retried on the same instance using the
// same idempotency key; a successful create may never be started again.
type AvatarSession struct {
	provider       string
	avatarID       string
	identity       string
	name           string
	extra          map[string]any
	baseURL        string
	credentials    Credentials
	httpClient     *http.Client
	connect        agents.APIConnectOptions
	metadata       RequestMetadata
	idempotencyKey string

	mu             sync.RWMutex
	startClaimed   bool
	startDone      chan struct{}
	created        bool
	closed         bool
	info           AvatarSessionInfo
	terminateToken string
	closeOperation *avatarCloseOperation
}

// NewAvatarSession validates options without starting network or background
// work, keeping import and process start-up cost minimal.
func NewAvatarSession(options AvatarSessionOptions) (*AvatarSession, error) {
	provider, avatarID, err := ParseAvatarModel(options.Model)
	if err != nil {
		return nil, err
	}
	resolvedCredentials, err := options.Credentials.Resolve()
	if err != nil {
		return nil, err
	}
	identity := strings.TrimSpace(options.AvatarParticipantIdentity)
	if identity == "" {
		identity = provider + "-inference-avatar"
	}
	name := strings.TrimSpace(options.AvatarParticipantName)
	if name == "" {
		name = identity
	}
	extra := cloneAvatarMap(options.ExtraKwargs)
	if avatarID != "" && (options.LemonSlice.ImageURL != "" || hasAvatarKey(extra, "image_url")) {
		return nil, fmt.Errorf("pass either a catalog id in the model string (%q) or image_url, not both", provider+"/<avatar_id>")
	}
	applyLemonSliceOptions(extra, options.LemonSlice)
	baseURL := strings.TrimRight(strings.TrimSpace(options.BaseURL), "/")
	if baseURL == "" {
		baseURL = DefaultURLFromEnvironment()
	}
	httpClient := options.HTTPClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	idempotencyKey := strings.TrimSpace(options.IdempotencyKey)
	if idempotencyKey == "" {
		idempotencyKey, err = newAvatarIdempotencyKey()
		if err != nil {
			return nil, err
		}
	}
	return &AvatarSession{
		provider: provider, avatarID: avatarID, identity: identity, name: name,
		extra: extra, baseURL: baseURL, credentials: resolvedCredentials,
		httpClient: httpClient, connect: options.ConnectOptions.Resolve(),
		metadata: options.Metadata, idempotencyKey: idempotencyKey,
	}, nil
}

// ParseAvatarModel parses "provider" or "provider/<avatar-id>". Slashes in
// the provider-specific id are preserved for parity with the JS SDK.
func ParseAvatarModel(model string) (provider, avatarID string, err error) {
	parts := strings.Split(model, "/")
	provider = strings.TrimSpace(parts[0])
	if provider == "" {
		return "", "", fmt.Errorf("invalid avatar model string %q (expected 'provider' or 'provider/<id>')", model)
	}
	if len(parts) > 1 {
		avatarID = strings.TrimSpace(strings.Join(parts[1:], "/"))
	}
	return provider, avatarID, nil
}

func (s *AvatarSession) Provider() string       { return s.provider }
func (s *AvatarSession) AvatarIdentity() string { return s.identity }
func (s *AvatarSession) AvatarName() string     { return s.name }

func (s *AvatarSession) SessionInfo() AvatarSessionInfo {
	s.mu.RLock()
	info := s.info
	s.mu.RUnlock()
	return info
}

func (s *AvatarSession) SessionID() string         { return s.SessionInfo().SessionID }
func (s *AvatarSession) ProviderSessionID() string { return s.SessionInfo().ProviderSessionID }
func (s *AvatarSession) Started() bool {
	s.mu.RLock()
	started := s.created
	s.mu.RUnlock()
	return started
}

// Start creates the remote session. The start claim is taken before any
// blocking work so overlapping callers cannot create two billed sessions.
func (s *AvatarSession) Start(ctx context.Context, options AvatarSessionStartOptions) (AvatarSessionInfo, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateAvatarStartOptions(options); err != nil {
		return AvatarSessionInfo{}, err
	}
	s.mu.Lock()
	if s.startClaimed || s.created || s.closed {
		s.mu.Unlock()
		return AvatarSessionInfo{}, ErrAvatarSessionAlreadyStarted
	}
	s.startClaimed = true
	s.startDone = make(chan struct{})
	s.mu.Unlock()

	created := false
	defer func() {
		if created {
			return
		}
		s.mu.Lock()
		s.startClaimed = false
		if s.startDone != nil {
			close(s.startDone)
			s.startDone = nil
		}
		s.mu.Unlock()
	}()

	payload := s.createPayload(options)
	var response avatarCreateResponse
	err := s.postJSONRetrying(ctx, s.baseURL+"/avatar/sessions", payload, true, &response, "avatar gateway create")
	if err != nil {
		return AvatarSessionInfo{}, err
	}
	info := response.AvatarSessionInfo
	if info.SampleRate <= 0 {
		info.SampleRate = DefaultAvatarSampleRate
	}
	s.mu.Lock()
	s.info = info
	s.terminateToken = response.TerminateToken
	s.created = true
	s.startClaimed = false
	if s.startDone != nil {
		close(s.startDone)
		s.startDone = nil
	}
	s.mu.Unlock()
	created = true
	return info, nil
}

// Close terminates a provider session when the gateway supplied both required
// fields. Concurrent callers share one operation. A failed operation is
// returned to all of its waiters and a later Close call may retry it.
func (s *AvatarSession) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	if s.startClaimed && !s.created {
		done := s.startDone
		s.mu.Unlock()
		select {
		case <-done:
			return s.Close(ctx)
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
	if operation := s.closeOperation; operation != nil {
		s.mu.Unlock()
		select {
		case <-operation.done:
			return operation.err
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
	operation := &avatarCloseOperation{done: make(chan struct{})}
	s.closeOperation = operation
	info := s.info
	terminateToken := s.terminateToken
	created := s.created
	s.mu.Unlock()

	var err error
	terminated := false
	if created && info.ProviderSessionID != "" && terminateToken != "" {
		payload := map[string]any{
			"provider": s.provider, "provider_session_id": info.ProviderSessionID,
			"terminate_token": terminateToken,
		}
		err = s.postJSONRetrying(ctx, s.baseURL+"/avatar/sessions/terminate", payload, false, nil, "avatar gateway terminate")
		if err != nil {
			err = fmt.Errorf("failed to terminate inference avatar session after retries (provider=%s, providerSessionId=%s); it will keep billing until its provider idle timeout: %w", s.provider, info.ProviderSessionID, err)
		} else {
			terminated = true
		}
	}

	s.mu.Lock()
	operation.err = err
	if err == nil {
		s.closed = true
		if terminated {
			s.info.ProviderSessionID = ""
			s.terminateToken = ""
		}
	}
	s.closeOperation = nil
	close(operation.done)
	s.mu.Unlock()
	return err
}

func validateAvatarStartOptions(options AvatarSessionStartOptions) error {
	for name, value := range map[string]string{
		"livekit URL": options.LiveKitURL, "room name": options.RoomName,
		"room SID": options.RoomSID, "agent identity": options.AgentIdentity,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("inference avatar %s is required", name)
		}
	}
	return nil
}

func (s *AvatarSession) createPayload(options AvatarSessionStartOptions) map[string]any {
	payload := map[string]any{
		"provider": s.provider, "livekit_url": options.LiveKitURL,
		"room_name": options.RoomName, "room_sid": options.RoomSID,
		"avatar_identity": s.identity, "avatar_name": s.name,
		"agent_identity": options.AgentIdentity,
	}
	if s.avatarID != "" {
		payload["avatar_id"] = s.avatarID
	}
	extra := make(map[string]any)
	for key, value := range s.extra {
		switch key {
		case "image_url", "prompt", "idle_prompt", "idle_timeout_s":
			payload[key] = value
		default:
			extra[key] = value
		}
	}
	if len(extra) != 0 {
		payload["extra_kwargs"] = extra
	}
	return payload
}

func (s *AvatarSession) postJSONRetrying(ctx context.Context, url string, payload map[string]any, create bool, output any, label string) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode %s request: %w", label, err)
	}
	var last error
	for attempt := 0; attempt <= s.connect.MaxRetries; attempt++ {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		retryAfter, last := s.postJSONAttempt(ctx, url, body, create, output, label, attempt)
		if last == nil {
			return nil
		}
		if !avatarRetryable(last) || attempt == s.connect.MaxRetries {
			return last
		}
		delay := s.connect.RetryDelay(attempt)
		if retryAfter != nil {
			delay = *retryAfter
		}
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return context.Cause(ctx)
		}
	}
	return last
}

func (s *AvatarSession) postJSONAttempt(ctx context.Context, url string, body []byte, create bool, output any, label string, attempt int) (*time.Duration, error) {
	attemptCtx, cancel := context.WithTimeout(ctx, s.connect.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(attemptCtx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, agents.NewAPIConnectionError("create "+label+" request", false, err)
	}
	token, err := AccessToken(s.credentials, 0)
	if err != nil {
		return nil, err
	}
	req.Header = MetadataHeaders(s.metadata)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set(ProviderHeader, s.provider)
	req.Header.Set("Content-Type", "application/json")
	if create {
		req.Header.Set("Idempotency-Key", s.idempotencyKey)
	}
	response, err := s.httpClient.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, context.Cause(ctx)
		}
		if errors.Is(attemptCtx.Err(), context.DeadlineExceeded) {
			return nil, agents.NewAPITimeoutError(fmt.Sprintf("%s timed out after attempt %d", label, attempt+1), true, err)
		}
		return nil, agents.NewAPIConnectionError(label+" connection failed", true, err)
	}
	defer response.Body.Close()
	data, readErr := io.ReadAll(io.LimitReader(response.Body, maxAvatarResponseBytes+1))
	if readErr != nil {
		return nil, agents.NewAPIConnectionError("read "+label+" response", true, readErr)
	}
	if len(data) > maxAvatarResponseBytes {
		return nil, agents.NewAPIError(label+" response exceeded 1 MiB", nil, false, nil)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		retryAfter := parseAvatarRetryAfter(response.Header.Get("Retry-After"), time.Now())
		statusErr := agents.NewAPIStatusError(
			fmt.Sprintf("avatar gateway returned an error: %s", strings.TrimSpace(string(data))),
			response.StatusCode, response.Header.Get("X-Request-Id"), map[string]any{"error": string(data)}, true, nil,
		)
		return retryAfter, statusErr
	}
	if output == nil || len(bytes.TrimSpace(data)) == 0 {
		return nil, nil
	}
	mediaType, _, parseErr := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if parseErr != nil || mediaType != "application/json" {
		return nil, agents.NewAPIError("avatar gateway returned a non-JSON response", map[string]any{"content_type": response.Header.Get("Content-Type")}, false, parseErr)
	}
	if err := json.Unmarshal(data, output); err != nil {
		return nil, agents.NewAPIError("decode avatar gateway response", map[string]any{"body": string(data)}, false, err)
	}
	return nil, nil
}

func avatarRetryable(err error) bool {
	type retryable interface{ Retryable() bool }
	var value retryable
	return errors.As(err, &value) && value.Retryable()
}

func parseAvatarRetryAfter(value string, now time.Time) *time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	if seconds, err := strconv.ParseFloat(value, 64); err == nil {
		duration := time.Duration(seconds * float64(time.Second))
		if duration < 0 {
			duration = 0
		}
		return &duration
	}
	if date, err := http.ParseTime(value); err == nil {
		duration := date.Sub(now)
		if duration < 0 {
			duration = 0
		}
		return &duration
	}
	return nil
}

func applyLemonSliceOptions(extra map[string]any, options LemonSliceOptions) {
	if options.ImageURL != "" {
		extra["image_url"] = options.ImageURL
	}
	if options.Prompt != "" {
		extra["prompt"] = options.Prompt
	}
	if options.IdlePrompt != "" {
		extra["idle_prompt"] = options.IdlePrompt
	}
	if options.IdleTimeout != nil {
		extra["idle_timeout_s"] = options.IdleTimeout.Seconds()
	}
}

func cloneAvatarMap(source map[string]any) map[string]any {
	copy := make(map[string]any, len(source))
	for key, value := range source {
		copy[key] = value
	}
	return copy
}

func hasAvatarKey(values map[string]any, key string) bool {
	_, ok := values[key]
	return ok
}

func newAvatarIdempotencyKey() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("create avatar idempotency key: %w", err)
	}
	return hex.EncodeToString(value[:]), nil
}
