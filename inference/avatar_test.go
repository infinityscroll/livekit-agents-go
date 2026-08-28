// SPDX-License-Identifier: Apache-2.0

package inference

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	agents "github.com/livekit/agents-go"
)

func avatarTestCredentials() Credentials {
	return Credentials{
		APIKey:    agents.NewSecretString("avatar-key"),
		APISecret: agents.NewSecretString("avatar-secret-with-sufficient-entropy"),
	}
}

func avatarStartOptions() AvatarSessionStartOptions {
	return AvatarSessionStartOptions{
		LiveKitURL: "wss://example.livekit.cloud", RoomName: "room",
		RoomSID: "RM_test", AgentIdentity: "agent",
	}
}

func TestParseAvatarModelAndDefaults(t *testing.T) {
	provider, id, err := ParseAvatarModel(" lemonslice/catalog/friendly ")
	if err != nil || provider != "lemonslice" || id != "catalog/friendly" {
		t.Fatalf("ParseAvatarModel = %q %q %v", provider, id, err)
	}
	if _, _, err := ParseAvatarModel(" /id"); err == nil {
		t.Fatal("expected empty-provider error")
	}
	session, err := NewAvatarSession(AvatarSessionOptions{Model: "lemonslice", Credentials: avatarTestCredentials()})
	if err != nil {
		t.Fatal(err)
	}
	if session.Provider() != "lemonslice" || session.AvatarIdentity() != "lemonslice-inference-avatar" || session.AvatarName() != session.AvatarIdentity() {
		t.Fatalf("defaults = provider=%q identity=%q name=%q", session.Provider(), session.AvatarIdentity(), session.AvatarName())
	}
}

func TestAvatarSessionCreatePayloadHeadersAndDefaults(t *testing.T) {
	idle := 12*time.Second + 500*time.Millisecond
	var received map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/avatar/sessions" || r.Method != http.MethodPost {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get(ProviderHeader) != "lemonslice" || r.Header.Get("Authorization") == "" || r.Header.Get("Idempotency-Key") != "stable-key" {
			t.Errorf("headers = %#v", r.Header)
		}
		if contentType := r.Header.Get("Content-Type"); contentType != "application/json" {
			t.Errorf("Content-Type = %q", contentType)
		}
		if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
			t.Error(err)
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_, _ = w.Write([]byte(`{"session_id":"session","provider_session_id":"provider-session","terminate_token":"token"}`))
	}))
	defer server.Close()
	session, err := NewAvatarSession(AvatarSessionOptions{
		Model: "lemonslice/avatar-id", BaseURL: server.URL, Credentials: avatarTestCredentials(),
		AvatarParticipantIdentity: "avatar", AvatarParticipantName: "Avatar",
		LemonSlice:  LemonSliceOptions{Prompt: "speak", IdlePrompt: "idle", IdleTimeout: &idle},
		ExtraKwargs: map[string]any{"vendor_flag": true}, IdempotencyKey: "stable-key",
		ConnectOptions: agents.APIConnectOptions{MaxRetries: -1, Timeout: time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	info, err := session.Start(t.Context(), avatarStartOptions())
	if err != nil {
		t.Fatal(err)
	}
	if info.SampleRate != DefaultAvatarSampleRate || session.SessionID() != "session" || session.ProviderSessionID() != "provider-session" {
		t.Fatalf("info = %#v", info)
	}
	for key, want := range map[string]any{
		"provider": "lemonslice", "avatar_id": "avatar-id", "avatar_identity": "avatar",
		"avatar_name": "Avatar", "agent_identity": "agent", "prompt": "speak",
		"idle_prompt": "idle", "idle_timeout_s": 12.5,
	} {
		if got := received[key]; got != want {
			t.Errorf("payload[%q] = %#v, want %#v", key, got, want)
		}
	}
	extra, ok := received["extra_kwargs"].(map[string]any)
	if !ok || extra["vendor_flag"] != true {
		t.Fatalf("extra_kwargs = %#v", received["extra_kwargs"])
	}
}

func TestAvatarSessionImageAndCatalogIDAreExclusive(t *testing.T) {
	_, err := NewAvatarSession(AvatarSessionOptions{
		Model: "lemonslice/id", Credentials: avatarTestCredentials(),
		LemonSlice: LemonSliceOptions{ImageURL: "https://example.test/avatar.png"},
	})
	if err == nil {
		t.Fatal("expected conflict")
	}
}

func TestAvatarSessionCreateRetriesWithStableIdempotency(t *testing.T) {
	var attempts atomic.Int32
	var mu sync.Mutex
	var keys []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		keys = append(keys, r.Header.Get("Idempotency-Key"))
		mu.Unlock()
		if attempts.Add(1) == 1 {
			w.Header().Set("Retry-After", "0")
			http.Error(w, "retry", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"session_id":"ok","sample_rate":24000}`))
	}))
	defer server.Close()
	session, err := NewAvatarSession(AvatarSessionOptions{
		Model: "lemonslice", BaseURL: server.URL, Credentials: avatarTestCredentials(),
		ConnectOptions: agents.APIConnectOptions{MaxRetries: 1, RetryInterval: time.Millisecond, Timeout: time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	info, err := session.Start(t.Context(), avatarStartOptions())
	if err != nil || info.SampleRate != 24000 {
		t.Fatalf("Start = %#v, %v", info, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(keys) != 2 || keys[0] == "" || keys[0] != keys[1] {
		t.Fatalf("idempotency keys = %#v", keys)
	}
}

func TestAvatarSessionFailedCreateCanRetrySameKey(t *testing.T) {
	var attempts atomic.Int32
	var keys sync.Map
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		keys.Store(attempts.Load(), r.Header.Get("Idempotency-Key"))
		if attempts.Add(1) == 1 {
			http.Error(w, "temporary", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"session_id":"replayed"}`))
	}))
	defer server.Close()
	session, err := NewAvatarSession(AvatarSessionOptions{
		Model: "lemonslice", BaseURL: server.URL, Credentials: avatarTestCredentials(),
		ConnectOptions: agents.APIConnectOptions{MaxRetries: -1, Timeout: time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Start(t.Context(), avatarStartOptions()); err == nil {
		t.Fatal("expected first create failure")
	}
	if _, err := session.Start(t.Context(), avatarStartOptions()); err != nil {
		t.Fatal(err)
	}
	first, _ := keys.Load(int32(0))
	second, _ := keys.Load(int32(1))
	if first == "" || first != second {
		t.Fatalf("keys = %#v %#v", first, second)
	}
}

func TestAvatarSessionConcurrentStartClaimsSynchronously(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"session_id":"one"}`))
	}))
	defer server.Close()
	session, err := NewAvatarSession(AvatarSessionOptions{
		Model: "lemonslice", BaseURL: server.URL, Credentials: avatarTestCredentials(),
		ConnectOptions: agents.APIConnectOptions{MaxRetries: -1, Timeout: 5 * time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := session.Start(context.Background(), avatarStartOptions()); done <- err }()
	<-entered
	if _, err := session.Start(t.Context(), avatarStartOptions()); !errors.Is(err, ErrAvatarSessionAlreadyStarted) {
		t.Fatalf("concurrent Start error = %v", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestAvatarSessionCloseWaitsForInFlightCreateThenTerminates(t *testing.T) {
	createEntered := make(chan struct{})
	releaseCreate := make(chan struct{})
	terminated := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/avatar/sessions" {
			close(createEntered)
			<-releaseCreate
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"provider_session_id":"paid","terminate_token":"proof"}`))
			return
		}
		terminated <- struct{}{}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	session, err := NewAvatarSession(AvatarSessionOptions{
		Model: "lemonslice", BaseURL: server.URL, Credentials: avatarTestCredentials(),
		ConnectOptions: agents.APIConnectOptions{MaxRetries: -1, Timeout: 5 * time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	startDone := make(chan error, 1)
	go func() { _, err := session.Start(context.Background(), avatarStartOptions()); startDone <- err }()
	<-createEntered
	closeDone := make(chan error, 1)
	go func() { closeDone <- session.Close(context.Background()) }()
	select {
	case <-terminated:
		t.Fatal("terminate raced ahead of create")
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseCreate)
	if err := <-startDone; err != nil {
		t.Fatal(err)
	}
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
	select {
	case <-terminated:
	default:
		t.Fatal("terminate was not called")
	}
}

func TestAvatarSessionConcurrentCloseAndRetryAfterFailure(t *testing.T) {
	var terminateAttempts atomic.Int32
	entered := make(chan struct{}, 2)
	release := make(chan struct{}, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/avatar/sessions" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"provider_session_id":"paid","terminate_token":"proof"}`))
			return
		}
		attempt := terminateAttempts.Add(1)
		entered <- struct{}{}
		<-release
		if attempt == 1 {
			http.Error(w, "failed", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	session, err := NewAvatarSession(AvatarSessionOptions{
		Model: "lemonslice", BaseURL: server.URL, Credentials: avatarTestCredentials(),
		ConnectOptions: agents.APIConnectOptions{MaxRetries: -1, Timeout: 5 * time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Start(t.Context(), avatarStartOptions()); err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 2)
	go func() { results <- session.Close(context.Background()) }()
	<-entered
	go func() { results <- session.Close(context.Background()) }()
	release <- struct{}{}
	for range 2 {
		if err := <-results; err == nil {
			t.Fatal("expected shared terminate failure")
		}
	}
	if terminateAttempts.Load() != 1 || session.ProviderSessionID() != "paid" {
		t.Fatalf("attempts=%d info=%#v", terminateAttempts.Load(), session.SessionInfo())
	}
	go func() { results <- session.Close(context.Background()) }()
	<-entered
	release <- struct{}{}
	if err := <-results; err != nil {
		t.Fatal(err)
	}
	if terminateAttempts.Load() != 2 || session.ProviderSessionID() != "" {
		t.Fatalf("attempts=%d info=%#v", terminateAttempts.Load(), session.SessionInfo())
	}
}

func TestAvatarSessionCancellationStopsRetryAfter(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "60")
		http.Error(w, "busy", http.StatusTooManyRequests)
	}))
	defer server.Close()
	session, err := NewAvatarSession(AvatarSessionOptions{
		Model: "lemonslice", BaseURL: server.URL, Credentials: avatarTestCredentials(),
		ConnectOptions: agents.APIConnectOptions{MaxRetries: 3, Timeout: time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err = session.Start(ctx, avatarStartOptions())
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > time.Second {
		t.Fatalf("Start error=%v elapsed=%s", err, time.Since(started))
	}
}
