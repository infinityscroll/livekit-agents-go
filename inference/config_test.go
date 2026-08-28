// SPDX-License-Identifier: Apache-2.0

package inference

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	agents "github.com/livekit/agents-go"
)

func TestDefaultURLFromEnvironment(t *testing.T) {
	t.Setenv("LIVEKIT_INFERENCE_URL", "")
	t.Setenv("LIVEKIT_URL", "wss://example.livekit.cloud")
	if got := DefaultURLFromEnvironment(); got != DefaultURL {
		t.Fatalf("production URL = %q", got)
	}
	t.Setenv("LIVEKIT_URL", "wss://project.staging.livekit.cloud")
	if got := DefaultURLFromEnvironment(); got != StagingURL {
		t.Fatalf("staging URL = %q", got)
	}
	t.Setenv("LIVEKIT_INFERENCE_URL", "http://localhost:8080/v1/")
	if got := DefaultURLFromEnvironment(); got != "http://localhost:8080/v1" {
		t.Fatalf("explicit URL = %q", got)
	}
}

func TestCredentialsPrecedenceAndRedaction(t *testing.T) {
	t.Setenv("LIVEKIT_INFERENCE_API_KEY", "inference-key")
	t.Setenv("LIVEKIT_INFERENCE_API_SECRET", "inference-secret")
	t.Setenv("LIVEKIT_API_KEY", "shared-key")
	t.Setenv("LIVEKIT_API_SECRET", "shared-secret")
	resolved, err := (Credentials{}).Resolve()
	if err != nil {
		t.Fatal(err)
	}
	if resolved.APIKey.Reveal() != "inference-key" || resolved.APISecret.Reveal() != "inference-secret" {
		t.Fatalf("wrong environment precedence: %#v", resolved)
	}
	explicit, err := (Credentials{APIKey: agents.NewSecretString("explicit-key"), APISecret: agents.NewSecretString("explicit-secret")}).Resolve()
	if err != nil {
		t.Fatal(err)
	}
	if explicit.APIKey.Reveal() != "explicit-key" || explicit.APISecret.Reveal() != "explicit-secret" {
		t.Fatal("explicit credentials did not win")
	}
	formatted := explicit.APIKey.String() + explicit.APISecret.String()
	if strings.Contains(formatted, "explicit") {
		t.Fatal("credentials leaked through String")
	}
}

func TestAccessTokenInferenceGrantAndTTL(t *testing.T) {
	before := time.Now()
	token, err := AccessToken(Credentials{
		APIKey: agents.NewSecretString("api-key"), APISecret: agents.NewSecretString("api-secret-with-enough-entropy"),
	}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("token has %d parts", len(parts))
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	var claims struct {
		Subject   string `json:"sub"`
		ExpiresAt int64  `json:"exp"`
		Inference struct {
			Perform bool `json:"perform"`
		} `json:"inference"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		t.Fatal(err)
	}
	if claims.Subject != "agent" || !claims.Inference.Perform {
		t.Fatalf("unexpected claims: %+v", claims)
	}
	expires := time.Unix(claims.ExpiresAt, 0)
	if expires.Before(before.Add(59*time.Second)) || expires.After(time.Now().Add(61*time.Second)) {
		t.Fatalf("unexpected expiration %s", expires)
	}
}

func TestMetadataHeaders(t *testing.T) {
	t.Setenv("LIVEKIT_WORKER_TOKEN", "environment-worker-token")
	headers := MetadataHeaders(RequestMetadata{RoomID: "RM_1", JobID: "AJ_1", AgentID: "PA_1"})
	for key, want := range map[string]string{
		"X-LiveKit-Room-Id": "RM_1", "X-LiveKit-Job-Id": "AJ_1",
		"X-LiveKit-Agent-Id": "PA_1", "X-LiveKit-Worker-Token": "environment-worker-token",
	} {
		if got := headers.Get(key); got != want {
			t.Errorf("%s = %q, want %q", key, got, want)
		}
	}
	if !strings.HasPrefix(headers.Get("User-Agent"), "livekit-agents-go/") {
		t.Fatalf("unexpected User-Agent %q", headers.Get("User-Agent"))
	}
}
