// SPDX-License-Identifier: Apache-2.0

// Package inference implements the LiveKit Cloud Inference adapters.
package inference

import (
	"fmt"
	"net/http"
	"os"
	"runtime"
	"strings"
	"time"

	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/livekit/protocol/auth"
)

const (
	// DefaultURL is the production LiveKit Agent Gateway URL.
	DefaultURL = "https://agent-gateway.livekit.cloud/v1"
	// StagingURL is selected automatically for staging LiveKit projects.
	StagingURL = "https://agent-gateway.staging.livekit.cloud/v1"

	ProviderHeader = "X-LiveKit-Inference-Provider"
	PriorityHeader = "X-LiveKit-Inference-Priority"

	defaultTokenTTL = 10 * time.Minute
)

// Class controls gateway scheduling. Low-priority work may yield to voice
// traffic and should only be used when no caller is synchronously waiting.
type Class string

const (
	ClassPriority Class = "priority"
	ClassStandard Class = "standard"
	ClassLow      Class = "low"
)

// DefaultURLFromEnvironment applies the same precedence as the TypeScript and
// Python SDKs: explicit inference URL, staging project detection, production.
func DefaultURLFromEnvironment() string {
	if value := strings.TrimSpace(os.Getenv("LIVEKIT_INFERENCE_URL")); value != "" {
		return strings.TrimRight(value, "/")
	}
	if strings.Contains(os.Getenv("LIVEKIT_URL"), ".staging.livekit.cloud") {
		return StagingURL
	}
	return DefaultURL
}

// Credentials are resolved without retaining plaintext in formatted output.
// Explicit values take precedence over inference-specific and shared LiveKit
// environment variables.
type Credentials struct {
	APIKey    agents.SecretString
	APISecret agents.SecretString
}

func (c Credentials) Resolve() (Credentials, error) {
	key := strings.TrimSpace(c.APIKey.Reveal())
	if key == "" {
		key = firstEnvironment("LIVEKIT_INFERENCE_API_KEY", "LIVEKIT_API_KEY")
	}
	if key == "" {
		return Credentials{}, &agents.MissingCredentialsError{Name: "LIVEKIT_API_KEY"}
	}
	secret := strings.TrimSpace(c.APISecret.Reveal())
	if secret == "" {
		secret = firstEnvironment("LIVEKIT_INFERENCE_API_SECRET", "LIVEKIT_API_SECRET")
	}
	if secret == "" {
		return Credentials{}, &agents.MissingCredentialsError{Name: "LIVEKIT_API_SECRET"}
	}
	return Credentials{APIKey: agents.NewSecretString(key), APISecret: agents.NewSecretString(secret)}, nil
}

func firstEnvironment(names ...string) string {
	for _, name := range names {
		if value := strings.TrimSpace(os.Getenv(name)); value != "" {
			return value
		}
	}
	return ""
}

// AccessToken creates the short-lived inference-grant JWT used by all gateway
// transports. A non-positive TTL uses the cross-SDK ten-minute default.
func AccessToken(credentials Credentials, ttl time.Duration) (string, error) {
	resolved, err := credentials.Resolve()
	if err != nil {
		return "", err
	}
	if ttl <= 0 {
		ttl = defaultTokenTTL
	}
	token, err := auth.NewAccessToken(resolved.APIKey.Reveal(), resolved.APISecret.Reveal()).
		SetIdentity("agent").
		SetValidFor(ttl).
		SetInferenceGrant(&auth.InferenceGrant{Perform: true}).
		ToJWT()
	if err != nil {
		return "", fmt.Errorf("create LiveKit inference access token: %w", err)
	}
	return token, nil
}

// RequestMetadata mirrors the contextual headers added by the JavaScript and
// Python SDKs without coupling this package to a global job context.
type RequestMetadata struct {
	RoomID      string
	JobID       string
	AgentID     string
	WorkerToken agents.SecretString
}

// MetadataHeaders returns a fresh map safe for caller mutation. AgentID should
// only be supplied after the room has connected.
func MetadataHeaders(metadata RequestMetadata) http.Header {
	headers := make(http.Header, 5)
	headers.Set("User-Agent", fmt.Sprintf("livekit-agents-go/%s (go %s; %s/%s)",
		strings.TrimPrefix(agents.Version, "v"), runtime.Version(), runtime.GOOS, runtime.GOARCH))
	if metadata.RoomID != "" {
		headers.Set("X-LiveKit-Room-Id", metadata.RoomID)
	}
	if metadata.JobID != "" {
		headers.Set("X-LiveKit-Job-Id", metadata.JobID)
	}
	if metadata.AgentID != "" {
		headers.Set("X-LiveKit-Agent-Id", metadata.AgentID)
	}
	workerToken := metadata.WorkerToken.Reveal()
	if workerToken == "" {
		workerToken = os.Getenv("LIVEKIT_WORKER_TOKEN")
	}
	if workerToken != "" {
		headers.Set("X-LiveKit-Worker-Token", workerToken)
	}
	return headers
}
