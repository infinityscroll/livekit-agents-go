// SPDX-License-Identifier: Apache-2.0

package inference

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	agents "github.com/livekit/agents-go"
	llmpkg "github.com/livekit/agents-go/llm"
)

func testCredentials() Credentials {
	return Credentials{APIKey: agents.NewSecretString("api-key"), APISecret: agents.NewSecretString("a-long-test-api-secret")}
}

func TestLLMStreamingRequestAndThinkingFilter(t *testing.T) {
	var requestBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Errorf("path = %q", r.URL.Path)
		}
		if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
			t.Error("missing bearer token")
		}
		for key, want := range map[string]string{
			ProviderHeader: "openai", PriorityHeader: "low", "X-Test": "yes",
			"X-LiveKit-Room-Id": "RM_test",
		} {
			if got := r.Header.Get(key); got != want {
				t.Errorf("%s = %q, want %q", key, got, want)
			}
		}
		if got := r.URL.Query().Get("region"); got != "us" {
			t.Errorf("query region = %q", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&requestBody); err != nil {
			t.Error(err)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"id\":\"req_1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hello <thi\"},\"finish_reason\":null}]}\n\n"))
		_, _ = w.Write([]byte("data: {\"id\":\"req_1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"nk>secret</think> world\"},\"finish_reason\":\"stop\"}]}\n\n"))
		_, _ = w.Write([]byte("data: {\"id\":\"req_1\",\"choices\":[],\"usage\":{\"completion_tokens\":2,\"prompt_tokens\":3,\"total_tokens\":5,\"prompt_tokens_details\":{\"cached_tokens\":1}}}\n\n"))
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer server.Close()
	client, err := NewLLM(LLMOptions{
		Model: "openai/gpt-4o-mini", Provider: "openai", BaseURL: server.URL + "/v1",
		Credentials: testCredentials(), InferenceClass: ClassLow,
		ModelOptions: ModelOptions{
			"temperature":   0.4,
			"extra_headers": map[string]any{"X-Test": "yes"},
			"extra_query":   map[string]any{"region": "us"},
		},
		Metadata: func(context.Context) RequestMetadata { return RequestMetadata{RoomID: "RM_test"} },
	})
	if err != nil {
		t.Fatal(err)
	}
	chat := llmpkg.NewChatContext(llmpkg.NewChatMessage(llmpkg.RoleUser, "hello"))
	stream, err := client.Chat(context.Background(), llmpkg.ChatOptions{
		ChatContext:    chat,
		ConnectOptions: agents.APIConnectOptions{MaxRetries: -1, Timeout: 2 * time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := stream.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Text != "Hello  world" {
		t.Fatalf("text = %q", result.Text)
	}
	if result.Usage == nil || result.Usage.TotalTokens != 5 || result.Usage.PromptCachedTokens != 1 {
		t.Fatalf("usage = %+v", result.Usage)
	}
	if requestBody["model"] != "openai/gpt-4o-mini" || requestBody["stream"] != true || requestBody["temperature"] != 0.4 {
		t.Fatalf("request = %#v", requestBody)
	}
	if _, exists := requestBody["extra_headers"]; exists {
		t.Fatal("transport options leaked into request body")
	}
}

func TestLLMToolStreamingAndStrictSchema(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		tools := body["tools"].([]any)
		function := tools[0].(map[string]any)["function"].(map[string]any)
		if function["strict"] != true {
			t.Error("strict schema missing")
		}
		if _, exists := function["parameters"].(map[string]any)["$schema"]; exists {
			t.Error("$schema was not stripped")
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"id\":\"resp_1\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"function\":{\"name\":\"weather\",\"arguments\":\"{\\\"city\\\":\"},\"extra_content\":{\"google\":{\"thought_signature\":\"sig\"}}}]},\"finish_reason\":null}]}\n\n"))
		_, _ = w.Write([]byte("data: {\"id\":\"resp_1\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"\\\"Paris\\\"}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\n"))
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}))
	defer server.Close()
	type weatherInput struct {
		City string `json:"city"`
	}
	tool := llmpkg.MustTool(llmpkg.FunctionToolOptions[weatherInput, string]{
		Name: "weather", Description: "Get weather",
		Parameters: json.RawMessage(`{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","properties":{"city":{"type":"string"}}}`),
		Execute:    func(context.Context, weatherInput, llmpkg.ToolOptions) (string, error) { return "sunny", nil },
	})
	toolContext, err := llmpkg.NewToolContext(tool)
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewLLM(LLMOptions{Model: "google/gemini-2.5-flash", BaseURL: server.URL, Credentials: testCredentials(), StrictToolSchema: true})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := client.Chat(context.Background(), llmpkg.ChatOptions{
		ChatContext: llmpkg.NewChatContext(llmpkg.NewChatMessage(llmpkg.RoleUser, "weather?")),
		ToolContext: toolContext, ConnectOptions: agents.APIConnectOptions{MaxRetries: -1, Timeout: 2 * time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := stream.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(result.ToolCalls) != 1 {
		t.Fatalf("tool calls = %#v", result.ToolCalls)
	}
	call := result.ToolCalls[0]
	if call.Name != "weather" || call.Arguments != `{"city":"Paris"}` || call.ThoughtSignature != "sig" || call.GroupID != "resp_1" {
		t.Fatalf("tool call = %+v", call)
	}
}

func TestLLMRetriesBeforeFirstResponse(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) == 1 {
			http.Error(w, "temporary", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"id\":\"r\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"))
	}))
	defer server.Close()
	client, err := NewLLM(LLMOptions{Model: "test/model", BaseURL: server.URL, Credentials: testCredentials()})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := client.Chat(context.Background(), llmpkg.ChatOptions{
		ConnectOptions: agents.APIConnectOptions{MaxRetries: 1, RetryInterval: time.Millisecond, Timeout: time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := stream.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Text != "ok" || attempts.Load() != 2 {
		t.Fatalf("result=%q attempts=%d", result.Text, attempts.Load())
	}
}

func TestLLMStatusErrorIsTyped(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("x-request-id", "req_bad")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":"bad token"}`))
	}))
	defer server.Close()
	client, err := NewLLM(LLMOptions{Model: "test/model", BaseURL: server.URL, Credentials: testCredentials()})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := client.Chat(context.Background(), llmpkg.ChatOptions{ConnectOptions: agents.APIConnectOptions{MaxRetries: -1}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = stream.Collect(context.Background())
	var status *agents.APIStatusError
	if !errors.As(err, &status) || status.StatusCode != http.StatusUnauthorized || status.RequestID != "req_bad" || status.Retryable() {
		t.Fatalf("status error = %#v (%v)", status, err)
	}
}

func TestThinkingFilterAcrossMarkers(t *testing.T) {
	filter := &thinkingFilter{start: "<think>", end: "</think>"}
	var output strings.Builder
	for _, part := range []string{"before<th", "ink>hidden", "</thi", "nk>after"} {
		output.WriteString(filter.Feed(part, false))
	}
	output.WriteString(filter.Feed("", true))
	if got := output.String(); got != "beforeafter" {
		t.Fatalf("visible output = %q", got)
	}
}
