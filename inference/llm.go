// SPDX-License-Identifier: Apache-2.0

package inference

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"

	agents "github.com/infinityscroll/livekit-agents-go"
	llmpkg "github.com/infinityscroll/livekit-agents-go/llm"
)

const (
	maxLLMErrorBodyBytes = 1 << 20
	maxSSELineBytes      = 8 << 20
)

type LLMOptions struct {
	Model            string
	Provider         string
	BaseURL          string
	Credentials      Credentials
	ModelOptions     ModelOptions
	StrictToolSchema bool
	InferenceClass   Class
	HTTPClient       *http.Client
	Metadata         func(context.Context) RequestMetadata
}

// LLM is an OpenAI-compatible LiveKit Inference client implemented directly
// on net/http to keep the dependency graph and process startup small.
type LLM struct {
	*llmpkg.Base

	mu      sync.RWMutex
	options LLMOptions
	client  *http.Client
}

func NewLLM(options LLMOptions) (*LLM, error) {
	if strings.TrimSpace(options.Model) == "" {
		return nil, errors.New("inference LLM model is required")
	}
	credentials, err := options.Credentials.Resolve()
	if err != nil {
		return nil, err
	}
	options.Credentials = credentials
	if options.BaseURL == "" {
		options.BaseURL = DefaultURLFromEnvironment()
	}
	options.BaseURL = strings.TrimRight(options.BaseURL, "/")
	parsed, err := url.Parse(options.BaseURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return nil, fmt.Errorf("invalid inference base URL %q", options.BaseURL)
	}
	if options.ModelOptions == nil {
		options.ModelOptions = ModelOptions{}
	}
	client := options.HTTPClient
	if client == nil {
		if base, ok := http.DefaultTransport.(*http.Transport); ok {
			transport := base.Clone()
			transport.MaxIdleConnsPerHost = 8
			client = &http.Client{Transport: transport}
		} else {
			client = &http.Client{Transport: http.DefaultTransport}
		}
	}
	base := llmpkg.NewBase("inference.LLM", "livekit", options.Model)
	result := &LLM{Base: base, options: cloneLLMOptions(options), client: client}
	base.SetPrewarm(result.prewarm)
	return result, nil
}

func LLMFromModelString(model string) (*LLM, error) {
	return NewLLM(LLMOptions{Model: model})
}

// UpdateOptions changes the persistent configuration used by subsequent chat
// calls. ModelOptions replaces, rather than merges, the previous options.
func (l *LLM) UpdateOptions(model string, modelOptions ModelOptions) error {
	if model == "" && modelOptions == nil {
		return nil
	}
	l.mu.Lock()
	if model != "" {
		l.options.Model = model
		l.SetModel(model)
	}
	if modelOptions != nil {
		l.options.ModelOptions = cloneModelOptions(modelOptions)
	}
	l.mu.Unlock()
	return nil
}

func (l *LLM) Chat(ctx context.Context, options llmpkg.ChatOptions) (llmpkg.LLMStream, error) {
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	l.mu.RLock()
	config := cloneLLMOptions(l.options)
	l.mu.RUnlock()
	chat := options.ChatContext
	if chat == nil {
		chat = llmpkg.EmptyChatContext()
	}
	messages, err := toOpenAIMessages(chat)
	if err != nil {
		return nil, fmt.Errorf("convert chat context to OpenAI format: %w", err)
	}
	body, err := buildChatRequest(config, options, messages)
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("encode inference chat request: %w", err)
	}
	baseStream := llmpkg.NewBaseStream(ctx, l.Base, options, 32)
	stream := &llmStream{
		BaseStream: baseStream, owner: l, config: config, connect: options.ConnectOptions,
		requestBody: encoded, transport: extractRequestTransport(config, options),
		filters: make(map[int]*thinkingFilter), calls: make(map[int]map[int]*toolCallBuilder),
		toolExtra: make(map[int]map[string]any), seenChoices: make(map[int]bool), finishedChoices: make(map[int]bool),
	}
	go stream.run()
	return stream, nil
}

func (l *LLM) prewarm(ctx context.Context) error {
	l.mu.RLock()
	config := cloneLLMOptions(l.options)
	l.mu.RUnlock()
	token, err := AccessToken(config.Credentials, 0)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, config.BaseURL+"/models", nil)
	if err != nil {
		return err
	}
	request.Header = MetadataHeaders(metadataFor(ctx, config))
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := l.client.Do(request)
	if err != nil {
		return agents.NewAPIConnectionError("prewarm LiveKit Inference LLM", true, err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return statusError(response, true)
	}
	_, err = io.Copy(io.Discard, io.LimitReader(response.Body, maxLLMErrorBodyBytes))
	return err
}

func (l *LLM) Close(ctx context.Context) error {
	err := l.Base.Close(ctx)
	if closer, ok := l.client.Transport.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
	return err
}

func buildChatRequest(config LLMOptions, options llmpkg.ChatOptions, messages []openAIMessage) (map[string]any, error) {
	requestOptions := cloneModelOptions(options.Extra)
	for key, value := range config.ModelOptions {
		requestOptions[key] = value
	}
	tools, err := openAITools(options.ToolContext, config.StrictToolSchema)
	if err != nil {
		return nil, err
	}
	requestOptions = dropUnsupportedParameters(config.Model, requestOptions, len(tools) != 0)
	delete(requestOptions, "extra_headers")
	delete(requestOptions, "extra_query")
	extraBody, _ := requestOptions["extra_body"].(map[string]any)
	delete(requestOptions, "extra_body")
	delete(requestOptions, "inference_class")
	request := make(map[string]any, len(requestOptions)+6)
	request["model"] = config.Model
	request["messages"] = messages
	request["stream"] = true
	request["stream_options"] = map[string]any{"include_usage": true}
	if len(tools) != 0 {
		request["tools"] = tools
		if options.ParallelToolCalls {
			request["parallel_tool_calls"] = true
		}
		if options.ToolChoice.Kind != "" {
			request["tool_choice"] = openAIToolChoice(options.ToolChoice)
		}
	} else {
		delete(requestOptions, "tool_choice")
	}
	for key, value := range requestOptions {
		request[key] = value
	}
	for key, value := range extraBody {
		request[key] = value
	}
	return request, nil
}

func openAITools(toolContext *llmpkg.Context, strict bool) ([]map[string]any, error) {
	if toolContext == nil {
		return nil, nil
	}
	functions := toolContext.SortedFunctionTools()
	result := make([]map[string]any, 0, len(functions))
	for _, tool := range functions {
		var parameters map[string]any
		if err := json.Unmarshal(tool.Parameters(), &parameters); err != nil {
			return nil, fmt.Errorf("decode schema for tool %q: %w", tool.Name(), err)
		}
		delete(parameters, "$schema")
		function := map[string]any{
			"name": tool.Name(), "description": tool.Description(), "parameters": parameters,
		}
		if strict {
			function["strict"] = true
		}
		result = append(result, map[string]any{"type": "function", "function": function})
	}
	return result, nil
}

func openAIToolChoice(choice llmpkg.ToolChoice) any {
	if choice.Kind == llmpkg.ToolChoiceFunction {
		return map[string]any{"type": "function", "function": map[string]any{"name": choice.Name}}
	}
	return string(choice.Kind)
}

func dropUnsupportedParameters(model string, input ModelOptions, hasTools bool) ModelOptions {
	result := cloneModelOptions(input)
	modelName := model
	if index := strings.LastIndexByte(model, '/'); index >= 0 {
		modelName = model[index+1:]
	}
	for _, prefix := range [...]string{"o1", "o3", "o4", "gpt-5"} {
		if strings.HasPrefix(modelName, prefix) {
			for _, key := range [...]string{
				"temperature", "top_p", "presence_penalty", "frequency_penalty",
				"logit_bias", "logprobs", "top_logprobs", "n",
			} {
				delete(result, key)
			}
			break
		}
	}
	for _, prefix := range [...]string{"grok-4-1-fast-reasoning", "grok-4.20-0309-reasoning", "grok-4.20-multi-agent"} {
		if strings.HasPrefix(modelName, prefix) {
			delete(result, "presence_penalty")
			delete(result, "frequency_penalty")
			delete(result, "stop")
			break
		}
	}
	if hasTools && (strings.HasPrefix(modelName, "gpt-5.2") || strings.HasPrefix(modelName, "gpt-5.4")) {
		delete(result, "reasoning_effort")
	}
	return result
}

type llmStream struct {
	*llmpkg.BaseStream
	owner       *LLM
	config      LLMOptions
	connect     agents.APIConnectOptions
	requestBody []byte
	transport   requestTransport
	responded   atomic.Bool

	filters         map[int]*thinkingFilter
	calls           map[int]map[int]*toolCallBuilder
	toolExtra       map[int]map[string]any
	seenChoices     map[int]bool
	finishedChoices map[int]bool
	sawDone         bool
}

func (s *llmStream) run() {
	err := llmpkg.RunWithRetry(s.Context(), s.owner.Base, s.connect, func(ctx context.Context, _ int) error {
		s.filters = make(map[int]*thinkingFilter)
		s.calls = make(map[int]map[int]*toolCallBuilder)
		s.toolExtra = make(map[int]map[string]any)
		s.seenChoices = make(map[int]bool)
		s.finishedChoices = make(map[int]bool)
		s.sawDone = false
		return s.runOnce(ctx)
	})
	s.Finish(err)
}

func (s *llmStream) runOnce(parent context.Context) error {
	options := s.connect.Resolve()
	ctx, cancel := context.WithTimeout(parent, options.Timeout)
	defer cancel()
	token, err := AccessToken(s.config.Credentials, 0)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.config.BaseURL+"/chat/completions", bytes.NewReader(s.requestBody))
	if err != nil {
		return agents.NewAPIConnectionError("create inference LLM request", true, err)
	}
	request.Header = MetadataHeaders(metadataFor(ctx, s.config))
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "text/event-stream")
	if s.config.Provider != "" {
		request.Header.Set(ProviderHeader, s.config.Provider)
	}
	class := s.config.InferenceClass
	for key, value := range s.transport.headers {
		request.Header.Set(key, value)
	}
	if s.transport.class != "" {
		class = s.transport.class
	}
	if class != "" {
		request.Header.Set(PriorityHeader, string(class))
	}
	if len(s.transport.query) != 0 {
		query := request.URL.Query()
		for key, value := range s.transport.query {
			query.Set(key, fmt.Sprint(value))
		}
		request.URL.RawQuery = query.Encode()
	}
	response, err := s.owner.client.Do(request)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(context.Cause(ctx), context.DeadlineExceeded) {
			return agents.NewAPITimeoutError("LiveKit Inference LLM request timed out", !s.responded.Load(), err)
		}
		if cause := context.Cause(parent); cause != nil {
			return cause
		}
		return agents.NewAPIConnectionError("connect to LiveKit Inference LLM", !s.responded.Load(), err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return statusError(response, !s.responded.Load())
	}
	if err := s.consumeSSE(ctx, response.Body); err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(context.Cause(ctx), context.DeadlineExceeded) {
			return agents.NewAPITimeoutError("LiveKit Inference LLM stream timed out", !s.responded.Load(), err)
		}
		if cause := context.Cause(parent); cause != nil {
			return cause
		}
		return agents.NewAPIConnectionError("read LiveKit Inference LLM stream", !s.responded.Load(), err)
	}
	return nil
}

func (s *llmStream) consumeSSE(ctx context.Context, body io.Reader) error {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 4096), maxSSELineBytes)
	data := make([]byte, 0, 4096)
	dispatch := func() error {
		if len(data) == 0 {
			return nil
		}
		payload := bytes.TrimSuffix(data, []byte{'\n'})
		data = data[:0]
		if bytes.Equal(payload, []byte("[DONE]")) {
			s.sawDone = true
			return io.EOF
		}
		var chunk openAIChunk
		if err := json.Unmarshal(payload, &chunk); err != nil {
			return fmt.Errorf("decode OpenAI stream chunk: %w", err)
		}
		return s.emitChunk(ctx, chunk)
	}
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			if err := dispatch(); err != nil {
				if errors.Is(err, io.EOF) {
					return nil
				}
				return err
			}
			continue
		}
		if line[0] == ':' {
			continue
		}
		if bytes.HasPrefix(line, []byte("data:")) {
			part := bytes.TrimPrefix(line, []byte("data:"))
			part = bytes.TrimPrefix(part, []byte{' '})
			if len(data)+len(part)+1 > maxSSELineBytes {
				return errors.New("SSE event exceeds maximum size")
			}
			data = append(data, part...)
			data = append(data, '\n')
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if err := dispatch(); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if !s.sawDone {
		for index := range s.seenChoices {
			if !s.finishedChoices[index] {
				return io.ErrUnexpectedEOF
			}
		}
		if len(s.seenChoices) == 0 {
			return io.ErrUnexpectedEOF
		}
	}
	return nil
}

type openAIChunk struct {
	ID      string         `json:"id"`
	Choices []openAIChoice `json:"choices"`
	Usage   *struct {
		CompletionTokens int64 `json:"completion_tokens"`
		PromptTokens     int64 `json:"prompt_tokens"`
		TotalTokens      int64 `json:"total_tokens"`
		PromptDetails    *struct {
			CachedTokens int64 `json:"cached_tokens"`
		} `json:"prompt_tokens_details"`
	} `json:"usage"`
}

type openAIChoice struct {
	Index        int         `json:"index"`
	Delta        openAIDelta `json:"delta"`
	FinishReason *string     `json:"finish_reason"`
}

type openAIDelta struct {
	Content      json.RawMessage  `json:"content"`
	ToolCalls    []openAIToolCall `json:"tool_calls"`
	ExtraContent map[string]any   `json:"extra_content"`
}

type openAIToolCall struct {
	Index        int            `json:"index"`
	ID           string         `json:"id"`
	Function     openAIFunction `json:"function"`
	ExtraContent map[string]any `json:"extra_content"`
}

type openAIFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type toolCallBuilder struct {
	id, name  string
	arguments strings.Builder
	extra     map[string]any
}

func (s *llmStream) emitChunk(ctx context.Context, chunk openAIChunk) error {
	for _, choice := range chunk.Choices {
		s.seenChoices[choice.Index] = true
		filter := s.filters[choice.Index]
		if filter == nil {
			start, end := "<think>", "</think>"
			if s.config.Model == "google/gemma-4-31b-it" {
				start, end = "<|channel>thought", "<channel|>"
			}
			filter = &thinkingFilter{start: start, end: end}
			s.filters[choice.Index] = filter
		}
		content, err := flattenDeltaContent(choice.Delta.Content)
		if err != nil {
			return err
		}
		visible := filter.Feed(content, choice.FinishReason != nil)
		if len(choice.Delta.ToolCalls) != 0 {
			builders := s.calls[choice.Index]
			if builders == nil {
				builders = make(map[int]*toolCallBuilder)
				s.calls[choice.Index] = builders
			}
			for _, delta := range choice.Delta.ToolCalls {
				builder := builders[delta.Index]
				if builder == nil {
					builder = &toolCallBuilder{extra: cloneAnyMap(s.toolExtra[choice.Index])}
					builders[delta.Index] = builder
				}
				if delta.ID != "" {
					builder.id = delta.ID
				}
				if delta.Function.Name != "" {
					builder.name = delta.Function.Name
				}
				builder.arguments.WriteString(delta.Function.Arguments)
				if len(delta.ExtraContent) != 0 {
					builder.extra = cloneAnyMap(delta.ExtraContent)
					s.toolExtra[choice.Index] = cloneAnyMap(delta.ExtraContent)
				}
			}
		}
		delta := &llmpkg.ChoiceDelta{Role: llmpkg.RoleAssistant, Content: visible, Extra: cloneAnyMap(choice.Delta.ExtraContent)}
		if choice.FinishReason != nil {
			s.finishedChoices[choice.Index] = true
			builders := s.calls[choice.Index]
			indices := make([]int, 0, len(builders))
			for index := range builders {
				indices = append(indices, index)
			}
			slicesSortInts(indices)
			for _, index := range indices {
				builder := builders[index]
				call := llmpkg.NewFunctionCall(builder.id, builder.name, builder.arguments.String())
				call.GroupID = chunk.ID
				call.Extra = cloneAnyMap(builder.extra)
				call.ThoughtSignature = extractThoughtSignature(builder.extra)
				delta.ToolCalls = append(delta.ToolCalls, call)
			}
			delete(s.calls, choice.Index)
			delete(s.toolExtra, choice.Index)
		}
		if delta.Content != "" || len(delta.ToolCalls) != 0 || len(delta.Extra) != 0 {
			s.responded.Store(true)
			if err := s.Emit(ctx, llmpkg.ChatChunk{ID: chunk.ID, Delta: delta}); err != nil {
				return err
			}
		}
	}
	if chunk.Usage != nil {
		usage := &llmpkg.CompletionUsage{
			CompletionTokens: chunk.Usage.CompletionTokens, PromptTokens: chunk.Usage.PromptTokens,
			TotalTokens: chunk.Usage.TotalTokens,
		}
		if chunk.Usage.PromptDetails != nil {
			usage.PromptCachedTokens = chunk.Usage.PromptDetails.CachedTokens
		}
		if err := s.Emit(ctx, llmpkg.ChatChunk{ID: chunk.ID, Usage: usage}); err != nil {
			return err
		}
	}
	return nil
}

func flattenDeltaContent(raw json.RawMessage) (string, error) {
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return "", nil
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return text, nil
	}
	var parts []any
	if err := json.Unmarshal(raw, &parts); err != nil {
		return "", fmt.Errorf("unexpected OpenAI delta content: %w", err)
	}
	var output strings.Builder
	for _, part := range parts {
		switch part := part.(type) {
		case string:
			output.WriteString(part)
		case map[string]any:
			if value, ok := part["text"].(string); ok {
				output.WriteString(value)
			}
		}
	}
	return output.String(), nil
}

type thinkingFilter struct {
	start, end string
	buffer     string
	inside     bool
}

func (f *thinkingFilter) Feed(content string, final bool) string {
	f.buffer += content
	var visible strings.Builder
	for f.buffer != "" {
		marker := f.start
		if f.inside {
			marker = f.end
		}
		if index := strings.Index(f.buffer, marker); index >= 0 {
			if !f.inside {
				visible.WriteString(f.buffer[:index])
			}
			f.buffer = f.buffer[index+len(marker):]
			f.inside = !f.inside
			continue
		}
		keep := partialMarkerLength(f.buffer, marker)
		flush := len(f.buffer) - keep
		if !f.inside {
			visible.WriteString(f.buffer[:flush])
		}
		f.buffer = f.buffer[flush:]
		break
	}
	if final {
		if !f.inside {
			visible.WriteString(f.buffer)
		}
		f.buffer = ""
		f.inside = false
	}
	return visible.String()
}

func partialMarkerLength(value, marker string) int {
	limit := min(len(value), len(marker)-1)
	for length := limit; length > 0; length-- {
		if strings.HasSuffix(value, marker[:length]) {
			return length
		}
	}
	return 0
}

func statusError(response *http.Response, retryable bool) error {
	body, readErr := io.ReadAll(io.LimitReader(response.Body, maxLLMErrorBodyBytes+1))
	if readErr != nil {
		body = []byte(readErr.Error())
	}
	truncated := len(body) > maxLLMErrorBodyBytes
	if truncated {
		body = body[:maxLLMErrorBodyBytes]
	}
	var decoded any
	if json.Unmarshal(body, &decoded) != nil {
		decoded = string(body)
	}
	if truncated {
		decoded = map[string]any{"truncated": true, "body": decoded}
	}
	requestID := response.Header.Get("x-request-id")
	if requestID == "" {
		requestID = response.Header.Get("x-livekit-request-id")
	}
	return agents.NewAPIStatusError(response.Status, response.StatusCode, requestID, decoded, retryable, nil)
}

func metadataFor(ctx context.Context, options LLMOptions) RequestMetadata {
	if options.Metadata == nil {
		return RequestMetadata{}
	}
	return options.Metadata(ctx)
}

func cloneLLMOptions(options LLMOptions) LLMOptions {
	options.ModelOptions = cloneModelOptions(options.ModelOptions)
	return options
}

type requestTransport struct {
	headers map[string]string
	query   map[string]any
	class   Class
}

func extractRequestTransport(config LLMOptions, options llmpkg.ChatOptions) requestTransport {
	merged := cloneModelOptions(options.Extra)
	for key, value := range config.ModelOptions {
		merged[key] = value
	}
	result := requestTransport{}
	switch headers := merged["extra_headers"].(type) {
	case map[string]string:
		result.headers = make(map[string]string, len(headers))
		for key, value := range headers {
			result.headers[key] = value
		}
	case map[string]any:
		result.headers = make(map[string]string, len(headers))
		for key, value := range headers {
			if text, ok := value.(string); ok {
				result.headers[key] = text
			}
		}
	}
	if query, ok := merged["extra_query"].(map[string]any); ok {
		result.query = cloneAnyMap(query)
	}
	if class, ok := merged["inference_class"].(Class); ok {
		result.class = class
	} else if class, ok := merged["inference_class"].(string); ok {
		result.class = Class(class)
	}
	return result
}

func cloneModelOptions(input map[string]any) ModelOptions {
	if input == nil {
		return ModelOptions{}
	}
	output := make(ModelOptions, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func cloneAnyMap(input map[string]any) map[string]any {
	if len(input) == 0 {
		return nil
	}
	output := make(map[string]any, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func extractThoughtSignature(extra map[string]any) string {
	google, _ := extra["google"].(map[string]any)
	for _, key := range [...]string{"thoughtSignature", "thought_signature"} {
		if value, _ := google[key].(string); value != "" {
			return value
		}
	}
	return ""
}

func slicesSortInts(values []int) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j] < values[j-1]; j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
}
