// SPDX-License-Identifier: Apache-2.0

package telemetry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/livekit/protocol/auth"
)

const (
	defaultMaxLogExportBytes = 8 << 20
	defaultMaxErrorBodyBytes = 64 << 10
	maxOTLPValueDepth        = 64
)

var defaultTelemetryHTTPClient = sync.OnceValue(func() *http.Client {
	return &http.Client{Timeout: 15 * time.Second}
})

type SimpleLogRecord struct {
	Body           string
	Timestamp      time.Time
	TimestampMS    float64 // TypeScript compatibility; used when Timestamp is zero.
	Attributes     map[string]any
	SeverityNumber int32
	SeverityText   string
	TraceID        string
	SpanID         string
}

type SimpleOTLPHTTPLogExporterConfig struct {
	CloudHostname      string
	Endpoint           string
	ResourceAttributes map[string]any
	ScopeName          string
	ScopeAttributes    map[string]any
	APIKey             agents.SecretString
	APISecret          agents.SecretString
	HTTPClient         *http.Client
	UploadGate         *UploadGate
	MaxRequestBytes    int64
	MaxErrorBodyBytes  int64
}

type SimpleOTLPHTTPLogExporter struct {
	config SimpleOTLPHTTPLogExporterConfig

	tokenMu sync.Mutex
	token   string
}

func NewSimpleOTLPHTTPLogExporter(config SimpleOTLPHTTPLogExporterConfig) (*SimpleOTLPHTTPLogExporter, error) {
	if config.ScopeName == "" {
		return nil, errors.New("telemetry: OTLP scope name is required")
	}
	if config.Endpoint == "" {
		if config.CloudHostname == "" {
			return nil, errors.New("telemetry: cloud hostname or endpoint is required")
		}
		if strings.ContainsAny(config.CloudHostname, "/?#") {
			return nil, errors.New("telemetry: cloud hostname must not contain a path, query, or fragment")
		}
		config.Endpoint = "https://" + config.CloudHostname + "/observability/logs/otlp/v0"
	}
	parsed, err := url.Parse(config.Endpoint)
	if err != nil || (parsed.Scheme != "https" && parsed.Scheme != "http") || parsed.Host == "" {
		return nil, fmt.Errorf("telemetry: invalid OTLP endpoint %q", config.Endpoint)
	}
	if parsed.User != nil {
		return nil, errors.New("telemetry: OTLP endpoint credentials in URL are not allowed")
	}
	if config.HTTPClient == nil {
		config.HTTPClient = defaultTelemetryHTTPClient()
	}
	if config.UploadGate == nil {
		config.UploadGate = DefaultUploadGate
	}
	if config.MaxRequestBytes <= 0 {
		config.MaxRequestBytes = defaultMaxLogExportBytes
	}
	if config.MaxErrorBodyBytes <= 0 {
		config.MaxErrorBodyBytes = defaultMaxErrorBodyBytes
	}
	config.ResourceAttributes = cloneAttributes(config.ResourceAttributes)
	config.ScopeAttributes = cloneAttributes(config.ScopeAttributes)
	return &SimpleOTLPHTTPLogExporter{config: config}, nil
}

type LogExportError struct {
	StatusCode int
	Status     string
	Body       string
}

func (e *LogExportError) Error() string {
	if e == nil {
		return "OTLP log export failed"
	}
	if e.Body == "" {
		return fmt.Sprintf("OTLP log export failed: %s", e.Status)
	}
	return fmt.Sprintf("OTLP log export failed: %s - %s", e.Status, e.Body)
}

func (e *SimpleOTLPHTTPLogExporter) Export(ctx context.Context, records []SimpleLogRecord) error {
	if e == nil || len(records) == 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	gate := e.config.UploadGate
	if gate.Disabled() {
		return nil
	}
	generation := gate.Generation()
	token, err := e.ensureToken()
	if err != nil {
		return err
	}
	payload, err := e.buildPayload(records)
	if err != nil {
		return err
	}
	if int64(len(payload)) > e.config.MaxRequestBytes {
		return fmt.Errorf("telemetry: OTLP log payload is %d bytes, limit is %d", len(payload), e.config.MaxRequestBytes)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, e.config.Endpoint, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("telemetry: build OTLP request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	response, err := e.config.HTTPClient.Do(request)
	if err != nil {
		return fmt.Errorf("telemetry: OTLP log request: %w", err)
	}
	defer response.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(response.Body, e.config.MaxErrorBodyBytes+1))
	if readErr != nil {
		return fmt.Errorf("telemetry: read OTLP response: %w", readErr)
	}
	if gate.IsDisabledResponse(response.StatusCode, body) {
		gate.Disable(generation)
		return nil
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		truncated := int64(len(body)) > e.config.MaxErrorBodyBytes
		if truncated {
			body = body[:e.config.MaxErrorBodyBytes]
		}
		message := strings.TrimSpace(string(body))
		if truncated {
			message += "…"
		}
		return &LogExportError{StatusCode: response.StatusCode, Status: response.Status, Body: message}
	}
	return nil
}

func (e *SimpleOTLPHTTPLogExporter) ensureToken() (string, error) {
	e.tokenMu.Lock()
	defer e.tokenMu.Unlock()
	if e.token != "" {
		return e.token, nil
	}
	apiKey := e.config.APIKey.Reveal()
	if apiKey == "" {
		apiKey = os.Getenv("LIVEKIT_API_KEY")
	}
	apiSecret := e.config.APISecret.Reveal()
	if apiSecret == "" {
		apiSecret = os.Getenv("LIVEKIT_API_SECRET")
	}
	if apiKey == "" || apiSecret == "" {
		return "", errors.New("telemetry: LIVEKIT_API_KEY and LIVEKIT_API_SECRET must be set")
	}
	token, err := auth.NewAccessToken(apiKey, apiSecret).
		SetIdentity("livekit-agents-telemetry").
		SetValidFor(6 * time.Hour).
		SetObservabilityGrant(&auth.ObservabilityGrant{Write: true}).
		ToJWT()
	if err != nil {
		return "", fmt.Errorf("telemetry: create observability token: %w", err)
	}
	e.token = token
	return token, nil
}

func (e *SimpleOTLPHTTPLogExporter) buildPayload(records []SimpleLogRecord) ([]byte, error) {
	resource := cloneAttributes(e.config.ResourceAttributes)
	if _, exists := resource["service.name"]; !exists {
		resource["service.name"] = "livekit-agents"
	}
	now := time.Now()
	logRecords := make([]any, 0, len(records))
	for _, record := range records {
		timestamp := record.Timestamp
		if timestamp.IsZero() && !math.IsNaN(record.TimestampMS) && !math.IsInf(record.TimestampMS, 0) && record.TimestampMS != 0 {
			seconds, fractional := math.Modf(record.TimestampMS / 1000)
			timestamp = time.Unix(int64(seconds), int64(fractional*float64(time.Second)))
		}
		if timestamp.IsZero() {
			timestamp = now
		}
		severityText := record.SeverityText
		if severityText == "" {
			severityText = "unspecified"
		}
		attributes, err := convertOTLPAttributes(record.Attributes)
		if err != nil {
			return nil, err
		}
		logRecords = append(logRecords, map[string]any{
			"timeUnixNano":         strconv.FormatInt(timestamp.UnixNano(), 10),
			"observedTimeUnixNano": strconv.FormatInt(now.UnixNano(), 10),
			"severityNumber":       record.SeverityNumber,
			"severityText":         severityText,
			"body":                 map[string]any{"stringValue": record.Body},
			"attributes":           attributes,
			"traceId":              record.TraceID,
			"spanId":               record.SpanID,
		})
	}
	resourceAttributes, err := convertOTLPAttributes(resource)
	if err != nil {
		return nil, err
	}
	scopeAttributes, err := convertOTLPAttributes(e.config.ScopeAttributes)
	if err != nil {
		return nil, err
	}
	payload := map[string]any{"resourceLogs": []any{map[string]any{
		"resource": map[string]any{"attributes": resourceAttributes},
		"scopeLogs": []any{map[string]any{
			"scope":      map[string]any{"name": e.config.ScopeName, "attributes": scopeAttributes},
			"logRecords": logRecords,
		}},
	}}}
	return json.Marshal(payload)
}

func cloneAttributes(input map[string]any) map[string]any {
	if input == nil {
		return make(map[string]any)
	}
	result := make(map[string]any, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}

type otlpKeyValue struct {
	Key   string `json:"key"`
	Value any    `json:"value"`
}

func convertOTLPAttributes(attributes map[string]any) ([]otlpKeyValue, error) {
	keys := make([]string, 0, len(attributes))
	for key := range attributes {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]otlpKeyValue, 0, len(keys))
	for _, key := range keys {
		value, err := convertOTLPValue(attributes[key], key, 0)
		if err != nil {
			return nil, fmt.Errorf("telemetry: encode attribute %q: %w", key, err)
		}
		result = append(result, otlpKeyValue{Key: key, Value: value})
	}
	return result, nil
}

func convertOTLPValue(value any, path string, depth int) (any, error) {
	if depth > maxOTLPValueDepth {
		return nil, errors.New("attribute nesting exceeds 64 levels")
	}
	if value == nil || typedNil(value) {
		return map[string]any{"stringValue": ""}, nil
	}
	switch current := value.(type) {
	case string:
		return map[string]any{"stringValue": current}, nil
	case bool:
		return map[string]any{"boolValue": current}, nil
	case json.Number:
		if integer, err := current.Int64(); err == nil && !forceDouble(path) {
			return map[string]any{"intValue": strconv.FormatInt(integer, 10)}, nil
		}
		double, err := current.Float64()
		if err != nil || math.IsNaN(double) || math.IsInf(double, 0) {
			return nil, fmt.Errorf("invalid number %q", current)
		}
		return map[string]any{"doubleValue": double}, nil
	case int:
		return map[string]any{"intValue": strconv.Itoa(current)}, nil
	case int8:
		return map[string]any{"intValue": strconv.FormatInt(int64(current), 10)}, nil
	case int16:
		return map[string]any{"intValue": strconv.FormatInt(int64(current), 10)}, nil
	case int32:
		return map[string]any{"intValue": strconv.FormatInt(int64(current), 10)}, nil
	case int64:
		return map[string]any{"intValue": strconv.FormatInt(current, 10)}, nil
	case uint:
		return map[string]any{"intValue": strconv.FormatUint(uint64(current), 10)}, nil
	case uint8:
		return map[string]any{"intValue": strconv.FormatUint(uint64(current), 10)}, nil
	case uint16:
		return map[string]any{"intValue": strconv.FormatUint(uint64(current), 10)}, nil
	case uint32:
		return map[string]any{"intValue": strconv.FormatUint(uint64(current), 10)}, nil
	case uint64:
		return map[string]any{"intValue": strconv.FormatUint(current, 10)}, nil
	case float32:
		return numericOTLPValue(float64(current), path)
	case float64:
		return numericOTLPValue(current, path)
	case []any:
		values := make([]any, len(current))
		for index, item := range current {
			converted, err := convertOTLPValue(item, fmt.Sprintf("%s[%d]", path, index), depth+1)
			if err != nil {
				return nil, err
			}
			values[index] = converted
		}
		return map[string]any{"arrayValue": map[string]any{"values": values}}, nil
	case map[string]any:
		values, err := convertOTLPAttributesDepth(current, path, depth+1)
		if err != nil {
			return nil, err
		}
		return map[string]any{"kvlistValue": map[string]any{"values": values}}, nil
	}

	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var normalized any
	if err := decoder.Decode(&normalized); err != nil {
		return nil, err
	}
	if _, same := normalized.(string); same && reflect.TypeOf(value).Kind() != reflect.String {
		return map[string]any{"stringValue": fmt.Sprint(value)}, nil
	}
	return convertOTLPValue(normalized, path, depth+1)
}

func convertOTLPAttributesDepth(attributes map[string]any, parent string, depth int) ([]otlpKeyValue, error) {
	keys := make([]string, 0, len(attributes))
	for key := range attributes {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]otlpKeyValue, 0, len(keys))
	for _, key := range keys {
		path := key
		if parent != "" {
			path = parent + "." + key
		}
		value, err := convertOTLPValue(attributes[key], path, depth)
		if err != nil {
			return nil, err
		}
		result = append(result, otlpKeyValue{Key: key, Value: value})
	}
	return result, nil
}

func numericOTLPValue(value float64, path string) (any, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return nil, errors.New("NaN and infinity are not valid OTLP attribute values")
	}
	if !forceDouble(path) && value == math.Trunc(value) && value >= math.MinInt64 && value <= math.MaxInt64 {
		return map[string]any{"intValue": strconv.FormatInt(int64(value), 10)}, nil
	}
	return map[string]any{"doubleValue": value}, nil
}

func forceDouble(path string) bool {
	leaf := path
	if index := strings.LastIndexByte(leaf, '.'); index >= 0 {
		leaf = leaf[index+1:]
	}
	if index := strings.IndexByte(leaf, '['); index >= 0 {
		leaf = leaf[:index]
	}
	switch leaf {
	case "transcriptConfidence", "transcriptionDelay", "endOfTurnDelay",
		"onUserTurnCompletedDelay", "llmNodeTtft", "ttsNodeTtfb", "e2eLatency":
		return true
	default:
		return false
	}
}

func typedNil(value any) bool {
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}
