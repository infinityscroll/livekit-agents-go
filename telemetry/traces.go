// SPDX-License-Identifier: Apache-2.0

package telemetry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/infinityscroll/livekit-agents-go/metrics"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const (
	InstrumentationName      = "livekit-agents"
	RedactedExceptionMessage = "exception details redacted"
)

type dynamicTracer struct {
	mu       sync.RWMutex
	provider trace.TracerProvider
	tracer   trace.Tracer
}

var frameworkTracer dynamicTracer

// Tracer returns the current framework tracer. The OpenTelemetry global
// provider is resolved lazily so importing telemetry has no SDK startup cost.
func Tracer() trace.Tracer {
	frameworkTracer.mu.RLock()
	tracer := frameworkTracer.tracer
	frameworkTracer.mu.RUnlock()
	if tracer != nil {
		return tracer
	}
	frameworkTracer.mu.Lock()
	defer frameworkTracer.mu.Unlock()
	if frameworkTracer.tracer == nil {
		frameworkTracer.provider = otel.GetTracerProvider()
		frameworkTracer.tracer = frameworkTracer.provider.Tracer(InstrumentationName)
	}
	return frameworkTracer.tracer
}

func TracerProvider() trace.TracerProvider {
	_ = Tracer()
	frameworkTracer.mu.RLock()
	provider := frameworkTracer.provider
	frameworkTracer.mu.RUnlock()
	return provider
}

// SetTracerProvider updates only the Agents framework tracer. Applications can
// separately call otel.SetTracerProvider when they want a process-wide change.
func SetTracerProvider(provider trace.TracerProvider) error {
	return SetTracerProviderWithOptions(provider, SetTracerProviderOptions{})
}

func setFrameworkTracerProvider(provider trace.TracerProvider) error {
	if provider == nil || reflect.ValueOf(provider).Kind() == reflect.Pointer && reflect.ValueOf(provider).IsNil() {
		return errors.New("telemetry: tracer provider is required")
	}
	frameworkTracer.mu.Lock()
	frameworkTracer.provider = provider
	frameworkTracer.tracer = provider.Tracer(InstrumentationName)
	frameworkTracer.mu.Unlock()
	return nil
}

type StartSpanOptions struct {
	Name       string
	Attributes map[string]any
	StartTime  time.Time
}

func StartSpan(ctx context.Context, options StartSpanOptions) (context.Context, trace.Span, error) {
	if options.Name == "" {
		return ctx, nil, errors.New("telemetry: span name is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	attributes, err := traceAttributes(options.Attributes)
	if err != nil {
		return ctx, nil, err
	}
	spanOptions := make([]trace.SpanStartOption, 0, 2)
	if len(attributes) != 0 {
		spanOptions = append(spanOptions, trace.WithAttributes(attributes...))
	}
	if !options.StartTime.IsZero() {
		spanOptions = append(spanOptions, trace.WithTimestamp(options.StartTime))
	}
	spanContext, span := Tracer().Start(ctx, options.Name, spanOptions...)
	return spanContext, span, nil
}

// WithSpan creates an active span in the supplied context, runs callback, and
// always ends the span. Callback errors are recorded without leaking them into
// span names or event names.
func WithSpan(ctx context.Context, options StartSpanOptions, callback func(context.Context, trace.Span) error) error {
	if callback == nil {
		return errors.New("telemetry: span callback is required")
	}
	spanContext, span, err := StartSpan(ctx, options)
	if err != nil {
		return err
	}
	defer span.End()
	if err := callback(spanContext, span); err != nil {
		RecordException(span, err, RecordExceptionOptions{})
		return err
	}
	return nil
}

type RecordExceptionOptions struct {
	Redacted bool
}

func RecordException(span trace.Span, err error, options RecordExceptionOptions) {
	if span == nil || err == nil {
		return
	}
	errorType := reflect.TypeOf(err).String()
	if options.Redacted {
		attributes := []attribute.KeyValue{
			attribute.String(AttrExceptionType, errorType),
			attribute.String(AttrExceptionMessage, RedactedExceptionMessage),
		}
		span.AddEvent("exception", trace.WithAttributes(attributes...))
		span.SetStatus(codes.Error, RedactedExceptionMessage)
		span.SetAttributes(attributes...)
		return
	}
	span.RecordError(err, trace.WithStackTrace(true))
	span.SetStatus(codes.Error, err.Error())
	span.SetAttributes(
		attribute.String(AttrExceptionType, errorType),
		attribute.String(AttrExceptionMessage, err.Error()),
		attribute.String(AttrExceptionTrace, fmt.Sprintf("%+v", err)),
	)
}

func RecordRealtimeMetrics(ctx context.Context, span trace.Span, value metrics.Realtime) error {
	if span == nil {
		return errors.New("telemetry: span is required")
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("telemetry: encode realtime metrics: %w", err)
	}
	attributes := []attribute.KeyValue{
		attribute.String(AttrGenAIRequestModel, firstNonemptyString(value.Label, "unknown")),
		attribute.String(AttrRealtimeModelMetrics, string(encoded)),
		attribute.Int64(AttrGenAIUsageInputTokens, value.InputTokens),
		attribute.Int64(AttrGenAIUsageOutputTokens, value.OutputTokens),
		attribute.Int64(AttrGenAIUsageInputTextTokens, value.InputDetails.Text),
		attribute.Int64(AttrGenAIUsageInputAudioTokens, value.InputDetails.Audio),
		attribute.Int64(AttrGenAIUsageInputCached, value.InputDetails.Cached),
		attribute.Int64(AttrGenAIUsageOutputTextTokens, value.OutputDetails.Text),
		attribute.Int64(AttrGenAIUsageOutputAudio, value.OutputDetails.Audio),
	}
	if value.TimeToFirstToken >= 0 && !value.Timestamp.IsZero() {
		attributes = append(attributes, attribute.String(AttrLangfuseCompletionStartTime,
			value.Timestamp.Add(value.TimeToFirstToken).UTC().Format(time.RFC3339Nano)))
	}
	if span.IsRecording() {
		span.SetAttributes(attributes...)
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = trace.ContextWithSpan(ctx, span)
	_, child := Tracer().Start(ctx, "realtime_metrics", trace.WithAttributes(attributes...))
	child.End()
	return nil
}

func traceAttributes(values map[string]any) ([]attribute.KeyValue, error) {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]attribute.KeyValue, 0, len(keys))
	for _, key := range keys {
		value, err := traceAttribute(key, values[key])
		if err != nil {
			return nil, fmt.Errorf("telemetry: span attribute %q: %w", key, err)
		}
		result = append(result, value)
	}
	return result, nil
}

func traceAttribute(key string, value any) (attribute.KeyValue, error) {
	switch current := value.(type) {
	case nil:
		return attribute.String(key, ""), nil
	case string:
		return attribute.String(key, current), nil
	case bool:
		return attribute.Bool(key, current), nil
	case int:
		return attribute.Int(key, current), nil
	case int64:
		return attribute.Int64(key, current), nil
	case float64:
		return attribute.Float64(key, current), nil
	case []string:
		return attribute.StringSlice(key, current), nil
	case []bool:
		return attribute.BoolSlice(key, current), nil
	case []int:
		return attribute.IntSlice(key, current), nil
	case []int64:
		return attribute.Int64Slice(key, current), nil
	case []float64:
		return attribute.Float64Slice(key, current), nil
	case time.Duration:
		return attribute.Float64(key, current.Seconds()), nil
	case time.Time:
		return attribute.String(key, current.UTC().Format(time.RFC3339Nano)), nil
	default:
		encoded, err := json.Marshal(value)
		if err != nil {
			return attribute.KeyValue{}, err
		}
		return attribute.String(key, string(encoded)), nil
	}
}

func firstNonemptyString(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
