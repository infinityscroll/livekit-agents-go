// SPDX-License-Identifier: Apache-2.0

package telemetry

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/livekit/protocol/auth"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const (
	DefaultCloudTraceQueueSize = 2048
	DefaultCloudTraceBatchSize = 512
	DefaultCloudTraceBatchWait = 5 * time.Second
	DefaultCloudExportTimeout  = 10 * time.Second
)

// CloudSpanProcessorOptions is passed to a custom cloud processor factory.
// Exporter already enforces the recording-disabled upload gate.
type CloudSpanProcessorOptions struct {
	URL      string
	Headers  map[string]string
	Exporter sdktrace.SpanExporter
}

// SetTracerProviderOptions describes how an application-owned provider can
// accept processors that are attached after construction. Go's SDK provider
// already supports this through RegisterSpanProcessor and
// UnregisterSpanProcessor; custom providers can expose equivalent callbacks.
type SetTracerProviderOptions struct {
	Metadata                 map[string]any
	RegisterSpanProcessor    func(sdktrace.SpanProcessor)
	UnregisterSpanProcessor  func(sdktrace.SpanProcessor)
	CreateCloudSpanProcessor func(CloudSpanProcessorOptions) sdktrace.SpanProcessor
}

type configuredTracerProvider struct {
	provider   trace.TracerProvider
	register   func(sdktrace.SpanProcessor)
	unregister func(sdktrace.SpanProcessor)
	factory    func(CloudSpanProcessorOptions) sdktrace.SpanProcessor
}

var configuredProvider struct {
	sync.RWMutex
	value configuredTracerProvider
}

// SetTracerProviderWithOptions updates the framework tracer and records the
// processor attachment seam used by SetupCloudTracer. It never changes the
// process-global OTel provider.
func SetTracerProviderWithOptions(provider trace.TracerProvider, options SetTracerProviderOptions) error {
	if provider == nil || isNilTracerProvider(provider) {
		return errors.New("telemetry: tracer provider is required")
	}
	register := options.RegisterSpanProcessor
	unregister := options.UnregisterSpanProcessor
	if concrete, ok := provider.(*sdktrace.TracerProvider); ok {
		if register == nil {
			register = concrete.RegisterSpanProcessor
		}
		if unregister == nil {
			unregister = concrete.UnregisterSpanProcessor
		}
	}
	if len(options.Metadata) != 0 {
		if register == nil {
			return errors.New("telemetry: provider metadata requires RegisterSpanProcessor")
		}
		processor, err := NewMetadataSpanProcessor(options.Metadata)
		if err != nil {
			return err
		}
		register(processor)
	}
	if err := setFrameworkTracerProvider(provider); err != nil {
		return err
	}
	configuredProvider.Lock()
	configuredProvider.value = configuredTracerProvider{
		provider: provider, register: register, unregister: unregister,
		factory: options.CreateCloudSpanProcessor,
	}
	configuredProvider.Unlock()
	return nil
}

func isNilTracerProvider(provider trace.TracerProvider) bool {
	value := reflect.ValueOf(provider)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// MetadataSpanProcessor adds immutable session metadata synchronously when a
// span starts. The hot path is only one SetAttributes call.
type MetadataSpanProcessor struct {
	attributes []attribute.KeyValue
}

func NewMetadataSpanProcessor(metadata map[string]any) (*MetadataSpanProcessor, error) {
	values, err := traceAttributes(metadata)
	if err != nil {
		return nil, err
	}
	return &MetadataSpanProcessor{attributes: values}, nil
}

func (p *MetadataSpanProcessor) OnStart(_ context.Context, span sdktrace.ReadWriteSpan) {
	if p != nil && len(p.attributes) != 0 {
		span.SetAttributes(p.attributes...)
	}
}
func (*MetadataSpanProcessor) OnEnd(sdktrace.ReadOnlySpan)      {}
func (*MetadataSpanProcessor) Shutdown(context.Context) error   { return nil }
func (*MetadataSpanProcessor) ForceFlush(context.Context) error { return nil }

// FanoutSpanProcessor lets a provider attach processors after construction.
// Add and callbacks are concurrency-safe; callbacks run over a snapshot so a
// processor can add another processor without deadlocking.
type FanoutSpanProcessor struct {
	mu         sync.RWMutex
	processors []sdktrace.SpanProcessor
	closed     bool
}

func (p *FanoutSpanProcessor) Add(processor sdktrace.SpanProcessor) error {
	if processor == nil || reflect.ValueOf(processor).Kind() == reflect.Pointer && reflect.ValueOf(processor).IsNil() {
		return errors.New("telemetry: span processor is required")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return errors.New("telemetry: fanout span processor is closed")
	}
	p.processors = append(p.processors, processor)
	return nil
}

func (p *FanoutSpanProcessor) snapshot() []sdktrace.SpanProcessor {
	p.mu.RLock()
	result := append([]sdktrace.SpanProcessor(nil), p.processors...)
	closed := p.closed
	p.mu.RUnlock()
	if closed {
		return nil
	}
	return result
}

func (p *FanoutSpanProcessor) OnStart(ctx context.Context, span sdktrace.ReadWriteSpan) {
	for _, processor := range p.snapshot() {
		processor.OnStart(ctx, span)
	}
}

func (p *FanoutSpanProcessor) OnEnd(span sdktrace.ReadOnlySpan) {
	for _, processor := range p.snapshot() {
		processor.OnEnd(span)
	}
}

func (p *FanoutSpanProcessor) ForceFlush(ctx context.Context) error {
	var errs []error
	for _, processor := range p.snapshot() {
		if err := processor.ForceFlush(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (p *FanoutSpanProcessor) Shutdown(ctx context.Context) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	processors := append([]sdktrace.SpanProcessor(nil), p.processors...)
	p.processors = nil
	p.mu.Unlock()
	var errs []error
	for _, processor := range processors {
		if err := processor.Shutdown(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

type SetupCloudTracerOptions struct {
	RoomID        string
	JobID         string
	CloudHostname string
	AgentName     string
	EnableTraces  *bool
	EnableLogs    *bool
	Metadata      map[string]any

	APIKey     agents.SecretString
	APISecret  agents.SecretString
	HTTPClient *http.Client
	UploadGate *UploadGate

	TraceQueueSize int
	TraceBatchSize int
	TraceBatchWait time.Duration
	ExportTimeout  time.Duration
	// SetGlobalProvider is opt-in because libraries should not overwrite an
	// application's process-global provider by surprise.
	SetGlobalProvider bool
}

// CloudTelemetry owns the processors/exporters created for one setup call.
// Shutdown is idempotent and must be called at job/session teardown.
type CloudTelemetry struct {
	provider    *sdktrace.TracerProvider
	processor   sdktrace.SpanProcessor
	unregister  func(sdktrace.SpanProcessor)
	attached    []sdktrace.SpanProcessor
	logExporter *SimpleOTLPHTTPLogExporter
	gate        *UploadGate

	closeOnce sync.Once
	closeErr  error
}

func (c *CloudTelemetry) LogExporter() *SimpleOTLPHTTPLogExporter {
	if c == nil {
		return nil
	}
	return c.logExporter
}

func (c *CloudTelemetry) ForceFlush(ctx context.Context) error {
	if c == nil || c.processor == nil {
		return nil
	}
	return c.processor.ForceFlush(ctx)
}

func (c *CloudTelemetry) Shutdown(ctx context.Context) error {
	if c == nil {
		return nil
	}
	c.closeOnce.Do(func() {
		if ctx == nil {
			ctx = context.Background()
		}
		if c.provider != nil {
			c.closeErr = c.provider.Shutdown(ctx)
			return
		}
		if c.unregister != nil && c.processor != nil {
			for index := len(c.attached) - 1; index >= 0; index-- {
				c.unregister(c.attached[index])
			}
			return
		}
		if c.processor != nil {
			c.closeErr = c.processor.Shutdown(ctx)
		}
	})
	return c.closeErr
}

// SetupCloudTracer installs LiveKit Cloud OTLP/HTTP protobuf trace export and
// constructs the matching raw-OTLP log exporter. No work occurs at package
// import time; credentials, DNS, and goroutines are touched only here.
func SetupCloudTracer(ctx context.Context, options SetupCloudTracerOptions) (*CloudTelemetry, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if options.RoomID == "" || options.JobID == "" {
		return nil, errors.New("telemetry: cloud tracer requires room and job IDs")
	}
	hostname, err := validateCloudHostname(options.CloudHostname)
	if err != nil {
		return nil, err
	}
	enableTraces := options.EnableTraces == nil || *options.EnableTraces
	enableLogs := options.EnableLogs == nil || *options.EnableLogs
	if !enableTraces && !enableLogs {
		return &CloudTelemetry{}, nil
	}
	gate := options.UploadGate
	if gate == nil {
		gate = DefaultUploadGate
	}
	gate.Reset()
	token, err := cloudObservabilityToken(options.APIKey, options.APISecret)
	if err != nil {
		return nil, err
	}
	baseMetadata := map[string]any{"room_id": options.RoomID, "job_id": options.JobID}
	if options.AgentName != "" {
		baseMetadata[AttrAgentName] = options.AgentName
	}
	if value := os.Getenv("LIVEKIT_AGENT_ID"); value != "" {
		baseMetadata[AttrCloudAgentID] = value
	}
	if value := os.Getenv("LIVEKIT_AGENT_DEPLOYMENT"); value != "" {
		baseMetadata[AttrDeploymentID] = value
	}
	sessionMetadata := cloneAttributes(baseMetadata)
	for key, value := range options.Metadata {
		sessionMetadata[key] = value
	}
	runtime := &CloudTelemetry{gate: gate}
	if enableLogs {
		runtime.logExporter, err = NewSimpleOTLPHTTPLogExporter(SimpleOTLPHTTPLogExporterConfig{
			CloudHostname: hostname, ScopeName: InstrumentationName,
			ResourceAttributes: baseMetadata, ScopeAttributes: sessionMetadata,
			APIKey: options.APIKey, APISecret: options.APISecret,
			HTTPClient: options.HTTPClient, UploadGate: gate,
		})
		if err != nil {
			return nil, err
		}
	}
	if !enableTraces {
		return runtime, nil
	}

	endpoint := "https://" + hostname + "/observability/traces/otlp/v0"
	client := gatedHTTPClient(options.HTTPClient, gate)
	exporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpointURL(endpoint),
		otlptracehttp.WithHeaders(map[string]string{"Authorization": "Bearer " + token}),
		otlptracehttp.WithCompression(otlptracehttp.GzipCompression),
		otlptracehttp.WithHTTPClient(client),
		otlptracehttp.WithMaxRequestSize(defaultMaxLogExportBytes),
	)
	if err != nil {
		return nil, fmt.Errorf("telemetry: create cloud trace exporter: %w", err)
	}
	gatedExporter := &uploadGateSpanExporter{delegate: exporter, gate: gate, generation: gate.Generation()}
	processorOptions := CloudSpanProcessorOptions{
		URL: endpoint, Headers: map[string]string{"Authorization": "Bearer " + token}, Exporter: gatedExporter,
	}

	configuredProvider.RLock()
	configured := configuredProvider.value
	configuredProvider.RUnlock()
	if configured.provider != nil && configured.register != nil {
		metadataProcessor, metadataErr := NewMetadataSpanProcessor(sessionMetadata)
		if metadataErr != nil {
			_ = exporter.Shutdown(ctx)
			return nil, metadataErr
		}
		configured.register(metadataProcessor)
		processor := makeCloudSpanProcessor(options, processorOptions, configured.factory)
		if processor == nil {
			if configured.unregister != nil {
				configured.unregister(metadataProcessor)
			}
			_ = exporter.Shutdown(ctx)
			return nil, errors.New("telemetry: cloud span processor factory returned nil")
		}
		configured.register(processor)
		runtime.processor = processor
		if configured.unregister != nil {
			runtime.unregister = configured.unregister
			runtime.attached = []sdktrace.SpanProcessor{metadataProcessor, processor}
		}
		return runtime, nil
	}

	metadataProcessor, err := NewMetadataSpanProcessor(sessionMetadata)
	if err != nil {
		_ = exporter.Shutdown(ctx)
		return nil, err
	}
	processor := makeCloudSpanProcessor(options, processorOptions, nil)
	attributes, err := traceAttributes(baseMetadata)
	if err != nil {
		_ = processor.Shutdown(ctx)
		return nil, err
	}
	attributes = append(attributes, attribute.String("service.name", "livekit-agents"))
	resources, err := resource.New(ctx,
		resource.WithFromEnv(), resource.WithTelemetrySDK(), resource.WithProcess(),
		resource.WithAttributes(attributes...),
	)
	if err != nil {
		_ = processor.Shutdown(ctx)
		return nil, fmt.Errorf("telemetry: create trace resource: %w", err)
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithResource(resources), sdktrace.WithSpanProcessor(metadataProcessor), sdktrace.WithSpanProcessor(processor),
	)
	if err := setFrameworkTracerProvider(provider); err != nil {
		_ = provider.Shutdown(ctx)
		return nil, err
	}
	if options.SetGlobalProvider {
		otel.SetTracerProvider(provider)
	}
	runtime.provider, runtime.processor = provider, processor
	return runtime, nil
}

func makeCloudSpanProcessor(options SetupCloudTracerOptions, cloud CloudSpanProcessorOptions, factory func(CloudSpanProcessorOptions) sdktrace.SpanProcessor) sdktrace.SpanProcessor {
	if factory != nil {
		return factory(cloud)
	}
	queueSize := options.TraceQueueSize
	if queueSize <= 0 {
		queueSize = DefaultCloudTraceQueueSize
	}
	batchSize := options.TraceBatchSize
	if batchSize <= 0 {
		batchSize = DefaultCloudTraceBatchSize
	}
	if batchSize > queueSize {
		batchSize = queueSize
	}
	wait := options.TraceBatchWait
	if wait <= 0 {
		wait = DefaultCloudTraceBatchWait
	}
	exportTimeout := options.ExportTimeout
	if exportTimeout <= 0 {
		exportTimeout = DefaultCloudExportTimeout
	}
	return sdktrace.NewBatchSpanProcessor(cloud.Exporter,
		sdktrace.WithMaxQueueSize(queueSize), sdktrace.WithMaxExportBatchSize(batchSize),
		sdktrace.WithBatchTimeout(wait), sdktrace.WithExportTimeout(exportTimeout),
	)
}

func validateCloudHostname(hostname string) (string, error) {
	hostname = strings.TrimSpace(hostname)
	if hostname == "" {
		return "", errors.New("telemetry: cloud hostname is required")
	}
	if strings.Contains(hostname, "://") {
		parsed, err := url.Parse(hostname)
		if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.Path != "" && parsed.Path != "/" || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.User != nil {
			return "", fmt.Errorf("telemetry: invalid cloud hostname %q", hostname)
		}
		return parsed.Host, nil
	}
	if strings.ContainsAny(hostname, "/?#@") {
		return "", fmt.Errorf("telemetry: invalid cloud hostname %q", hostname)
	}
	parsed, err := url.Parse("https://" + hostname)
	if err != nil || parsed.Host == "" {
		return "", fmt.Errorf("telemetry: invalid cloud hostname %q", hostname)
	}
	return parsed.Host, nil
}

func cloudObservabilityToken(key, secret agents.SecretString) (string, error) {
	apiKey := key.Reveal()
	if apiKey == "" {
		apiKey = os.Getenv("LIVEKIT_API_KEY")
	}
	apiSecret := secret.Reveal()
	if apiSecret == "" {
		apiSecret = os.Getenv("LIVEKIT_API_SECRET")
	}
	if apiKey == "" || apiSecret == "" {
		return "", errors.New("telemetry: LIVEKIT_API_KEY and LIVEKIT_API_SECRET must be set for cloud tracing")
	}
	token, err := auth.NewAccessToken(apiKey, apiSecret).
		SetIdentity("livekit-agents-telemetry").SetValidFor(6 * time.Hour).
		SetObservabilityGrant(&auth.ObservabilityGrant{Write: true}).ToJWT()
	if err != nil {
		return "", fmt.Errorf("telemetry: create observability token: %w", err)
	}
	return token, nil
}

type uploadGateSpanExporter struct {
	delegate   sdktrace.SpanExporter
	gate       *UploadGate
	generation uint64
	closeOnce  sync.Once
	closeErr   error
}

func (e *uploadGateSpanExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	if e.gate.Disabled() || e.generation != e.gate.Generation() {
		return nil
	}
	return e.delegate.ExportSpans(ctx, spans)
}

func (e *uploadGateSpanExporter) Shutdown(ctx context.Context) error {
	e.closeOnce.Do(func() { e.closeErr = e.delegate.Shutdown(ctx) })
	return e.closeErr
}

func gatedHTTPClient(base *http.Client, gate *UploadGate) *http.Client {
	var result http.Client
	if base != nil {
		result = *base
	} else {
		result.Timeout = DefaultCloudExportTimeout
	}
	transport := result.Transport
	if transport == nil {
		if defaults, ok := http.DefaultTransport.(*http.Transport); ok {
			transport = defaults.Clone()
		} else {
			transport = http.DefaultTransport
		}
	}
	result.Transport = &uploadGateRoundTripper{delegate: transport, gate: gate, generation: gate.Generation()}
	return &result
}

type uploadGateRoundTripper struct {
	delegate   http.RoundTripper
	gate       *UploadGate
	generation uint64
}

func (t *uploadGateRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	if t.gate.Disabled() || t.generation != t.gate.Generation() {
		return &http.Response{
			StatusCode: http.StatusNoContent, Status: "204 No Content", Header: make(http.Header),
			Body: io.NopCloser(bytes.NewReader(nil)), Request: request,
		}, nil
	}
	response, err := t.delegate.RoundTrip(request)
	if err != nil || response == nil || response.Body == nil {
		return response, err
	}
	prefix, readErr := io.ReadAll(io.LimitReader(response.Body, defaultMaxErrorBodyBytes))
	if readErr != nil {
		_ = response.Body.Close()
		return nil, readErr
	}
	response.Body = &prefixReadCloser{Reader: io.MultiReader(bytes.NewReader(prefix), response.Body), closer: response.Body}
	if t.gate.IsDisabledResponse(response.StatusCode, prefix) {
		t.gate.Disable(t.generation)
	}
	return response, nil
}

type prefixReadCloser struct {
	io.Reader
	closer io.Closer
}

func (r *prefixReadCloser) Close() error { return r.closer.Close() }
