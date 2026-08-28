// SPDX-License-Identifier: Apache-2.0

package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/trace"
)

const (
	DefaultCloudLogCapacity      = 1024
	DefaultCloudLogBatchSize     = 128
	DefaultCloudLogFlushInterval = 200 * time.Millisecond
)

type CloudLogHandlerOptions struct {
	Exporter      *SimpleOTLPHTTPLogExporter
	Next          slog.Handler
	Level         slog.Leveler
	Capacity      int
	BatchSize     int
	FlushInterval time.Duration
	StaticAttrs   map[string]any
}

type cloudLogCore struct {
	exporter *SimpleOTLPHTTPLogExporter
	next     slog.Handler
	level    slog.Level
	queue    chan SimpleLogRecord
	flush    chan chan error
	stop     chan struct{}
	done     chan struct{}
	batch    int
	interval time.Duration

	lifecycle sync.RWMutex
	closed    bool
	closeOnce sync.Once
	closeErr  error
	dropped   atomic.Uint64
}

// CloudLogHandler is a bounded slog bridge to the standalone LiveKit Cloud
// OTLP log exporter. Low-severity logs drop rather than stalling audio when the
// queue is saturated; errors apply caller-context backpressure. Flush and
// Shutdown deterministically drain accepted records.
type CloudLogHandler struct {
	core   *cloudLogCore
	attrs  []boundSlogAttr
	groups []string
}

type boundSlogAttr struct {
	prefix string
	attr   slog.Attr
}

func NewCloudLogHandler(options CloudLogHandlerOptions) (*CloudLogHandler, error) {
	if options.Exporter == nil {
		return nil, errors.New("telemetry: cloud log exporter is required")
	}
	capacity := options.Capacity
	if capacity <= 0 {
		capacity = DefaultCloudLogCapacity
	}
	batch := options.BatchSize
	if batch <= 0 {
		batch = DefaultCloudLogBatchSize
	}
	if batch > capacity {
		batch = capacity
	}
	interval := options.FlushInterval
	if interval <= 0 {
		interval = DefaultCloudLogFlushInterval
	}
	level := slog.LevelDebug
	if options.Level != nil {
		level = options.Level.Level()
	}
	core := &cloudLogCore{
		exporter: options.Exporter, next: options.Next, level: level,
		queue: make(chan SimpleLogRecord, capacity), flush: make(chan chan error),
		stop: make(chan struct{}), done: make(chan struct{}), batch: batch, interval: interval,
	}
	handler := &CloudLogHandler{core: core}
	keys := make([]string, 0, len(options.StaticAttrs))
	for key := range options.StaticAttrs {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		handler.attrs = append(handler.attrs, boundSlogAttr{attr: slog.Any(key, options.StaticAttrs[key])})
	}
	go core.run()
	return handler, nil
}

func (h *CloudLogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	if h == nil || h.core == nil {
		return false
	}
	return level >= h.core.level || h.core.next != nil && h.core.next.Enabled(ctx, level)
}

func (h *CloudLogHandler) Handle(ctx context.Context, record slog.Record) error {
	if h == nil || h.core == nil {
		return errors.New("telemetry: cloud log handler is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var nextErr error
	if h.core.next != nil && h.core.next.Enabled(ctx, record.Level) {
		nextErr = h.core.next.Handle(ctx, record.Clone())
	}
	if record.Level < h.core.level {
		return nextErr
	}
	converted := h.convert(ctx, record)
	h.core.lifecycle.RLock()
	if h.core.closed {
		h.core.lifecycle.RUnlock()
		return nextErr
	}
	if record.Level >= slog.LevelError {
		select {
		case h.core.queue <- converted:
		case <-ctx.Done():
			nextErr = errors.Join(nextErr, context.Cause(ctx))
		case <-h.core.stop:
		}
	} else {
		select {
		case h.core.queue <- converted:
		default:
			h.core.dropped.Add(1)
		}
	}
	h.core.lifecycle.RUnlock()
	return nextErr
}

func (h *CloudLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	copy := *h
	copy.attrs = slices.Clone(h.attrs)
	prefix := strings.Join(h.groups, ".")
	for _, attr := range attrs {
		copy.attrs = append(copy.attrs, boundSlogAttr{prefix: prefix, attr: attr})
	}
	return &copy
}

func (h *CloudLogHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	copy := *h
	copy.groups = append(slices.Clone(h.groups), name)
	return &copy
}

func (h *CloudLogHandler) Dropped() uint64 {
	if h == nil || h.core == nil {
		return 0
	}
	return h.core.dropped.Load()
}

func (h *CloudLogHandler) Flush(ctx context.Context) error {
	if h == nil || h.core == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	response := make(chan error, 1)
	h.core.lifecycle.RLock()
	closed := h.core.closed
	if !closed {
		select {
		case h.core.flush <- response:
			h.core.lifecycle.RUnlock()
		case <-ctx.Done():
			h.core.lifecycle.RUnlock()
			return context.Cause(ctx)
		}
	} else {
		h.core.lifecycle.RUnlock()
		select {
		case <-h.core.done:
			return h.core.closeErr
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
	select {
	case err := <-response:
		return err
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (h *CloudLogHandler) Shutdown(ctx context.Context) error {
	if h == nil || h.core == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	h.core.closeOnce.Do(func() {
		h.core.lifecycle.Lock()
		h.core.closed = true
		close(h.core.stop)
		h.core.lifecycle.Unlock()
	})
	select {
	case <-h.core.done:
		return h.core.closeErr
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (h *CloudLogHandler) convert(ctx context.Context, record slog.Record) SimpleLogRecord {
	attributes := make(map[string]any, len(h.attrs)+record.NumAttrs()+2)
	group := strings.Join(h.groups, ".")
	for _, attr := range h.attrs {
		flattenSlogAttr(attributes, attr.prefix, attr.attr)
	}
	record.Attrs(func(attr slog.Attr) bool {
		flattenSlogAttr(attributes, group, attr)
		return true
	})
	if record.PC != 0 {
		frame, _ := runtime.CallersFrames([]uintptr{record.PC}).Next()
		attributes["code.file.path"] = frame.File
		attributes["code.function.name"] = frame.Function
		attributes["code.line.number"] = frame.Line
	}
	span := trace.SpanContextFromContext(ctx)
	traceID, spanID := "", ""
	if span.IsValid() {
		traceID, spanID = span.TraceID().String(), span.SpanID().String()
	}
	severityNumber, severityText := otlpSeverity(record.Level)
	return SimpleLogRecord{
		Body: record.Message, Timestamp: record.Time, Attributes: attributes,
		SeverityNumber: severityNumber, SeverityText: severityText, TraceID: traceID, SpanID: spanID,
	}
}

func (c *cloudLogCore) run() {
	defer close(c.done)
	timer := time.NewTicker(c.interval)
	defer timer.Stop()
	records := make([]SimpleLogRecord, 0, c.batch)
	var pendingErr error
	export := func(ctx context.Context) {
		if len(records) == 0 {
			return
		}
		if err := c.exporter.Export(ctx, records); err != nil {
			pendingErr = errors.Join(pendingErr, err)
		}
		clear(records)
		records = records[:0]
	}
	drain := func() {
		for {
			select {
			case record := <-c.queue:
				records = append(records, record)
				if len(records) >= c.batch {
					export(context.Background())
				}
			default:
				return
			}
		}
	}
	for {
		select {
		case record := <-c.queue:
			records = append(records, record)
			if len(records) >= c.batch {
				export(context.Background())
			}
		case response := <-c.flush:
			drain()
			export(context.Background())
			response <- pendingErr
			pendingErr = nil
		case <-timer.C:
			export(context.Background())
		case <-c.stop:
			drain()
			export(context.Background())
			c.closeErr = pendingErr
			return
		}
	}
}

func flattenSlogAttr(output map[string]any, prefix string, attr slog.Attr) {
	attr.Value = attr.Value.Resolve()
	if attr.Equal(slog.Attr{}) {
		return
	}
	key := attr.Key
	if prefix != "" {
		key = prefix + "." + key
	}
	if attr.Value.Kind() == slog.KindGroup {
		for _, child := range attr.Value.Group() {
			flattenSlogAttr(output, key, child)
		}
		return
	}
	value := attr.Value.Any()
	if err, ok := value.(error); ok {
		output[key] = map[string]any{"type": fmt.Sprintf("%T", err), "message": err.Error()}
		return
	}
	output[key] = value
}

func otlpSeverity(level slog.Level) (int32, string) {
	switch {
	case level >= slog.LevelError+4:
		return 21, "fatal"
	case level >= slog.LevelError:
		return 17, "error"
	case level >= slog.LevelWarn:
		return 13, "warn"
	case level >= slog.LevelInfo:
		return 9, "info"
	case level >= slog.LevelDebug:
		return 5, "debug"
	default:
		return 1, "trace"
	}
}
