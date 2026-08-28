// SPDX-License-Identifier: Apache-2.0

package agents

import (
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
)

type LogFormat string

const (
	LogFormatJSON LogFormat = "json"
	LogFormatText LogFormat = "text"

	LogLevelTrace  slog.Level = slog.LevelDebug - 4
	LogLevelSilent slog.Level = slog.LevelError + 100
)

// LoggerOptions configures the process-local framework logger. Handler wins
// over Writer/Format/Level. The zero value creates JSON logs on stdout without
// replacing slog.Default.
type LoggerOptions struct {
	Level       slog.Leveler
	Format      LogFormat
	Writer      io.Writer
	Handler     slog.Handler
	AddSource   bool
	SetDefault  bool
	ReplaceAttr func([]string, slog.Attr) slog.Attr
}

type ResolvedLoggerOptions struct {
	Level      slog.Level
	Format     LogFormat
	AddSource  bool
	SetDefault bool
}

var frameworkLogger struct {
	value atomic.Pointer[slog.Logger]
	mu    sync.RWMutex
	opts  ResolvedLoggerOptions
}

// InitializeLogger installs the logger returned by Log. It performs no work
// until called and never changes slog.Default unless SetDefault is true.
func InitializeLogger(options LoggerOptions) (*slog.Logger, error) {
	level := slog.LevelInfo
	if options.Level != nil {
		level = options.Level.Level()
	}
	format := options.Format
	if format == "" {
		format = LogFormatJSON
	}
	if format != LogFormatJSON && format != LogFormatText {
		return nil, errors.New("agents: logger format must be json or text")
	}
	handler := options.Handler
	if handler == nil {
		writer := options.Writer
		if writer == nil {
			writer = os.Stdout
		}
		handlerOptions := &slog.HandlerOptions{Level: level, AddSource: options.AddSource, ReplaceAttr: options.ReplaceAttr}
		if format == LogFormatText {
			handler = slog.NewTextHandler(writer, handlerOptions)
		} else {
			handler = slog.NewJSONHandler(writer, handlerOptions)
		}
	}
	if region := os.Getenv("LIVEKIT_REGION_NAME"); region != "" {
		handler = handler.WithAttrs([]slog.Attr{slog.String("region", region)})
	}
	logger := slog.New(handler)
	frameworkLogger.mu.Lock()
	frameworkLogger.opts = ResolvedLoggerOptions{Level: level, Format: format, AddSource: options.AddSource, SetDefault: options.SetDefault}
	frameworkLogger.value.Store(logger)
	frameworkLogger.mu.Unlock()
	if options.SetDefault {
		slog.SetDefault(logger)
	}
	return logger, nil
}

// SetLogger installs an application-owned logger without mutating the global
// slog default. Passing nil is rejected instead of creating a latent panic.
func SetLogger(logger *slog.Logger) error {
	if logger == nil {
		return errors.New("agents: logger is required")
	}
	frameworkLogger.value.Store(logger)
	return nil
}

// Log returns the framework logger or slog.Default when the application has
// not initialized one. This makes library use safe outside the CLI.
func Log() *slog.Logger {
	if logger := frameworkLogger.value.Load(); logger != nil {
		return logger
	}
	return slog.Default()
}

func CurrentLoggerOptions() (ResolvedLoggerOptions, bool) {
	if frameworkLogger.value.Load() == nil {
		return ResolvedLoggerOptions{}, false
	}
	frameworkLogger.mu.RLock()
	options := frameworkLogger.opts
	frameworkLogger.mu.RUnlock()
	return options, true
}

func ParseLogLevel(value string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "info":
		return slog.LevelInfo, nil
	case "trace":
		return LogLevelTrace, nil
	case "debug":
		return slog.LevelDebug, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error", "fatal":
		return slog.LevelError, nil
	case "silent", "off":
		return LogLevelSilent, nil
	default:
		return 0, errors.New("agents: unknown log level " + value)
	}
}
