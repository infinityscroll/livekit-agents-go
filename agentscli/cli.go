// SPDX-License-Identifier: Apache-2.0

// Package agentscli provides the embedded LiveKit Agents command-line runtime.
// Parsing help and version commands performs no RTC, network, plugin, or
// telemetry initialization.
package agentscli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	agents "github.com/infinityscroll/livekit-agents-go"
)

type ConsoleOptions struct {
	ConnectAddress string
	Record         bool
	Logger         *slog.Logger
}

type ConsoleRunner func(context.Context, ConsoleOptions) error

type AppOptions[UserData any] struct {
	Server  agents.ServerOptions[UserData]
	Console ConsoleRunner
	Plugins []agents.Plugin
}

type IO struct {
	Stdin   io.Reader
	Stdout  io.Writer
	Stderr  io.Writer
	Getenv  func(string) string
	Signals <-chan os.Signal
}

func defaultIO() IO {
	return IO{Stdin: os.Stdin, Stdout: os.Stdout, Stderr: os.Stderr, Getenv: os.Getenv}
}

func normalizeIO(input IO) IO {
	if input.Stdin == nil {
		input.Stdin = strings.NewReader("")
	}
	if input.Stdout == nil {
		input.Stdout = io.Discard
	}
	if input.Stderr == nil {
		input.Stderr = io.Discard
	}
	if input.Getenv == nil {
		input.Getenv = os.Getenv
	}
	return input
}

// Main runs the embedded CLI and returns a process exit code. The caller is
// responsible for os.Exit, which keeps deferred cleanup reliable and testable.
func Main[UserData any](options agents.ServerOptions[UserData], args []string) int {
	return MainApp(context.Background(), AppOptions[UserData]{Server: options}, args, defaultIO())
}

func MainApp[UserData any](ctx context.Context, app AppOptions[UserData], args []string, streams IO) int {
	streams = normalizeIO(streams)
	config, err := parseArguments(args, streams.Getenv)
	if err != nil {
		fmt.Fprintln(streams.Stderr, "agents:", err)
		fmt.Fprintln(streams.Stderr, "Run 'agents help' for usage.")
		return 2
	}
	if config.command == "" || config.command == "help" {
		_, _ = io.WriteString(streams.Stdout, usageText)
		return 0
	}
	if config.command == "version" {
		_, _ = io.WriteString(streams.Stdout, FormatVersionInfo()+"\n")
		return 0
	}
	logger, err := newLogger(config.logLevel, config.logFormat, streams.Stderr)
	if err != nil {
		fmt.Fprintln(streams.Stderr, "agents:", err)
		return 2
	}
	if config.command == "download-files" {
		failures := DownloadPluginFiles(ctx, logger, mergePlugins(app.Plugins, agents.RegisteredPlugins()))
		if len(failures) != 0 {
			fmt.Fprintln(streams.Stderr, FormatDownloadFailureMessage(failures))
			return 1
		}
		return 0
	}
	if config.command == "console" {
		runner := app.Console
		if runner == nil {
			runner = NewConsoleRunner(app.Server)
		}
		return runConsole(ctx, runner, ConsoleOptions{ConnectAddress: config.connectAddress, Record: config.record, Logger: logger}, streams)
	}

	options := app.Server
	applyConfiguration(&options, config, streams.Getenv, logger)
	server, err := agents.NewAgentServer(options)
	if err != nil {
		fmt.Fprintln(streams.Stderr, "agents: configure worker:", err)
		return 1
	}
	return runServer(ctx, server, options, config, streams, logger)
}

func runConsole(parent context.Context, runner ConsoleRunner, options ConsoleOptions, streams IO) int {
	if parent == nil {
		parent = context.Background()
	}
	runContext, cancel := context.WithCancel(parent)
	defer cancel()
	signals, stopSignals := signalSource(streams.Signals)
	defer stopSignals()
	done := make(chan error, 1)
	go func() { done <- runner(runContext, options) }()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			fmt.Fprintln(streams.Stderr, "agents: console mode failed:", err)
			return 1
		}
		return 0
	case received := <-signals:
		exitCode := signalExitCode(received)
		cancel()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) {
				fmt.Fprintln(streams.Stderr, "agents: console shutdown failed:", err)
			}
			return exitCode
		case <-signals:
			fmt.Fprintln(streams.Stderr, "Force exit (signal received twice)")
			return exitCode
		}
	case <-parent.Done():
		cancel()
		err := <-done
		if err != nil && !errors.Is(err, context.Canceled) {
			fmt.Fprintln(streams.Stderr, "agents: console shutdown failed:", err)
			return 1
		}
		return 0
	}
}

func applyConfiguration[UserData any](options *agents.ServerOptions[UserData], config cliConfiguration, getenv func(string) string, logger *slog.Logger) {
	if config.url != "" {
		options.URL = config.url
		options.WSURL = config.url
	}
	if config.apiKey != "" {
		options.APIKey = agents.NewSecretString(config.apiKey)
	}
	if config.apiSecret != "" {
		options.APISecret = agents.NewSecretString(config.apiSecret)
	}
	if config.workerToken != "" {
		options.WorkerToken = agents.NewSecretString(config.workerToken)
	}
	options.Production = config.production
	options.Simulation = config.simulation || options.Simulation
	options.Logger = logger
	if config.drainTimeoutSet {
		options.DrainTimeout = config.drainTimeout
	}
	_ = getenv // reserved for future configuration fields without global reads.
}

func runServer[UserData any](parent context.Context, server *agents.AgentServer[UserData], options agents.ServerOptions[UserData], config cliConfiguration, streams IO, logger *slog.Logger) int {
	if parent == nil {
		parent = context.Background()
	}
	runContext, cancelRun := context.WithCancel(parent)
	defer cancelRun()

	registered := make(chan struct{})
	var registeredOnce sync.Once
	unsubscribe := server.Events().Subscribe(func(event agents.WorkerEvent) {
		if event.Type == agents.WorkerEventRegistered {
			registeredOnce.Do(func() { close(registered) })
		}
	})
	defer unsubscribe()

	devClient := startDevClient(config.cliAddress, effectiveAgentName(options, streams.Getenv), effectiveURL(options, streams.Getenv), logger)
	if devClient != nil {
		defer devClient.Close()
	}

	runDone := make(chan error, 1)
	go func() { runDone <- server.Run(runContext) }()

	if config.command == "connect" {
		select {
		case <-registered:
			if err := server.SimulateJob(runContext, config.room, config.participantIdentity); err != nil {
				logger.Error("failed to connect simulation job", "error", err, "room", config.room)
				cancelRun()
				<-runDone
				return 1
			}
		case err := <-runDone:
			if err != nil {
				logger.Error("worker stopped before registration", "error", err)
				return 1
			}
			return 0
		case <-parent.Done():
			cancelRun()
			<-runDone
			return 0
		}
	}

	signals, stopSignals := signalSource(streams.Signals)
	defer stopSignals()
	select {
	case err := <-runDone:
		if err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("worker stopped", "error", err)
			return 1
		}
		return 0
	case received := <-signals:
		exitCode := signalExitCode(received)
		cancelRun()
		select {
		case err := <-runDone:
			if err != nil && !errors.Is(err, context.Canceled) {
				logger.Error("worker shutdown failed", "error", err)
			}
			return exitCode
		case <-signals:
			fmt.Fprintln(streams.Stderr, "Force exit (signal received twice)")
			forceContext, cancel := context.WithTimeout(context.Background(), time.Second)
			_ = server.Close(forceContext)
			cancel()
			return exitCode
		}
	case <-parent.Done():
		cancelRun()
		err := <-runDone
		if err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("worker shutdown failed", "error", err)
			return 1
		}
		return 0
	}
}

func signalSource(injected <-chan os.Signal) (<-chan os.Signal, func()) {
	if injected != nil {
		return injected, func() {}
	}
	channel := make(chan os.Signal, 2)
	signal.Notify(channel, os.Interrupt, syscall.SIGTERM)
	return channel, func() { signal.Stop(channel) }
}

func signalExitCode(signal os.Signal) int {
	if signal == syscall.SIGTERM {
		return 143
	}
	return 130
}

type cliConfiguration struct {
	command             string
	url                 string
	apiKey              string
	apiSecret           string
	workerToken         string
	logLevel            string
	logFormat           string
	production          bool
	development         bool
	simulation          bool
	drainTimeout        time.Duration
	drainTimeoutSet     bool
	cliAddress          string
	room                string
	participantIdentity string
	connectAddress      string
	record              bool
}

func parseArguments(args []string, getenv func(string) string) (cliConfiguration, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	config := cliConfiguration{logLevel: firstNonempty(getenv("LIVEKIT_LOG_LEVEL"), getenv("LOG_LEVEL"))}
	for index := 0; index < len(args); index++ {
		argument := args[index]
		if argument == "-h" || argument == "--help" {
			config.command = "help"
			continue
		}
		if argument == "--version" || argument == "-version" {
			config.command = "version"
			continue
		}
		if !strings.HasPrefix(argument, "-") {
			if config.command != "" {
				return config, fmt.Errorf("unexpected argument %q", argument)
			}
			switch argument {
			case "start", "dev", "connect", "console", "download-files", "version", "help":
				config.command = argument
			default:
				return config, fmt.Errorf("unknown command %q", argument)
			}
			continue
		}

		name, inline, hasInline := strings.Cut(argument, "=")
		value := func() (string, error) {
			if hasInline {
				return inline, nil
			}
			if index+1 >= len(args) {
				return "", fmt.Errorf("%s requires a value", name)
			}
			index++
			return args[index], nil
		}
		switch name {
		case "--url":
			var err error
			config.url, err = value()
			if err != nil {
				return config, err
			}
			if config.url == "" {
				return config, errors.New("--url requires a non-empty value")
			}
		case "--api-key":
			var err error
			config.apiKey, err = value()
			if err != nil {
				return config, err
			}
		case "--api-secret":
			var err error
			config.apiSecret, err = value()
			if err != nil {
				return config, err
			}
		case "--worker-token":
			var err error
			config.workerToken, err = value()
			if err != nil {
				return config, err
			}
		case "--log-level":
			var err error
			config.logLevel, err = value()
			if err != nil {
				return config, err
			}
		case "--log-format":
			var err error
			config.logFormat, err = value()
			if err != nil {
				return config, err
			}
		case "--drain-timeout":
			raw, err := value()
			if err != nil {
				return config, err
			}
			config.drainTimeout, err = time.ParseDuration(raw)
			if err != nil || config.drainTimeout <= 0 {
				return config, fmt.Errorf("invalid --drain-timeout %q", raw)
			}
			config.drainTimeoutSet = true
		case "--simulation":
			boolValue, err := parseOptionalBool(inline, hasInline)
			if err != nil {
				return config, fmt.Errorf("--simulation: %w", err)
			}
			config.simulation = boolValue
		case "--dev":
			boolValue, err := parseOptionalBool(inline, hasInline)
			if err != nil {
				return config, fmt.Errorf("--dev: %w", err)
			}
			config.development = boolValue
		case "--cli-addr", "--reload-addr":
			var err error
			config.cliAddress, err = value()
			if err != nil {
				return config, err
			}
		case "--room":
			var err error
			config.room, err = value()
			if err != nil {
				return config, err
			}
		case "--participant-identity":
			var err error
			config.participantIdentity, err = value()
			if err != nil {
				return config, err
			}
		case "--connect-addr":
			var err error
			config.connectAddress, err = value()
			if err != nil {
				return config, err
			}
		case "--record":
			boolValue, err := parseOptionalBool(inline, hasInline)
			if err != nil {
				return config, fmt.Errorf("--record: %w", err)
			}
			config.record = boolValue
		default:
			return config, fmt.Errorf("unknown option %q", name)
		}
	}

	if config.command == "start" {
		config.production = true
	}
	if config.command == "dev" || config.command == "connect" || config.command == "console" {
		config.production = false
	}
	if config.command == "start" && config.development {
		config.production = false
	}
	if config.command == "" || config.command == "help" || config.command == "version" {
		return config, nil
	}
	if config.logLevel == "" {
		if config.production {
			config.logLevel = "info"
		} else {
			config.logLevel = "debug"
		}
	}
	config.logLevel = strings.ToLower(config.logLevel)
	if !validLogLevel(config.logLevel) {
		return config, fmt.Errorf("invalid log level %q", config.logLevel)
	}
	if config.logFormat == "" {
		if config.production {
			config.logFormat = "json"
		} else {
			config.logFormat = "text"
		}
	}
	config.logFormat = strings.ToLower(config.logFormat)
	if config.logFormat == "colored" || config.logFormat == "pretty" {
		config.logFormat = "text"
	}
	if config.logFormat != "json" && config.logFormat != "text" {
		return config, fmt.Errorf("invalid log format %q", config.logFormat)
	}
	if config.command == "connect" && config.room == "" {
		return config, errors.New("connect requires --room")
	}
	if config.command == "console" && config.connectAddress == "" {
		return config, errors.New("console requires --connect-addr")
	}
	if config.cliAddress != "" && config.production {
		return config, errors.New("--cli-addr requires development mode")
	}
	return config, nil
}

func parseOptionalBool(inline string, hasInline bool) (bool, error) {
	if !hasInline {
		return true, nil
	}
	return strconv.ParseBool(inline)
}

func firstNonempty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func validLogLevel(value string) bool {
	switch value {
	case "trace", "debug", "info", "warn", "warning", "error", "fatal":
		return true
	default:
		return false
	}
}

func newLogger(level, format string, output io.Writer) (*slog.Logger, error) {
	var slogLevel slog.Level
	switch level {
	case "trace":
		slogLevel = slog.LevelDebug - 4
	case "debug":
		slogLevel = slog.LevelDebug
	case "info":
		slogLevel = slog.LevelInfo
	case "warn", "warning":
		slogLevel = slog.LevelWarn
	case "error":
		slogLevel = slog.LevelError
	case "fatal":
		slogLevel = slog.LevelError + 4
	default:
		return nil, fmt.Errorf("invalid log level %q", level)
	}
	options := &slog.HandlerOptions{Level: slogLevel}
	if format == "json" {
		return slog.New(slog.NewJSONHandler(output, options)), nil
	}
	return slog.New(slog.NewTextHandler(output, options)), nil
}

const usageText = `LiveKit Agents CLI

Usage:
  agents [global options] <command> [command options]

Commands:
  start           Start the worker in production mode
  dev             Start in development mode (deprecated; use lk agent dev)
  connect         Connect to a specific room
  console         Attach the in-process agent to a local broker over TCP
  download-files  Download registered plugin dependency files
  version         Print SDK, protocol, toolchain, and build versions
  help            Show this help

Global options:
  --url <url>             LiveKit websocket URL (env LIVEKIT_URL)
  --api-key <key>         LiveKit API key (env LIVEKIT_API_KEY)
  --api-secret <secret>   LiveKit API secret (env LIVEKIT_API_SECRET)
  --log-level <level>     trace, debug, info, warn, error, or fatal
  --log-format <format>   json or text
  --drain-timeout <dur>   Graceful drain duration, for example 30s
  -h, --help              Show help
  --version               Show version

Command options:
  start:   --simulation, --dev
  connect: --room <name> [--participant-identity <identity>]
  console: --connect-addr <host:port> [--record]
`
