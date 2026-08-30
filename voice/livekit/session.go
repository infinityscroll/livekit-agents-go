// SPDX-License-Identifier: Apache-2.0

package livekit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/telemetry"
	"github.com/infinityscroll/livekit-agents-go/voice"
	"github.com/infinityscroll/livekit-agents-go/voice/recorderio"
	"github.com/infinityscroll/livekit-agents-go/voice/roomio"
)

const (
	DefaultEventCapacity   = 256
	DefaultMaxReportEvents = 4096
	DefaultMaxReportBytes  = 8 << 20
	DefaultCloseTimeout    = 30 * time.Second
	DefaultFinalizeTimeout = 15 * time.Minute
)

var (
	ErrJobContextRequired        = errors.New("voice/livekit: a job context is required")
	ErrAudioRecordingUnavailable = errors.New("voice/livekit: audio recording requires both audio input and output")
	ErrReportEventLimit          = errors.New("voice/livekit: session report event retention limit reached")
	errFakeJobRoomIO             = errors.New("voice/livekit: a supplied RoomIO cannot be used by a fake console job")
	errInactiveRecorder          = errors.New("voice/livekit: a supplied Recorder requires enabled audio recording on a non-fake job")
)

// StartOptions controls the high-level LiveKit binding. Zero values select the
// production defaults. After compatibility validation succeeds, supplying
// RoomIO or Recorder transfers ownership to the Start transaction: the
// returned Runtime closes them on success and partial-start rollback closes
// them on failure. Callers that need borrowed ownership should compose the
// lower-level roomio/recorderio packages directly.
type StartOptions[UserData any] struct {
	Job            *agents.JobContext[UserData]
	ConnectOptions agents.ConnectOptions

	RoomInput  *roomio.RoomInputOptions
	RoomOutput *roomio.RoomOutputOptions
	RoomIO     *roomio.RoomIO

	// Recording inherits livekit.Job.enable_recording when zero-valued. A
	// concrete sparse update explicitly selects its granular policy.
	Recording       agents.Override[voice.RecordingOptionsUpdate]
	Recorder        *recorderio.RecorderIO
	RecorderOptions recorderio.RecorderOptions

	EventCapacity   int
	MaxReportEvents int
	MaxReportBytes  int
	CloseTimeout    time.Duration
	FinalizeTimeout time.Duration

	Logger     *slog.Logger
	HTTPClient *http.Client
	Metadata   map[string]any
	UploadGate *telemetry.UploadGate
	// ObservabilityURL overrides Cloud endpoint discovery. It must be an
	// origin URL; LIVEKIT_OBSERVABILITY_URL is used when this field is empty.
	ObservabilityURL      string
	RecordingEndpoint     string
	LogEndpoint           string
	DisableCloudTelemetry bool

	// OnReport runs after the immutable report is assembled and before local
	// persistence or Cloud upload. It must honor ctx.
	OnReport func(context.Context, *voice.SessionReport) error
	// OnError receives asynchronous RoomIO and report-capture errors. Panics are
	// isolated. The Runtime also exposes the first capture error in ReportStats.
	OnError func(error)
}

// ReportStats makes bounded-retention loss explicit. RetainedBytes counts the
// immutable canonical event bytes stored at the time of the snapshot.
type ReportStats struct {
	RetainedEvents uint64
	RetainedBytes  uint64
	DroppedEvents  uint64
	CaptureError   error
}

// Runtime owns the components started by Start and implements
// agents.JobSessionLifecycle. Primary runtimes are closed and finalized by the
// job runner; a secondary runtime must be closed by its caller.
type Runtime[UserData any] struct {
	job     *agents.JobContext[UserData]
	session *voice.AgentSession[UserData]
	agent   *voice.Agent[UserData]
	opts    StartOptions[UserData]
	logger  *slog.Logger

	registration *agents.JobSessionRegistration
	recording    voice.RecordingOptions
	startedAt    time.Time

	roomIO         *roomio.RoomIO
	ownRoomIO      bool
	recorder       *recorderio.RecorderIO
	ownRecorder    bool
	originalInput  voice.AudioInput
	originalOutput voice.AudioOutput
	recordedInput  voice.AudioInput
	recordedOutput voice.AudioOutput
	console        *voice.AgentsConsole

	cloud      *telemetry.CloudTelemetry
	logHandler *telemetry.CloudLogHandler
	uploader   *telemetry.SessionReportUploader

	eventCtx    context.Context
	eventCancel context.CancelCauseFunc
	eventSub    *voice.EventSubscription
	eventDone   chan struct{}
	eventMu     sync.Mutex
	events      []voice.Event
	eventBytes  int
	dropped     uint64
	captureErr  error

	closeOnce sync.Once
	closeDone chan struct{}
	closeErr  error

	finalizeOnce sync.Once
	finalizeDone chan struct{}
	finalizeErr  error
	reportMu     sync.RWMutex
	report       *voice.SessionReport
}

// Start connects the current job if needed, atomically claims the primary
// session role, installs bounded event capture and media IO, configures lazy
// Cloud telemetry when applicable, and starts the AgentSession. Every partial
// start is rolled back in reverse ownership order.
func Start[UserData any](ctx context.Context, session *voice.AgentSession[UserData], agent *voice.Agent[UserData], options StartOptions[UserData]) (_ *Runtime[UserData], resultErr error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if session == nil {
		return nil, errors.New("voice/livekit: session is required")
	}
	if agent == nil {
		return nil, errors.New("voice/livekit: agent is required")
	}
	job := options.Job
	if job == nil {
		job, _ = agents.JobFromContext[UserData](ctx)
	}
	if job == nil {
		return nil, ErrJobContextRequired
	}
	if session.Started() {
		return nil, voice.ErrSessionAlreadyStarted
	}
	resolved, err := resolveStartOptions(options)
	if err != nil {
		return nil, err
	}
	// A fake job is bound to the process console rather than a LiveKit room.
	// Reject ignored caller-owned resources before the Start transaction takes
	// ownership of them.
	if job.IsFakeJob() && resolved.RoomIO != nil {
		return nil, errFakeJobRoomIO
	}
	if job.IsFakeJob() && resolved.Recorder != nil {
		return nil, errInactiveRecorder
	}
	_, requestedRecording := resolveRecordingSelection(resolved.Recording)
	if resolved.Recorder != nil && !resolved.Recording.IsInherited() && !requestedRecording.Audio {
		return nil, errInactiveRecorder
	}
	r := &Runtime[UserData]{
		job: job, session: session, agent: agent, opts: resolved, logger: resolved.Logger,
		startedAt: time.Now(), eventDone: make(chan struct{}), closeDone: make(chan struct{}),
		finalizeDone: make(chan struct{}), roomIO: resolved.RoomIO, ownRoomIO: resolved.RoomIO != nil,
		recorder: resolved.Recorder, ownRecorder: resolved.Recorder != nil,
	}
	r.eventCtx, r.eventCancel = context.WithCancelCause(context.Background())
	defer func() {
		if resultErr != nil {
			resultErr = errors.Join(resultErr, r.rollback())
		}
	}()

	r.eventSub, err = session.Subscribe(voice.EventSubscriptionOptions{Capacity: resolved.EventCapacity, DropTelemetry: true})
	if err != nil {
		return nil, fmt.Errorf("voice/livekit: subscribe to session events: %w", err)
	}
	go r.captureEvents()

	registrationRecording, recording := resolveRecordingSelection(resolved.Recording)
	r.registration, err = job.RegisterSession(agents.JobSessionRegistrationOptions{Lifecycle: r, Recording: registrationRecording})
	if err != nil {
		return nil, err
	}
	if !r.registration.RecordingEnabled {
		recording = voice.DisabledRecordingOptions()
	}
	// An inherited recording policy can be demoted only after primary-session
	// registration. A supplied recorder was already accepted into this Start
	// transaction, so rollback deterministically closes it on this error path.
	if r.opts.Recorder != nil && !recording.Audio {
		return nil, errInactiveRecorder
	}
	recording.Redaction = recording.Enabled() && (recording.Redaction || r.registration.RedactionEnabled)
	if recording.Redaction && recording.Audio && !recording.Transcript {
		return nil, errors.New("voice/livekit: redacted audio recording requires transcript recording")
	}
	r.recording = recording

	if err := job.Connect(ctx, resolved.ConnectOptions); err != nil {
		return nil, fmt.Errorf("voice/livekit: connect job room: %w", err)
	}
	if err := r.setupCloud(ctx); err != nil {
		return nil, err
	}
	if err := r.setupRoomIO(ctx); err != nil {
		return nil, err
	}
	if err := r.setupRecording(ctx); err != nil {
		return nil, err
	}
	if err := session.Start(ctx, agent); err != nil {
		return nil, fmt.Errorf("voice/livekit: start agent session: %w", err)
	}
	return r, nil
}

func resolveStartOptions[UserData any](options StartOptions[UserData]) (StartOptions[UserData], error) {
	if options.Logger == nil {
		options.Logger = slog.Default()
	}
	if options.EventCapacity == 0 {
		options.EventCapacity = DefaultEventCapacity
	}
	if options.MaxReportEvents == 0 {
		options.MaxReportEvents = DefaultMaxReportEvents
	}
	if options.MaxReportBytes == 0 {
		options.MaxReportBytes = DefaultMaxReportBytes
	}
	if options.CloseTimeout == 0 {
		options.CloseTimeout = DefaultCloseTimeout
	}
	if options.FinalizeTimeout == 0 {
		options.FinalizeTimeout = DefaultFinalizeTimeout
	}
	if options.EventCapacity < 1 || options.MaxReportEvents < 1 || options.MaxReportBytes < 1 || options.CloseTimeout < 0 || options.FinalizeTimeout < 0 {
		return options, errors.New("voice/livekit: capacities and byte limits must be positive and timeouts non-negative")
	}
	if options.RoomIO != nil && (options.RoomInput != nil || options.RoomOutput != nil) {
		return options, errors.New("voice/livekit: provided RoomIO cannot be combined with RoomInput or RoomOutput")
	}
	if options.ObservabilityURL == "" {
		options.ObservabilityURL = os.Getenv("LIVEKIT_OBSERVABILITY_URL")
	}
	return options, nil
}

func resolveRecordingSelection(value agents.Override[voice.RecordingOptionsUpdate]) (agents.Override[bool], voice.RecordingOptions) {
	if value.IsDisabled() {
		return agents.Use(false), voice.DisabledRecordingOptions()
	}
	if update, ok := value.Value(); ok {
		resolved := voice.ResolveRecordingOptions(update)
		return agents.Use(resolved.Enabled()), resolved
	}
	return agents.Override[bool]{}, voice.DefaultRecordingOptions()
}

func (r *Runtime[UserData]) setupCloud(ctx context.Context) error {
	if r.opts.DisableCloudTelemetry || !r.recording.Enabled() || r.job.IsFakeJob() {
		return nil
	}
	info := r.job.Info()
	hostname, enabled, err := observabilityHostname(info.URL, r.opts.ObservabilityURL)
	if err != nil {
		return err
	}
	if !enabled {
		return nil
	}
	job := r.job.Job()
	jobID, roomID, agentName := "", "", ""
	if job != nil {
		jobID, agentName = job.GetId(), job.GetAgentName()
		if job.GetRoom() != nil {
			roomID = job.GetRoom().GetSid()
		}
	}
	if roomID == "" && !r.job.IsFakeJob() && r.job.Room() != nil {
		roomID = r.job.Room().SID()
	}
	gate := r.opts.UploadGate
	if gate == nil {
		gate = telemetry.NewUploadGate(r.logger)
	}
	r.opts.UploadGate = gate
	traces, logs := r.recording.Traces, r.recording.Logs
	r.cloud, err = telemetry.SetupCloudTracer(ctx, telemetry.SetupCloudTracerOptions{
		RoomID: roomID, JobID: jobID, CloudHostname: hostname, AgentName: agentName,
		EnableTraces: &traces, EnableLogs: &logs, Metadata: r.opts.Metadata,
		APIKey: info.APIKey, APISecret: info.APISecret, HTTPClient: r.opts.HTTPClient, UploadGate: gate,
	})
	if err != nil {
		return fmt.Errorf("voice/livekit: setup Cloud telemetry: %w", err)
	}
	if exporter := r.cloud.LogExporter(); exporter != nil {
		r.logHandler, err = telemetry.NewCloudLogHandler(telemetry.CloudLogHandlerOptions{
			Exporter: exporter, Next: r.logger.Handler(), StaticAttrs: r.opts.Metadata,
		})
		if err != nil {
			return fmt.Errorf("voice/livekit: setup Cloud log handler: %w", err)
		}
		r.logger = slog.New(r.logHandler)
	}
	r.uploader, err = telemetry.NewSessionReportUploader(telemetry.SessionReportUploaderConfig{
		AgentName: agentName, CloudHostname: hostname,
		RecordingEndpoint: r.opts.RecordingEndpoint, LogEndpoint: r.opts.LogEndpoint,
		APIKey: info.APIKey, APISecret: info.APISecret, HTTPClient: r.opts.HTTPClient,
		UploadGate: gate, Metadata: r.opts.Metadata, Logger: r.logger,
	})
	if err != nil {
		return fmt.Errorf("voice/livekit: configure session report upload: %w", err)
	}
	return nil
}

func observabilityHostname(livekitURL, override string) (string, bool, error) {
	selected := override
	if selected == "" {
		selected = livekitURL
	}
	parsed, err := url.Parse(selected)
	if err != nil || parsed.Hostname() == "" {
		return "", false, fmt.Errorf("voice/livekit: invalid observability URL %q", selected)
	}
	if override != "" && (parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Path != "" && parsed.Path != "/") {
		return "", false, errors.New("voice/livekit: observability URL must be an origin without path, query, or fragment")
	}
	hostname := parsed.Hostname()
	if override == "" && !strings.HasSuffix(hostname, ".livekit.cloud") && !strings.HasSuffix(hostname, ".livekit.run") {
		return hostname, false, nil
	}
	if port := parsed.Port(); port != "" {
		hostname += ":" + port
	}
	return hostname, true, nil
}

func (r *Runtime[UserData]) setupRoomIO(ctx context.Context) error {
	if r.job.IsFakeJob() {
		r.console = voice.DefaultAgentsConsole()
		return nil
	}
	value := r.roomIO
	if value == nil {
		var err error
		value, err = roomio.NewRoomIO(r.job.Context(), roomio.RoomIOOptions{
			Room: r.job.Room(), Bridge: r.job.RTCBridge(), Session: r.session,
			Input: r.opts.RoomInput, Output: r.opts.RoomOutput, Logger: r.logger,
			OnError: r.reportError,
		})
		if err != nil {
			return fmt.Errorf("voice/livekit: create RoomIO: %w", err)
		}
		r.ownRoomIO = true
	} else {
		r.ownRoomIO = true
	}
	r.roomIO = value
	if err := value.Start(ctx); err != nil {
		return fmt.Errorf("voice/livekit: start RoomIO: %w", err)
	}
	return nil
}

func (r *Runtime[UserData]) setupRecording(ctx context.Context) error {
	if !r.recording.Audio || r.job.IsFakeJob() {
		return nil
	}
	input, output := r.session.Input().Audio(), r.session.Output().Audio()
	if input == nil || output == nil {
		return ErrAudioRecordingUnavailable
	}
	recorder := r.recorder
	if recorder == nil {
		options := r.opts.RecorderOptions
		options.AgentSession = r.session
		previousOnError := options.OnError
		options.OnError = func(err error) {
			if previousOnError != nil {
				invokeError(previousOnError, err)
			}
			r.reportError(err)
		}
		var err error
		recorder, err = recorderio.NewRecorderIO(options)
		if err != nil {
			return fmt.Errorf("voice/livekit: create RecorderIO: %w", err)
		}
		r.ownRecorder = true
	} else {
		r.ownRecorder = true
	}
	r.recorder = recorder
	r.originalInput, r.originalOutput = input, output
	recordedInput, err := recorder.RecordInput(input)
	if err != nil {
		return fmt.Errorf("voice/livekit: wrap audio input: %w", err)
	}
	recordedOutput, err := recorder.RecordOutput(output)
	if err != nil {
		return fmt.Errorf("voice/livekit: wrap audio output: %w", err)
	}
	r.session.Input().SetAudio(recordedInput)
	r.session.Output().SetAudio(recordedOutput)
	r.recordedInput, r.recordedOutput = recordedInput, recordedOutput
	path := filepath.Join(r.job.SessionDirectory(), "audio.ogg")
	if err := recorder.Start(ctx, path); err != nil {
		return fmt.Errorf("voice/livekit: start audio recording: %w", err)
	}
	return nil
}

func (r *Runtime[UserData]) captureEvents() {
	defer close(r.eventDone)
	for {
		event, err := r.eventSub.Recv(r.eventCtx)
		if err != nil {
			return
		}
		if event.Type() == voice.EventMetricsCollected || event.Type() == voice.EventSessionUsageUpdated {
			continue
		}
		frozen, err := voice.FreezeReportEvent(event)
		if err != nil {
			r.eventMu.Lock()
			r.dropped++
			if r.captureErr == nil {
				r.captureErr = err
			}
			r.eventMu.Unlock()
			r.reportError(fmt.Errorf("voice/livekit: freeze report event: %w", err))
			continue
		}
		r.eventMu.Lock()
		if frozen.EncodedSize() > r.opts.MaxReportBytes {
			r.dropped++
			if r.captureErr == nil {
				r.captureErr = ErrReportEventLimit
			}
			r.eventMu.Unlock()
			continue
		}
		for len(r.events) >= r.opts.MaxReportEvents || r.eventBytes+frozen.EncodedSize() > r.opts.MaxReportBytes {
			if len(r.events) == 0 {
				break
			}
			if old, ok := r.events[0].(*voice.FrozenReportEvent); ok {
				r.eventBytes -= old.EncodedSize()
			}
			copy(r.events, r.events[1:])
			r.events = r.events[:len(r.events)-1]
			r.dropped++
			if r.captureErr == nil {
				r.captureErr = ErrReportEventLimit
			}
		}
		r.events = append(r.events, frozen)
		r.eventBytes += frozen.EncodedSize()
		r.eventMu.Unlock()
	}
}

func (r *Runtime[UserData]) rollback() error {
	var errs []error
	r.restoreRecordingIO()
	ctx, cancel := context.WithTimeout(context.Background(), r.opts.CloseTimeout)
	defer cancel()
	if r.recorder != nil && r.ownRecorder {
		errs = append(errs, r.recorder.Close(ctx))
	}
	if r.roomIO != nil && r.ownRoomIO {
		errs = append(errs, r.roomIO.Close(ctx))
	}
	if r.logHandler != nil {
		errs = append(errs, r.logHandler.Shutdown(ctx))
	}
	if r.cloud != nil {
		errs = append(errs, r.cloud.Shutdown(ctx))
	}
	if r.eventCancel != nil {
		r.eventCancel(errors.New("voice/livekit: start rolled back"))
	}
	if r.eventSub != nil {
		_ = r.eventSub.Close()
	}
	if r.eventDone != nil {
		select {
		case <-r.eventDone:
		case <-ctx.Done():
			errs = append(errs, context.Cause(ctx))
		}
	}
	// Keep the primary claim until all owned resources have been unwound. This
	// prevents another Start from observing a primary slot whose predecessor is
	// still tearing down Cloud/media workers.
	if r.registration != nil {
		r.registration.Release()
	}
	return errors.Join(errs...)
}

func (r *Runtime[UserData]) restoreRecordingIO() {
	if r == nil || r.session == nil {
		return
	}
	if r.originalInput != nil && r.recordedInput != nil && r.session.Input().Audio() == r.recordedInput {
		r.session.Input().SetAudio(r.originalInput)
	}
	if r.originalOutput != nil && r.recordedOutput != nil && r.session.Output().Audio() == r.recordedOutput {
		r.session.Output().SetAudio(r.originalOutput)
	}
}

func (r *Runtime[UserData]) closeRecording(ctx context.Context) error {
	var errs []error
	if r.recorder != nil && r.ownRecorder {
		errs = append(errs, r.recorder.Close(ctx))
	}
	if r.console != nil && r.registration != nil && r.registration.Primary {
		errs = append(errs, r.console.CloseRecording(ctx))
	}
	return errors.Join(errs...)
}

// CloseSession implements agents.JobSessionLifecycle. Cleanup continues under
// an internal deadline if the caller stops waiting.
func (r *Runtime[UserData]) CloseSession(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.closeOnce.Do(func() { go r.closeWorker() })
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-r.closeDone:
		return r.closeErr
	}
}

func (r *Runtime[UserData]) closeWorker() {
	defer close(r.closeDone)
	ctx, cancel := context.WithTimeout(context.Background(), r.opts.CloseTimeout)
	defer cancel()
	var errs []error
	if err := r.session.Close(ctx, voice.CloseOptions{Reason: voice.CloseReasonJobShutdown}); err != nil {
		errs = append(errs, err)
	}
	if r.eventSub != nil {
		_ = r.eventSub.Close()
	}
	select {
	case <-r.eventDone:
	case <-ctx.Done():
		cause := context.Cause(ctx)
		errs = append(errs, cause)
		r.eventMu.Lock()
		if r.captureErr == nil {
			r.captureErr = fmt.Errorf("voice/livekit: report event capture did not drain: %w", cause)
		}
		r.eventMu.Unlock()
		if r.eventCancel != nil {
			r.eventCancel(cause)
		}
	}
	// Keep RecorderIO installed through the session drain so trailing playout is
	// captured. A successful close has already cleared session IO; on timeout,
	// conditionally replace only a still-installed recorder decorator before the
	// recorder itself is closed.
	r.restoreRecordingIO()
	errs = append(errs, r.closeRecording(ctx))
	if r.roomIO != nil && r.ownRoomIO {
		errs = append(errs, r.roomIO.Close(ctx))
	}
	r.closeErr = errors.Join(errs...)
}

// FinalizeSession creates the immutable report, persists console reports,
// uploads Cloud observability data when configured, and drains telemetry.
func (r *Runtime[UserData]) FinalizeSession(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.finalizeOnce.Do(func() { go r.finalizeWorker() })
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-r.finalizeDone:
		return r.finalizeErr
	}
}

func (r *Runtime[UserData]) finalizeWorker() {
	defer close(r.finalizeDone)
	ctx, cancel := context.WithTimeout(context.Background(), r.opts.FinalizeTimeout)
	defer cancel()
	var errs []error
	if err := r.CloseSession(ctx); err != nil {
		errs = append(errs, err)
	}
	// CloseSession has a deliberately shorter internal deadline. RecorderIO and
	// AgentsConsole continue their idempotent cleanup after a caller timeout;
	// join that cleanup under the finalization budget before freezing metadata
	// and uploading the report.
	if err := r.closeRecording(ctx); err != nil {
		errs = append(errs, err)
	}
	report := r.createReport()
	r.reportMu.Lock()
	r.report = report
	r.reportMu.Unlock()
	if r.opts.OnReport != nil {
		if err := invokeReportCallback(ctx, r.opts.OnReport, report); err != nil {
			errs = append(errs, err)
		}
	}
	if r.job.IsFakeJob() && r.registration != nil && r.registration.Primary && (r.registration.RecordingEnabled || (r.console != nil && r.console.Enabled() && r.console.Record())) {
		if err := writeReportFile(r.job.SessionDirectory(), report); err != nil {
			errs = append(errs, err)
		}
	}
	if r.logHandler != nil {
		errs = append(errs, r.logHandler.Flush(ctx))
	}
	if r.cloud != nil {
		errs = append(errs, r.cloud.ForceFlush(ctx))
	}
	if r.uploader != nil {
		errs = append(errs, r.uploader.Upload(ctx, report))
	}
	if r.logHandler != nil {
		errs = append(errs, r.logHandler.Shutdown(ctx))
	}
	if r.cloud != nil {
		errs = append(errs, r.cloud.Shutdown(ctx))
	}
	r.finalizeErr = errors.Join(errs...)
}

func (r *Runtime[UserData]) createReport() *voice.SessionReport {
	r.eventMu.Lock()
	events := append([]voice.Event(nil), r.events...)
	r.eventMu.Unlock()
	job := r.job.Job()
	jobID, roomID, roomName := "", "", ""
	if job != nil {
		jobID = job.GetId()
		if job.GetRoom() != nil {
			roomID, roomName = job.GetRoom().GetSid(), job.GetRoom().GetName()
		}
	}
	if roomID == "" && !r.job.IsFakeJob() && r.job.Room() != nil {
		roomID = r.job.Room().SID()
	}
	if roomName == "" && !r.job.IsFakeJob() && r.job.Room() != nil {
		roomName = r.job.Room().Name()
	}
	path := ""
	var startedAt *time.Time
	if r.recorder != nil {
		path, _ = r.recorder.OutputPath()
		if value, ok := r.recorder.RecordingStartedAt(); ok {
			startedAt = &value
		}
	} else if r.console != nil {
		info := r.console.RecordingInfo()
		path = info.OutputPath
		if info.HasStarted {
			value := info.StartedAt
			startedAt = &value
		}
	}
	return voice.CreateSessionReport(voice.SessionReportOptions{
		JobID: jobID, RoomID: roomID, Room: roomName,
		Options: r.session.ReportOptions(r.recording), Events: events,
		ChatHistory: r.session.ChatContext(), EnableRecording: r.recording.Enabled(),
		StartedAt: r.startedAt, Timestamp: time.Now(), AudioRecordingPath: path,
		AudioRecordingStartedAt: startedAt, ModelUsage: r.session.Usage().ModelUsage,
	})
}

func writeReportFile(directory string, report *voice.SessionReport) error {
	if directory == "" {
		return errors.New("voice/livekit: session report directory is empty")
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("voice/livekit: create report directory: %w", err)
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("voice/livekit: encode session report: %w", err)
	}
	data = append(data, '\n')
	temporary, err := os.CreateTemp(directory, ".session-report-*.json")
	if err != nil {
		return fmt.Errorf("voice/livekit: create temporary session report: %w", err)
	}
	temporaryName := temporary.Name()
	cleanup := func() { _ = os.Remove(temporaryName) }
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		cleanup()
		return err
	}
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		cleanup()
		return fmt.Errorf("voice/livekit: write session report: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		cleanup()
		return fmt.Errorf("voice/livekit: sync session report: %w", err)
	}
	if err := temporary.Close(); err != nil {
		cleanup()
		return fmt.Errorf("voice/livekit: close session report: %w", err)
	}
	if err := os.Rename(temporaryName, filepath.Join(directory, "session_report.json")); err != nil {
		cleanup()
		return fmt.Errorf("voice/livekit: commit session report: %w", err)
	}
	return nil
}

// Close performs both lifecycle stages. The job runner calls the two explicit
// methods itself to preserve room-disconnect ordering.
func (r *Runtime[UserData]) Close(ctx context.Context) error {
	return errors.Join(r.CloseSession(ctx), r.FinalizeSession(ctx))
}

func (r *Runtime[UserData]) Primary() bool {
	return r != nil && r.registration != nil && r.registration.Primary
}
func (r *Runtime[UserData]) RecordingOptions() voice.RecordingOptions {
	if r == nil {
		return voice.DisabledRecordingOptions()
	}
	return r.recording
}
func (r *Runtime[UserData]) RoomIO() *roomio.RoomIO {
	if r == nil {
		return nil
	}
	return r.roomIO
}
func (r *Runtime[UserData]) RecorderIO() *recorderio.RecorderIO {
	if r == nil {
		return nil
	}
	return r.recorder
}
func (r *Runtime[UserData]) Logger() *slog.Logger {
	if r == nil || r.logger == nil {
		return slog.Default()
	}
	return r.logger
}
func (r *Runtime[UserData]) Report() *voice.SessionReport {
	if r == nil {
		return nil
	}
	r.reportMu.RLock()
	report := r.report
	r.reportMu.RUnlock()
	return cloneReport(report)
}
func (r *Runtime[UserData]) ReportStats() ReportStats {
	if r == nil {
		return ReportStats{}
	}
	r.eventMu.Lock()
	result := ReportStats{RetainedEvents: uint64(len(r.events)), RetainedBytes: uint64(r.eventBytes), DroppedEvents: r.dropped, CaptureError: r.captureErr}
	r.eventMu.Unlock()
	if r.eventSub != nil {
		result.DroppedEvents += r.eventSub.Dropped()
	}
	if result.DroppedEvents != 0 && result.CaptureError == nil {
		result.CaptureError = ErrReportEventLimit
	}
	return result
}

func cloneReport(report *voice.SessionReport) *voice.SessionReport {
	if report == nil {
		return nil
	}
	return voice.CreateSessionReport(voice.SessionReportOptions{
		JobID: report.JobID, RoomID: report.RoomID, Room: report.Room, Options: cloneReportOptions(report.Options),
		Events: report.Events, ChatHistory: report.ChatHistory, EnableRecording: report.EnableRecording,
		StartedAt: report.StartedAt, Timestamp: report.Timestamp, AudioRecordingPath: report.AudioRecordingPath,
		AudioRecordingStartedAt: report.AudioRecordingStartedAt, ModelUsage: report.ModelUsage,
	})
}

func cloneReportOptions(options voice.ReportSessionOptions) voice.ReportSessionOptions {
	result := options
	result.Interruption.Detector = nil
	if options.Interruption.FalseInterruptionTimeout != nil {
		value := *options.Interruption.FalseInterruptionTimeout
		result.Interruption.FalseInterruptionTimeout = &value
	}
	if options.Interruption.BackchannelBoundary != nil {
		value := *options.Interruption.BackchannelBoundary
		result.Interruption.BackchannelBoundary = &value
	}
	if options.UserAwayTimeout != nil {
		value := *options.UserAwayTimeout
		result.UserAwayTimeout = &value
	}
	return result
}

func (r *Runtime[UserData]) reportError(err error) {
	if err == nil {
		return
	}
	r.logger.Error("LiveKit session integration error", "error", err)
	if r.opts.OnError != nil {
		invokeError(r.opts.OnError, err)
	}
}

func invokeError(callback func(error), err error) {
	defer func() { _ = recover() }()
	callback(err)
}

func invokeReportCallback(ctx context.Context, callback func(context.Context, *voice.SessionReport) error, report *voice.SessionReport) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("voice/livekit: OnReport panicked: %v", recovered)
		}
	}()
	if err := callback(ctx, cloneReport(report)); err != nil {
		return fmt.Errorf("voice/livekit: OnReport: %w", err)
	}
	return nil
}

var _ agents.JobSessionLifecycle = (*Runtime[struct{}])(nil)
