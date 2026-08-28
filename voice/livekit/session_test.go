// SPDX-License-Identifier: Apache-2.0

package livekit

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/llm"
	"github.com/livekit/agents-go/voice"
	"github.com/livekit/agents-go/voice/recorderio"
	"github.com/livekit/agents-go/voice/roomio"
	agentpb "github.com/livekit/protocol/livekit/agent"
)

func boolPointer(value bool) *bool { return &value }

type testSessionTransport struct{}

func (testSessionTransport) Start(context.Context) error { return nil }
func (testSessionTransport) SendMessage(context.Context, *agentpb.AgentSessionMessage) error {
	return nil
}
func (testSessionTransport) Recv(ctx context.Context) (*agentpb.AgentSessionMessage, error) {
	<-ctx.Done()
	return nil, context.Cause(ctx)
}
func (testSessionTransport) Close(context.Context) error { return nil }

type delayedReportRecording struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
	closed  atomic.Bool
	path    string
	start   time.Time
}

func (r *delayedReportRecording) Close(ctx context.Context) error {
	r.once.Do(func() { close(r.started) })
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-r.release:
		r.closed.Store(true)
		return nil
	}
}

func (r *delayedReportRecording) OutputPath() (string, bool) {
	return r.path, r.closed.Load()
}

func (r *delayedReportRecording) RecordingStartedAt() (time.Time, bool) {
	return r.start, r.closed.Load()
}

func TestStartFakePrimaryFinalizesBoundedReport(t *testing.T) {
	directory := t.TempDir()
	var runtime *Runtime[struct{}]
	var callbackReport *voice.SessionReport
	server := agents.ServerOptions[struct{}]{
		JobEntrypoint: func(ctx context.Context, job *agents.JobContext[struct{}]) error {
			session, err := voice.NewAgentSession(voice.AgentSessionOptions[struct{}]{
				DisableUserAwayTimeout: true,
			})
			if err != nil {
				return err
			}
			agent := voice.MustAgent(voice.AgentOptions[struct{}]{ID: "report_agent"})
			runtime, err = Start(ctx, session, agent, StartOptions[struct{}]{
				Job:             job,
				Recording:       agents.Use(voice.RecordingOptionsUpdate{Audio: boolPointer(false)}),
				MaxReportEvents: 1,
				OnReport: func(_ context.Context, report *voice.SessionReport) error {
					callbackReport = report
					return nil
				},
			})
			if err != nil {
				return err
			}
			job.Shutdown("test complete")
			return nil
		},
	}
	if err := agents.RunConsoleJob(t.Context(), agents.ConsoleJobOptions[struct{}]{
		Server: server, SessionDirectory: directory,
	}); err != nil {
		t.Fatal(err)
	}
	if runtime == nil || !runtime.Primary() || runtime.Report() == nil || callbackReport == nil {
		t.Fatalf("runtime was not finalized: runtime=%v report=%v callback=%v", runtime != nil, runtime != nil && runtime.Report() != nil, callbackReport != nil)
	}
	stats := runtime.ReportStats()
	if stats.RetainedEvents > 1 || stats.DroppedEvents == 0 {
		t.Fatalf("bounded event stats=%#v", stats)
	}
	if !errors.Is(stats.CaptureError, ErrReportEventLimit) {
		t.Fatalf("bounded event capture error=%v", stats.CaptureError)
	}
	report := runtime.Report()
	if len(report.Events) != 1 || report.Events[0].Type() != voice.EventClose {
		t.Fatalf("final retained events=%v", report.Events)
	}
	path := filepath.Join(directory, "session_report.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]any
	if err := json.Unmarshal(data, &wire); err != nil {
		t.Fatal(err)
	}
	if wire["job_id"] == "" || wire["sdk_version"] != agents.Version {
		t.Fatalf("unexpected report wire: %#v", wire)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("report mode=%#o", info.Mode().Perm())
	}

	callbackTimeout := callbackReport.Options.Interruption.FalseInterruptionTimeout
	callbackBoundary := callbackReport.Options.Interruption.BackchannelBoundary
	if callbackTimeout == nil || callbackBoundary == nil {
		t.Fatal("callback report omitted resolved interruption defaults")
	}
	*callbackTimeout = 99 * time.Second
	callbackBoundary.Start = 99 * time.Second
	first := runtime.Report()
	if *first.Options.Interruption.FalseInterruptionTimeout == 99*time.Second || first.Options.Interruption.BackchannelBoundary.Start == 99*time.Second {
		t.Fatal("OnReport mutation changed the retained report")
	}
	*first.Options.Interruption.FalseInterruptionTimeout = 88 * time.Second
	first.Options.Interruption.BackchannelBoundary.Start = 88 * time.Second
	second := runtime.Report()
	if *second.Options.Interruption.FalseInterruptionTimeout == 88*time.Second || second.Options.Interruption.BackchannelBoundary.Start == 88*time.Second {
		t.Fatal("Report returned mutable option aliases")
	}
}

func TestStartRollbackReleasesPrimaryAfterOwnedCleanup(t *testing.T) {
	sentinel := errors.New("toolset setup failed")
	var replacement *Runtime[struct{}]
	server := agents.ServerOptions[struct{}]{JobEntrypoint: func(ctx context.Context, job *agents.JobContext[struct{}]) error {
		toolset, err := llm.NewToolset(llm.ToolsetOptions{
			ID: "failing", Setup: func(context.Context, llm.ToolsetContext) error { return sentinel },
		})
		if err != nil {
			return err
		}
		tools, err := llm.NewToolContext(toolset)
		if err != nil {
			return err
		}
		failed, err := voice.NewAgentSession(voice.AgentSessionOptions[struct{}]{DisableUserAwayTimeout: true})
		if err != nil {
			return err
		}
		_, startErr := Start(ctx, failed, voice.MustAgent(voice.AgentOptions[struct{}]{ID: "failing", Tools: tools}), StartOptions[struct{}]{Job: job})
		if !errors.Is(startErr, sentinel) {
			return errors.New("failed Start did not preserve toolset setup error")
		}
		_ = failed.Close(ctx)

		session, err := voice.NewAgentSession(voice.AgentSessionOptions[struct{}]{DisableUserAwayTimeout: true})
		if err != nil {
			return err
		}
		replacement, err = Start(ctx, session, voice.MustAgent(voice.AgentOptions[struct{}]{ID: "replacement"}), StartOptions[struct{}]{Job: job})
		if err != nil {
			return err
		}
		if !replacement.Primary() {
			return errors.New("replacement did not claim the released primary slot")
		}
		job.Shutdown("complete")
		return nil
	}}
	if err := agents.RunConsoleJob(t.Context(), agents.ConsoleJobOptions[struct{}]{Server: server, SessionDirectory: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
}

func TestStartPrimarySecondaryRecordingSelection(t *testing.T) {
	server := agents.ServerOptions[struct{}]{JobEntrypoint: func(ctx context.Context, job *agents.JobContext[struct{}]) error {
		newSession := func() (*voice.AgentSession[struct{}], error) {
			return voice.NewAgentSession(voice.AgentSessionOptions[struct{}]{DisableUserAwayTimeout: true})
		}
		primarySession, err := newSession()
		if err != nil {
			return err
		}
		primary, err := Start(ctx, primarySession, voice.MustAgent(voice.AgentOptions[struct{}]{ID: "primary"}), StartOptions[struct{}]{Job: job})
		if err != nil {
			return err
		}
		if !primary.Primary() || !primary.RecordingOptions().Enabled() {
			return errors.New("primary did not inherit job recording")
		}

		secondarySession, err := newSession()
		if err != nil {
			return err
		}
		secondary, err := Start(ctx, secondarySession, voice.MustAgent(voice.AgentOptions[struct{}]{ID: "secondary"}), StartOptions[struct{}]{Job: job})
		if err != nil {
			return err
		}
		if secondary.Primary() || secondary.RecordingOptions().Enabled() {
			return errors.New("secondary inherited primary recording ownership")
		}

		explicitSession, err := newSession()
		if err != nil {
			return err
		}
		_, err = Start(ctx, explicitSession, voice.MustAgent(voice.AgentOptions[struct{}]{ID: "explicit"}), StartOptions[struct{}]{
			Job: job, Recording: agents.Use(voice.RecordingOptionsUpdate{}),
		})
		if !errors.Is(err, agents.ErrPrimarySessionRecording) {
			return errors.New("explicit secondary recording did not fail with ErrPrimarySessionRecording")
		}
		_ = explicitSession.Close(ctx)
		if err := secondary.Close(ctx); err != nil {
			return err
		}
		job.Shutdown("complete")
		return nil
	}}
	if err := agents.RunConsoleJob(t.Context(), agents.ConsoleJobOptions[struct{}]{Server: server, Record: true, SessionDirectory: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
}

func TestStartRejectsIgnoredFakeJobOwnedResources(t *testing.T) {
	providedRoom := new(roomio.RoomIO)
	providedRecorder, err := recorderio.NewRecorderIO(recorderio.RecorderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	server := agents.ServerOptions[struct{}]{JobEntrypoint: func(ctx context.Context, job *agents.JobContext[struct{}]) error {
		first, err := voice.NewAgentSession(voice.AgentSessionOptions[struct{}]{DisableUserAwayTimeout: true})
		if err != nil {
			return err
		}
		_, err = Start(ctx, first, voice.MustAgent(voice.AgentOptions[struct{}]{ID: "room"}), StartOptions[struct{}]{Job: job, RoomIO: providedRoom})
		if !errors.Is(err, errFakeJobRoomIO) {
			return errors.New("fake job did not reject supplied RoomIO")
		}
		_ = first.Close(ctx)

		second, err := voice.NewAgentSession(voice.AgentSessionOptions[struct{}]{DisableUserAwayTimeout: true})
		if err != nil {
			return err
		}
		_, err = Start(ctx, second, voice.MustAgent(voice.AgentOptions[struct{}]{ID: "recorder"}), StartOptions[struct{}]{Job: job, Recorder: providedRecorder})
		if !errors.Is(err, errInactiveRecorder) {
			return errors.New("fake job did not reject supplied Recorder")
		}
		_ = second.Close(ctx)

		good, err := voice.NewAgentSession(voice.AgentSessionOptions[struct{}]{DisableUserAwayTimeout: true})
		if err != nil {
			return err
		}
		runtime, err := Start(ctx, good, voice.MustAgent(voice.AgentOptions[struct{}]{ID: "good"}), StartOptions[struct{}]{Job: job})
		if err != nil || !runtime.Primary() {
			return errors.New("rejected resources consumed the primary registration")
		}
		job.Shutdown("complete")
		return nil
	}}
	if err := agents.RunConsoleJob(t.Context(), agents.ConsoleJobOptions[struct{}]{Server: server, SessionDirectory: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	input := voice.NewBaseAudioInput(t.Context(), 1, nil)
	if _, err := providedRecorder.RecordInput(input); err != nil {
		t.Fatalf("rejected Recorder ownership was consumed: %v", err)
	}
	if err := providedRecorder.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestCloseSessionDoesNotRetainClosedRecorderDecorators(t *testing.T) {
	closeStarted := make(chan struct{})
	releaseClose := make(chan struct{})
	var closeOnce sync.Once
	toolset, err := llm.NewToolset(llm.ToolsetOptions{
		ID: "slow-close",
		Close: func(ctx context.Context) error {
			closeOnce.Do(func() { close(closeStarted) })
			select {
			case <-ctx.Done():
				return context.Cause(ctx)
			case <-releaseClose:
				return nil
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	tools, err := llm.NewToolContext(toolset)
	if err != nil {
		t.Fatal(err)
	}
	session, err := voice.NewAgentSession(voice.AgentSessionOptions[struct{}]{DisableUserAwayTimeout: true, ShutdownTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	input := voice.NewBaseAudioInput(t.Context(), 1, nil)
	output, err := voice.NewManagedAudioOutput(voice.AudioOutputOptions{SampleRate: 48_000})
	if err != nil {
		t.Fatal(err)
	}
	recorder, err := recorderio.NewRecorderIO(recorderio.RecorderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	recordedInput, err := recorder.RecordInput(input)
	if err != nil {
		t.Fatal(err)
	}
	recordedOutput, err := recorder.RecordOutput(output)
	if err != nil {
		t.Fatal(err)
	}
	session.Input().SetAudio(recordedInput)
	session.Output().SetAudio(recordedOutput)
	if err := session.Start(t.Context(), voice.MustAgent(voice.AgentOptions[struct{}]{ID: "slow_close", Tools: tools})); err != nil {
		t.Fatal(err)
	}
	eventDone := make(chan struct{})
	close(eventDone)
	runtime := &Runtime[struct{}]{
		session: session, recorder: recorder, ownRecorder: true,
		originalInput: input, originalOutput: output, recordedInput: recordedInput, recordedOutput: recordedOutput, eventDone: eventDone,
		closeDone: make(chan struct{}), opts: StartOptions[struct{}]{CloseTimeout: 30 * time.Millisecond},
	}
	if err := runtime.CloseSession(t.Context()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("CloseSession error=%v", err)
	}
	select {
	case <-closeStarted:
	default:
		t.Fatal("session did not reach the blocking final drain")
	}
	if session.Input().Audio() != input || session.Output().Audio() != output {
		t.Fatalf("timed-out session retained recorder decorators: input=%T output=%T", session.Input().Audio(), session.Output().Audio())
	}
	close(releaseClose)
	if err := session.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestCloseSessionMakesIncompleteEventDrainVisible(t *testing.T) {
	session, err := voice.NewAgentSession(voice.AgentSessionOptions[struct{}]{DisableUserAwayTimeout: true})
	if err != nil {
		t.Fatal(err)
	}
	eventCtx, eventCancel := context.WithCancelCause(context.Background())
	runtime := &Runtime[struct{}]{
		session: session, eventCtx: eventCtx, eventCancel: eventCancel, eventDone: make(chan struct{}),
		closeDone: make(chan struct{}), opts: StartOptions[struct{}]{CloseTimeout: 5 * time.Millisecond},
	}
	if err := runtime.CloseSession(t.Context()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("CloseSession error=%v", err)
	}
	if !errors.Is(runtime.ReportStats().CaptureError, context.DeadlineExceeded) {
		t.Fatalf("capture error=%v", runtime.ReportStats().CaptureError)
	}
}

func TestConsoleRecordForcesFakePrimaryReportPersistence(t *testing.T) {
	directory := t.TempDir()
	console, err := voice.NewAgentsConsole(voice.AgentsConsoleOptions{Enabled: true, Record: true, Transport: testSessionTransport{}})
	if err != nil {
		t.Fatal(err)
	}
	restore := voice.SetDefaultAgentsConsole(console)
	defer restore()
	server := agents.ServerOptions[struct{}]{JobEntrypoint: func(ctx context.Context, job *agents.JobContext[struct{}]) error {
		session, err := voice.NewAgentSession(voice.AgentSessionOptions[struct{}]{DisableUserAwayTimeout: true})
		if err != nil {
			return err
		}
		_, err = Start(ctx, session, voice.MustAgent(voice.AgentOptions[struct{}]{ID: "console_record"}), StartOptions[struct{}]{
			Job: job, Recording: agents.Disable[voice.RecordingOptionsUpdate](),
		})
		if err != nil {
			return err
		}
		job.Shutdown("complete")
		return nil
	}}
	if err := agents.RunConsoleJob(t.Context(), agents.ConsoleJobOptions[struct{}]{Server: server, Record: false, SessionDirectory: directory}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(directory, "session_report.json")); err != nil {
		t.Fatalf("console --record did not persist the primary report: %v", err)
	}
	if err := console.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestFinalizeWaitsForConsoleRecordingAfterCloseDeadline(t *testing.T) {
	directory := t.TempDir()
	recording := &delayedReportRecording{
		started: make(chan struct{}), release: make(chan struct{}),
		path: filepath.Join(directory, "audio.ogg"), start: time.Now().Add(-time.Second),
	}
	console, err := voice.NewAgentsConsole(voice.AgentsConsoleOptions{Enabled: true, Record: true, Transport: testSessionTransport{}})
	if err != nil {
		t.Fatal(err)
	}
	input := voice.NewBaseAudioInput(t.Context(), 1, nil)
	output, err := voice.NewManagedAudioOutput(voice.AudioOutputOptions{SampleRate: 48_000})
	if err != nil {
		t.Fatal(err)
	}
	if err := console.SetRecordingIO(input, output, recording); err != nil {
		t.Fatal(err)
	}
	restore := voice.SetDefaultAgentsConsole(console)
	defer restore()

	var callbackSawFinalized atomic.Bool
	server := agents.ServerOptions[struct{}]{JobEntrypoint: func(ctx context.Context, job *agents.JobContext[struct{}]) error {
		session, err := voice.NewAgentSession(voice.AgentSessionOptions[struct{}]{DisableUserAwayTimeout: true})
		if err != nil {
			return err
		}
		_, err = Start(ctx, session, voice.MustAgent(voice.AgentOptions[struct{}]{ID: "delayed_recording"}), StartOptions[struct{}]{
			Job: job, Recording: agents.Disable[voice.RecordingOptionsUpdate](),
			CloseTimeout: 5 * time.Millisecond, FinalizeTimeout: time.Second,
			OnReport: func(_ context.Context, report *voice.SessionReport) error {
				if recording.closed.Load() && report.AudioRecordingPath == recording.path && report.AudioRecordingStartedAt != nil {
					callbackSawFinalized.Store(true)
				}
				return nil
			},
		})
		if err != nil {
			return err
		}
		go func() {
			<-recording.started
			time.Sleep(25 * time.Millisecond)
			close(recording.release)
		}()
		job.Shutdown("complete")
		return nil
	}}
	runErr := agents.RunConsoleJob(t.Context(), agents.ConsoleJobOptions[struct{}]{Server: server, SessionDirectory: directory})
	if runErr != nil && !errors.Is(runErr, context.DeadlineExceeded) {
		t.Fatal(runErr)
	}
	if !recording.closed.Load() || !callbackSawFinalized.Load() {
		t.Fatalf("report raced recorder finalization: closed=%v callback=%v", recording.closed.Load(), callbackSawFinalized.Load())
	}
	if err := console.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestObservabilityHostname(t *testing.T) {
	tests := []struct {
		url, override, host string
		enabled             bool
		wantErr             bool
	}{
		{"wss://project.livekit.cloud", "", "project.livekit.cloud", true, false},
		{"wss://self-hosted.example", "", "self-hosted.example", false, false},
		{"wss://self-hosted.example", "https://observe.example:8443", "observe.example:8443", true, false},
		{"wss://self-hosted.example", "https://observe.example/path", "", false, true},
	}
	for _, test := range tests {
		host, enabled, err := observabilityHostname(test.url, test.override)
		if (err != nil) != test.wantErr || host != test.host || enabled != test.enabled {
			t.Fatalf("observabilityHostname(%q,%q)=(%q,%v,%v)", test.url, test.override, host, enabled, err)
		}
	}
}
