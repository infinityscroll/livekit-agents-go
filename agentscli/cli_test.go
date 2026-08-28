// SPDX-License-Identifier: Apache-2.0

package agentscli

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/voice"
	agentpb "github.com/livekit/protocol/livekit/agent"
	"google.golang.org/protobuf/proto"
)

func testEnvironment(values map[string]string) func(string) string {
	return func(key string) string { return values[key] }
}

func TestParseArgumentsPrecedenceAndModes(t *testing.T) {
	config, err := parseArguments([]string{
		"--url", "wss://cli.example", "start", "--dev", "--log-level=error",
		"--log-format", "colored", "--drain-timeout", "30s", "--cli-addr", "127.0.0.1:9090",
	}, testEnvironment(map[string]string{"LIVEKIT_LOG_LEVEL": "warn", "LOG_LEVEL": "debug"}))
	if err != nil {
		t.Fatal(err)
	}
	if config.command != "start" || config.production || config.url != "wss://cli.example" || config.logLevel != "error" || config.logFormat != "text" {
		t.Fatalf("config = %#v", config)
	}
	if !config.drainTimeoutSet || config.drainTimeout != 30*time.Second || config.cliAddress != "127.0.0.1:9090" {
		t.Fatalf("duration/dev config = %#v", config)
	}

	config, err = parseArguments([]string{"start", "--dev=false"}, testEnvironment(map[string]string{"LIVEKIT_LOG_LEVEL": "warn", "LOG_LEVEL": "debug"}))
	if err != nil {
		t.Fatal(err)
	}
	if !config.production || config.logLevel != "warn" || config.logFormat != "json" {
		t.Fatalf("production config = %#v", config)
	}
}

func TestParseArgumentsValidation(t *testing.T) {
	for _, test := range []struct {
		args []string
		want string
	}{
		{[]string{"connect"}, "requires --room"},
		{[]string{"console"}, "requires --connect-addr"},
		{[]string{"start", "--cli-addr", "127.0.0.1:1"}, "requires development mode"},
		{[]string{"start", "--drain-timeout", "0s"}, "invalid --drain-timeout"},
		{[]string{"unknown"}, "unknown command"},
		{[]string{"start", "--unknown"}, "unknown option"},
	} {
		_, err := parseArguments(test.args, testEnvironment(nil))
		if err == nil || !strings.Contains(err.Error(), test.want) {
			t.Fatalf("parse %v = %v, want %q", test.args, err, test.want)
		}
	}
}

func TestHelpAndVersionAreLazy(t *testing.T) {
	for _, args := range [][]string{nil, {"help"}, {"version"}} {
		var stdout, stderr bytes.Buffer
		code := MainApp(context.Background(), AppOptions[struct{}]{}, args, IO{
			Stdout: &stdout, Stderr: &stderr,
			Getenv: testEnvironment(map[string]string{"LIVEKIT_LOG_LEVEL": "not-a-level"}),
		})
		if code != 0 || stderr.Len() != 0 || stdout.Len() == 0 {
			t.Fatalf("args=%v code=%d stdout=%q stderr=%q", args, code, stdout.String(), stderr.String())
		}
	}
}

type downloadTestPlugin struct {
	title, version, packageName string
	err                         error
	called                      *int
}

func (p downloadTestPlugin) Title() string   { return p.title }
func (p downloadTestPlugin) Version() string { return p.version }
func (p downloadTestPlugin) Package() string { return p.packageName }
func (p downloadTestPlugin) DownloadFiles(context.Context) error {
	*p.called++
	return p.err
}

func TestDownloadPluginFilesDeterministicAndAggregated(t *testing.T) {
	var first, second int
	plugins := []agents.Plugin{
		downloadTestPlugin{title: "Second", version: "2", packageName: "z.example", called: &second, err: errors.New("offline")},
		downloadTestPlugin{title: "First", version: "1", packageName: "a.example", called: &first},
	}
	failures := DownloadPluginFiles(context.Background(), slog.New(slog.NewTextHandler(io.Discard, nil)), plugins)
	if first != 1 || second != 1 || len(failures) != 1 {
		t.Fatalf("calls=%d/%d failures=%#v", first, second, failures)
	}
	message := FormatDownloadFailureMessage(failures)
	if message != "Failed to download files for 1 plugin:\n- Second (z.example@2): offline" {
		t.Fatalf("message = %q", message)
	}
}

func TestDevClientServerInfoFrame(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	client := startDevClient(listener.Addr().String(), "concierge", "wss://example.livekit.cloud", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if client == nil {
		t.Fatal("client not started")
	}
	defer client.Close()
	connection, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	_ = connection.SetReadDeadline(time.Now().Add(time.Second))
	header := make([]byte, 4)
	if _, err := io.ReadFull(connection, header); err != nil {
		t.Fatal(err)
	}
	size := binary.BigEndian.Uint32(header)
	if size == 0 || size > 1<<20 {
		t.Fatalf("frame size = %d", size)
	}
	payload := make([]byte, size)
	if _, err := io.ReadFull(connection, payload); err != nil {
		t.Fatal(err)
	}
	var message agentpb.AgentDevMessage
	if err := proto.Unmarshal(payload, &message); err != nil {
		t.Fatal(err)
	}
	if message.GetServerInfo().GetAgentName() != "concierge" || message.GetServerInfo().GetUrl() != "wss://example.livekit.cloud" {
		t.Fatalf("server info = %#v", message.GetServerInfo())
	}
}

type payloadFailWriter struct {
	calls int
	err   error
}

func (w *payloadFailWriter) Write(payload []byte) (int, error) {
	w.calls++
	if w.calls == 2 {
		return 0, w.err
	}
	return len(payload), nil
}

func TestWriteDevFrameReturnsPayloadWriteError(t *testing.T) {
	t.Parallel()
	want := errors.New("payload write failed")
	writer := &payloadFailWriter{err: want}
	if err := writeDevFrame(writer, []byte("payload")); !errors.Is(err, want) {
		t.Fatalf("writeDevFrame() error = %v, want %v", err, want)
	}
	if writer.calls != 2 {
		t.Fatalf("write calls = %d, want 2", writer.calls)
	}
}

func TestMainAppConsoleRunner(t *testing.T) {
	var called int
	var received ConsoleOptions
	app := AppOptions[struct{}]{Console: func(_ context.Context, options ConsoleOptions) error {
		called++
		received = options
		return nil
	}}
	code := MainApp(context.Background(), app, []string{"console", "--connect-addr", "127.0.0.1:4000", "--record"}, IO{
		Stdout: io.Discard, Stderr: io.Discard, Getenv: testEnvironment(nil),
	})
	if code != 0 || called != 1 || received.ConnectAddress != "127.0.0.1:4000" || !received.Record {
		t.Fatalf("code=%d called=%d options=%#v", code, called, received)
	}
}

func TestMainAppConsoleAutoRunner(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	signals := make(chan os.Signal, 1)
	started := make(chan struct{})
	var accepted atomic.Bool
	connectionDone := make(chan struct{})
	go func() {
		connection, acceptErr := listener.Accept()
		if acceptErr != nil {
			close(connectionDone)
			return
		}
		accepted.Store(true)
		defer connection.Close()
		<-started
		signals <- osInterruptSignal{}
		<-connectionDone
	}()
	app := AppOptions[struct{}]{Server: agents.ServerOptions[struct{}]{JobEntrypoint: func(_ context.Context, job *agents.JobContext[struct{}]) error {
		session, sessionErr := voice.NewAgentSession(voice.AgentSessionOptions[struct{}]{DisableUserAwayTimeout: true})
		if sessionErr != nil {
			return sessionErr
		}
		if err := job.AddShutdownCallback(func(ctx context.Context, _ string) error { return session.Close(ctx) }); err != nil {
			return err
		}
		if err := session.Start(job.Context(), voice.MustAgent(voice.AgentOptions[struct{}]{ID: "console_agent"})); err != nil {
			return err
		}
		close(started)
		return nil
	}}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	code := MainApp(ctx, app, []string{"console", "--connect-addr", listener.Addr().String()}, IO{
		Stdout: io.Discard, Stderr: io.Discard, Getenv: testEnvironment(nil), Signals: signals,
	})
	close(connectionDone)
	if code != 130 || !accepted.Load() {
		t.Fatalf("code=%d accepted=%t", code, accepted.Load())
	}
}

func TestSplitConsoleAddress(t *testing.T) {
	for _, value := range []string{"127.0.0.1:4000", "localhost:65535", "[::1]:4000"} {
		if _, _, err := splitConsoleAddress(value); err != nil {
			t.Fatalf("split %q: %v", value, err)
		}
	}
	for _, value := range []string{"localhost", "::1:4000", "localhost:0", "localhost:65536"} {
		if _, _, err := splitConsoleAddress(value); err == nil {
			t.Fatalf("split %q succeeded", value)
		}
	}
}

func TestMergePluginsFirstRegistrationWins(t *testing.T) {
	var calls int
	first := downloadTestPlugin{title: "first", version: "1", packageName: "same", called: &calls}
	second := downloadTestPlugin{title: "second", version: "2", packageName: "same", called: &calls}
	merged := mergePlugins([]agents.Plugin{first}, []agents.Plugin{second})
	if len(merged) != 1 || merged[0].Title() != "first" {
		t.Fatalf("merged = %#v", merged)
	}
}

func TestSignalExitCode(t *testing.T) {
	if signalExitCode(osInterruptSignal{}) != 130 {
		t.Fatal("non-SIGTERM signal should use 130")
	}
}

type osInterruptSignal struct{}

func (osInterruptSignal) String() string { return "interrupt" }
func (osInterruptSignal) Signal()        {}
