// SPDX-License-Identifier: Apache-2.0

package roomio

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	agents "github.com/livekit/agents-go"
	lksdk "github.com/livekit/server-sdk-go/v2"
)

type fakeTextPublisher struct {
	mu      sync.Mutex
	local   string
	trackID string
	writers []*fakeTextWriter
}

func (*fakeTextPublisher) Connected() bool                   { return true }
func (p *fakeTextPublisher) LocalIdentity() string           { return p.local }
func (p *fakeTextPublisher) MicrophoneTrackID(string) string { return p.trackID }
func (p *fakeTextPublisher) Stream(options lksdk.StreamTextOptions) textWriter {
	writer := &fakeTextWriter{options: options}
	writer.options.Attributes = cloneStringMap(options.Attributes)
	p.mu.Lock()
	p.writers = append(p.writers, writer)
	p.mu.Unlock()
	return writer
}
func (p *fakeTextPublisher) snapshot() []*fakeTextWriter {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]*fakeTextWriter(nil), p.writers...)
}

type fakeTextWriter struct {
	mu      sync.Mutex
	options lksdk.StreamTextOptions
	writes  []string
	closed  bool
}

type blockingTextPublisher struct{ writer textWriter }

func (*blockingTextPublisher) Connected() bool                             { return true }
func (*blockingTextPublisher) LocalIdentity() string                       { return "agent" }
func (*blockingTextPublisher) MicrophoneTrackID(string) string             { return "" }
func (p *blockingTextPublisher) Stream(lksdk.StreamTextOptions) textWriter { return p.writer }

type deadlineTextWriter struct{ started chan struct{} }

func (w *deadlineTextWriter) Write(ctx context.Context, _ string) error {
	select {
	case <-w.started:
	default:
		close(w.started)
	}
	<-ctx.Done()
	return context.Cause(ctx)
}
func (*deadlineTextWriter) Close() {}

type blockedTextWriter struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *blockedTextWriter) Write(context.Context, string) error {
	w.once.Do(func() { close(w.started) })
	<-w.release
	return nil
}
func (w *blockedTextWriter) Close() { <-w.release }

func (w *fakeTextWriter) Write(ctx context.Context, text string) error {
	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	default:
	}
	w.mu.Lock()
	w.writes = append(w.writes, text)
	w.mu.Unlock()
	return nil
}
func (w *fakeTextWriter) Close() { w.mu.Lock(); w.closed = true; w.mu.Unlock() }
func (w *fakeTextWriter) snapshot() ([]string, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.writes...), w.closed
}

func TestParticipantTranscriptionDeltaMarkupAndAttributes(t *testing.T) {
	t.Parallel()
	publisher := &fakeTextPublisher{local: "agent", trackID: "TR_mic"}
	options := DefaultRoomOutputOptions()
	options.ExpressiveEnabled = func() bool { return true }
	output := newParticipantTranscriptionOutput(context.Background(), publisher, true, "agent", options)
	output.OnAttached()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := output.Close(ctx); err != nil {
			t.Errorf("Close: %v", err)
		}
	})

	if err := output.CaptureText(context.Background(), agents.TimedString{Text: `<emotion value="happy"/>Hello`}); err != nil {
		t.Fatal(err)
	}
	if err := output.CaptureText(context.Background(), agents.TimedString{Text: " world"}); err != nil {
		t.Fatal(err)
	}
	if err := output.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	writers := publisher.snapshot()
	if len(writers) != 1 {
		t.Fatalf("writers = %d", len(writers))
	}
	writes, closed := writers[0].snapshot()
	if strings.Join(writes, "") != "Hello world" || !closed {
		t.Fatalf("writes=%q closed=%v", writes, closed)
	}
	attributes := writers[0].options.Attributes
	if attributes[AttributeTranscriptionFinal] != "false" || attributes[AttributeTranscriptionTrackID] != "TR_mic" || attributes[AttributeTranscriptionSegmentID] == "" {
		t.Fatalf("attributes = %#v", attributes)
	}
	if attributes[agents.AttributeTranscriptionExpression] == "" {
		t.Fatalf("expression attribute missing: %#v", attributes)
	}
}

func TestParticipantTranscriptionNonDeltaJSONFinal(t *testing.T) {
	t.Parallel()
	publisher := &fakeTextPublisher{local: "agent", trackID: "TR_user"}
	options := DefaultRoomOutputOptions()
	options.JSONFormat = true
	output := newParticipantTranscriptionOutput(context.Background(), publisher, false, "user", options)
	output.OnAttached()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = output.Close(ctx)
	})
	start, end, confidence := 100*time.Millisecond, 300*time.Millisecond, 0.9
	if err := output.CaptureText(context.Background(), agents.TimedString{Text: "hello", StartTime: &start, EndTime: &end, Confidence: &confidence}); err != nil {
		t.Fatal(err)
	}
	if err := output.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	writers := publisher.snapshot()
	if len(writers) != 2 {
		t.Fatalf("writers = %d", len(writers))
	}
	for index, final := range []string{"false", "true"} {
		writes, closed := writers[index].snapshot()
		if len(writes) != 1 || !closed || !strings.HasSuffix(writes[0], "\n") || !strings.Contains(writes[0], `"start_time":0.1`) {
			t.Fatalf("writer[%d] writes=%q closed=%v", index, writes, closed)
		}
		attributes := writers[index].options.Attributes
		if attributes[AttributeTranscriptionFinal] != final || attributes[AttributeTranscribedParticipant] != "user" || attributes[AttributeTranscriptionTrackID] != "TR_user" {
			t.Fatalf("writer[%d] attributes=%#v", index, attributes)
		}
	}
}

func TestParticipantTranscriptionParticipantSwitchFlushes(t *testing.T) {
	t.Parallel()
	publisher := &fakeTextPublisher{local: "agent"}
	options := DefaultRoomOutputOptions()
	output := newParticipantTranscriptionOutput(context.Background(), publisher, true, "one", options)
	output.OnAttached()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = output.Close(ctx)
	})
	if err := output.CaptureText(context.Background(), agents.TimedString{Text: "old"}); err != nil {
		t.Fatal(err)
	}
	if err := output.SetParticipant(context.Background(), "two"); err != nil {
		t.Fatal(err)
	}
	if err := output.CaptureText(context.Background(), agents.TimedString{Text: "new"}); err != nil {
		t.Fatal(err)
	}
	writers := publisher.snapshot()
	if len(writers) != 2 {
		t.Fatalf("writers = %d", len(writers))
	}
	if _, closed := writers[0].snapshot(); !closed {
		t.Fatal("old participant stream was not closed")
	}
	if writers[1].options.Attributes[AttributeTranscribedParticipant] != "two" {
		t.Fatalf("new attribution = %#v", writers[1].options.Attributes)
	}
}

func TestParticipantTranscriptionAppliesOperationTimeout(t *testing.T) {
	t.Parallel()
	writer := &deadlineTextWriter{started: make(chan struct{})}
	options := DefaultRoomOutputOptions()
	options.TextOperationTimeout = 15 * time.Millisecond
	output := newParticipantTranscriptionOutput(context.Background(), &blockingTextPublisher{writer: writer}, true, "agent", options)
	output.OnAttached()
	startedAt := time.Now()
	err := output.CaptureText(context.Background(), agents.TimedString{Text: "blocked"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("CaptureText error = %v", err)
	}
	if elapsed := time.Since(startedAt); elapsed > 250*time.Millisecond {
		t.Fatalf("operation timeout was not prompt: %v", elapsed)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := output.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestParticipantTranscriptionCloseHonorsCallerDeadlineWithBlockedSDKWriter(t *testing.T) {
	t.Parallel()
	writer := &blockedTextWriter{started: make(chan struct{}), release: make(chan struct{})}
	options := DefaultRoomOutputOptions()
	options.TextOperationTimeout = time.Second
	output := newParticipantTranscriptionOutput(context.Background(), &blockingTextPublisher{writer: writer}, true, "agent", options)
	output.OnAttached()
	captureDone := make(chan error, 1)
	go func() { captureDone <- output.CaptureText(context.Background(), agents.TimedString{Text: "blocked"}) }()
	select {
	case <-writer.started:
	case <-time.After(time.Second):
		t.Fatal("writer did not block")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
	startedAt := time.Now()
	err := output.Close(ctx)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close error = %v", err)
	}
	if elapsed := time.Since(startedAt); elapsed > 250*time.Millisecond {
		t.Fatalf("Close blocked past caller deadline: %v", elapsed)
	}
	close(writer.release)
	select {
	case <-captureDone:
	case <-time.After(time.Second):
		t.Fatal("capture did not unwind after SDK writer release")
	}
	closeCtx, closeCancel := context.WithTimeout(context.Background(), time.Second)
	defer closeCancel()
	if err := output.Close(closeCtx); err != nil {
		t.Fatal(err)
	}
}

func cloneStringMap(source map[string]string) map[string]string {
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}
