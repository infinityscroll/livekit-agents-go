// SPDX-License-Identifier: Apache-2.0

package agents

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/livekit/agents-go/rtcbridge"
	"github.com/livekit/protocol/livekit"
)

func newSessionTestJob(t *testing.T, recording, redaction bool) *JobContext[struct{}] {
	t.Helper()
	job := newJobContext(t.Context(), newJobProcess[struct{}](1), RunningJobInfo{
		Job: &livekit.Job{Id: "job", EnableRecording: recording, EnableRedaction: redaction}, FakeJob: true,
	}, nil)
	t.Cleanup(func() {
		job.Shutdown("test cleanup")
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_ = job.finish(ctx)
		cancel()
	})
	return job
}

func noOpSessionLifecycle() JobSessionLifecycle {
	return JobSessionLifecycleFuncs{}
}

func TestJobSessionPrimaryClaimAndRecordingResolution(t *testing.T) {
	job := newSessionTestJob(t, true, true)
	primary, err := job.RegisterSession(JobSessionRegistrationOptions{Lifecycle: noOpSessionLifecycle()})
	if err != nil {
		t.Fatal(err)
	}
	if !primary.Primary || !primary.RecordingEnabled || !primary.RedactionEnabled {
		t.Fatalf("primary claim = %#v", primary)
	}

	secondary, err := job.RegisterSession(JobSessionRegistrationOptions{Lifecycle: noOpSessionLifecycle()})
	if err != nil {
		t.Fatal(err)
	}
	if secondary.Primary || secondary.RecordingEnabled || !secondary.RedactionEnabled {
		t.Fatalf("inherited secondary claim = %#v", secondary)
	}
	if _, err := job.RegisterSession(JobSessionRegistrationOptions{
		Lifecycle: noOpSessionLifecycle(), Recording: Use(true),
	}); !errors.Is(err, ErrPrimarySessionRecording) {
		t.Fatalf("explicit secondary recording error = %v", err)
	}
	explicitOff, err := job.RegisterSession(JobSessionRegistrationOptions{
		Lifecycle: noOpSessionLifecycle(), Recording: Use(false),
	})
	if err != nil || explicitOff.Primary || explicitOff.RecordingEnabled {
		t.Fatalf("explicit-off secondary claim = %#v, %v", explicitOff, err)
	}

	primary.Release()
	primary.Release()
	replacement, err := job.RegisterSession(JobSessionRegistrationOptions{
		Lifecycle: noOpSessionLifecycle(), Recording: Disable[bool](),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !replacement.Primary || replacement.RecordingEnabled {
		t.Fatalf("replacement primary claim = %#v", replacement)
	}
}

func TestJobSessionClaimIsAtomic(t *testing.T) {
	job := newSessionTestJob(t, false, false)
	const contenders = 32
	var primaryCount atomic.Int32
	var wg sync.WaitGroup
	for range contenders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			registration, err := job.RegisterSession(JobSessionRegistrationOptions{
				Lifecycle: noOpSessionLifecycle(), Recording: Use(false),
			})
			if err != nil {
				t.Errorf("RegisterSession: %v", err)
				return
			}
			if registration.Primary {
				primaryCount.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := primaryCount.Load(); got != 1 {
		t.Fatalf("primary count = %d, want 1", got)
	}
}

func TestJobFinishClosesAndFinalizesPrimaryBeforeCallbacks(t *testing.T) {
	job := newJobContext(context.Background(), newJobProcess[struct{}](1), RunningJobInfo{
		Job: &livekit.Job{Id: "ordered"}, FakeJob: true,
	}, nil)
	var mu sync.Mutex
	order := make([]string, 0, 3)
	appendOrder := func(value string) {
		mu.Lock()
		order = append(order, value)
		mu.Unlock()
	}
	closeErr := errors.New("close failed")
	registration, err := job.RegisterSession(JobSessionRegistrationOptions{Lifecycle: JobSessionLifecycleFuncs{
		Close: func(context.Context) error { appendOrder("close"); return closeErr },
		Finalize: func(context.Context) error {
			appendOrder("finalize")
			return errors.New("upload failed")
		},
	}})
	if err != nil || !registration.Primary {
		t.Fatalf("registration = %#v, %v", registration, err)
	}
	if err := job.AddShutdownCallback(func(context.Context, string) error {
		appendOrder("callback")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	job.Shutdown("done")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	err = job.finish(ctx)
	cancel()
	if !errors.Is(err, closeErr) {
		t.Fatalf("finish error = %v", err)
	}
	mu.Lock()
	got := append([]string(nil), order...)
	mu.Unlock()
	if !slices.Equal(got, []string{"close", "finalize", "callback"}) {
		t.Fatalf("lifecycle order = %v", got)
	}
	if _, err := job.RegisterSession(JobSessionRegistrationOptions{Lifecycle: noOpSessionLifecycle()}); !errors.Is(err, ErrJobSessionFinishing) {
		t.Fatalf("registration after finish error = %v", err)
	}
}

func TestJobSessionDirectoryIsSafeAndOverridable(t *testing.T) {
	job := newSessionTestJob(t, false, false)
	if !strings.HasSuffix(job.SessionDirectory(), filepath.Join("livekit-agents", "job-job")) {
		t.Fatalf("default session directory = %q", job.SessionDirectory())
	}
	unsafe := RunningJobInfo{Job: &livekit.Job{Id: "../../room/\x00long"}}
	resolved := resolveSessionDirectory(unsafe)
	if strings.Contains(filepath.Base(resolved), "/") || strings.Contains(resolved, "..") || !strings.Contains(filepath.Base(resolved), "room") {
		t.Fatalf("unsafe job ID was not sanitized: %q", resolved)
	}
	override := filepath.Join(t.TempDir(), "session")
	if got := resolveSessionDirectory(RunningJobInfo{SessionDirectory: override}); got != filepath.Clean(override) {
		t.Fatalf("override session directory = %q, want %q", got, override)
	}
}

func TestJobContextInstallsConstructionTimeRTCBridge(t *testing.T) {
	job := newSessionTestJob(t, false, false)
	bridge := job.RTCBridge()
	if bridge == nil {
		t.Fatal("JobContext RTCBridge is nil")
	}
	subscription, err := bridge.SubscribeTypes(1, rtcbridge.RTCEventReconnected)
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	bridge.Callback().OnReconnected()
	event, err := subscription.Recv(t.Context())
	if err != nil || event.Type != rtcbridge.RTCEventReconnected {
		t.Fatalf("RTC bridge event = %#v, %v", event, err)
	}
}
