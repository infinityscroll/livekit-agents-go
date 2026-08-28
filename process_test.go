// SPDX-License-Identifier: Apache-2.0

package agents

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/livekit/protocol/livekit"
)

type processTestState struct{ Value string }

func processTestOptions(t *testing.T, mode ExecutorMode) ServerOptions[processTestState] {
	t.Helper()
	t.Setenv("LIVEKIT_API_KEY", "test-key")
	t.Setenv("LIVEKIT_API_SECRET", "test-secret")
	t.Setenv("LIVEKIT_URL", "ws://localhost:7880")
	opts, err := normalizeServerOptions(ServerOptions[processTestState]{
		ExecutorMode: mode, MaxConcurrentJobs: 1, NumIdleProcesses: 1,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Prewarm: func(_ context.Context, process *JobProcess[processTestState]) error {
			process.State().Value = "prewarmed"
			return nil
		},
		JobEntrypoint: func(ctx context.Context, job *JobContext[processTestState]) error {
			if job.State().Value != "prewarmed" {
				return errors.New("prewarm state was not preserved")
			}
			if err := job.Connect(ctx, ConnectOptions{}); err != nil {
				return err
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return opts
}

func TestInProcessPoolReservationLifecycle(t *testing.T) {
	opts := processTestOptions(t, ExecutorModeInProcess)
	status := make(chan livekit.JobStatus, 4)
	pool := newExecutorPool(opts, func(_ string, value livekit.JobStatus, _ error) { status <- value })
	if err := pool.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	reservation, err := pool.Reserve(context.Background(), "job-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Reserve(context.Background(), "job-2"); !errors.Is(err, ErrNoJobCapacity) {
		t.Fatalf("second reservation error = %v", err)
	}
	info := RunningJobInfo{Job: &livekit.Job{Id: "job-1", Room: &livekit.Room{Name: "fake"}}, FakeJob: true}
	if err := reservation.Launch(context.Background(), info); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-status:
		if got != livekit.JobStatus_JS_RUNNING {
			t.Fatalf("first status = %s", got)
		}
	case <-time.After(time.Second):
		t.Fatal("job did not report running")
	}
	if jobs := pool.ActiveJobs(); len(jobs) != 1 || jobs[0].Job.Id != "job-1" {
		t.Fatalf("ActiveJobs = %#v", jobs)
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := pool.Terminate(stopCtx, "job-1", "test complete"); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-status:
		if got != livekit.JobStatus_JS_SUCCESS {
			t.Fatalf("terminal status = %s", got)
		}
	case <-time.After(time.Second):
		t.Fatal("job did not report terminal status")
	}
	if err := pool.Drain(stopCtx); err != nil {
		t.Fatal(err)
	}
	if err := pool.Close(stopCtx); err != nil {
		t.Fatal(err)
	}
}

func TestPoolReservationReleaseRestoresCapacity(t *testing.T) {
	opts := processTestOptions(t, ExecutorModeInProcess)
	opts.NumIdleProcesses = 0
	pool := newExecutorPool(opts, nil)
	if err := pool.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	first, err := pool.Reserve(context.Background(), "first")
	if err != nil {
		t.Fatal(err)
	}
	first.Release()
	second, err := pool.Reserve(context.Background(), "second")
	if err != nil {
		t.Fatalf("capacity was not restored: %v", err)
	}
	second.Release()
	if load := pool.Load(); load != 0 {
		t.Fatalf("Load = %f, want 0", load)
	}
	_ = pool.Close(context.Background())
}

func TestProcessPoolFailedStartRollsBack(t *testing.T) {
	opts := processTestOptions(t, ExecutorModeInProcess)
	want := errors.New("prewarm failed")
	opts.Prewarm = func(context.Context, *JobProcess[processTestState]) error { return want }
	pool := newExecutorPool(opts, nil)
	if err := pool.Start(context.Background()); !errors.Is(err, want) {
		t.Fatalf("Start error = %v, want %v", err, want)
	}
	if _, err := pool.Reserve(context.Background(), "after-failure"); !errors.Is(err, ErrExecutorClosed) {
		t.Fatalf("Reserve after failed Start = %v, want %v", err, ErrExecutorClosed)
	}
	if err := pool.Healthy(); !errors.Is(err, ErrExecutorClosed) {
		t.Fatalf("Healthy after failed Start = %v, want %v", err, ErrExecutorClosed)
	}
	if err := pool.Close(context.Background()); err != nil {
		t.Fatalf("second Close = %v", err)
	}
}

func TestProcessExecutorIntegration(t *testing.T) {
	opts := processTestOptions(t, ExecutorModeProcess)
	if isJobChildProcess() {
		if err := runJobChildProcess(context.Background(), opts); err != nil {
			t.Fatal(err)
		}
		return
	}
	t.Setenv(jobChildTestRunEnv, "^TestProcessExecutorIntegration$")
	status := make(chan livekit.JobStatus, 2)
	initCtx, cancelInit := context.WithTimeout(context.Background(), 10*time.Second)
	executor, err := newProcessExecutor(initCtx, opts, func(_ string, value livekit.JobStatus, _ error) { status <- value })
	cancelInit()
	if err != nil {
		t.Fatal(err)
	}
	info := RunningJobInfo{Job: &livekit.Job{Id: "process-job", Room: &livekit.Room{Name: "fake"}}, FakeJob: true}
	if err := executor.Launch(context.Background(), info); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-status:
		if got != livekit.JobStatus_JS_RUNNING {
			t.Fatalf("status = %s", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("process job did not report running")
	}
	// A nil entrypoint result intentionally keeps the job alive until shutdown,
	// matching AgentSession.Start behavior in Python and TypeScript.
	stopCtx, cancelStop := context.WithTimeout(context.Background(), 5*time.Second)
	if err := executor.Stop(stopCtx, "test complete"); err != nil {
		cancelStop()
		t.Fatal(err)
	}
	cancelStop()
	select {
	case result := <-executor.Done():
		if !result.success || result.err != nil {
			t.Fatalf("process result = %#v", result)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("process job did not finish")
	}
	select {
	case <-executor.Exited():
	case <-time.After(5 * time.Second):
		t.Fatal("job child did not exit")
	}
}
