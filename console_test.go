// SPDX-License-Identifier: Apache-2.0

package agents

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunConsoleJobUsesNormalPrewarmEntrypointAndShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered := make(chan struct{})
	shutdown := make(chan string, 1)
	var setupCalled atomic.Bool
	options := ConsoleJobOptions[int]{Server: ServerOptions[int]{
		Prewarm: func(_ context.Context, process *JobProcess[int]) error {
			*process.State() = 42
			return nil
		},
		JobEntrypoint: func(_ context.Context, job *JobContext[int]) error {
			if !job.IsFakeJob() || job.Job().GetRoom().GetName() != "console-room" || *job.State() != 42 {
				t.Errorf("console job mismatch: %#v state=%d", job.Info(), *job.State())
			}
			if err := job.AddShutdownCallback(func(_ context.Context, reason string) error {
				shutdown <- reason
				return nil
			}); err != nil {
				return err
			}
			close(entered)
			return nil
		},
	}, Setup: func(_ context.Context, job *JobContext[int], _ SimulationEndFunc[int]) error {
		setupCalled.Store(job != nil)
		return nil
	}, ShutdownTimeout: time.Second}
	done := make(chan error, 1)
	go func() { done <- RunConsoleJob(ctx, options) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("console entrypoint was not called")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("RunConsoleJob did not stop")
	}
	if !setupCalled.Load() {
		t.Fatal("console setup was not called")
	}
	select {
	case reason := <-shutdown:
		if reason != "console stopped" {
			t.Fatalf("shutdown reason=%q", reason)
		}
	default:
		t.Fatal("shutdown callback was not called")
	}
}

func TestRunConsoleJobRejectsConflictingEntrypoints(t *testing.T) {
	first := JobEntrypoint[struct{}](func(context.Context, *JobContext[struct{}]) error { return nil })
	second := JobEntrypoint[struct{}](func(context.Context, *JobContext[struct{}]) error { return nil })
	err := RunConsoleJob(t.Context(), ConsoleJobOptions[struct{}]{Server: ServerOptions[struct{}]{JobEntrypoint: first, Entrypoint: second}})
	if err == nil {
		t.Fatal("conflicting entrypoints were accepted")
	}
}
