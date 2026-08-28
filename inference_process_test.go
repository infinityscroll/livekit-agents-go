// SPDX-License-Identifier: Apache-2.0

package agents

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/livekit/agents-go/ipc"
	"github.com/livekit/protocol/livekit"
)

func registerInferenceProcessTestRunner(t *testing.T, method string, factory InferenceRunnerFactory) {
	t.Helper()
	if err := RegisterInferenceRunner(method, factory); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		inferenceRunnerRegistry.Lock()
		delete(inferenceRunnerRegistry.factories, method)
		inferenceRunnerRegistry.Unlock()
	})
}

func TestInferenceProcessSameBinaryLifecycle(t *testing.T) {
	const method = "test/inference-process-lifecycle"
	registerInferenceProcessTestRunner(t, method, func() (InferenceRunner, error) {
		return InferenceRunnerFuncs{RunFunc: func(ctx context.Context, input any) (any, error) {
			object, _ := input.(map[string]any)
			action, _ := object["action"].(string)
			switch action {
			case "echo":
				return map[string]any{"pid": os.Getpid(), "value": object["value"]}, nil
			case "wait":
				<-ctx.Done()
				return nil, context.Cause(ctx)
			case "deadline":
				deadline, ok := ctx.Deadline()
				return map[string]any{"set": ok, "unix_nano": deadline.UnixNano()}, nil
			case "panic":
				panic("native runner panic")
			case "error":
				return nil, errors.New("native runner failure")
			case "crash":
				os.Exit(23)
			}
			return nil, fmt.Errorf("unknown action %q", action)
		}}, nil
	})
	if isInferenceChildProcess() {
		if err := runInferenceChildProcess(context.Background()); err != nil {
			t.Fatal(err)
		}
		return
	}
	t.Setenv(inferenceChildTestRunEnv, "^TestInferenceProcessSameBinaryLifecycle$")

	initializeCtx, cancelInitialize := context.WithTimeout(context.Background(), 15*time.Second)
	executor, err := newInferenceProcessExecutor(initializeCtx, []string{method}, 2*time.Second)
	cancelInitialize()
	if err != nil {
		t.Fatal(err)
	}
	if executor.PID() == 0 || executor.PID() == os.Getpid() {
		t.Fatalf("inference PID = %d, parent PID = %d", executor.PID(), os.Getpid())
	}
	if err := executor.Healthy(); err != nil {
		t.Fatalf("Healthy = %v", err)
	}

	result, err := executor.DoInference(context.Background(), method, map[string]any{"action": "echo", "value": "ready"})
	if err != nil {
		t.Fatal(err)
	}
	object, ok := result.(map[string]any)
	if !ok || object["value"] != "ready" || int(object["pid"].(float64)) != executor.PID() {
		t.Fatalf("result = %#v, child PID = %d", result, executor.PID())
	}
	deadlineCtx, cancelDeadline := context.WithTimeout(context.Background(), 2*time.Second)
	deadlineResult, err := executor.DoInference(deadlineCtx, method, map[string]any{"action": "deadline"})
	cancelDeadline()
	if err != nil {
		t.Fatal(err)
	}
	deadlineObject, ok := deadlineResult.(map[string]any)
	if !ok || deadlineObject["set"] != true || deadlineObject["unix_nano"].(float64) == 0 {
		t.Fatalf("child deadline result = %#v", deadlineResult)
	}

	_, err = executor.DoInference(context.Background(), "test/missing-inference-process-method", nil)
	var unknown *ipc.UnknownMethodError
	if !errors.As(err, &unknown) || unknown.Method != "test/missing-inference-process-method" {
		t.Fatalf("unknown method error = %#v", err)
	}

	cancelCtx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	_, err = executor.DoInference(cancelCtx, method, map[string]any{"action": "wait"})
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("canceled inference error = %v", err)
	}

	_, err = executor.DoInference(context.Background(), method, map[string]any{"action": "panic"})
	var processErr *InferenceProcessError
	if !errors.As(err, &processErr) || processErr.Code != inferenceErrorRunnerPanic || !strings.Contains(err.Error(), "native runner panic") {
		t.Fatalf("panic error = %#v", err)
	}
	_, err = executor.DoInference(context.Background(), method, map[string]any{"action": "error"})
	if !errors.As(err, &processErr) || processErr.Code != inferenceErrorInternal || !strings.Contains(err.Error(), "native runner failure") {
		t.Fatalf("runner error = %#v", err)
	}

	_, err = executor.DoInference(context.Background(), method, map[string]any{"action": "crash"})
	if !errors.Is(err, ErrInferenceProcessCrashed) {
		t.Fatalf("crash request error = %v", err)
	}
	select {
	case <-executor.exitDone:
	case <-time.After(5 * time.Second):
		t.Fatal("crashed inference child was not reaped")
	}
	if err := executor.Healthy(); !errors.Is(err, ErrInferenceProcessCrashed) {
		t.Fatalf("Healthy after crash = %v", err)
	}
	closeCtx, cancelClose := context.WithTimeout(context.Background(), 5*time.Second)
	closeErr := executor.Close(closeCtx)
	cancelClose()
	if !errors.Is(closeErr, ErrInferenceProcessCrashed) {
		t.Fatalf("Close after crash = %v", closeErr)
	}
}

func TestInferenceProcessRegistrationMismatchRollsBack(t *testing.T) {
	const method = "test/inference-process-parent-only-registration"
	if isInferenceChildProcess() {
		err := runInferenceChildProcess(context.Background())
		if !errors.Is(err, ErrInferenceRegistrationMismatch) {
			t.Fatalf("child mismatch error = %v", err)
		}
		return
	}
	registerInferenceProcessTestRunner(t, method, func() (InferenceRunner, error) {
		return InferenceRunnerFuncs{RunFunc: func(context.Context, any) (any, error) { return nil, nil }}, nil
	})
	t.Setenv(inferenceChildTestRunEnv, "^TestInferenceProcessRegistrationMismatchRollsBack$")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	_, err := newInferenceProcessExecutor(ctx, []string{method}, time.Second)
	cancel()
	if !errors.Is(err, ErrInferenceRegistrationMismatch) {
		t.Fatalf("constructor error = %v", err)
	}
}

func TestInferenceProcessConcurrentCloseIsIdempotent(t *testing.T) {
	const method = "test/inference-process-concurrent-close"
	registerInferenceProcessTestRunner(t, method, func() (InferenceRunner, error) {
		return InferenceRunnerFuncs{RunFunc: func(context.Context, any) (any, error) { return "ok", nil }}, nil
	})
	if isInferenceChildProcess() {
		if err := runInferenceChildProcess(context.Background()); err != nil {
			t.Fatal(err)
		}
		return
	}
	t.Setenv(inferenceChildTestRunEnv, "^TestInferenceProcessConcurrentCloseIsIdempotent$")
	initializeCtx, cancelInitialize := context.WithTimeout(context.Background(), 10*time.Second)
	executor, err := newInferenceProcessExecutor(initializeCtx, []string{method}, 2*time.Second)
	cancelInitialize()
	if err != nil {
		t.Fatal(err)
	}
	const callers = 16
	errorsOut := make(chan error, callers)
	var wait sync.WaitGroup
	for range callers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			errorsOut <- executor.Close(closeCtx)
		}()
	}
	wait.Wait()
	close(errorsOut)
	for closeErr := range errorsOut {
		if closeErr != nil {
			t.Fatalf("Close = %v", closeErr)
		}
	}
	select {
	case <-executor.exitDone:
	case <-time.After(3 * time.Second):
		t.Fatal("gracefully closed inference child was not reaped")
	}
}

func TestInferenceProcessInitializationFailureRollsBack(t *testing.T) {
	const method = "test/inference-process-initialize-failure"
	registerInferenceProcessTestRunner(t, method, func() (InferenceRunner, error) {
		return InferenceRunnerFuncs{
			InitializeFunc: func(context.Context) error { return errors.New("test model initialize failed") },
			RunFunc:        func(context.Context, any) (any, error) { return nil, nil },
		}, nil
	})
	if isInferenceChildProcess() {
		err := runInferenceChildProcess(context.Background())
		if err == nil || !strings.Contains(err.Error(), "test model initialize failed") {
			t.Fatalf("child initialization error = %v", err)
		}
		return
	}
	t.Setenv(inferenceChildTestRunEnv, "^TestInferenceProcessInitializationFailureRollsBack$")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	_, err := newInferenceProcessExecutor(ctx, []string{method}, time.Second)
	cancel()
	if err == nil || !strings.Contains(err.Error(), "test model initialize failed") {
		t.Fatalf("constructor error = %v", err)
	}
}

func TestInferenceProcessCloseDeadlineForcesTermination(t *testing.T) {
	const method = "test/inference-process-blocking-close"
	registerInferenceProcessTestRunner(t, method, func() (InferenceRunner, error) {
		return InferenceRunnerFuncs{RunFunc: func(_ context.Context, input any) (any, error) {
			object, _ := input.(map[string]any)
			readyPath, _ := object["ready_path"].(string)
			if err := os.WriteFile(readyPath, []byte("ready"), 0o600); err != nil {
				return nil, err
			}
			select {}
		}}, nil
	})
	if isInferenceChildProcess() {
		if err := runInferenceChildProcess(context.Background()); err != nil {
			t.Fatal(err)
		}
		return
	}
	t.Setenv(inferenceChildTestRunEnv, "^TestInferenceProcessCloseDeadlineForcesTermination$")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	executor, err := newInferenceProcessExecutor(ctx, []string{method}, 75*time.Millisecond)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	readyPath := filepath.Join(t.TempDir(), "runner-ready")
	requestDone := make(chan error, 1)
	go func() {
		_, requestErr := executor.DoInference(context.Background(), method, map[string]any{"ready_path": readyPath})
		requestDone <- requestErr
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, statErr := os.Stat(readyPath); statErr == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("blocking runner did not start")
		}
		time.Sleep(5 * time.Millisecond)
	}
	closeCtx, cancelClose := context.WithTimeout(context.Background(), 3*time.Second)
	closeErr := executor.Close(closeCtx)
	cancelClose()
	if !errors.Is(closeErr, context.DeadlineExceeded) {
		t.Fatalf("Close error = %v", closeErr)
	}
	select {
	case requestErr := <-requestDone:
		if requestErr == nil {
			t.Fatal("blocking request unexpectedly succeeded")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("blocking request was not released by forced termination")
	}
	select {
	case <-executor.exitDone:
	case <-time.After(3 * time.Second):
		t.Fatal("forced inference child was not reaped")
	}
}

func TestProcessPoolEmptyRegistryHasNoInferenceProcess(t *testing.T) {
	inferenceRunnerRegistry.Lock()
	saved := inferenceRunnerRegistry.factories
	inferenceRunnerRegistry.factories = nil
	inferenceRunnerRegistry.Unlock()
	defer func() {
		inferenceRunnerRegistry.Lock()
		inferenceRunnerRegistry.factories = saved
		inferenceRunnerRegistry.Unlock()
	}()
	opts := processTestOptions(t, ExecutorModeProcess)
	opts.NumIdleProcesses = 0
	pool := newExecutorPool(opts, nil).(*processPool[processTestState])
	if err := pool.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	pool.mu.Lock()
	inference := pool.inference
	pool.mu.Unlock()
	if inference != nil {
		t.Fatalf("empty registry created inference executor %T", inference)
	}
	if err := pool.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestProcessPoolSharesOneInferenceChild(t *testing.T) {
	const method = "test/process-pool-shared-inference-child"
	var factoryCalls atomic.Int32
	registerInferenceProcessTestRunner(t, method, func() (InferenceRunner, error) {
		factoryCalls.Add(1)
		return InferenceRunnerFuncs{RunFunc: func(context.Context, any) (any, error) { return os.Getpid(), nil }}, nil
	})
	opts := processTestOptions(t, ExecutorModeProcess)
	opts.MaxConcurrentJobs = 2
	opts.NumIdleProcesses = 0
	opts.JobEntrypoint = func(ctx context.Context, job *JobContext[processTestState]) error {
		result, err := job.InferenceExecutor().DoInference(ctx, method, nil)
		if err != nil {
			return err
		}
		pid, ok := result.(float64)
		if !ok || int(pid) == os.Getpid() {
			return fmt.Errorf("inference result PID = %#v, job PID = %d", result, os.Getpid())
		}
		return job.Connect(ctx, ConnectOptions{})
	}
	if isInferenceChildProcess() {
		if err := runInferenceChildProcess(context.Background()); err != nil {
			t.Fatal(err)
		}
		return
	}
	if isJobChildProcess() {
		if err := runJobChildProcess(context.Background(), opts); err != nil {
			t.Fatal(err)
		}
		return
	}
	t.Setenv(inferenceChildTestRunEnv, "^TestProcessPoolSharesOneInferenceChild$")
	t.Setenv(jobChildTestRunEnv, "^TestProcessPoolSharesOneInferenceChild$")
	running := make(chan string, 2)
	pool := newExecutorPool(opts, func(jobID string, status livekit.JobStatus, _ error) {
		if status == livekit.JobStatus_JS_RUNNING {
			running <- jobID
		}
	}).(*processPool[processTestState])
	startCtx, cancelStart := context.WithTimeout(context.Background(), 15*time.Second)
	if err := pool.Start(startCtx); err != nil {
		cancelStart()
		t.Fatal(err)
	}
	cancelStart()
	child, ok := pool.inference.(*inferenceProcessExecutor)
	if !ok || child.PID() == 0 {
		t.Fatalf("pool inference executor = %T", pool.inference)
	}
	if calls := factoryCalls.Load(); calls != 0 {
		t.Fatalf("native runner factory executed %d times in worker parent", calls)
	}

	createCtx, cancelCreate := context.WithTimeout(context.Background(), 15*time.Second)
	first, err := pool.createExecutor(createCtx)
	if err != nil {
		cancelCreate()
		t.Fatal(err)
	}
	second, err := pool.createExecutor(createCtx)
	cancelCreate()
	if err != nil {
		_ = first.Stop(context.Background(), "test setup failed")
		t.Fatal(err)
	}
	firstProcess := first.(*processExecutor[processTestState])
	secondProcess := second.(*processExecutor[processTestState])
	if firstProcess.inference != pool.inference || secondProcess.inference != pool.inference {
		t.Fatal("job executors did not share the worker inference child proxy")
	}
	if err := first.Launch(context.Background(), RunningJobInfo{
		Job: &livekit.Job{Id: "shared-inference-job-1", Room: &livekit.Room{Name: "fake"}}, FakeJob: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := second.Launch(context.Background(), RunningJobInfo{
		Job: &livekit.Job{Id: "shared-inference-job-2", Room: &livekit.Room{Name: "fake"}}, FakeJob: true,
	}); err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]bool, 2)
	for len(seen) != 2 {
		select {
		case jobID := <-running:
			seen[jobID] = true
		case result := <-first.Done():
			t.Fatalf("first job failed before inference completed: %#v", result)
		case result := <-second.Done():
			t.Fatalf("second job failed before inference completed: %#v", result)
		case <-time.After(5 * time.Second):
			t.Fatal("jobs did not complete shared inference requests")
		}
	}
	stopCtx, cancelStop := context.WithTimeout(context.Background(), 5*time.Second)
	if err := errors.Join(first.Stop(stopCtx, "test complete"), second.Stop(stopCtx, "test complete")); err != nil {
		cancelStop()
		t.Fatal(err)
	}
	cancelStop()
	closeCtx, cancelClose := context.WithTimeout(context.Background(), 5*time.Second)
	if err := pool.Close(closeCtx); err != nil {
		cancelClose()
		t.Fatal(err)
	}
	cancelClose()
}
