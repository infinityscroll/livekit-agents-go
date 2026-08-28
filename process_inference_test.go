// SPDX-License-Identifier: Apache-2.0

package agents

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/livekit/agents-go/internal/workerprotocol"
	"github.com/livekit/agents-go/ipc"
	"github.com/livekit/protocol/livekit"
)

func TestProcessInferenceIntegration(t *testing.T) {
	const method = "test/process-inference-integration"
	if err := RegisterInferenceRunner(method, func() (InferenceRunner, error) {
		return InferenceRunnerFuncs{RunFunc: func(_ context.Context, input any) (any, error) {
			object, ok := input.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("input type = %T", input)
			}
			value, _ := object["value"].(string)
			return map[string]any{"value": value + "!"}, nil
		}}, nil
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		inferenceRunnerRegistry.Lock()
		delete(inferenceRunnerRegistry.factories, method)
		inferenceRunnerRegistry.Unlock()
	})

	opts := processTestOptions(t, ExecutorModeProcess)
	opts.JobEntrypoint = func(ctx context.Context, job *JobContext[processTestState]) error {
		_, unknownErr := job.InferenceExecutor().DoInference(ctx, "test/process-inference-missing", nil)
		var unknown *ipc.UnknownMethodError
		if !errors.As(unknownErr, &unknown) || unknown.Method != "test/process-inference-missing" {
			return fmt.Errorf("unknown inference error = %v", unknownErr)
		}
		result, err := job.InferenceExecutor().DoInference(ctx, method, map[string]any{"value": "ready"})
		if err != nil {
			return err
		}
		object, ok := result.(map[string]any)
		if !ok || object["value"] != "ready!" {
			return fmt.Errorf("inference result = %#v", result)
		}
		return job.Connect(ctx, ConnectOptions{})
	}
	if isJobChildProcess() {
		if err := runJobChildProcess(context.Background(), opts); err != nil {
			t.Fatal(err)
		}
		return
	}
	t.Setenv(jobChildTestRunEnv, "^TestProcessInferenceIntegration$")

	shared, err := NewLocalInferenceExecutor(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := shared.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = shared.Close(context.Background()) })
	status := make(chan livekit.JobStatus, 2)
	initCtx, cancelInit := context.WithTimeout(context.Background(), 10*time.Second)
	executor, err := newProcessExecutor(initCtx, opts, func(_ string, value livekit.JobStatus, _ error) { status <- value }, shared)
	cancelInit()
	if err != nil {
		t.Fatal(err)
	}
	info := RunningJobInfo{Job: &livekit.Job{Id: "inference-process-job", Room: &livekit.Room{Name: "fake"}}, FakeJob: true}
	if err := executor.Launch(context.Background(), info); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-status:
		if got != livekit.JobStatus_JS_RUNNING {
			t.Fatalf("status = %s", got)
		}
	case result := <-executor.Done():
		t.Fatalf("job failed before reporting running: %#v", result)
	case <-time.After(5 * time.Second):
		t.Fatal("process inference job did not report running")
	}
	stopCtx, cancelStop := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelStop()
	if err := executor.Stop(stopCtx, "test complete"); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-executor.Done():
		if !result.success || result.err != nil {
			t.Fatalf("process result = %#v", result)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("process inference job did not finish")
	}
}

func TestJobIPCInferenceClientCancellation(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	client := newJobIPCInferenceClient(workerprotocol.NewFramedConn(left))
	server := workerprotocol.NewFramedConn(right)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := client.DoInference(ctx, "slow", map[string]any{"value": true})
		done <- err
	}()
	request, err := server.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if request.Type != workerprotocol.IPCTypeInferenceRequest || request.RequestID == "" {
		t.Fatalf("request = %#v", request)
	}
	if request.DeadlineUnixNano == 0 {
		t.Fatal("request context deadline was not forwarded")
	}
	cancelMessage, err := server.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if cancelMessage.Type != workerprotocol.IPCTypeInferenceCancel || cancelMessage.RequestID != request.RequestID {
		t.Fatalf("cancel = %#v", cancelMessage)
	}
	if err := <-done; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("DoInference error = %v", err)
	}
	client.close(nil)
}

func TestJobContextUnknownInferenceMethod(t *testing.T) {
	job := newJobContext(context.Background(), newJobProcess[struct{}](1), RunningJobInfo{
		Job: &livekit.Job{Id: "job"}, FakeJob: true,
	}, nil)
	t.Cleanup(func() {
		job.Shutdown("test complete")
		_ = job.finish(context.Background())
	})
	_, err := job.InferenceExecutor().DoInference(context.Background(), "missing", nil)
	var unknown *ipc.UnknownMethodError
	if !errors.As(err, &unknown) || unknown.Method != "missing" {
		t.Fatalf("unknown inference error = %v", err)
	}
}
