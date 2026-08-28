// SPDX-License-Identifier: Apache-2.0

package agents

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/livekit/protocol/livekit"
	lksdk "github.com/livekit/server-sdk-go/v2"
)

func TestJobRequestAcceptDefaultsAndSingleAnswer(t *testing.T) {
	job := &livekit.Job{Id: "job-123", AgentName: "support", Room: &livekit.Room{Name: "room-a"}}
	var accepted JobAcceptOptions
	request := newJobRequest(job, true, func(_ context.Context, options JobAcceptOptions) error {
		accepted = options
		return nil
	}, func(context.Context, JobRejectOptions) error {
		t.Fatal("reject callback called")
		return nil
	})
	attributes := map[string]string{"tier": "gold"}
	if err := request.Accept(context.Background(), JobAcceptOptions{Attributes: attributes}); err != nil {
		t.Fatal(err)
	}
	attributes["tier"] = "mutated"
	if accepted.Identity != "agent-job-123" {
		t.Fatalf("identity = %q", accepted.Identity)
	}
	if accepted.Attributes["tier"] != "gold" {
		t.Fatalf("attributes were not cloned: %#v", accepted.Attributes)
	}
	if !request.Resuming() || !request.Answered() || request.AgentName() != "support" {
		t.Fatal("request metadata mismatch")
	}
	if err := request.Reject(context.Background(), JobRejectOptions{}); !errors.Is(err, ErrJobRequestAnswered) {
		t.Fatalf("second answer error = %v", err)
	}
	returned := request.Job()
	returned.AgentName = "changed"
	if request.AgentName() != "support" {
		t.Fatal("Job returned mutable request state")
	}
}

func TestJobRequestRejectAndCallbackError(t *testing.T) {
	sentinel := errors.New("send failed")
	request := newJobRequest(&livekit.Job{Id: "job"}, false, nil, func(context.Context, JobRejectOptions) error { return sentinel })
	if err := request.Reject(context.Background(), JobRejectOptions{}); !errors.Is(err, sentinel) {
		t.Fatalf("Reject error = %v", err)
	}
	if !request.Answered() {
		t.Fatal("failed callback must still consume the one-shot answer")
	}
}

func TestFakeJobContextConnectStateAndShutdown(t *testing.T) {
	process := newJobProcess[int](42)
	*process.State() = 7
	var connected atomic.Int32
	job := newJobContext(context.Background(), process, RunningJobInfo{
		Job: &livekit.Job{Id: "job", Room: &livekit.Room{Name: "fake"}}, FakeJob: true,
	}, func() { connected.Add(1) })
	if err := job.Connect(context.Background(), ConnectOptions{AutoSubscribe: SubscribeAudioOnly}); err != nil {
		t.Fatal(err)
	}
	if err := job.Connect(context.Background(), ConnectOptions{}); err != nil {
		t.Fatal(err)
	}
	if connected.Load() != 1 || !job.IsConnected() || job.Agent() != nil || *job.State() != 7 {
		t.Fatalf("fake connection state mismatch: connected=%d state=%d", connected.Load(), *job.State())
	}
	if err := job.DeleteRoom(context.Background(), "fake"); err != nil {
		t.Fatal(err)
	}

	callbackReasons := make(chan string, 2)
	for range 2 {
		if err := job.AddShutdownCallback(func(_ context.Context, reason string) error {
			callbackReasons <- reason
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	job.Shutdown("complete")
	job.Shutdown("ignored")
	finishCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := job.finish(finishCtx); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if reason := <-callbackReasons; reason != "complete" {
			t.Fatalf("shutdown reason = %q", reason)
		}
	}
	if job.ShutdownReason() != "complete" {
		t.Fatalf("ShutdownReason = %q", job.ShutdownReason())
	}
}

func TestJobContextWaitAndContextPropagation(t *testing.T) {
	job := newJobContext(context.Background(), newJobProcess[struct{}](1), RunningJobInfo{
		Job: &livekit.Job{Id: "job"}, FakeJob: true,
	}, nil)
	if _, err := job.WaitForParticipant(context.Background(), ""); !errors.Is(err, ErrRoomNotConnected) {
		t.Fatalf("unconnected WaitForParticipant error = %v", err)
	}
	if err := job.Connect(context.Background(), ConnectOptions{}); err != nil {
		t.Fatal(err)
	}
	waitDone := make(chan error, 1)
	go func() {
		_, err := job.WaitForParticipant(context.Background(), "missing", lksdk.ParticipantStandard)
		waitDone <- err
	}()
	job.Shutdown("test")
	select {
	case err := <-waitDone:
		if err == nil {
			t.Fatal("WaitForParticipant succeeded without a participant")
		}
	case <-time.After(time.Second):
		t.Fatal("WaitForParticipant did not unblock on shutdown")
	}
	ctx := contextWithJob(context.Background(), job)
	got, ok := JobFromContext[struct{}](ctx)
	if !ok || got != job {
		t.Fatal("JobFromContext did not return the propagated job")
	}
}

func TestJobContextRejectsDuplicateParticipantEntrypoint(t *testing.T) {
	job := newJobContext(context.Background(), newJobProcess[struct{}](1), RunningJobInfo{Job: &livekit.Job{Id: "job"}, FakeJob: true}, nil)
	callback := ParticipantEntrypoint[struct{}](func(context.Context, *JobContext[struct{}], *lksdk.RemoteParticipant) error { return nil })
	if err := job.AddParticipantEntrypoint(callback); err != nil {
		t.Fatal(err)
	}
	if err := job.AddParticipantEntrypoint(callback); !errors.Is(err, ErrFunctionExists) {
		t.Fatalf("duplicate callback error = %v", err)
	}
	job.Shutdown("test complete")
}

func TestJobContextConnectValidation(t *testing.T) {
	job := newJobContext(context.Background(), newJobProcess[struct{}](1), RunningJobInfo{Job: &livekit.Job{Id: "job"}, FakeJob: true}, nil)
	if err := job.Connect(context.Background(), ConnectOptions{AutoSubscribe: AutoSubscribe(99)}); err == nil {
		t.Fatal("invalid AutoSubscribe was accepted")
	}
	job.Shutdown("test complete")
}
