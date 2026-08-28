// SPDX-License-Identifier: Apache-2.0

package agents

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/livekit/agents-go/internal/workerprotocol"
	"github.com/livekit/protocol/livekit"
	"google.golang.org/protobuf/proto"
)

type fakeWorkerTransport struct {
	reads  chan *livekit.ServerMessage
	writes chan *livekit.WorkerMessage
	closed chan struct{}
	once   sync.Once
}

func newFakeWorkerTransport() *fakeWorkerTransport {
	return &fakeWorkerTransport{
		reads: make(chan *livekit.ServerMessage, 16), writes: make(chan *livekit.WorkerMessage, 32), closed: make(chan struct{}),
	}
}

func (t *fakeWorkerTransport) Read(ctx context.Context) (*livekit.ServerMessage, error) {
	select {
	case message := <-t.reads:
		return proto.Clone(message).(*livekit.ServerMessage), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-t.closed:
		return nil, workerprotocol.ErrClosed
	}
}

func (t *fakeWorkerTransport) Write(ctx context.Context, message *livekit.WorkerMessage) error {
	select {
	case t.writes <- proto.Clone(message).(*livekit.WorkerMessage):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-t.closed:
		return workerprotocol.ErrClosed
	}
}

func (t *fakeWorkerTransport) Close() error {
	t.once.Do(func() { close(t.closed) })
	return nil
}

type fakeWorkerDialer struct {
	transport workerprotocol.Transport
	endpoint  *url.URL
	header    http.Header
}

func (d *fakeWorkerDialer) Dial(_ context.Context, endpoint *url.URL, header http.Header) (workerprotocol.Transport, error) {
	d.endpoint = endpoint
	d.header = header.Clone()
	return d.transport, nil
}

type fakeReservation[T any] struct {
	mu        sync.Mutex
	launched  bool
	released  bool
	launches  chan RunningJobInfo
	launchErr error
}

func (r *fakeReservation[T]) Launch(_ context.Context, info RunningJobInfo) error {
	r.mu.Lock()
	if r.released {
		r.mu.Unlock()
		return ErrReservationReleased
	}
	r.launched = true
	err := r.launchErr
	r.mu.Unlock()
	if err == nil {
		r.launches <- info.Clone()
	}
	return err
}

func (r *fakeReservation[T]) Release() {
	r.mu.Lock()
	if !r.launched {
		r.released = true
	}
	r.mu.Unlock()
}

func (r *fakeReservation[T]) wasReleased() bool {
	r.mu.Lock()
	released := r.released
	r.mu.Unlock()
	return released
}

type fakeExecutorPool[T any] struct {
	mu          sync.Mutex
	reservation *fakeReservation[T]
	reserveErr  error
	active      []RunningJobInfo
	started     bool
	drained     bool
	closed      bool
	load        float64
}

func (p *fakeExecutorPool[T]) Start(context.Context) error {
	p.mu.Lock()
	p.started = true
	p.mu.Unlock()
	return nil
}
func (p *fakeExecutorPool[T]) Reserve(context.Context, string) (jobReservation[T], error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.reserveErr != nil {
		return nil, p.reserveErr
	}
	p.reservation = &fakeReservation[T]{launches: make(chan RunningJobInfo, 1)}
	return p.reservation, nil
}
func (p *fakeExecutorPool[T]) Terminate(context.Context, string, string) error { return ErrJobNotFound }
func (p *fakeExecutorPool[T]) ActiveJobs() []RunningJobInfo {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]RunningJobInfo, len(p.active))
	for index := range p.active {
		out[index] = p.active[index].Clone()
	}
	return out
}
func (p *fakeExecutorPool[T]) Load() float64  { p.mu.Lock(); defer p.mu.Unlock(); return p.load }
func (p *fakeExecutorPool[T]) Healthy() error { return nil }
func (p *fakeExecutorPool[T]) Drain(ctx context.Context) error {
	p.mu.Lock()
	p.drained = true
	p.mu.Unlock()
	return ctx.Err()
}
func (p *fakeExecutorPool[T]) Close(context.Context) error {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	return nil
}

func workerTestOptions[T any](entrypoint JobEntrypoint[T]) ServerOptions[T] {
	return ServerOptions[T]{
		JobEntrypoint: entrypoint, URL: "ws://livekit.test", APIKey: NewSecretString("key"), APISecret: NewSecretString("secret"),
		AgentName: "support", Simulation: true, ExecutorMode: ExecutorModeInProcess,
		StatusUpdateInterval: time.Hour, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func TestWorkerRegistrationAvailabilityAssignmentAndDrain(t *testing.T) {
	server, err := NewAgentServer(workerTestOptions(func(context.Context, *JobContext[struct{}]) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	transport := newFakeWorkerTransport()
	dialer := &fakeWorkerDialer{transport: transport}
	pool := &fakeExecutorPool[struct{}]{}
	server.dialer = dialer
	server.pool = pool
	registered := make(chan WorkerEvent, 1)
	server.Events().Subscribe(func(event WorkerEvent) {
		if event.Type == WorkerEventRegistered {
			registered <- event
		}
	})
	transport.reads <- &livekit.ServerMessage{Message: &livekit.ServerMessage_Register{Register: &livekit.RegisterWorkerResponse{WorkerId: "worker-1"}}}
	runCtx, cancelRun := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- server.Run(runCtx) }()

	register := nextWorkerWrite(t, transport)
	if register.GetRegister().GetAgentName() != "support" || register.GetRegister().GetVersion() != Version {
		t.Fatalf("registration = %#v", register.GetRegister())
	}
	permissions := register.GetRegister().GetAllowedPermissions()
	if permissions == nil {
		t.Fatal("default permissions were not registered")
	}
	agentField := permissions.ProtoReflect().Descriptor().Fields().ByName("agent")
	if !permissions.GetCanPublish() || agentField == nil || !permissions.ProtoReflect().Get(agentField).Bool() {
		t.Fatal("default permissions were not registered")
	}
	select {
	case event := <-registered:
		if event.WorkerID != "worker-1" {
			t.Fatalf("worker ID = %q", event.WorkerID)
		}
	case <-time.After(time.Second):
		t.Fatal("worker_registered event not emitted")
	}
	if dialer.endpoint.String() != "ws://livekit.test/agent" || dialer.header.Get("Authorization") == "" {
		t.Fatalf("dial endpoint/header = %v %#v", dialer.endpoint, dialer.header)
	}

	job := &livekit.Job{Id: "job-1", AgentName: "support", Room: &livekit.Room{Name: "room"}}
	transport.reads <- &livekit.ServerMessage{Message: &livekit.ServerMessage_Availability{Availability: &livekit.AvailabilityRequest{Job: job}}}
	availability := nextWorkerWrite(t, transport).GetAvailability()
	if availability == nil || !availability.Available || availability.ParticipantIdentity != "agent-job-1" {
		t.Fatalf("availability = %#v", availability)
	}
	if availability.ParticipantAttributes[AttributeAgentName] != "support" {
		t.Fatalf("participant attributes = %#v", availability.ParticipantAttributes)
	}
	transport.reads <- &livekit.ServerMessage{Message: &livekit.ServerMessage_Assignment{Assignment: &livekit.JobAssignment{Job: job, Token: "room-token"}}}
	select {
	case info := <-pool.reservation.launches:
		if info.URL != "ws://livekit.test" || info.Token.Reveal() != "room-token" || info.WorkerID != "worker-1" {
			t.Fatalf("running job info = %#v", info)
		}
	case <-time.After(time.Second):
		t.Fatal("assignment was not launched")
	}

	cancelRun()
	select {
	case err := <-runDone:
		if err != nil {
			t.Fatalf("Run = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not drain and close")
	}
	pool.mu.Lock()
	drained, closed := pool.drained, pool.closed
	pool.mu.Unlock()
	if !drained || !closed {
		t.Fatalf("pool lifecycle drained=%t closed=%t", drained, closed)
	}
}

func TestWorkerAutomaticallyRejectsUnansweredRequest(t *testing.T) {
	opts := workerTestOptions(func(context.Context, *JobContext[struct{}]) error { return nil })
	opts.RequestHandler = func(context.Context, *JobRequest) error { return nil }
	server, err := NewAgentServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	pool := &fakeExecutorPool[struct{}]{}
	server.pool = pool
	server.stateMu.Lock()
	server.registered = true
	server.generation = 1
	server.stateMu.Unlock()
	server.handleAvailability(context.Background(), 1, &livekit.AvailabilityRequest{Job: &livekit.Job{Id: "job"}})
	select {
	case message := <-server.outgoing:
		if response := message.message.GetAvailability(); response == nil || response.Available {
			t.Fatalf("response = %#v", response)
		}
	case <-time.After(time.Second):
		t.Fatal("automatic rejection was not queued")
	}
	if !pool.reservation.wasReleased() {
		t.Fatal("reservation was not released")
	}
	_ = server.Close()
}

func TestWorkerAssignmentTimeoutReleasesReservation(t *testing.T) {
	opts := workerTestOptions(func(context.Context, *JobContext[struct{}]) error { return nil })
	opts.AssignmentTimeout = 20 * time.Millisecond
	server, err := NewAgentServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	pool := &fakeExecutorPool[struct{}]{}
	server.pool = pool
	server.stateMu.Lock()
	server.registered = true
	server.generation = 1
	server.stateMu.Unlock()
	started := time.Now()
	server.handleAvailability(context.Background(), 1, &livekit.AvailabilityRequest{Job: &livekit.Job{Id: "job"}})
	if elapsed := time.Since(started); elapsed < opts.AssignmentTimeout {
		t.Fatalf("handler returned before assignment timeout: %s", elapsed)
	}
	message := <-server.outgoing
	if response := message.message.GetAvailability(); response == nil || !response.Available {
		t.Fatalf("accept response = %#v", response)
	}
	if !pool.reservation.wasReleased() {
		t.Fatal("timed-out reservation was not released")
	}
	server.stateMu.Lock()
	_, pending := server.pending["job"]
	server.stateMu.Unlock()
	if pending {
		t.Fatal("timed-out assignment remained pending")
	}
	_ = server.Close()
}

func TestWorkerDefaultsAndStatus(t *testing.T) {
	server, err := NewAgentServer(workerTestOptions(func(context.Context, *JobContext[int]) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	if server.opts.AssignmentTimeout != DefaultAssignmentTimeout || server.opts.DrainTimeout != DefaultDrainTimeout || server.opts.MaxReconnects != DefaultMaxReconnects {
		t.Fatalf("defaults = %#v", server.opts)
	}
	pool := &fakeExecutorPool[int]{load: 0.8}
	server.pool = pool
	server.opts.LoadThreshold = 0.7
	status := server.statusMessage(0.8).GetUpdateWorker()
	if status.GetStatus() != livekit.WorkerStatus_WS_FULL || status.GetLoad() != float32(0.8) {
		t.Fatalf("status = %#v", status)
	}
	_ = server.Close()
}

type fixedCPUMonitor struct {
	load float64
	err  error
}

func (m fixedCPUMonitor) CPUCount() float64 { return 1 }

func (m fixedCPUMonitor) CPUPercent(context.Context, time.Duration) (float64, error) {
	return m.load, m.err
}

func TestWorkerDefaultLoadUsesCPUMonitor(t *testing.T) {
	server, err := NewAgentServer(workerTestOptions(func(context.Context, *JobContext[int]) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	server.cpu = fixedCPUMonitor{load: 0.42}
	server.pool = &fakeExecutorPool[int]{load: 0.91}
	load, err := server.currentLoad(context.Background())
	if err != nil || load != 0.42 {
		t.Fatalf("currentLoad=%v err=%v", load, err)
	}

	wantErr := errors.New("cpu failed")
	server.cpu = fixedCPUMonitor{err: wantErr}
	if _, err := server.currentLoad(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("currentLoad error=%v", err)
	}
	_ = server.Close()
}

func TestAgentServerSupportsPreRunRegistration(t *testing.T) {
	opts := workerTestOptions[int](nil)
	server, err := NewAgentServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	if err := server.RegisterRTCSession("registered", func(context.Context, *JobContext[int]) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if server.opts.AgentName != "registered" || server.opts.JobEntrypoint == nil {
		t.Fatal("RegisterRTCSession did not install the handler")
	}
	if err := server.RegisterAgent("second", func(context.Context, *JobContext[int]) error { return nil }); err == nil {
		t.Fatal("second distinct registration succeeded")
	}
	_ = server.Close()
}

func TestTerminalJobStatusIsRetainedUntilWritten(t *testing.T) {
	server, err := NewAgentServer(workerTestOptions(func(context.Context, *JobContext[int]) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	server.publishJobStatus("job", livekit.JobStatus_JS_FAILED, errors.New("boom"))
	server.stateMu.RLock()
	pending := server.deferredJobUpdates["job"]
	server.stateMu.RUnlock()
	if pending == nil || pending.GetUpdateJob().GetError() != "boom" {
		t.Fatalf("deferred update = %#v", pending)
	}
	server.acknowledgeJobUpdate(pending)
	server.stateMu.RLock()
	_, exists := server.deferredJobUpdates["job"]
	server.stateMu.RUnlock()
	if exists {
		t.Fatal("acknowledged terminal update was retained")
	}
	_ = server.Close()
}

func nextWorkerWrite(t *testing.T, transport *fakeWorkerTransport) *livekit.WorkerMessage {
	t.Helper()
	select {
	case message := <-transport.writes:
		return message
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for worker protocol write")
		return nil
	}
}
