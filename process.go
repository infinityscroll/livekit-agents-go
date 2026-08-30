// SPDX-License-Identifier: Apache-2.0

package agents

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/infinityscroll/livekit-agents-go/internal/workerprotocol"
	"github.com/infinityscroll/livekit-agents-go/ipc"
	"github.com/livekit/protocol/livekit"
	"google.golang.org/protobuf/proto"
)

const (
	jobChildEnvironment = "LIVEKIT_AGENTS_GO_JOB_CHILD"
	jobChildAddressEnv  = "LIVEKIT_AGENTS_GO_IPC_ADDRESS"
	jobChildTokenEnv    = "LIVEKIT_AGENTS_GO_IPC_TOKEN"
	jobChildTestRunEnv  = "LIVEKIT_AGENTS_GO_TEST_CHILD_RUN"
	processMemoryPoll   = 5 * time.Second
)

var (
	ErrNoJobCapacity       = errors.New("agents: worker has no available job capacity")
	ErrJobNotFound         = errors.New("agents: job was not found")
	ErrReservationReleased = errors.New("agents: job reservation was released")
	ErrExecutorClosed      = errors.New("agents: job executor is closed")
)

// ExecutorMode selects job isolation. Process isolation is the production
// default and preserves the Python/TypeScript crash and memory boundary.
type ExecutorMode uint8

const (
	ExecutorModeProcess ExecutorMode = iota
	ExecutorModeInProcess
)

type jobStatusPublisher func(string, livekit.JobStatus, error)

type executorPool[T any] interface {
	Start(context.Context) error
	Reserve(context.Context, string) (jobReservation[T], error)
	Terminate(context.Context, string, string) error
	ActiveJobs() []RunningJobInfo
	Load() float64
	Healthy() error
	Drain(context.Context) error
	Close(context.Context) error
}

type jobReservation[T any] interface {
	Launch(context.Context, RunningJobInfo) error
	Release()
}

type executorResult struct {
	success bool
	err     error
}

type oneShotExecutor[T any] interface {
	Launch(context.Context, RunningJobInfo) error
	Stop(context.Context, string) error
	Done() <-chan executorResult
	Exited() <-chan struct{}
	Alive() bool
	PID() int
}

type processPool[T any] struct {
	opts      ServerOptions[T]
	status    jobStatusPublisher
	logger    *slog.Logger
	inference managedInferenceExecutor

	mu           sync.Mutex
	started      bool
	closed       bool
	draining     bool
	ctx          context.Context
	cancel       context.CancelCauseFunc
	capacity     chan struct{}
	reservations map[string]*poolReservation[T]
	active       map[string]*activeExecutor[T]
	idle         []oneShotExecutor[T]
	spawning     int
	changed      chan struct{}
	closeOnce    sync.Once
	closeErr     error
}

type activeExecutor[T any] struct {
	info     RunningJobInfo
	executor oneShotExecutor[T]
}

type poolReservation[T any] struct {
	pool  *processPool[T]
	jobID string

	mu       sync.Mutex
	launched bool
	released bool
}

func newExecutorPool[T any](opts ServerOptions[T], status jobStatusPublisher) executorPool[T] {
	return &processPool[T]{
		opts:         opts,
		status:       status,
		logger:       opts.Logger,
		capacity:     make(chan struct{}, opts.MaxConcurrentJobs),
		reservations: make(map[string]*poolReservation[T]),
		active:       make(map[string]*activeExecutor[T]),
		changed:      make(chan struct{}, 1),
	}
}

func (p *processPool[T]) Start(parent context.Context) (startErr error) {
	if parent == nil {
		parent = context.Background()
	}
	p.mu.Lock()
	if p.started {
		p.mu.Unlock()
		return errors.New("agents: executor pool is already started")
	}
	p.started = true
	p.ctx, p.cancel = context.WithCancelCause(parent)
	p.mu.Unlock()
	// A failed start is terminal for this pool. Roll back every resource that
	// may already have been created (shared inference runners, idle executors,
	// and the pool context) so callers never have to guess whether a partially
	// initialized pool is safe to reuse.
	started := false
	defer func() {
		if started {
			return
		}
		closeCtx, cancel := context.WithTimeout(context.Background(), p.opts.ShutdownProcessTimeout)
		defer cancel()
		startErr = errors.Join(startErr, p.Close(closeCtx))
	}()
	if p.opts.ExecutorMode == ExecutorModeProcess && p.opts.JobMemoryLimitMB > 0 && !memoryMonitoringSupported() {
		return errors.New("agents: JobMemoryLimitMB is not supported on this operating system")
	}

	// Registration is a startup contract. The snapshot is taken exactly once,
	// before any jobs are accepted. An empty registry allocates no executor and
	// starts no subprocess. Production process mode re-executes the linked
	// factories in one shared child; explicit in-process mode retains the local
	// development executor.
	factories := RegisteredInferenceRunners()
	if len(factories) != 0 {
		initializeCtx, cancelInitialize := context.WithTimeout(p.ctx, DefaultInferenceInitializeTimeout)
		var inference managedInferenceExecutor
		if p.opts.ExecutorMode == ExecutorModeInProcess {
			local, err := NewLocalInferenceExecutor(factories)
			if err != nil {
				cancelInitialize()
				return fmt.Errorf("agents: create shared inference executor: %w", err)
			}
			p.mu.Lock()
			p.inference = local
			p.mu.Unlock()
			if err := local.Initialize(initializeCtx); err != nil {
				cancelInitialize()
				return fmt.Errorf("agents: initialize shared inference executor: %w", err)
			}
			inference = local
		} else {
			child, err := newInferenceProcessExecutor(
				initializeCtx, sortedInferenceMethods(factories), p.opts.ShutdownProcessTimeout,
				"LIVEKIT_URL="+p.opts.URL,
				"LIVEKIT_API_KEY="+p.opts.APIKey.Reveal(),
				"LIVEKIT_API_SECRET="+p.opts.APISecret.Reveal(),
				"LIVEKIT_WORKER_TOKEN="+p.opts.WorkerToken.Reveal(),
			)
			if err != nil {
				cancelInitialize()
				return fmt.Errorf("agents: initialize shared inference process: %w", err)
			}
			inference = child
		}
		p.mu.Lock()
		p.inference = inference
		p.mu.Unlock()
		cancelInitialize()
	}

	if p.opts.NumIdleProcesses == 0 {
		started = true
		return nil
	}

	type initialized struct {
		executor oneShotExecutor[T]
		err      error
	}
	results := make(chan initialized, p.opts.NumIdleProcesses)
	for range p.opts.NumIdleProcesses {
		go func() {
			executor, err := p.createExecutor(p.ctx)
			results <- initialized{executor: executor, err: err}
		}()
	}
	created := make([]oneShotExecutor[T], 0, p.opts.NumIdleProcesses)
	var initializeErr error
	for range p.opts.NumIdleProcesses {
		result := <-results
		if result.err != nil {
			initializeErr = errors.Join(initializeErr, result.err)
			continue
		}
		created = append(created, result.executor)
	}
	if initializeErr != nil {
		for _, executor := range created {
			closeCtx, cancel := context.WithTimeout(context.Background(), p.opts.ShutdownProcessTimeout)
			_ = executor.Stop(closeCtx, "executor pool initialization failed")
			cancel()
		}
		return initializeErr
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		for _, executor := range created {
			closeCtx, cancel := context.WithTimeout(context.Background(), p.opts.ShutdownProcessTimeout)
			_ = executor.Stop(closeCtx, "executor pool closed during initialization")
			cancel()
		}
		return ErrExecutorClosed
	}
	p.idle = append(p.idle, created...)
	p.mu.Unlock()
	for _, executor := range created {
		p.watchIdle(executor)
	}
	started = true
	return nil
}

func (p *processPool[T]) Reserve(ctx context.Context, jobID string) (jobReservation[T], error) {
	if jobID == "" {
		return nil, errors.New("agents: cannot reserve an empty job ID")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.mu.Lock()
	if !p.started || p.closed {
		p.mu.Unlock()
		return nil, ErrExecutorClosed
	}
	if p.draining {
		p.mu.Unlock()
		return nil, errors.New("agents: executor pool is draining")
	}
	if _, exists := p.reservations[jobID]; exists {
		p.mu.Unlock()
		return nil, fmt.Errorf("agents: job %s is already reserved", jobID)
	}
	if _, exists := p.active[jobID]; exists {
		p.mu.Unlock()
		return nil, fmt.Errorf("agents: job %s is already active", jobID)
	}
	select {
	case p.capacity <- struct{}{}:
	default:
		p.mu.Unlock()
		return nil, ErrNoJobCapacity
	}
	reservation := &poolReservation[T]{pool: p, jobID: jobID}
	p.reservations[jobID] = reservation
	p.signalChangedLocked()
	p.mu.Unlock()
	return reservation, nil
}

func (r *poolReservation[T]) Launch(ctx context.Context, info RunningJobInfo) error {
	if info.Job == nil || info.Job.Id == "" {
		return errors.New("agents: cannot launch an empty job")
	}
	if info.Job.Id != r.jobID {
		return fmt.Errorf("agents: assignment job ID %q does not match reservation %q", info.Job.Id, r.jobID)
	}
	r.mu.Lock()
	if r.released {
		r.mu.Unlock()
		return ErrReservationReleased
	}
	if r.launched {
		r.mu.Unlock()
		return errors.New("agents: job reservation was already launched")
	}
	r.launched = true
	r.mu.Unlock()
	if err := r.pool.launch(ctx, r, info.Clone()); err != nil {
		r.pool.releaseReservation(r)
		return err
	}
	return nil
}

func (r *poolReservation[T]) Release() {
	if r == nil || r.pool == nil {
		return
	}
	r.mu.Lock()
	if r.released || r.launched {
		r.mu.Unlock()
		return
	}
	r.released = true
	r.mu.Unlock()
	r.pool.releaseReservation(r)
}

func (p *processPool[T]) releaseReservation(reservation *poolReservation[T]) {
	p.mu.Lock()
	if current, exists := p.reservations[reservation.jobID]; exists && current == reservation {
		delete(p.reservations, reservation.jobID)
		select {
		case <-p.capacity:
		default:
		}
		p.signalChangedLocked()
	}
	p.mu.Unlock()
}

func (p *processPool[T]) launch(ctx context.Context, reservation *poolReservation[T], info RunningJobInfo) error {
	p.mu.Lock()
	current, exists := p.reservations[reservation.jobID]
	if !exists || current != reservation || p.closed {
		p.mu.Unlock()
		return ErrReservationReleased
	}
	var executor oneShotExecutor[T]
	for len(p.idle) > 0 {
		last := len(p.idle) - 1
		candidate := p.idle[last]
		p.idle = p.idle[:last]
		if candidate.Alive() {
			executor = candidate
			break
		}
	}
	p.mu.Unlock()

	if executor == nil {
		var err error
		executor, err = p.createExecutor(ctx)
		if err != nil {
			return err
		}
	}
	if err := executor.Launch(ctx, info); err != nil {
		closeCtx, cancel := context.WithTimeout(context.Background(), p.opts.ShutdownProcessTimeout)
		_ = executor.Stop(closeCtx, "job launch failed")
		cancel()
		return err
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		closeCtx, cancel := context.WithTimeout(context.Background(), p.opts.ShutdownProcessTimeout)
		_ = executor.Stop(closeCtx, "executor pool closed")
		cancel()
		return ErrExecutorClosed
	}
	delete(p.reservations, reservation.jobID)
	p.active[reservation.jobID] = &activeExecutor[T]{info: info.Clone(), executor: executor}
	p.signalChangedLocked()
	p.mu.Unlock()

	go p.awaitExecutor(reservation.jobID, executor)
	p.ensureIdle()
	return nil
}

func (p *processPool[T]) awaitExecutor(jobID string, executor oneShotExecutor[T]) {
	result := <-executor.Done()
	status := livekit.JobStatus_JS_SUCCESS
	if !result.success || result.err != nil {
		status = livekit.JobStatus_JS_FAILED
	}
	if p.status != nil {
		p.status(jobID, status, result.err)
	}
	p.mu.Lock()
	if active, exists := p.active[jobID]; exists && active.executor == executor {
		delete(p.active, jobID)
		select {
		case <-p.capacity:
		default:
		}
		p.signalChangedLocked()
	}
	p.mu.Unlock()
	p.ensureIdle()
}

func (p *processPool[T]) createExecutor(ctx context.Context) (oneShotExecutor[T], error) {
	initCtx, cancel := context.WithTimeout(ctx, p.opts.InitializeProcessTimeout)
	defer cancel()
	p.mu.Lock()
	inference := p.inference
	p.mu.Unlock()
	if p.opts.ExecutorMode == ExecutorModeInProcess {
		return newInProcessExecutor(initCtx, p.opts, p.status, inference)
	}
	return newProcessExecutor(initCtx, p.opts, p.status, inference)
}

func (p *processPool[T]) ensureIdle() {
	p.mu.Lock()
	if p.closed || p.draining || !p.started {
		p.mu.Unlock()
		return
	}
	needed := p.opts.NumIdleProcesses - len(p.idle) - p.spawning
	if needed <= 0 {
		p.mu.Unlock()
		return
	}
	p.spawning += needed
	ctx := p.ctx
	p.mu.Unlock()
	for range needed {
		go func() {
			executor, err := p.createExecutor(ctx)
			p.mu.Lock()
			p.spawning--
			if err == nil && !p.closed && !p.draining {
				p.idle = append(p.idle, executor)
				p.signalChangedLocked()
				p.mu.Unlock()
				p.watchIdle(executor)
				return
			}
			p.mu.Unlock()
			if executor != nil {
				closeCtx, cancel := context.WithTimeout(context.Background(), p.opts.ShutdownProcessTimeout)
				_ = executor.Stop(closeCtx, "idle executor no longer needed")
				cancel()
			}
			if err != nil && ctx.Err() == nil {
				p.logger.Error("failed to replenish idle job executor", "error", err)
				timer := time.NewTimer(2 * time.Second)
				select {
				case <-timer.C:
					p.ensureIdle()
				case <-ctx.Done():
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
				}
			}
		}()
	}
}

func (p *processPool[T]) watchIdle(executor oneShotExecutor[T]) {
	go func() {
		<-executor.Exited()
		p.mu.Lock()
		removed := false
		for index, idle := range p.idle {
			if idle == executor {
				p.idle = append(p.idle[:index], p.idle[index+1:]...)
				removed = true
				break
			}
		}
		if removed {
			p.signalChangedLocked()
		}
		p.mu.Unlock()
		if removed {
			p.logger.Warn("idle job executor exited unexpectedly", "pid", executor.PID())
			p.ensureIdle()
		}
	}()
}

func (p *processPool[T]) Terminate(ctx context.Context, jobID, reason string) error {
	p.mu.Lock()
	active, exists := p.active[jobID]
	p.mu.Unlock()
	if !exists {
		return ErrJobNotFound
	}
	if reason == "" {
		reason = "server requested termination"
	}
	return active.executor.Stop(ctx, reason)
}

func (p *processPool[T]) ActiveJobs() []RunningJobInfo {
	p.mu.Lock()
	jobs := make([]RunningJobInfo, 0, len(p.active))
	for _, active := range p.active {
		jobs = append(jobs, active.info.Clone())
	}
	p.mu.Unlock()
	sort.Slice(jobs, func(left, right int) bool {
		return jobs[left].Job.GetId() < jobs[right].Job.GetId()
	})
	return jobs
}

func (p *processPool[T]) Load() float64 {
	p.mu.Lock()
	load := float64(len(p.active)+len(p.reservations)) / float64(cap(p.capacity))
	p.mu.Unlock()
	return load
}

func (p *processPool[T]) Healthy() error {
	p.mu.Lock()
	if !p.started {
		p.mu.Unlock()
		return errors.New("executor pool is not started")
	}
	if p.closed {
		p.mu.Unlock()
		return ErrExecutorClosed
	}
	for _, executor := range p.idle {
		if !executor.Alive() {
			p.mu.Unlock()
			return fmt.Errorf("idle executor %d is not alive", executor.PID())
		}
	}
	inference := p.inference
	p.mu.Unlock()
	if inference != nil {
		if err := inference.Healthy(); err != nil {
			return fmt.Errorf("shared inference executor is unhealthy: %w", err)
		}
	}
	return nil
}

func (p *processPool[T]) Drain(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	p.mu.Lock()
	p.draining = true
	idle := append([]oneShotExecutor[T](nil), p.idle...)
	p.idle = nil
	p.signalChangedLocked()
	p.mu.Unlock()
	for _, executor := range idle {
		if err := executor.Stop(ctx, "worker draining"); err != nil {
			return err
		}
	}
	for {
		p.mu.Lock()
		done := len(p.active) == 0 && len(p.reservations) == 0
		p.mu.Unlock()
		if done {
			return nil
		}
		select {
		case <-p.changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (p *processPool[T]) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		p.draining = true
		if p.cancel != nil {
			p.cancel(ErrExecutorClosed)
		}
		reservations := make([]*poolReservation[T], 0, len(p.reservations))
		for _, reservation := range p.reservations {
			reservations = append(reservations, reservation)
		}
		executors := make([]oneShotExecutor[T], 0, len(p.idle)+len(p.active))
		executors = append(executors, p.idle...)
		for _, active := range p.active {
			executors = append(executors, active.executor)
		}
		inference := p.inference
		p.inference = nil
		p.idle = nil
		p.mu.Unlock()
		for _, reservation := range reservations {
			reservation.Release()
		}

		var wg sync.WaitGroup
		errs := make(chan error, len(executors))
		for _, executor := range executors {
			executor := executor
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := executor.Stop(ctx, "worker closed"); err != nil && !errors.Is(err, context.Canceled) {
					errs <- err
				}
			}()
		}
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		completed := false
		select {
		case <-done:
			completed = true
		case <-ctx.Done():
			p.closeErr = errors.Join(p.closeErr, ctx.Err())
		}
		if completed {
			close(errs)
			for err := range errs {
				p.closeErr = errors.Join(p.closeErr, err)
			}
		}
		if inference != nil {
			p.closeErr = errors.Join(p.closeErr, inference.Close(ctx))
		}
	})
	return p.closeErr
}

func (p *processPool[T]) signalChangedLocked() {
	select {
	case p.changed <- struct{}{}:
	default:
	}
}

type processExecutor[T any] struct {
	opts      ServerOptions[T]
	status    jobStatusPublisher
	logger    *slog.Logger
	inference ipc.InferenceExecutor
	cmd       *exec.Cmd
	conn      *workerprotocol.FramedConn
	pid       int

	mu       sync.Mutex
	info     RunningJobInfo
	launched bool
	closed   bool

	done      chan executorResult
	doneOnce  sync.Once
	terminal  chan struct{}
	exitDone  chan struct{}
	exitErr   error
	cancel    context.CancelCauseFunc
	warnedRSS atomic.Bool

	inferenceMu     sync.Mutex
	inferenceCancel map[string]context.CancelCauseFunc
	inferenceSlots  chan struct{}
}

func newProcessExecutor[T any](ctx context.Context, opts ServerOptions[T], status jobStatusPublisher, inferenceExecutors ...ipc.InferenceExecutor) (*processExecutor[T], error) {
	var inference ipc.InferenceExecutor
	if len(inferenceExecutors) != 0 && !isNilInferenceExecutor(inferenceExecutors[0]) {
		inference = inferenceExecutors[0]
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("agents: listen for job child: %w", err)
	}
	defer listener.Close()
	token := os.Getenv("LIVEKIT_AGENTS_GO_TEST_IPC_TOKEN")
	if token == "" {
		token = ShortUUID("ipc_") + ShortUUID("")
	}
	executable, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("agents: locate executable: %w", err)
	}
	arguments := append([]string(nil), os.Args[1:]...)
	if testRun := os.Getenv(jobChildTestRunEnv); testRun != "" {
		arguments = append(arguments, "-test.run="+testRun)
	}
	command := exec.Command(executable, arguments...)
	configureJobCommand(command)
	command.Env = replaceProcessEnvironment(os.Environ(),
		jobChildEnvironment+"=1",
		jobChildAddressEnv+"="+listener.Addr().String(),
		jobChildTokenEnv+"="+token,
		"LIVEKIT_URL="+opts.URL,
		"LIVEKIT_API_KEY="+opts.APIKey.Reveal(),
		"LIVEKIT_API_SECRET="+opts.APISecret.Reveal(),
		"LIVEKIT_WORKER_TOKEN="+opts.WorkerToken.Reveal(),
	)
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	if err := command.Start(); err != nil {
		return nil, fmt.Errorf("agents: start job child: %w", err)
	}
	executor := &processExecutor[T]{
		opts: opts, status: status, logger: opts.Logger, inference: inference, cmd: command, pid: command.Process.Pid,
		done: make(chan executorResult, 1), terminal: make(chan struct{}), exitDone: make(chan struct{}),
		inferenceCancel: make(map[string]context.CancelCauseFunc), inferenceSlots: make(chan struct{}, jobInferenceConcurrency),
	}
	go func() {
		executor.exitErr = command.Wait()
		close(executor.exitDone)
		executor.mu.Lock()
		launched := executor.launched
		executor.mu.Unlock()
		if launched {
			select {
			case <-executor.terminal:
				return
			case <-time.After(100 * time.Millisecond):
			}
			exitErr := executor.exitErr
			if exitErr == nil {
				exitErr = errors.New("job child exited before reporting completion")
			}
			executor.finish(executorResult{err: fmt.Errorf("job child exited: %w", exitErr)})
		}
	}()

	framed, err := acceptAuthenticatedJobChild(ctx, listener, token, executor.exitDone)
	if err != nil {
		_ = killJobProcess(command)
		return nil, err
	}
	ready, err := framed.Read(ctx)
	if err != nil {
		_ = framed.Close()
		_ = killJobProcess(command)
		return nil, fmt.Errorf("agents: initialize job child: %w", err)
	}
	if ready.Type != workerprotocol.IPCTypeReady {
		_ = framed.Close()
		_ = killJobProcess(command)
		return nil, fmt.Errorf("agents: expected ready from job child, got %q", ready.Type)
	}
	if ready.Error != "" {
		_ = framed.Close()
		_ = killJobProcess(command)
		return nil, errors.New(ready.Error)
	}
	executor.conn = framed
	return executor, nil
}

func acceptAuthenticatedJobChild(ctx context.Context, listener net.Listener, token string, childExited <-chan struct{}) (*workerprotocol.FramedConn, error) {
	return acceptAuthenticatedChild(ctx, listener, token, childExited, "job child")
}

func replaceProcessEnvironment(base []string, replacements ...string) []string {
	keys := make(map[string]struct{}, len(replacements))
	for _, replacement := range replacements {
		key, _, _ := strings.Cut(replacement, "=")
		keys[key] = struct{}{}
	}
	out := make([]string, 0, len(base)+len(replacements))
	for _, value := range base {
		key, _, _ := strings.Cut(value, "=")
		if _, replaced := keys[key]; !replaced {
			out = append(out, value)
		}
	}
	return append(out, replacements...)
}

func (e *processExecutor[T]) Launch(ctx context.Context, info RunningJobInfo) error {
	jobBytes, err := proto.Marshal(info.Job)
	if err != nil {
		return fmt.Errorf("agents: encode assigned job: %w", err)
	}
	e.mu.Lock()
	if e.closed || !e.Alive() {
		e.mu.Unlock()
		return ErrExecutorClosed
	}
	if e.launched {
		e.mu.Unlock()
		return errors.New("agents: process executor is one-shot")
	}
	e.launched = true
	e.info = info.Clone()
	runCtx, cancel := context.WithCancelCause(context.Background())
	e.cancel = cancel
	e.mu.Unlock()
	message := workerprotocol.IPCMessage{
		Type: workerprotocol.IPCTypeStart, Job: jobBytes, URL: info.URL, RoomToken: info.Token.Reveal(),
		WorkerID: info.WorkerID, APIKey: info.APIKey.Reveal(), APISecret: info.APISecret.Reveal(), FakeJob: info.FakeJob,
		ParticipantName: info.AcceptArguments.Name, ParticipantIdentity: info.AcceptArguments.Identity,
		ParticipantMetadata: info.AcceptArguments.Metadata, ParticipantAttributes: cloneStrings(info.AcceptArguments.Attributes),
	}
	if err := e.conn.Write(ctx, message); err != nil {
		cancel(err)
		e.finish(executorResult{err: err})
		return err
	}
	go e.readJob(runCtx)
	if memoryMonitoringSupported() && (e.opts.JobMemoryWarnMB > 0 || e.opts.JobMemoryLimitMB > 0) {
		go e.monitorMemory(runCtx)
	}
	return nil
}

func (e *processExecutor[T]) readJob(ctx context.Context) {
	defer e.cancelAllInference(context.Cause(ctx))
	for {
		message, err := e.conn.Read(ctx)
		if err != nil {
			if ctx.Err() == nil {
				e.finish(executorResult{err: fmt.Errorf("job child IPC failed: %w", err)})
			}
			return
		}
		switch message.Type {
		case workerprotocol.IPCTypeStatus:
			status := livekit.JobStatus(message.Status)
			if status == livekit.JobStatus_JS_RUNNING && e.status != nil {
				e.status(e.info.Job.GetId(), status, nil)
			}
		case workerprotocol.IPCTypeDone:
			var resultErr error
			if message.Error != "" {
				resultErr = errors.New(message.Error)
			}
			e.finish(executorResult{success: message.Success, err: resultErr})
			_ = e.conn.Close()
			go e.ensureExited()
			return
		case workerprotocol.IPCTypeInferenceRequest:
			e.startInference(ctx, message)
		case workerprotocol.IPCTypeInferenceCancel:
			e.cancelInference(message.RequestID)
		default:
			e.finish(executorResult{err: fmt.Errorf("agents: unexpected job child message %q", message.Type)})
			return
		}
	}
}

func (e *processExecutor[T]) monitorMemory(ctx context.Context) {
	ticker := time.NewTicker(processMemoryPoll)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			rss, err := processRSSBytes(e.pid)
			if err != nil {
				e.logger.Debug("failed to read job process memory", "pid", e.pid, "error", err)
				continue
			}
			megabytes := rss / (1024 * 1024)
			if e.opts.JobMemoryLimitMB > 0 && megabytes > int64(e.opts.JobMemoryLimitMB) {
				err := fmt.Errorf("job process exceeded memory limit: %d MB > %d MB", megabytes, e.opts.JobMemoryLimitMB)
				e.logger.Error("terminating job process for memory limit", "pid", e.pid, "rss_mb", megabytes, "limit_mb", e.opts.JobMemoryLimitMB)
				e.finish(executorResult{err: err})
				_ = killJobProcess(e.cmd)
				return
			}
			if e.opts.JobMemoryWarnMB > 0 && megabytes > int64(e.opts.JobMemoryWarnMB) && e.warnedRSS.CompareAndSwap(false, true) {
				e.logger.Warn("job process memory is high", "pid", e.pid, "rss_mb", megabytes, "warn_mb", e.opts.JobMemoryWarnMB)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (e *processExecutor[T]) Stop(ctx context.Context, reason string) error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	launched := e.launched
	e.mu.Unlock()
	if reason == "" {
		reason = "job executor stopped"
	}
	writeErr := e.conn.Write(ctx, workerprotocol.IPCMessage{Type: workerprotocol.IPCTypeStop, Reason: reason})
	if launched {
		select {
		case <-e.terminal:
		case <-ctx.Done():
			_ = killJobProcess(e.cmd)
			<-e.exitDone
			return errors.Join(writeErr, ctx.Err())
		case <-e.exitDone:
		}
		select {
		case <-e.exitDone:
		case <-ctx.Done():
			_ = killJobProcess(e.cmd)
			<-e.exitDone
			return errors.Join(writeErr, ctx.Err())
		}
	} else {
		select {
		case <-e.exitDone:
		case <-ctx.Done():
			_ = killJobProcess(e.cmd)
			<-e.exitDone
			return errors.Join(writeErr, ctx.Err())
		}
	}
	_ = e.conn.Close()
	return writeErr
}

func (e *processExecutor[T]) ensureExited() {
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case <-e.exitDone:
	case <-timer.C:
		e.logger.Warn("job child did not exit after completion; killing it", "pid", e.pid)
		_ = killJobProcess(e.cmd)
		<-e.exitDone
	}
}

func (e *processExecutor[T]) finish(result executorResult) {
	e.doneOnce.Do(func() {
		if e.cancel != nil {
			e.cancel(result.err)
		}
		e.done <- result
		close(e.terminal)
	})
}

func (e *processExecutor[T]) Done() <-chan executorResult { return e.done }
func (e *processExecutor[T]) Exited() <-chan struct{}     { return e.exitDone }
func (e *processExecutor[T]) PID() int                    { return e.pid }
func (e *processExecutor[T]) Alive() bool {
	select {
	case <-e.exitDone:
		return false
	default:
		return true
	}
}

type inProcessExecutor[T any] struct {
	opts      ServerOptions[T]
	status    jobStatusPublisher
	proc      *JobProcess[T]
	inference ipc.InferenceExecutor

	mu       sync.Mutex
	launched bool
	closed   bool
	job      *JobContext[T]
	done     chan executorResult
	doneOnce sync.Once
	exited   chan struct{}
}

func newInProcessExecutor[T any](ctx context.Context, opts ServerOptions[T], status jobStatusPublisher, inferenceExecutors ...ipc.InferenceExecutor) (*inProcessExecutor[T], error) {
	var inference ipc.InferenceExecutor
	if len(inferenceExecutors) != 0 && !isNilInferenceExecutor(inferenceExecutors[0]) {
		inference = inferenceExecutors[0]
	}
	executor := &inProcessExecutor[T]{
		opts: opts, status: status, proc: newJobProcess[T](os.Getpid()), inference: inference,
		done: make(chan executorResult, 1), exited: make(chan struct{}),
	}
	if err := callPrewarm(ctx, opts.Prewarm, executor.proc); err != nil {
		return nil, err
	}
	return executor, nil
}

func (e *inProcessExecutor[T]) Launch(_ context.Context, info RunningJobInfo) error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return ErrExecutorClosed
	}
	if e.launched {
		e.mu.Unlock()
		return errors.New("agents: in-process executor is one-shot")
	}
	e.launched = true
	job := newJobContext(context.Background(), e.proc, info, func() {
		if e.status != nil {
			e.status(info.Job.GetId(), livekit.JobStatus_JS_RUNNING, nil)
		}
	}, e.inference)
	e.job = job
	e.mu.Unlock()
	go func() {
		err := runJobEntrypoint(e.opts.JobEntrypoint, job)
		if errors.Is(err, context.Canceled) && job.Context().Err() != nil {
			err = nil
		}
		if err == nil {
			<-job.Done()
		} else {
			job.Shutdown(err.Error())
		}
		finishCtx, cancel := context.WithTimeout(context.Background(), e.opts.ShutdownProcessTimeout)
		finishErr := job.finish(finishCtx)
		cancel()
		err = errors.Join(err, finishErr)
		e.doneOnce.Do(func() {
			e.done <- executorResult{success: err == nil, err: err}
			close(e.exited)
		})
	}()
	return nil
}

func (e *inProcessExecutor[T]) Stop(ctx context.Context, reason string) error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	job := e.job
	launched := e.launched
	e.mu.Unlock()
	if !launched {
		e.doneOnce.Do(func() {
			e.done <- executorResult{success: true}
			close(e.exited)
		})
		return nil
	}
	job.Shutdown(reason)
	select {
	case <-e.exited:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *inProcessExecutor[T]) Done() <-chan executorResult { return e.done }
func (e *inProcessExecutor[T]) Exited() <-chan struct{}     { return e.exited }
func (e *inProcessExecutor[T]) PID() int                    { return os.Getpid() }
func (e *inProcessExecutor[T]) Alive() bool {
	select {
	case <-e.exited:
		return false
	default:
		return true
	}
}

func isJobChildProcess() bool { return os.Getenv(jobChildEnvironment) == "1" }

func runJobChildProcess[T any](ctx context.Context, opts ServerOptions[T]) error {
	address := os.Getenv(jobChildAddressEnv)
	token := os.Getenv(jobChildTokenEnv)
	if address == "" || token == "" {
		return errors.New("agents: job child IPC environment is incomplete")
	}
	initCtx, cancelInit := context.WithTimeout(ctx, opts.InitializeProcessTimeout)
	defer cancelInit()
	raw, err := (&net.Dialer{}).DialContext(initCtx, "tcp", address)
	if err != nil {
		return fmt.Errorf("agents: connect to worker parent: %w", err)
	}
	conn := workerprotocol.NewFramedConn(raw)
	defer conn.Close()
	if err := conn.Write(initCtx, workerprotocol.IPCMessage{Type: workerprotocol.IPCTypeHello, Token: token}); err != nil {
		return err
	}
	proc := newJobProcess[T](os.Getpid())
	if err := callPrewarm(initCtx, opts.Prewarm, proc); err != nil {
		_ = conn.Write(initCtx, workerprotocol.IPCMessage{Type: workerprotocol.IPCTypeReady, Error: err.Error()})
		return err
	}
	if err := conn.Write(initCtx, workerprotocol.IPCMessage{Type: workerprotocol.IPCTypeReady}); err != nil {
		return err
	}
	first, err := conn.Read(ctx)
	if err != nil {
		return err
	}
	if first.Type == workerprotocol.IPCTypeStop {
		_ = conn.Write(context.Background(), workerprotocol.IPCMessage{Type: workerprotocol.IPCTypeDone, Success: true, Reason: first.Reason})
		return nil
	}
	if first.Type != workerprotocol.IPCTypeStart {
		return fmt.Errorf("agents: expected start from worker parent, got %q", first.Type)
	}
	job := new(livekit.Job)
	if err := proto.Unmarshal(first.Job, job); err != nil {
		_ = conn.Write(context.Background(), workerprotocol.IPCMessage{Type: workerprotocol.IPCTypeDone, Error: err.Error()})
		return fmt.Errorf("agents: decode assigned job: %w", err)
	}
	info := RunningJobInfo{
		AcceptArguments: JobAcceptOptions{
			Name: first.ParticipantName, Identity: first.ParticipantIdentity, Metadata: first.ParticipantMetadata,
			Attributes: cloneStrings(first.ParticipantAttributes),
		},
		Job: job, URL: first.URL, Token: NewSecretString(first.RoomToken), WorkerID: first.WorkerID,
		APIKey: NewSecretString(first.APIKey), APISecret: NewSecretString(first.APISecret), FakeJob: first.FakeJob,
	}
	inferenceClient := newJobIPCInferenceClient(conn)
	defer inferenceClient.close(errors.New("agents: job child inference client closed"))
	jobContext := newJobContext(ctx, proc, info, func() {
		statusCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Write(statusCtx, workerprotocol.IPCMessage{Type: workerprotocol.IPCTypeStatus, Status: int32(livekit.JobStatus_JS_RUNNING)})
	}, inferenceClient)
	entryDone := make(chan error, 1)
	go func() { entryDone <- runJobEntrypoint(opts.JobEntrypoint, jobContext) }()
	control := make(chan workerprotocol.IPCMessage, 1)
	controlErr := make(chan error, 1)
	go func() {
		for {
			message, readErr := conn.Read(ctx)
			if readErr != nil {
				inferenceClient.close(readErr)
				controlErr <- readErr
				return
			}
			switch message.Type {
			case workerprotocol.IPCTypeInferenceResponse:
				inferenceClient.dispatchResponse(message)
			case workerprotocol.IPCTypeStop:
				control <- message
				return
			default:
				err := fmt.Errorf("agents: unexpected parent control message %q", message.Type)
				inferenceClient.close(err)
				controlErr <- err
				return
			}
		}
	}()

	var entryErr error
	entryReturned := false
	select {
	case entryErr = <-entryDone:
		entryReturned = true
		if entryErr != nil {
			jobContext.Shutdown(entryErr.Error())
		}
	case message := <-control:
		if message.Type != workerprotocol.IPCTypeStop {
			entryErr = fmt.Errorf("agents: unexpected parent control message %q", message.Type)
			jobContext.Shutdown(entryErr.Error())
		} else {
			jobContext.Shutdown(message.Reason)
		}
	case readErr := <-controlErr:
		entryErr = fmt.Errorf("agents: parent IPC disconnected: %w", readErr)
		jobContext.Shutdown(entryErr.Error())
	case <-jobContext.Done():
	case <-ctx.Done():
		entryErr = ctx.Err()
		jobContext.Shutdown(entryErr.Error())
	}
	if entryReturned && entryErr == nil {
		select {
		case message := <-control:
			if message.Type == workerprotocol.IPCTypeStop {
				jobContext.Shutdown(message.Reason)
			} else {
				entryErr = fmt.Errorf("agents: unexpected parent control message %q", message.Type)
				jobContext.Shutdown(entryErr.Error())
			}
		case readErr := <-controlErr:
			entryErr = fmt.Errorf("agents: parent IPC disconnected: %w", readErr)
			jobContext.Shutdown(entryErr.Error())
		case <-jobContext.Done():
		case <-ctx.Done():
			entryErr = ctx.Err()
			jobContext.Shutdown(entryErr.Error())
		}
	}
	if !entryReturned {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), opts.ShutdownProcessTimeout)
		select {
		case returnedErr := <-entryDone:
			entryErr = errors.Join(entryErr, returnedErr)
		case <-shutdownCtx.Done():
			entryErr = errors.Join(entryErr, errors.New("agents: job entrypoint did not stop before shutdown timeout"))
		}
		cancel()
	}
	if errors.Is(entryErr, context.Canceled) && jobContext.Context().Err() != nil {
		entryErr = nil
	}
	finishCtx, cancelFinish := context.WithTimeout(context.Background(), opts.ShutdownProcessTimeout)
	finishErr := jobContext.finish(finishCtx)
	cancelFinish()
	entryErr = errors.Join(entryErr, finishErr)
	done := workerprotocol.IPCMessage{Type: workerprotocol.IPCTypeDone, Success: entryErr == nil, Reason: jobContext.ShutdownReason()}
	if entryErr != nil {
		done.Error = entryErr.Error()
	}
	writeCtx, cancelWrite := context.WithTimeout(context.Background(), 5*time.Second)
	writeErr := conn.Write(writeCtx, done)
	cancelWrite()
	return errors.Join(entryErr, writeErr)
}

func callPrewarm[T any](ctx context.Context, prewarm PrewarmFunc[T], process *JobProcess[T]) (err error) {
	if prewarm == nil {
		return nil
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("agents: prewarm panicked: %v", recovered)
		}
	}()
	if err := prewarm(ctx, process); err != nil {
		return fmt.Errorf("agents: prewarm failed: %w", err)
	}
	return nil
}

func runJobEntrypoint[T any](entrypoint JobEntrypoint[T], job *JobContext[T]) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("agents: job entrypoint panicked: %v", recovered)
		}
	}()
	ctx := contextWithJob(job.Context(), job)
	return entrypoint(ctx, job)
}
