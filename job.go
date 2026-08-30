// SPDX-License-Identifier: Apache-2.0

package agents

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"sync"

	"github.com/infinityscroll/livekit-agents-go/ipc"
	"github.com/infinityscroll/livekit-agents-go/rtcbridge"
	"github.com/livekit/protocol/livekit"
	lksdk "github.com/livekit/server-sdk-go/v2"
	"google.golang.org/protobuf/proto"
)

// AutoSubscribe controls which remote tracks JobContext.Connect subscribes to.
// The values intentionally follow the TypeScript and Python SDK ordering.
type AutoSubscribe uint8

const (
	SubscribeAll AutoSubscribe = iota
	SubscribeNone
	SubscribeVideoOnly
	SubscribeAudioOnly

	// Upper-case aliases make mechanical TypeScript/Python migrations easier.
	SUBSCRIBE_ALL  = SubscribeAll
	SUBSCRIBE_NONE = SubscribeNone
	VIDEO_ONLY     = SubscribeVideoOnly
	AUDIO_ONLY     = SubscribeAudioOnly
)

var (
	ErrJobRequestAnswered = errors.New("agents: job request has already been answered")
	ErrRoomNotConnected   = errors.New("agents: room is not connected")
	ErrFunctionExists     = errors.New("agents: function has already been registered")
)

// FunctionExistsError reports duplicate callback registration.
type FunctionExistsError struct{ Message string }

func (e *FunctionExistsError) Error() string {
	if e == nil || e.Message == "" {
		return ErrFunctionExists.Error()
	}
	return e.Message
}

func (e *FunctionExistsError) Unwrap() error { return ErrFunctionExists }

// JobAcceptOptions controls the participant created for an accepted job.
type JobAcceptOptions struct {
	Name       string
	Identity   string
	Metadata   string
	Attributes map[string]string
}

// JobRejectOptions is reserved for protocol-compatible rejection metadata.
// The current worker protocol carries only the availability decision.
type JobRejectOptions struct{}

// RunningJobInfo is the complete, immutable assignment passed to an executor.
// API credentials are redacted when formatted or marshaled.
type RunningJobInfo struct {
	AcceptArguments JobAcceptOptions
	Job             *livekit.Job
	URL             string
	Token           SecretString
	WorkerID        string
	APIKey          SecretString
	APISecret       SecretString
	FakeJob         bool
	// SessionDirectory overrides the per-job directory used for recordings and
	// reports. Production assignments normally leave this empty and receive a
	// traversal-safe directory below os.TempDir. Console runners set it to their
	// user-visible session directory.
	SessionDirectory string
}

// Clone returns a deep copy suitable for crossing ownership boundaries.
func (i RunningJobInfo) Clone() RunningJobInfo {
	out := i
	out.AcceptArguments.Attributes = cloneStrings(i.AcceptArguments.Attributes)
	if i.Job != nil {
		out.Job = proto.Clone(i.Job).(*livekit.Job)
	}
	return out
}

// JobProcess is process-local state initialized by a prewarm callback. In
// process-executor mode PID is the child PID; in explicit in-process mode it is
// the current process PID.
type JobProcess[T any] struct {
	pid       int
	stateOnce sync.Once
	state     *T
}

func newJobProcess[T any](pid int) *JobProcess[T] {
	return &JobProcess[T]{pid: pid, state: new(T)}
}

func (p *JobProcess[T]) PID() int {
	if p == nil {
		return 0
	}
	return p.pid
}

// State returns the typed, process-local value shared by Prewarm and the job.
func (p *JobProcess[T]) State() *T {
	if p == nil {
		return nil
	}
	p.stateOnce.Do(func() {
		if p.state == nil {
			p.state = new(T)
		}
	})
	return p.state
}

// UserData is a compatibility alias for State.
func (p *JobProcess[T]) UserData() *T { return p.State() }

type JobEntrypoint[T any] func(context.Context, *JobContext[T]) error
type Entrypoint[T any] = JobEntrypoint[T]
type PrewarmFunc[T any] func(context.Context, *JobProcess[T]) error
type RequestHandler func(context.Context, *JobRequest) error
type ShutdownCallback func(context.Context, string) error
type ParticipantEntrypoint[T any] func(context.Context, *JobContext[T], *lksdk.RemoteParticipant) error

type JobAcceptArguments = JobAcceptOptions

// ConnectOptions controls room connection and automatic subscription.
type ConnectOptions struct {
	AutoSubscribe AutoSubscribe
	RoomOptions   []lksdk.ConnectOption
}

type participantTask[T any] struct {
	ctx         *JobContext[T]
	callback    ParticipantEntrypoint[T]
	participant *lksdk.RemoteParticipant
}

type participantCallbackRegistration[T any] struct {
	id       uint64
	callback ParticipantEntrypoint[T]
}

const (
	participantCallbackWorkers = 8
	participantCallbackQueue   = 128
)

// JobContext is the job and room environment passed to an entrypoint.
type JobContext[T any] struct {
	proc              *JobProcess[T]
	info              RunningJobInfo
	room              *lksdk.Room
	rtcBridge         *rtcbridge.RTCBridge
	inferenceExecutor ipc.InferenceExecutor
	sessionDirectory  string

	ctx    context.Context
	cancel context.CancelCauseFunc

	connectMu       sync.Mutex
	connected       bool
	connectInFlight bool
	connectDone     chan struct{}
	connectErr      error
	autoSubscribe   AutoSubscribe
	onConnect       func()
	shutdownOnce    sync.Once
	shutdownMu      sync.RWMutex
	shutdownReason  string

	callbackMu                sync.RWMutex
	shutdownCallbacks         []ShutdownCallback
	participantCallbacks      []participantCallbackRegistration[T]
	nextParticipantCallbackID uint64
	participantQueue          chan participantTask[T]
	participantWorkers        sync.WaitGroup

	simulationOnce sync.Once
	simulation     *SimulationContext[T]

	sessionMu        sync.Mutex
	primarySession   registeredJobSession
	nextSessionID    uint64
	sessionFinishing bool
}

func newJobContext[T any](parent context.Context, proc *JobProcess[T], info RunningJobInfo, onConnect func(), executors ...ipc.InferenceExecutor) *JobContext[T] {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancelCause(parent)
	inferenceExecutor := ipc.InferenceExecutor(ipc.InferenceExecutorFunc(func(_ context.Context, method string, _ any) (any, error) {
		return nil, &ipc.UnknownMethodError{Method: method}
	}))
	if len(executors) != 0 && !isNilInferenceExecutor(executors[0]) {
		inferenceExecutor = executors[0]
	}
	j := &JobContext[T]{
		proc:              proc,
		info:              info.Clone(),
		ctx:               ctx,
		cancel:            cancel,
		onConnect:         onConnect,
		inferenceExecutor: inferenceExecutor,
		participantQueue:  make(chan participantTask[T], participantCallbackQueue),
		sessionDirectory:  resolveSessionDirectory(info),
	}
	callback := lksdk.NewRoomCallback()
	callback.OnParticipantConnected = j.onParticipantConnected
	callback.OnParticipantDisconnected = j.onParticipantDisconnected
	callback.OnDisconnected = func() {
		j.connectMu.Lock()
		j.connected = false
		j.connectMu.Unlock()
		j.Shutdown("room disconnected")
	}
	callback.OnTrackPublished = j.onTrackPublished
	j.rtcBridge = rtcbridge.NewRTCBridge(callback, rtcbridge.RTCBridgeOptions{
		OnCallbackPanic: func(err error) {
			slog.Error("room callback panicked", "job_id", info.Job.GetId(), "error", err)
		},
	})
	j.room = lksdk.NewRoom(j.rtcBridge.Callback())
	for range participantCallbackWorkers {
		j.participantWorkers.Add(1)
		go j.runParticipantCallbacks()
	}
	return j
}

func (j *JobContext[T]) Process() *JobProcess[T] { return j.proc }
func (j *JobContext[T]) State() *T               { return j.proc.State() }
func (j *JobContext[T]) Room() *lksdk.Room       { return j.room }

// RTCBridge returns the construction-time callback bridge installed on Room.
// Higher-level RoomIO uses it to subscribe without polling or unsafe callback
// replacement. The JobContext owns and closes the bridge.
func (j *JobContext[T]) RTCBridge() *rtcbridge.RTCBridge {
	if j == nil {
		return nil
	}
	return j.rtcBridge
}

func (j *JobContext[T]) WorkerID() string         { return j.info.WorkerID }
func (j *JobContext[T]) IsFakeJob() bool          { return j.info.FakeJob }
func (j *JobContext[T]) Context() context.Context { return j.ctx }

// SessionDirectory is the traversal-safe per-job location for recordings and
// session artifacts. The directory is created lazily by the component that
// writes an artifact.
func (j *JobContext[T]) SessionDirectory() string {
	if j == nil {
		return ""
	}
	return j.sessionDirectory
}

// InferenceExecutor returns the worker-global local inference executor. In
// process-isolated jobs this is a cancellation-aware IPC client backed by the
// same shared runner instances as every other job.
func (j *JobContext[T]) InferenceExecutor() ipc.InferenceExecutor {
	if j == nil || isNilInferenceExecutor(j.inferenceExecutor) {
		return ipc.InferenceExecutorFunc(func(_ context.Context, method string, _ any) (any, error) {
			return nil, &ipc.UnknownMethodError{Method: method}
		})
	}
	return j.inferenceExecutor
}

func (j *JobContext[T]) Job() *livekit.Job {
	if j == nil || j.info.Job == nil {
		return nil
	}
	return proto.Clone(j.info.Job).(*livekit.Job)
}

func (j *JobContext[T]) Info() RunningJobInfo {
	if j == nil {
		return RunningJobInfo{}
	}
	return j.info.Clone()
}

// SimulationContext resolves and caches the simulation dispatch carried on
// the assigned job. Malformed or incomplete attributes are ignored after one
// warning, matching the TypeScript/Python entrypoint behavior.
func (j *JobContext[T]) SimulationContext() (*SimulationContext[T], bool) {
	if j == nil {
		return nil, false
	}
	j.simulationOnce.Do(func() {
		job := j.info.Job
		if job == nil {
			return
		}
		raw := job.GetAttributes()[AttributeSimulatorDispatch]
		if raw == "" {
			return
		}
		dispatch, err := ParseSimulationDispatch(raw)
		if err != nil {
			slog.Warn("failed to parse simulation dispatch job attribute", "error", err)
			return
		}
		if dispatch.GetSimulationRunId() == "" {
			slog.Warn("simulation dispatch attribute has no simulation_run_id; ignoring")
			return
		}
		j.simulation, err = NewSimulationContext(dispatch, j)
		if err != nil {
			slog.Warn("failed to create simulation context", "error", err)
		}
	})
	return j.simulation, j.simulation != nil
}

func (j *JobContext[T]) Agent() *lksdk.LocalParticipant {
	if j == nil || j.info.FakeJob || !j.IsConnected() || j.room == nil {
		return nil
	}
	return j.room.LocalParticipant
}

func (j *JobContext[T]) IsConnected() bool {
	if j == nil {
		return false
	}
	j.connectMu.Lock()
	connected := j.connected
	j.connectMu.Unlock()
	return connected
}

// Connect joins the assigned room. It is idempotent, safe for concurrent use,
// and applies audio/video-only subscription rules to existing and future tracks.
func (j *JobContext[T]) Connect(ctx context.Context, opts ConnectOptions) error {
	if j == nil {
		return errors.New("agents: nil job context")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if opts.AutoSubscribe > SubscribeAudioOnly {
		return fmt.Errorf("agents: invalid auto-subscribe value %d", opts.AutoSubscribe)
	}

	j.connectMu.Lock()
	if j.connected {
		j.connectMu.Unlock()
		return nil
	}
	if j.connectInFlight {
		done := j.connectDone
		j.connectMu.Unlock()
		select {
		case <-done:
			j.connectMu.Lock()
			err := j.connectErr
			connected := j.connected
			j.connectMu.Unlock()
			if connected {
				return nil
			}
			return err
		case <-ctx.Done():
			return ctx.Err()
		case <-j.ctx.Done():
			return context.Cause(j.ctx)
		}
	}
	if err := ctx.Err(); err != nil {
		j.connectMu.Unlock()
		return err
	}
	if err := j.ctx.Err(); err != nil {
		j.connectMu.Unlock()
		return context.Cause(j.ctx)
	}
	j.connectInFlight = true
	j.connectDone = make(chan struct{})
	j.connectErr = nil
	j.autoSubscribe = opts.AutoSubscribe
	j.connectMu.Unlock()

	complete := func(err error) error {
		j.connectMu.Lock()
		j.connectErr = err
		j.connected = err == nil
		j.connectInFlight = false
		close(j.connectDone)
		j.connectMu.Unlock()
		return err
	}
	if j.info.FakeJob {
		complete(nil)
		if j.onConnect != nil {
			j.onConnect()
		}
		return nil
	}
	if j.info.URL == "" {
		return complete(errors.New("agents: assigned room URL is empty"))
	}
	if j.info.Token.Reveal() == "" {
		return complete(errors.New("agents: assigned room token is empty"))
	}

	roomOpts := make([]lksdk.ConnectOption, 0, len(opts.RoomOptions)+1)
	roomOpts = append(roomOpts, opts.RoomOptions...)
	roomOpts = append(roomOpts, lksdk.WithAutoSubscribe(opts.AutoSubscribe == SubscribeAll))
	if err := j.room.JoinWithContextAndToken(ctx, j.info.URL, j.info.Token.Reveal(), roomOpts...); err != nil {
		return complete(fmt.Errorf("agents: connect to room: %w", err))
	}
	complete(nil)
	if j.onConnect != nil {
		j.onConnect()
	}
	for _, participant := range j.room.GetRemoteParticipants() {
		j.onParticipantConnected(participant)
		j.subscribeMatchingTracks(participant)
	}
	return nil
}

// WaitForParticipant waits for a non-agent remote participant. If kinds is
// empty, every participant kind except agent is accepted.
func (j *JobContext[T]) WaitForParticipant(ctx context.Context, identity string, kinds ...lksdk.ParticipantKind) (*lksdk.RemoteParticipant, error) {
	if j == nil || !j.IsConnected() {
		return nil, ErrRoomNotConnected
	}
	if ctx == nil {
		ctx = context.Background()
	}
	match := func(p *lksdk.RemoteParticipant) bool {
		if p == nil || p.Kind() == lksdk.ParticipantAgent {
			return false
		}
		return (identity == "" || p.Identity() == identity) && (len(kinds) == 0 || slices.Contains(kinds, p.Kind()))
	}
	for _, participant := range j.room.GetRemoteParticipants() {
		if match(participant) {
			return participant, nil
		}
	}

	// A temporary callback is registered in the same ordered list used by the
	// room callback, avoiding a check-then-subscribe race.
	found := make(chan *lksdk.RemoteParticipant, 1)
	callback := ParticipantEntrypoint[T](func(_ context.Context, _ *JobContext[T], p *lksdk.RemoteParticipant) error {
		if match(p) {
			select {
			case found <- p:
			default:
			}
		}
		return nil
	})
	registrationID, err := j.addParticipantCallback(callback)
	if err != nil {
		return nil, err
	}
	defer j.removeParticipantCallback(registrationID)
	// Close the race with a participant that arrived just before registration.
	for _, participant := range j.room.GetRemoteParticipants() {
		if match(participant) {
			return participant, nil
		}
	}
	select {
	case participant := <-found:
		return participant, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-j.ctx.Done():
		return nil, fmt.Errorf("agents: room disconnected while waiting for participant: %w", context.Cause(j.ctx))
	}
}

// AddParticipantEntrypoint registers a bounded, asynchronous participant hook.
// The same function cannot be added twice.
func (j *JobContext[T]) AddParticipantEntrypoint(callback ParticipantEntrypoint[T]) error {
	if callback == nil {
		return errors.New("agents: nil participant entrypoint")
	}
	if _, err := j.addParticipantCallback(callback); err != nil {
		return err
	}
	if j.IsConnected() {
		for _, participant := range j.room.GetRemoteParticipants() {
			j.enqueueParticipantCallback(callback, participant)
		}
	}
	return nil
}

func (j *JobContext[T]) addParticipantCallback(callback ParticipantEntrypoint[T]) (uint64, error) {
	pointer := reflect.ValueOf(callback).Pointer()
	j.callbackMu.Lock()
	defer j.callbackMu.Unlock()
	for _, existing := range j.participantCallbacks {
		if reflect.ValueOf(existing.callback).Pointer() == pointer {
			return 0, &FunctionExistsError{Message: "agents: participant entrypoints cannot be added more than once"}
		}
	}
	j.nextParticipantCallbackID++
	registrationID := j.nextParticipantCallbackID
	j.participantCallbacks = append(j.participantCallbacks, participantCallbackRegistration[T]{id: registrationID, callback: callback})
	return registrationID, nil
}

func (j *JobContext[T]) removeParticipantCallback(registrationID uint64) {
	j.callbackMu.Lock()
	for index, existing := range j.participantCallbacks {
		if existing.id == registrationID {
			j.participantCallbacks = slices.Delete(j.participantCallbacks, index, index+1)
			break
		}
	}
	j.callbackMu.Unlock()
}

func (j *JobContext[T]) AddShutdownCallback(callback ShutdownCallback) error {
	if j == nil || callback == nil {
		return errors.New("agents: nil shutdown callback")
	}
	j.callbackMu.Lock()
	defer j.callbackMu.Unlock()
	if j.ctx.Err() != nil {
		return errors.New("agents: job is already shutting down")
	}
	j.shutdownCallbacks = append(j.shutdownCallbacks, callback)
	return nil
}

// Shutdown requests graceful job termination. Calling it more than once is a
// no-op and preserves the first reason.
func (j *JobContext[T]) Shutdown(reason string) {
	if j == nil {
		return
	}
	j.shutdownOnce.Do(func() {
		j.shutdownMu.Lock()
		j.shutdownReason = reason
		j.shutdownMu.Unlock()
		cause := errors.New("job shutdown requested")
		if reason != "" {
			cause = fmt.Errorf("job shutdown requested: %s", reason)
		}
		j.cancel(cause)
	})
}

func (j *JobContext[T]) Done() <-chan struct{} { return j.ctx.Done() }
func (j *JobContext[T]) ShutdownReason() string {
	j.shutdownMu.RLock()
	reason := j.shutdownReason
	j.shutdownMu.RUnlock()
	return reason
}

// DeleteRoom deletes the assigned room, or name when provided. Fake jobs are
// deliberately a no-op.
func (j *JobContext[T]) DeleteRoom(ctx context.Context, name string) error {
	if j == nil {
		return errors.New("agents: nil job context")
	}
	if j.info.FakeJob {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if name == "" && j.room != nil {
		name = j.room.Name()
	}
	if name == "" && j.info.Job != nil && j.info.Job.Room != nil {
		name = j.info.Job.Room.Name
	}
	if name == "" {
		return errors.New("agents: room name is missing")
	}
	if j.info.APIKey.Reveal() == "" {
		return &MissingCredentialsError{Name: "LIVEKIT_API_KEY"}
	}
	if j.info.APISecret.Reveal() == "" {
		return &MissingCredentialsError{Name: "LIVEKIT_API_SECRET"}
	}
	client := lksdk.NewRoomServiceClient(j.info.URL, j.info.APIKey.Reveal(), j.info.APISecret.Reveal())
	if _, err := client.DeleteRoom(ctx, &livekit.DeleteRoomRequest{Room: name}); err != nil {
		return fmt.Errorf("agents: delete room %q: %w", name, err)
	}
	return nil
}

func (j *JobContext[T]) onParticipantConnected(participant *lksdk.RemoteParticipant) {
	if participant == nil || !j.IsConnected() {
		return
	}
	j.callbackMu.RLock()
	callbacks := append([]participantCallbackRegistration[T](nil), j.participantCallbacks...)
	j.callbackMu.RUnlock()
	for _, registration := range callbacks {
		j.enqueueParticipantCallback(registration.callback, participant)
	}
	if j.IsConnected() {
		j.subscribeMatchingTracks(participant)
	}
}

func (j *JobContext[T]) onParticipantDisconnected(participant *lksdk.RemoteParticipant) {
	if participant == nil {
		return
	}
	if _, ok := participant.Attributes()[AttributeSimulator]; ok {
		j.Shutdown("simulation completed")
	}
}

func (j *JobContext[T]) onTrackPublished(publication *lksdk.RemoteTrackPublication, _ *lksdk.RemoteParticipant) {
	if publication == nil || !j.IsConnected() {
		return
	}
	if j.trackMatches(publication.Kind()) {
		if err := publication.SetSubscribed(true); err != nil {
			slog.Warn("failed to subscribe to participant track", "job_id", j.info.Job.GetId(), "error", err)
		}
	}
}

func (j *JobContext[T]) subscribeMatchingTracks(participant *lksdk.RemoteParticipant) {
	if participant == nil || (j.autoSubscribe != SubscribeAudioOnly && j.autoSubscribe != SubscribeVideoOnly) {
		return
	}
	for _, publication := range participant.TrackPublications() {
		remote, ok := publication.(*lksdk.RemoteTrackPublication)
		if ok && j.trackMatches(remote.Kind()) {
			if err := remote.SetSubscribed(true); err != nil {
				slog.Warn("failed to subscribe to participant track", "job_id", j.info.Job.GetId(), "error", err)
			}
		}
	}
}

func (j *JobContext[T]) trackMatches(kind lksdk.TrackKind) bool {
	return (j.autoSubscribe == SubscribeAudioOnly && kind == lksdk.TrackKindAudio) ||
		(j.autoSubscribe == SubscribeVideoOnly && kind == lksdk.TrackKindVideo)
}

func (j *JobContext[T]) enqueueParticipantCallback(callback ParticipantEntrypoint[T], participant *lksdk.RemoteParticipant) {
	select {
	case j.participantQueue <- participantTask[T]{ctx: j, callback: callback, participant: participant}:
	case <-j.ctx.Done():
	default:
		slog.Warn("participant callback queue is full; dropping callback", "job_id", j.info.Job.GetId(), "participant", participant.Identity())
	}
}

func (j *JobContext[T]) runParticipantCallbacks() {
	defer j.participantWorkers.Done()
	for {
		select {
		case task := <-j.participantQueue:
			func() {
				defer func() {
					if recovered := recover(); recovered != nil {
						slog.Error("participant entrypoint panicked", "job_id", j.info.Job.GetId(), "panic", recovered)
					}
				}()
				if err := task.callback(j.ctx, task.ctx, task.participant); err != nil && !errors.Is(err, context.Canceled) {
					slog.Error("participant entrypoint failed", "job_id", j.info.Job.GetId(), "error", err)
				}
			}()
		case <-j.ctx.Done():
			return
		}
	}
}

func (j *JobContext[T]) finish(ctx context.Context) error {
	if j == nil {
		return nil
	}
	if j.ctx.Err() == nil {
		j.Shutdown("")
	}
	sessionErr := j.finishPrimarySession(ctx)
	if j.room != nil && !j.info.FakeJob {
		j.room.Disconnect()
	}
	if j.rtcBridge != nil {
		_ = j.rtcBridge.Close()
	}
	j.callbackMu.RLock()
	callbacks := append([]ShutdownCallback(nil), j.shutdownCallbacks...)
	j.callbackMu.RUnlock()

	errorsOut := make(chan error, len(callbacks))
	callbackQueue := make(chan ShutdownCallback, len(callbacks))
	for _, callback := range callbacks {
		callbackQueue <- callback
	}
	close(callbackQueue)
	var callbackWorkers sync.WaitGroup
	for range min(len(callbacks), participantCallbackWorkers) {
		callbackWorkers.Add(1)
		go func() {
			defer callbackWorkers.Done()
			for callback := range callbackQueue {
				func() {
					defer func() {
						if recovered := recover(); recovered != nil {
							errorsOut <- fmt.Errorf("shutdown callback panicked: %v", recovered)
						}
					}()
					if err := callback(ctx, j.ShutdownReason()); err != nil {
						errorsOut <- err
					}
				}()
			}
		}()
	}
	done := make(chan struct{})
	go func() {
		callbackWorkers.Wait()
		j.participantWorkers.Wait()
		close(done)
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
	}
	close(errorsOut)
	callbackErrors := make([]error, 0, len(callbacks)+1)
	if sessionErr != nil {
		callbackErrors = append(callbackErrors, sessionErr)
	}
	for err := range errorsOut {
		callbackErrors = append(callbackErrors, err)
	}
	return errors.Join(callbackErrors...)
}

type jobContextKey struct{}
type inferenceExecutorContextKey struct{}

func contextWithJob[T any](ctx context.Context, job *JobContext[T]) context.Context {
	ctx = context.WithValue(ctx, jobContextKey{}, job)
	return context.WithValue(ctx, inferenceExecutorContextKey{}, job.InferenceExecutor())
}

// JobFromContext returns the typed job explicitly propagated by the executor.
func JobFromContext[T any](ctx context.Context) (*JobContext[T], bool) {
	if ctx == nil {
		return nil, false
	}
	job, ok := ctx.Value(jobContextKey{}).(*JobContext[T])
	return job, ok
}

// InferenceExecutorFromContext returns the worker-global executor propagated to
// a job entrypoint without requiring the caller to know JobContext's user-data
// type. Model adapters use this to share native runners across isolated jobs.
func InferenceExecutorFromContext(ctx context.Context) (ipc.InferenceExecutor, bool) {
	if ctx == nil {
		return nil, false
	}
	executor, ok := ctx.Value(inferenceExecutorContextKey{}).(ipc.InferenceExecutor)
	return executor, ok && !isNilInferenceExecutor(executor)
}

// WithInferenceExecutor explicitly propagates an executor to model/session
// constructors outside a worker entrypoint. It is useful for console hosts and
// tests; a typed nil executor leaves the context unchanged.
func WithInferenceExecutor(ctx context.Context, executor ipc.InferenceExecutor) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if isNilInferenceExecutor(executor) {
		return ctx
	}
	return context.WithValue(ctx, inferenceExecutorContextKey{}, executor)
}

// JobRequest is the immutable view given to the request handler. Exactly one
// Accept or Reject call succeeds.
type JobRequest struct {
	job      *livekit.Job
	resuming bool

	mu       sync.Mutex
	answered bool
	accept   func(context.Context, JobAcceptOptions) error
	reject   func(context.Context, JobRejectOptions) error
}

func newJobRequest(job *livekit.Job, resuming bool, accept func(context.Context, JobAcceptOptions) error, reject func(context.Context, JobRejectOptions) error) *JobRequest {
	if job != nil {
		job = proto.Clone(job).(*livekit.Job)
	}
	return &JobRequest{job: job, resuming: resuming, accept: accept, reject: reject}
}

func (r *JobRequest) ID() string {
	if r == nil || r.job == nil {
		return ""
	}
	return r.job.Id
}

func (r *JobRequest) Job() *livekit.Job {
	if r == nil || r.job == nil {
		return nil
	}
	return proto.Clone(r.job).(*livekit.Job)
}

func (r *JobRequest) Room() *livekit.Room {
	if r == nil || r.job == nil || r.job.Room == nil {
		return nil
	}
	return proto.Clone(r.job.Room).(*livekit.Room)
}

func (r *JobRequest) Publisher() *livekit.ParticipantInfo {
	if r == nil || r.job == nil || r.job.Participant == nil {
		return nil
	}
	return proto.Clone(r.job.Participant).(*livekit.ParticipantInfo)
}

func (r *JobRequest) AgentName() string {
	if r == nil || r.job == nil {
		return ""
	}
	return r.job.AgentName
}

func (r *JobRequest) Resuming() bool { return r != nil && r.resuming }

func (r *JobRequest) Answered() bool {
	if r == nil {
		return false
	}
	r.mu.Lock()
	answered := r.answered
	r.mu.Unlock()
	return answered
}

func (r *JobRequest) Accept(ctx context.Context, opts JobAcceptOptions) error {
	if r == nil || r.accept == nil {
		return errors.New("agents: invalid job request")
	}
	if opts.Identity == "" {
		opts.Identity = "agent-" + r.ID()
	}
	if ctx == nil {
		ctx = context.Background()
	}
	opts.Attributes = cloneStrings(opts.Attributes)
	if err := r.claim(); err != nil {
		return err
	}
	if err := r.accept(ctx, opts); err != nil {
		return err
	}
	return nil
}

func (r *JobRequest) Reject(ctx context.Context, opts JobRejectOptions) error {
	if r == nil || r.reject == nil {
		return errors.New("agents: invalid job request")
	}
	if err := r.claim(); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return r.reject(ctx, opts)
}

func (r *JobRequest) claim() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.answered {
		return ErrJobRequestAnswered
	}
	r.answered = true
	return nil
}

func cloneStrings(source map[string]string) map[string]string {
	if source == nil {
		return nil
	}
	clone := make(map[string]string, len(source))
	for key, value := range source {
		clone[key] = value
	}
	return clone
}
