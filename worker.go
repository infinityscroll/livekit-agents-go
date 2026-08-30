// SPDX-License-Identifier: Apache-2.0

package agents

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"os"
	"os/signal"
	"reflect"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/infinityscroll/livekit-agents-go/internal/workerprotocol"
	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"
	lksdk "github.com/livekit/server-sdk-go/v2"
	"google.golang.org/protobuf/proto"
)

const (
	DefaultMaxReconnects          = 10
	DefaultAssignmentTimeout      = 7500 * time.Millisecond
	DefaultStatusUpdateInterval   = 2500 * time.Millisecond
	DefaultDrainTimeout           = time.Hour
	DefaultShutdownProcessTimeout = time.Minute
	DefaultInitializeTimeout      = 10 * time.Second
	DefaultJobMemoryWarnMB        = 1000
	DefaultWorkerPort             = 8081
	workerOutgoingQueueSize       = 128
	workerAvailabilityConcurrency = 64
	AttributeAgentName            = "lk.agent.name"
)

type ServerType = livekit.JobType

const (
	ServerTypeRoom      = livekit.JobType_JT_ROOM
	ServerTypePublisher = livekit.JobType_JT_PUBLISHER
)

// WorkerPermissions are applied to every agent participant created by this
// worker. Use DefaultWorkerPermissions when modifying individual fields.
type WorkerPermissions struct {
	CanPublish            bool
	CanSubscribe          bool
	CanPublishData        bool
	CanUpdateMetadata     bool
	CanPublishSources     []livekit.TrackSource
	Hidden                bool
	CanSubscribeMetrics   bool
	CanManageAgentSession bool
}

func DefaultWorkerPermissions() WorkerPermissions {
	return WorkerPermissions{
		CanPublish:        true,
		CanSubscribe:      true,
		CanPublishData:    true,
		CanUpdateMetadata: true,
	}
}

func (p WorkerPermissions) protocol() *livekit.ParticipantPermission {
	permission := &livekit.ParticipantPermission{
		CanPublish:            p.CanPublish,
		CanSubscribe:          p.CanSubscribe,
		CanPublishData:        p.CanPublishData,
		CanUpdateMetadata:     p.CanUpdateMetadata,
		CanPublishSources:     slices.Clone(p.CanPublishSources),
		Hidden:                p.Hidden,
		CanSubscribeMetrics:   p.CanSubscribeMetrics,
		CanManageAgentSession: p.CanManageAgentSession,
	}
	// RegisterWorkerRequest has no ParticipantInfo.Kind field. The deprecated
	// permission bit remains the only wire-compatible way to mark the worker's
	// participants as agents for servers that consume this registration field.
	//lint:ignore SA1019 required for worker registration protocol compatibility
	permission.Agent = true
	return permission
}

type LoadFunc[T any] func(context.Context, *AgentServer[T]) (float64, error)

// ServerOptions configures the worker runtime. Zero-valued optional fields use
// the same production/development defaults as the Python and TypeScript SDKs.
type ServerOptions[T any] struct {
	// Agent is the defineAgent-compatible immutable module. Direct entrypoint
	// fields remain supported for idiomatic Go construction.
	Agent         *AgentDefinition[T]
	JobEntrypoint JobEntrypoint[T]
	// Entrypoint is the legacy compatibility name. JobEntrypoint is canonical.
	Entrypoint     JobEntrypoint[T]
	RequestHandler RequestHandler
	// RequestFunc is the TypeScript-compatible alias for RequestHandler.
	RequestFunc     RequestHandler
	Prewarm         PrewarmFunc[T]
	OnSimulationEnd SimulationEndFunc[T]

	AgentName      string
	AgentNameIsEnv bool
	ServerType     ServerType
	Deployment     string

	ExecutorMode      ExecutorMode
	MaxConcurrentJobs int
	NumIdleProcesses  int

	LoadFunc      LoadFunc[T]
	LoadThreshold float64
	Production    bool
	Simulation    bool

	DrainTimeout             time.Duration
	ShutdownProcessTimeout   time.Duration
	InitializeProcessTimeout time.Duration
	AssignmentTimeout        time.Duration
	StatusUpdateInterval     time.Duration
	MaxReconnects            int
	MaxReconnectsSet         bool

	Permissions    WorkerPermissions
	PermissionsSet bool

	URL         string
	WSURL       string
	APIKey      SecretString
	APISecret   SecretString
	WorkerToken SecretString

	Host string
	Port int

	JobMemoryWarnMB    int
	JobMemoryWarnMBSet bool
	JobMemoryLimitMB   int
	Logger             *slog.Logger
}

// WorkerOptions is retained as a source-compatible alias.
type WorkerOptions[T any] = ServerOptions[T]

type WorkerEventType string

const (
	WorkerEventRegistered WorkerEventType = "worker_registered"
	WorkerEventMessage    WorkerEventType = "worker_msg"
	WorkerEventClosed     WorkerEventType = "worker_closed"
)

type WorkerEvent struct {
	Type       WorkerEventType
	WorkerID   string
	ServerInfo *livekit.ServerInfo
	Message    *livekit.WorkerMessage
	Error      error
}

type outboundMessage struct {
	generation uint64
	message    *livekit.WorkerMessage
}

type pendingAssignment struct {
	generation uint64
	result     chan *livekit.JobAssignment
}

// AgentServer owns worker registration, job reservations, executors, health
// serving, reconnection, and graceful draining.
type AgentServer[T any] struct {
	opts ServerOptions[T]

	logger *slog.Logger
	dialer workerprotocol.Dialer
	pool   executorPool[T]
	http   *workerHTTPServer
	cpu    CPUMonitor

	events EventEmitter[WorkerEvent]

	stateMu    sync.RWMutex
	started    bool
	closed     bool
	draining   bool
	connecting bool
	registered bool
	workerID   string
	generation uint64
	transport  workerprotocol.Transport
	runCtx     context.Context
	cancelRun  context.CancelCauseFunc

	outgoing           chan outboundMessage
	pending            map[string]pendingAssignment
	deferredJobUpdates map[string]*livekit.WorkerMessage

	availabilitySlots  chan struct{}
	loadSlot           chan struct{}
	taskMu             sync.Mutex
	acceptingTasks     bool
	tasks              sync.WaitGroup
	closeOnce          sync.Once
	closedCh           chan struct{}
	closeErr           error
	closedEventEmitted atomic.Bool

	reconnectDelay       func(int) time.Duration
	successfulGeneration atomic.Uint64
}

// Worker is the deprecated name for AgentServer.
type Worker[T any] = AgentServer[T]

func NewAgentServer[T any](options ServerOptions[T]) (*AgentServer[T], error) {
	opts, err := normalizeServerOptions(options)
	if err != nil {
		return nil, err
	}
	server := &AgentServer[T]{
		opts:               opts,
		logger:             opts.Logger,
		dialer:             workerprotocol.GorillaDialer{},
		cpu:                GetCPUMonitor(),
		outgoing:           make(chan outboundMessage, workerOutgoingQueueSize),
		pending:            make(map[string]pendingAssignment),
		deferredJobUpdates: make(map[string]*livekit.WorkerMessage),
		availabilitySlots:  make(chan struct{}, min(opts.MaxConcurrentJobs, workerAvailabilityConcurrency)),
		loadSlot:           make(chan struct{}, 1),
		acceptingTasks:     true,
		closedCh:           make(chan struct{}),
		reconnectDelay: func(attempt int) time.Duration {
			return min(time.Duration(attempt)*2*time.Second, 10*time.Second)
		},
	}
	server.pool = newExecutorPool(opts, server.publishJobStatus)
	if !opts.Simulation {
		server.http = newWorkerHTTPServer(opts.Host, opts.Port, server.healthSnapshot, server.workerSnapshot)
	}
	return server, nil
}

// NewWorker is the deprecated constructor name for NewAgentServer.
func NewWorker[T any](options WorkerOptions[T]) (*Worker[T], error) {
	return NewAgentServer(ServerOptions[T](options))
}

func normalizeServerOptions[T any](opts ServerOptions[T]) (ServerOptions[T], error) {
	if opts.Agent != nil {
		if !IsAgent(opts.Agent) {
			return opts, errors.New("agents: invalid agent definition")
		}
		definitionEntrypoint := opts.Agent.Entrypoint()
		if opts.JobEntrypoint != nil && reflectFunctionPointer(opts.JobEntrypoint) != reflectFunctionPointer(definitionEntrypoint) {
			return opts, errors.New("agents: Agent and JobEntrypoint cannot specify different functions")
		}
		if opts.Entrypoint != nil && reflectFunctionPointer(opts.Entrypoint) != reflectFunctionPointer(definitionEntrypoint) {
			return opts, errors.New("agents: Agent and Entrypoint cannot specify different functions")
		}
		if opts.Prewarm != nil && opts.Agent.Prewarm() != nil && reflectFunctionPointer(opts.Prewarm) != reflectFunctionPointer(opts.Agent.Prewarm()) {
			return opts, errors.New("agents: Agent and Prewarm cannot specify different functions")
		}
		if opts.OnSimulationEnd != nil && opts.Agent.SimulationEnd() != nil && reflectFunctionPointer(opts.OnSimulationEnd) != reflectFunctionPointer(opts.Agent.SimulationEnd()) {
			return opts, errors.New("agents: Agent and OnSimulationEnd cannot specify different functions")
		}
		opts.JobEntrypoint = definitionEntrypoint
		opts.Entrypoint = definitionEntrypoint
		if opts.Prewarm == nil {
			opts.Prewarm = opts.Agent.Prewarm()
		}
		if opts.OnSimulationEnd == nil {
			opts.OnSimulationEnd = opts.Agent.SimulationEnd()
		}
	}
	if opts.JobEntrypoint == nil {
		opts.JobEntrypoint = opts.Entrypoint
	}
	if opts.JobEntrypoint != nil && opts.Entrypoint != nil && reflectFunctionPointer(opts.Entrypoint) != reflectFunctionPointer(opts.JobEntrypoint) {
		return opts, errors.New("agents: JobEntrypoint and Entrypoint cannot specify different functions")
	}
	if opts.RequestHandler == nil {
		opts.RequestHandler = opts.RequestFunc
	}
	if opts.RequestHandler == nil {
		opts.RequestHandler = func(ctx context.Context, request *JobRequest) error {
			return request.Accept(ctx, JobAcceptOptions{})
		}
	}

	if override := os.Getenv("LIVEKIT_AGENT_NAME_OVERRIDE"); override != "" {
		opts.AgentName = override
		opts.AgentNameIsEnv = true
	} else if opts.AgentName == "" {
		opts.AgentName = os.Getenv("LIVEKIT_AGENT_NAME")
		opts.AgentNameIsEnv = opts.AgentName != ""
	}
	if opts.Deployment == "" {
		opts.Deployment = os.Getenv("LIVEKIT_AGENT_DEPLOYMENT")
	}
	if opts.URL == "" {
		opts.URL = opts.WSURL
	}
	if opts.URL == "" {
		opts.URL = os.Getenv("LIVEKIT_URL")
	}
	if opts.URL == "" {
		opts.URL = "ws://localhost:7880"
	}
	if opts.APIKey.Reveal() == "" {
		opts.APIKey = NewSecretString(os.Getenv("LIVEKIT_API_KEY"))
	}
	if opts.APISecret.Reveal() == "" {
		opts.APISecret = NewSecretString(os.Getenv("LIVEKIT_API_SECRET"))
	}
	if opts.WorkerToken.Reveal() == "" {
		opts.WorkerToken = NewSecretString(os.Getenv("LIVEKIT_WORKER_TOKEN"))
	}
	if opts.APIKey.Reveal() == "" {
		return opts, &MissingCredentialsError{Name: "LIVEKIT_API_KEY"}
	}
	if opts.APISecret.Reveal() == "" {
		return opts, &MissingCredentialsError{Name: "LIVEKIT_API_SECRET"}
	}
	if _, err := workerprotocol.AgentEndpoint(opts.URL, opts.WorkerToken.Reveal()); err != nil {
		return opts, err
	}

	if opts.ServerType != livekit.JobType_JT_ROOM && opts.ServerType != livekit.JobType_JT_PUBLISHER {
		return opts, fmt.Errorf("agents: unsupported server type %s", opts.ServerType)
	}
	if opts.ExecutorMode != ExecutorModeProcess && opts.ExecutorMode != ExecutorModeInProcess {
		return opts, fmt.Errorf("agents: unsupported executor mode %d", opts.ExecutorMode)
	}
	if opts.MaxConcurrentJobs <= 0 {
		opts.MaxConcurrentJobs = max(4, 2*runtime.GOMAXPROCS(0))
	}
	if opts.NumIdleProcesses == 0 && opts.Production {
		opts.NumIdleProcesses = min(runtime.GOMAXPROCS(0), 4)
	}
	if opts.NumIdleProcesses < 0 || opts.NumIdleProcesses > opts.MaxConcurrentJobs {
		return opts, errors.New("agents: NumIdleProcesses must be between zero and MaxConcurrentJobs")
	}
	if opts.LoadThreshold == 0 {
		if opts.Production {
			opts.LoadThreshold = 0.7
		} else {
			opts.LoadThreshold = math.Inf(1)
		}
	}
	if opts.Simulation {
		opts.LoadThreshold = math.Inf(1)
	}
	if math.IsNaN(opts.LoadThreshold) || opts.LoadThreshold < 0 {
		return opts, errors.New("agents: LoadThreshold cannot be negative")
	}
	if opts.DrainTimeout == 0 {
		opts.DrainTimeout = DefaultDrainTimeout
	}
	if opts.ShutdownProcessTimeout == 0 {
		opts.ShutdownProcessTimeout = DefaultShutdownProcessTimeout
	}
	if opts.InitializeProcessTimeout == 0 {
		opts.InitializeProcessTimeout = DefaultInitializeTimeout
	}
	if opts.AssignmentTimeout == 0 {
		opts.AssignmentTimeout = DefaultAssignmentTimeout
	}
	if opts.StatusUpdateInterval == 0 {
		opts.StatusUpdateInterval = DefaultStatusUpdateInterval
	}
	if opts.DrainTimeout < 0 || opts.ShutdownProcessTimeout < 0 || opts.InitializeProcessTimeout < 0 || opts.AssignmentTimeout < 0 || opts.StatusUpdateInterval < 0 {
		return opts, errors.New("agents: timeouts and intervals cannot be negative")
	}
	if opts.MaxReconnects == 0 && !opts.MaxReconnectsSet {
		opts.MaxReconnects = DefaultMaxReconnects
	}
	if opts.MaxReconnects < 0 {
		return opts, errors.New("agents: MaxReconnects cannot be negative")
	}
	if !opts.PermissionsSet {
		opts.Permissions = DefaultWorkerPermissions()
	}
	if opts.Host == "" {
		opts.Host = "0.0.0.0"
	}
	if opts.Port == 0 && opts.Production {
		opts.Port = DefaultWorkerPort
	}
	if opts.Port < 0 || opts.Port > 65535 {
		return opts, errors.New("agents: Port must be between zero and 65535")
	}
	if opts.JobMemoryWarnMB == 0 && !opts.JobMemoryWarnMBSet {
		opts.JobMemoryWarnMB = DefaultJobMemoryWarnMB
	}
	if opts.JobMemoryWarnMB < 0 || opts.JobMemoryLimitMB < 0 {
		return opts, errors.New("agents: job memory limits cannot be negative")
	}
	if opts.JobMemoryLimitMB > 0 && opts.JobMemoryWarnMB > opts.JobMemoryLimitMB {
		opts.JobMemoryWarnMB = opts.JobMemoryLimitMB
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.WorkerToken.Reveal() != "" {
		if opts.LoadFunc != nil {
			opts.Logger.Warn("custom LoadFunc is not supported with a Cloud worker token; using executor saturation")
			opts.LoadFunc = nil
		}
		cloudThreshold := math.Inf(1)
		if opts.Production {
			cloudThreshold = 0.7
		}
		if opts.LoadThreshold != cloudThreshold {
			opts.Logger.Warn("custom LoadThreshold is not supported with a Cloud worker token; using the default", "load_threshold", cloudThreshold)
			opts.LoadThreshold = cloudThreshold
		}
	}
	return opts, nil
}

func reflectFunctionPointer(fn any) uintptr {
	return reflect.ValueOf(fn).Pointer()
}

// RegisterAgent mirrors Python's pre-run AgentServer registration style.
func (s *AgentServer[T]) RegisterAgent(name string, entrypoint JobEntrypoint[T]) error {
	if entrypoint == nil {
		return errors.New("agents: nil job entrypoint")
	}
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	if s.started {
		return errors.New("agents: cannot register an agent after Run")
	}
	if s.closed {
		return errors.New("agents: cannot register an agent on a closed server")
	}
	if s.opts.JobEntrypoint != nil && reflectFunctionPointer(s.opts.JobEntrypoint) != reflectFunctionPointer(entrypoint) {
		return errors.New("agents: a job entrypoint is already registered")
	}
	s.opts.AgentName = name
	s.opts.JobEntrypoint = entrypoint
	s.opts.Entrypoint = entrypoint
	s.pool = newExecutorPool(s.opts, s.publishJobStatus)
	return nil
}

// RegisterRTCSession is the Python-compatible name for RegisterAgent.
func (s *AgentServer[T]) RegisterRTCSession(name string, entrypoint JobEntrypoint[T]) error {
	return s.RegisterAgent(name, entrypoint)
}

func (s *AgentServer[T]) Events() *EventEmitter[WorkerEvent] { return &s.events }

func (s *AgentServer[T]) ID() string {
	s.stateMu.RLock()
	id := s.workerID
	s.stateMu.RUnlock()
	if id == "" {
		return "unregistered"
	}
	return id
}

func (s *AgentServer[T]) ActiveJobs() []RunningJobInfo { return s.pool.ActiveJobs() }

// HTTPAddress returns the bound health endpoint after Run has started it.
func (s *AgentServer[T]) HTTPAddress() string {
	if s.http == nil {
		return ""
	}
	return s.http.Address()
}

func (s *AgentServer[T]) Registered() bool {
	s.stateMu.RLock()
	registered := s.registered
	s.stateMu.RUnlock()
	return registered
}

func (s *AgentServer[T]) Draining() bool {
	s.stateMu.RLock()
	draining := s.draining
	s.stateMu.RUnlock()
	return draining
}

// Run blocks until the worker is closed, its context is canceled, or a fatal
// registration/reconnection error occurs. A canceled context triggers a drain.
func (s *AgentServer[T]) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if isInferenceChildProcess() {
		return runInferenceChildProcess(ctx)
	}
	if s.opts.JobEntrypoint == nil {
		return &WorkerError{Message: "no job entrypoint is registered"}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if isJobChildProcess() {
		return runJobChildProcess(ctx, s.opts)
	}

	s.stateMu.Lock()
	if s.started {
		s.stateMu.Unlock()
		return &WorkerError{Message: "worker is already running"}
	}
	if s.closed {
		s.stateMu.Unlock()
		return &WorkerError{Message: "worker is closed"}
	}
	s.started = true
	s.closed = false
	s.runCtx, s.cancelRun = context.WithCancelCause(context.Background())
	s.stateMu.Unlock()

	if err := s.pool.Start(s.runCtx); err != nil {
		_ = s.Close(context.Background())
		return &WorkerError{Message: "failed to start job executor pool", Cause: err}
	}
	if s.http != nil {
		if err := s.http.Start(); err != nil {
			_ = s.Close(context.Background())
			return &WorkerError{Message: "failed to start worker HTTP server", Cause: err}
		}
	}

	connectionDone := make(chan error, 1)
	go func() { connectionDone <- s.runConnections(s.runCtx) }()

	var runErr error
	select {
	case <-ctx.Done():
		drainCtx, cancel := context.WithTimeout(context.Background(), s.opts.DrainTimeout)
		runErr = s.Drain(drainCtx)
		cancel()
	case runErr = <-connectionDone:
	case <-s.closedCh:
	}
	closeCtx, cancelClose := context.WithTimeout(context.Background(), s.opts.ShutdownProcessTimeout)
	closeErr := s.Close(closeCtx)
	cancelClose()
	if runErr == nil || errors.Is(runErr, context.Canceled) {
		runErr = closeErr
	} else if closeErr != nil {
		runErr = errors.Join(runErr, closeErr)
	}
	return runErr
}

func (s *AgentServer[T]) runConnections(ctx context.Context) error {
	retries := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := s.runConnection(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		s.stateMu.RLock()
		generation := s.generation
		s.stateMu.RUnlock()
		if s.successfulGeneration.Load() == generation {
			retries = 0
		}
		if retries >= s.opts.MaxReconnects {
			return &WorkerError{Message: fmt.Sprintf("failed to connect to LiveKit server (%s) after %d attempts", s.opts.URL, retries), Cause: err}
		}
		retries++
		delay := s.reconnectDelay(retries)
		s.logger.Warn("worker connection failed; retrying", "error", err, "retry_count", retries, "max_retry", s.opts.MaxReconnects, "delay", delay)
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return ctx.Err()
		}
	}
}

func (s *AgentServer[T]) runConnection(ctx context.Context) (returnErr error) {
	endpoint, err := workerprotocol.AgentEndpoint(s.opts.URL, s.opts.WorkerToken.Reveal())
	if err != nil {
		return err
	}
	jwt, err := auth.NewAccessToken(s.opts.APIKey.Reveal(), s.opts.APISecret.Reveal()).SetAgentGrant(&auth.AgentGrant{}).ToJWT()
	if err != nil {
		return fmt.Errorf("agents: create worker token: %w", err)
	}

	s.stateMu.Lock()
	s.generation++
	generation := s.generation
	s.connecting = true
	s.registered = false
	s.stateMu.Unlock()

	header := make(http.Header, 1)
	header.Set("Authorization", "Bearer "+jwt)
	transport, err := s.dialer.Dial(ctx, endpoint, header)
	if err != nil {
		s.markDisconnected(generation)
		return err
	}
	defer func() {
		_ = transport.Close()
		s.markDisconnected(generation)
	}()

	register := &livekit.WorkerMessage{Message: &livekit.WorkerMessage_Register{Register: &livekit.RegisterWorkerRequest{
		Type:               s.opts.ServerType,
		AgentName:          s.opts.AgentName,
		Version:            Version,
		AllowedPermissions: s.opts.Permissions.protocol(),
		Deployment:         s.opts.Deployment,
	}}}
	if err := transport.Write(ctx, register); err != nil {
		return fmt.Errorf("agents: register worker: %w", err)
	}
	first, err := transport.Read(ctx)
	if err != nil {
		return fmt.Errorf("agents: read registration response: %w", err)
	}
	registration := first.GetRegister()
	if registration == nil {
		return &WorkerError{Message: "expected register response as first message"}
	}
	if registration.WorkerId == "" {
		return &WorkerError{Message: "register response did not include a worker ID"}
	}

	connectionCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	s.stateMu.Lock()
	s.workerID = registration.WorkerId
	s.connecting = false
	s.registered = true
	s.transport = transport
	s.stateMu.Unlock()
	s.successfulGeneration.Store(generation)
	s.emitEvent(WorkerEvent{Type: WorkerEventRegistered, WorkerID: registration.WorkerId, ServerInfo: cloneServerInfo(registration.ServerInfo)})
	s.logger.Info("registered worker", "worker_id", registration.WorkerId, "agent_name", s.opts.AgentName, "deployment", s.opts.Deployment)

	writerDone := make(chan error, 1)
	go func() { writerDone <- s.runWriter(connectionCtx, generation, transport) }()
	statusDone := make(chan struct{})
	go func() {
		s.runStatusUpdates(connectionCtx)
		close(statusDone)
	}()
	if err := s.flushDeferredJobUpdates(connectionCtx); err != nil {
		return err
	}
	if jobs := s.pool.ActiveJobs(); len(jobs) > 0 {
		ids := make([]string, 0, len(jobs))
		for _, job := range jobs {
			if job.Job != nil {
				ids = append(ids, job.Job.Id)
			}
		}
		if len(ids) > 0 {
			_ = s.send(connectionCtx, &livekit.WorkerMessage{Message: &livekit.WorkerMessage_MigrateJob{MigrateJob: &livekit.MigrateJobRequest{JobIds: ids}}})
		}
	}

	for {
		message, readErr := transport.Read(connectionCtx)
		if readErr != nil {
			cancel()
			<-statusDone
			select {
			case writerErr := <-writerDone:
				if writerErr != nil && !errors.Is(writerErr, context.Canceled) {
					return errors.Join(readErr, writerErr)
				}
			default:
			}
			return readErr
		}
		if err := s.handleServerMessage(connectionCtx, generation, message); err != nil {
			return err
		}
		select {
		case writerErr := <-writerDone:
			if writerErr != nil {
				return writerErr
			}
			return workerprotocol.ErrClosed
		default:
		}
	}
}

func (s *AgentServer[T]) runWriter(ctx context.Context, generation uint64, transport workerprotocol.Transport) error {
	for {
		select {
		case outbound := <-s.outgoing:
			if outbound.generation != generation {
				continue
			}
			if err := transport.Write(ctx, outbound.message); err != nil {
				_ = transport.Close()
				return err
			}
			s.acknowledgeJobUpdate(outbound.message)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *AgentServer[T]) runStatusUpdates(ctx context.Context) {
	ticker := time.NewTicker(s.opts.StatusUpdateInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			load, err := s.currentLoad(ctx)
			if err != nil {
				s.logger.Warn("failed to measure worker load", "error", err)
				continue
			}
			_ = s.send(ctx, s.statusMessage(load))
		case <-ctx.Done():
			return
		}
	}
}

func (s *AgentServer[T]) currentLoad(ctx context.Context) (load float64, err error) {
	if s.opts.LoadFunc != nil {
		select {
		case s.loadSlot <- struct{}{}:
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
			return 0, errors.New("agents: previous load function call is still running")
		}
		type loadResult struct {
			load float64
			err  error
		}
		result := make(chan loadResult, 1)
		go func() {
			defer func() {
				<-s.loadSlot
				if recovered := recover(); recovered != nil {
					result <- loadResult{err: fmt.Errorf("load function panicked: %v", recovered)}
				}
			}()
			value, callErr := s.opts.LoadFunc(ctx, s)
			result <- loadResult{load: value, err: callErr}
		}()
		select {
		case measured := <-result:
			load, err = measured.load, measured.err
			if err != nil {
				return 0, err
			}
		case <-ctx.Done():
			return 0, ctx.Err()
		}
		if math.IsNaN(load) || math.IsInf(load, 0) {
			return 0, errors.New("agents: load function returned a non-finite value")
		}
		return min(max(0, load), 1), nil
	}
	// Match the TypeScript and Python production default: admission load is
	// actual CPU utilization, including the container quota when cgroups are
	// available. The monitor samples synchronously with ctx, so this path does
	// not allocate a helper goroutine or leave work behind after cancellation.
	if s.cpu == nil {
		return 0, ErrCPUUsageUnavailable
	}
	return s.cpu.CPUPercent(ctx, DefaultCPUSampleInterval)
}

func (s *AgentServer[T]) statusMessage(load float64) *livekit.WorkerMessage {
	status := livekit.WorkerStatus_WS_AVAILABLE
	if s.Draining() || load >= s.opts.LoadThreshold {
		status = livekit.WorkerStatus_WS_FULL
	}
	jobCount := len(s.pool.ActiveJobs())
	return &livekit.WorkerMessage{Message: &livekit.WorkerMessage_UpdateWorker{UpdateWorker: &livekit.UpdateWorkerStatus{
		Status:   &status,
		Load:     float32(min(load, math.MaxFloat32)),
		JobCount: uint32(jobCount),
	}}}
}

func (s *AgentServer[T]) handleServerMessage(ctx context.Context, generation uint64, message *livekit.ServerMessage) error {
	if message == nil {
		return errors.New("agents: received nil server message")
	}
	switch value := message.Message.(type) {
	case *livekit.ServerMessage_Register:
		return &WorkerError{Message: "register response is only valid as the first message"}
	case *livekit.ServerMessage_Availability:
		if value.Availability == nil || value.Availability.Job == nil {
			return nil
		}
		s.dispatchAvailability(ctx, generation, value.Availability)
	case *livekit.ServerMessage_Assignment:
		if value.Assignment == nil || value.Assignment.Job == nil {
			return nil
		}
		s.deliverAssignment(generation, value.Assignment)
	case *livekit.ServerMessage_Termination:
		if value.Termination == nil || value.Termination.JobId == "" {
			return nil
		}
		if !s.startTask() {
			return nil
		}
		go func(termination *livekit.JobTermination) {
			defer s.tasks.Done()
			terminateCtx, cancel := context.WithTimeout(context.Background(), s.opts.ShutdownProcessTimeout)
			defer cancel()
			if err := s.pool.Terminate(terminateCtx, termination.JobId, "server requested termination"); err != nil && !errors.Is(err, ErrJobNotFound) {
				s.logger.Error("failed to terminate job", "job_id", termination.JobId, "error", err)
			}
		}(value.Termination)
	case *livekit.ServerMessage_Pong:
		// Pongs are deliberately ignored; a successful Read already proves liveness.
	default:
		s.logger.Warn("received empty or unsupported worker protocol message")
	}
	return nil
}

func (s *AgentServer[T]) dispatchAvailability(ctx context.Context, generation uint64, request *livekit.AvailabilityRequest) {
	select {
	case s.availabilitySlots <- struct{}{}:
		if !s.startTask() {
			<-s.availabilitySlots
			return
		}
		go func() {
			defer s.tasks.Done()
			defer func() { <-s.availabilitySlots }()
			defer func() {
				if recovered := recover(); recovered != nil {
					s.logger.Error("availability handler panicked", "job_id", request.Job.GetId(), "panic", recovered)
				}
			}()
			s.handleAvailability(ctx, generation, request)
		}()
	default:
		s.logger.Warn("availability handler is saturated; rejecting job", "job_id", request.Job.GetId())
		_ = s.send(ctx, availabilityMessage(request.Job.GetId(), false, JobAcceptOptions{}, s.opts.AgentName))
	}
}

func (s *AgentServer[T]) handleAvailability(ctx context.Context, generation uint64, availability *livekit.AvailabilityRequest) {
	job := availability.Job
	if job == nil {
		return
	}
	if s.Draining() {
		_ = s.send(ctx, availabilityMessage(job.Id, false, JobAcceptOptions{}, s.opts.AgentName))
		return
	}
	reservation, err := s.pool.Reserve(ctx, job.Id)
	if err != nil {
		s.logger.Info("worker has no job capacity; rejecting request", "job_id", job.Id, "error", err)
		_ = s.send(ctx, availabilityMessage(job.Id, false, JobAcceptOptions{}, s.opts.AgentName))
		return
	}
	defer reservation.Release()

	request := newJobRequest(job, availability.Resuming,
		func(acceptCtx context.Context, accept JobAcceptOptions) error {
			pending := pendingAssignment{generation: generation, result: make(chan *livekit.JobAssignment, 1)}
			s.stateMu.Lock()
			if current, exists := s.pending[job.Id]; exists {
				s.stateMu.Unlock()
				return fmt.Errorf("agents: assignment already pending for job %s in generation %d", job.Id, current.generation)
			}
			s.pending[job.Id] = pending
			s.stateMu.Unlock()
			defer func() {
				s.stateMu.Lock()
				delete(s.pending, job.Id)
				s.stateMu.Unlock()
			}()

			if err := s.send(acceptCtx, availabilityMessage(job.Id, true, accept, s.opts.AgentName)); err != nil {
				return err
			}
			timer := time.NewTimer(s.opts.AssignmentTimeout)
			defer timer.Stop()
			var assignment *livekit.JobAssignment
			select {
			case assignment = <-pending.result:
			case <-timer.C:
				return &AssignmentTimeoutError{Message: fmt.Sprintf("assignment for job %s timed out after %s", job.Id, s.opts.AssignmentTimeout)}
			case <-acceptCtx.Done():
				return acceptCtx.Err()
			case <-ctx.Done():
				return ctx.Err()
			}
			if assignment == nil || assignment.Job == nil {
				return errors.New("agents: received empty assignment")
			}
			if assignment.Job.Id != job.Id {
				return fmt.Errorf("agents: assignment job ID %q does not match request %q", assignment.Job.Id, job.Id)
			}
			assignedURL := assignment.GetUrl()
			if assignedURL == "" {
				assignedURL = s.opts.URL
			}
			info := RunningJobInfo{
				AcceptArguments: accept,
				Job:             assignment.Job,
				URL:             assignedURL,
				Token:           NewSecretString(assignment.Token),
				WorkerID:        s.ID(),
				APIKey:          s.opts.APIKey,
				APISecret:       s.opts.APISecret,
			}
			if err := reservation.Launch(acceptCtx, info); err != nil {
				s.publishJobStatus(job.Id, livekit.JobStatus_JS_FAILED, err)
				return fmt.Errorf("agents: launch job %s: %w", job.Id, err)
			}
			return nil
		},
		func(rejectCtx context.Context, _ JobRejectOptions) error {
			return s.send(rejectCtx, availabilityMessage(job.Id, false, JobAcceptOptions{}, s.opts.AgentName))
		})

	err = func() (handlerErr error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				handlerErr = fmt.Errorf("job request handler panicked: %v", recovered)
			}
		}()
		return s.opts.RequestHandler(ctx, request)
	}()
	if err != nil {
		s.logger.Error("job request handler failed", "job_id", job.Id, "error", err)
	}
	if !request.Answered() {
		if rejectErr := request.Reject(ctx, JobRejectOptions{}); rejectErr != nil && ctx.Err() == nil {
			s.logger.Error("failed to automatically reject unanswered job", "job_id", job.Id, "error", rejectErr)
		}
	}
}

func availabilityMessage(jobID string, available bool, accept JobAcceptOptions, agentName string) *livekit.WorkerMessage {
	attributes := cloneStrings(accept.Attributes)
	if available {
		if attributes == nil {
			attributes = make(map[string]string, 1)
		}
		attributes[AttributeAgentName] = agentName
	}
	return &livekit.WorkerMessage{Message: &livekit.WorkerMessage_Availability{Availability: &livekit.AvailabilityResponse{
		JobId:                 jobID,
		Available:             available,
		ParticipantName:       accept.Name,
		ParticipantIdentity:   accept.Identity,
		ParticipantMetadata:   accept.Metadata,
		ParticipantAttributes: attributes,
	}}}
}

func (s *AgentServer[T]) deliverAssignment(generation uint64, assignment *livekit.JobAssignment) {
	jobID := assignment.Job.GetId()
	s.stateMu.RLock()
	pending, exists := s.pending[jobID]
	s.stateMu.RUnlock()
	if !exists || pending.generation != generation {
		s.logger.Warn("received assignment for unknown job", "job_id", jobID)
		return
	}
	select {
	case pending.result <- proto.Clone(assignment).(*livekit.JobAssignment):
	default:
		s.logger.Warn("received duplicate assignment", "job_id", jobID)
	}
}

func (s *AgentServer[T]) send(ctx context.Context, message *livekit.WorkerMessage) error {
	if message == nil {
		return errors.New("agents: nil worker message")
	}
	s.stateMu.RLock()
	generation := s.generation
	registered := s.registered
	closed := s.closed
	s.stateMu.RUnlock()
	if closed {
		return errors.New("agents: worker is closed")
	}
	if !registered {
		return errors.New("agents: worker is not registered")
	}
	copyMessage := proto.Clone(message).(*livekit.WorkerMessage)
	select {
	case s.outgoing <- outboundMessage{generation: generation, message: copyMessage}:
		s.emitEvent(WorkerEvent{Type: WorkerEventMessage, WorkerID: s.ID(), Message: proto.Clone(copyMessage).(*livekit.WorkerMessage)})
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.closedCh:
		return errors.New("agents: worker is closed")
	}
}

func (s *AgentServer[T]) publishJobStatus(jobID string, status livekit.JobStatus, jobErr error) {
	update := &livekit.UpdateJobStatus{JobId: jobID, Status: status}
	if jobErr != nil {
		update.Error = jobErr.Error()
	}
	message := &livekit.WorkerMessage{Message: &livekit.WorkerMessage_UpdateJob{UpdateJob: update}}
	if status == livekit.JobStatus_JS_SUCCESS || status == livekit.JobStatus_JS_FAILED {
		s.stateMu.Lock()
		s.deferredJobUpdates[jobID] = proto.Clone(message).(*livekit.WorkerMessage)
		s.stateMu.Unlock()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.send(ctx, message); err != nil && !s.isClosed() {
		s.logger.Warn("failed to publish job status", "job_id", jobID, "status", status, "error", err)
	}
}

func (s *AgentServer[T]) flushDeferredJobUpdates(ctx context.Context) error {
	s.stateMu.RLock()
	messages := make([]*livekit.WorkerMessage, 0, len(s.deferredJobUpdates))
	for _, message := range s.deferredJobUpdates {
		messages = append(messages, proto.Clone(message).(*livekit.WorkerMessage))
	}
	s.stateMu.RUnlock()
	for _, message := range messages {
		if err := s.send(ctx, message); err != nil {
			return err
		}
	}
	return nil
}

func (s *AgentServer[T]) acknowledgeJobUpdate(message *livekit.WorkerMessage) {
	update := message.GetUpdateJob()
	if update == nil || (update.Status != livekit.JobStatus_JS_SUCCESS && update.Status != livekit.JobStatus_JS_FAILED) {
		return
	}
	s.stateMu.Lock()
	if pending := s.deferredJobUpdates[update.JobId]; pending != nil {
		pendingUpdate := pending.GetUpdateJob()
		if pendingUpdate != nil && pendingUpdate.Status == update.Status && pendingUpdate.Error == update.Error {
			delete(s.deferredJobUpdates, update.JobId)
		}
	}
	s.stateMu.Unlock()
}

// Drain immediately advertises FULL and waits for accepted jobs to finish.
func (s *AgentServer[T]) Drain(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	s.stateMu.Lock()
	if s.draining {
		s.stateMu.Unlock()
		return s.pool.Drain(ctx)
	}
	s.draining = true
	registered := s.registered
	s.stateMu.Unlock()
	if registered {
		_ = s.send(ctx, s.statusMessage(1))
	}
	if err := s.pool.Drain(ctx); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return &WorkerError{Message: "timed out draining worker", Cause: err}
		}
		return err
	}
	return nil
}

// Close stops registration, the health server, and all remaining executors.
// It is idempotent. With no context argument it uses a background context.
func (s *AgentServer[T]) Close(contexts ...context.Context) error {
	if len(contexts) > 1 {
		return errors.New("agents: Close accepts at most one context")
	}
	ctx := context.Background()
	var cancel context.CancelFunc
	if len(contexts) > 0 && contexts[0] != nil {
		ctx = contexts[0]
	} else {
		ctx, cancel = context.WithTimeout(ctx, s.opts.ShutdownProcessTimeout)
		defer cancel()
	}
	s.closeOnce.Do(func() {
		s.stateMu.Lock()
		s.closed = true
		transport := s.transport
		cancel := s.cancelRun
		s.stateMu.Unlock()
		s.taskMu.Lock()
		s.acceptingTasks = false
		s.taskMu.Unlock()
		close(s.closedCh)
		if cancel != nil {
			cancel(errors.New("worker closed"))
		}
		if transport != nil {
			s.closeErr = errors.Join(s.closeErr, transport.Close())
		}
		if s.http != nil {
			s.closeErr = errors.Join(s.closeErr, s.http.Close(ctx))
		}
		s.closeErr = errors.Join(s.closeErr, s.pool.Close(ctx))
		tasksDone := make(chan struct{})
		go func() {
			s.tasks.Wait()
			close(tasksDone)
		}()
		select {
		case <-tasksDone:
		case <-ctx.Done():
			s.closeErr = errors.Join(s.closeErr, ctx.Err())
		}
	})
	if s.closedEventEmitted.CompareAndSwap(false, true) {
		s.emitEvent(WorkerEvent{Type: WorkerEventClosed, WorkerID: s.ID(), Error: s.closeErr})
	}
	return s.closeErr
}

func (s *AgentServer[T]) startTask() bool {
	s.taskMu.Lock()
	defer s.taskMu.Unlock()
	if !s.acceptingTasks {
		return false
	}
	s.tasks.Add(1)
	return true
}

func (s *AgentServer[T]) emitEvent(event WorkerEvent) {
	defer func() {
		if recovered := recover(); recovered != nil {
			s.logger.Error("worker event listener panicked", "event", event.Type, "panic", recovered)
		}
	}()
	s.events.Emit(event)
}

func (s *AgentServer[T]) markDisconnected(generation uint64) {
	s.stateMu.Lock()
	if s.generation == generation {
		s.connecting = false
		s.registered = false
		s.transport = nil
	}
	s.stateMu.Unlock()
}

func (s *AgentServer[T]) isClosed() bool {
	s.stateMu.RLock()
	closed := s.closed
	s.stateMu.RUnlock()
	return closed
}

// SimulateJob asks the connected server to synthesize a job request.
func (s *AgentServer[T]) SimulateJob(ctx context.Context, roomName, participantIdentity string) error {
	if roomName == "" {
		return errors.New("agents: room name is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	client := lksdk.NewRoomServiceClient(s.opts.URL, s.opts.APIKey.Reveal(), s.opts.APISecret.Reveal())
	room, err := client.CreateRoom(ctx, &livekit.CreateRoomRequest{Name: roomName})
	if err != nil {
		return fmt.Errorf("agents: create simulation room: %w", err)
	}
	var participant *livekit.ParticipantInfo
	if participantIdentity != "" {
		participant, err = client.GetParticipant(ctx, &livekit.RoomParticipantIdentity{Room: roomName, Identity: participantIdentity})
		if err != nil {
			return fmt.Errorf("agents: get simulation participant %q: %w", participantIdentity, err)
		}
	}
	return s.send(ctx, &livekit.WorkerMessage{Message: &livekit.WorkerMessage_SimulateJob{SimulateJob: &livekit.SimulateJobRequest{
		Type: livekit.JobType_JT_PUBLISHER, Room: room, Participant: participant,
	}}})
}

// Run constructs a worker, handles SIGINT/SIGTERM, drains, and closes it.
func Run[T any](ctx context.Context, options WorkerOptions[T]) error {
	if ctx == nil {
		ctx = context.Background()
	}
	// A same-binary inference child does not need worker option validation,
	// HTTP setup, signal ownership, or a JobEntrypoint. Dispatch before building
	// the worker to keep its native-runner startup path minimal.
	if isInferenceChildProcess() {
		return runInferenceChildProcess(ctx)
	}
	server, err := NewAgentServer(ServerOptions[T](options))
	if err != nil {
		return err
	}
	signalCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	return server.Run(signalCtx)
}

func cloneServerInfo(info *livekit.ServerInfo) *livekit.ServerInfo {
	if info == nil {
		return nil
	}
	return proto.Clone(info).(*livekit.ServerInfo)
}
