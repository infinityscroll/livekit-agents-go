// SPDX-License-Identifier: Apache-2.0

package agents

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/livekit/protocol/livekit"
)

// ConsoleJobOptions configures the in-process fake job used by the console
// command. It deliberately does not require LiveKit credentials or initialize
// a worker websocket.
type ConsoleJobOptions[UserData any] struct {
	Server ServerOptions[UserData]
	Record bool
	// SessionDirectory is shared with AgentSession recording/report lifecycle.
	// An empty value uses the normal traversal-safe temporary job directory.
	SessionDirectory string
	ShutdownTimeout  time.Duration
	// Setup runs after prewarm and before the entrypoint. The CLI uses it to
	// bind process-local console IO without introducing a root-package import
	// cycle with voice.
	Setup func(context.Context, *JobContext[UserData], SimulationEndFunc[UserData]) error
}

// RunConsoleJob executes the normal prewarm, entrypoint, job-context, and
// shutdown-callback lifecycle in-process. Cancellation is the console lifetime
// signal and is treated as a clean shutdown.
func RunConsoleJob[UserData any](ctx context.Context, options ConsoleJobOptions[UserData]) (resultErr error) {
	if ctx == nil {
		ctx = context.Background()
	}
	entrypoint, prewarm, simulationEnd, err := resolveConsoleDefinition(options.Server)
	if err != nil {
		return err
	}
	if entrypoint == nil {
		return errors.New("agents: console requires a JobEntrypoint")
	}
	shutdownTimeout := options.ShutdownTimeout
	if shutdownTimeout == 0 {
		shutdownTimeout = options.Server.ShutdownProcessTimeout
	}
	if shutdownTimeout == 0 {
		shutdownTimeout = DefaultShutdownProcessTimeout
	}
	if shutdownTimeout < 0 {
		return errors.New("agents: console shutdown timeout must not be negative")
	}
	process := newJobProcess[UserData](os.Getpid())
	if err := callPrewarm(ctx, prewarm, process); err != nil {
		return err
	}
	inference, err := NewLocalInferenceExecutor(nil)
	if err != nil {
		return fmt.Errorf("agents: create console inference executor: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		resultErr = errors.Join(resultErr, inference.Close(closeCtx))
		cancel()
	}()
	if inference.HasInferenceRunners() {
		initializeCtx, cancel := context.WithTimeout(ctx, DefaultInferenceInitializeTimeout)
		initializeErr := inference.Initialize(initializeCtx)
		cancel()
		if initializeErr != nil {
			return fmt.Errorf("agents: initialize console inference executor: %w", initializeErr)
		}
	}
	job := &livekit.Job{
		Id: ShortUUID("console-job-"), Type: livekit.JobType_JT_ROOM,
		Room: &livekit.Room{Name: "console-room"}, EnableRecording: options.Record,
	}
	info := RunningJobInfo{
		AcceptArguments: JobAcceptOptions{Identity: "console"},
		Job:             job, WorkerID: "console", FakeJob: true,
		SessionDirectory: options.SessionDirectory,
	}
	jobContext := newJobContext(ctx, process, info, func() {}, inference)
	finish := func() error {
		finishCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		return jobContext.finish(finishCtx)
	}
	if options.Setup != nil {
		if err := options.Setup(ctx, jobContext, simulationEnd); err != nil {
			jobContext.Shutdown(err.Error())
			return errors.Join(fmt.Errorf("agents: setup console job: %w", err), finish())
		}
	}

	entryDone := make(chan error, 1)
	go func() { entryDone <- runJobEntrypoint(entrypoint, jobContext) }()
	entryReturned := false
	var entryErr error
	select {
	case entryErr = <-entryDone:
		entryReturned = true
		if entryErr != nil {
			jobContext.Shutdown(entryErr.Error())
		}
	case <-jobContext.Done():
		if ctx.Err() != nil {
			jobContext.Shutdown("console stopped")
		}
	case <-ctx.Done():
		jobContext.Shutdown("console stopped")
	}
	if entryReturned && entryErr == nil {
		select {
		case <-jobContext.Done():
			if ctx.Err() != nil {
				jobContext.Shutdown("console stopped")
			}
		case <-ctx.Done():
			jobContext.Shutdown("console stopped")
		}
	}
	if !entryReturned {
		waitCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		select {
		case entryErr = <-entryDone:
		case <-waitCtx.Done():
			entryErr = errors.New("agents: console entrypoint did not stop before shutdown timeout")
		}
		cancel()
	}
	if errors.Is(entryErr, context.Canceled) && jobContext.Context().Err() != nil {
		entryErr = nil
	}
	return errors.Join(entryErr, finish())
}

func resolveConsoleDefinition[UserData any](options ServerOptions[UserData]) (JobEntrypoint[UserData], PrewarmFunc[UserData], SimulationEndFunc[UserData], error) {
	entrypoint, prewarm, simulationEnd := options.JobEntrypoint, options.Prewarm, options.OnSimulationEnd
	if entrypoint == nil {
		entrypoint = options.Entrypoint
	}
	if options.JobEntrypoint != nil && options.Entrypoint != nil && reflectFunctionPointer(options.JobEntrypoint) != reflectFunctionPointer(options.Entrypoint) {
		return nil, nil, nil, errors.New("agents: JobEntrypoint and Entrypoint cannot specify different functions")
	}
	if options.Agent != nil {
		if !IsAgent(options.Agent) {
			return nil, nil, nil, errors.New("agents: invalid agent definition")
		}
		definitionEntry := options.Agent.Entrypoint()
		if entrypoint != nil && reflectFunctionPointer(entrypoint) != reflectFunctionPointer(definitionEntry) {
			return nil, nil, nil, errors.New("agents: Agent and JobEntrypoint cannot specify different functions")
		}
		entrypoint = definitionEntry
		if definitionPrewarm := options.Agent.Prewarm(); definitionPrewarm != nil {
			if prewarm != nil && reflectFunctionPointer(prewarm) != reflectFunctionPointer(definitionPrewarm) {
				return nil, nil, nil, errors.New("agents: Agent and Prewarm cannot specify different functions")
			}
			prewarm = definitionPrewarm
		}
		if definitionSimulationEnd := options.Agent.SimulationEnd(); definitionSimulationEnd != nil {
			if simulationEnd != nil && reflectFunctionPointer(simulationEnd) != reflectFunctionPointer(definitionSimulationEnd) {
				return nil, nil, nil, errors.New("agents: Agent and OnSimulationEnd cannot specify different functions")
			}
			simulationEnd = definitionSimulationEnd
		}
	}
	return entrypoint, prewarm, simulationEnd, nil
}
