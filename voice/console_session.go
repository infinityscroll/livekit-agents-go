// SPDX-License-Identifier: Apache-2.0

package voice

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	agents "github.com/infinityscroll/livekit-agents-go"
)

// acquireDefaultConsoleIO is the only console-mode branch on the normal
// AgentSession start path. Its common case is one atomic load and comparison.
func acquireDefaultConsoleIO[UserData any](ctx context.Context, session *AgentSession[UserData]) (func() error, error) {
	console := defaultAgentsConsole.Load()
	if console == nil || !console.Enabled() {
		return nil, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	console.mu.Lock()
	if !console.enabled.Load() {
		console.mu.Unlock()
		return nil, nil
	}
	if console.acquired != nil {
		if console.acquired == session && console.host != nil {
			host := console.host
			console.mu.Unlock()
			startCtx, cancel := consoleStartContext(ctx)
			defer cancel()
			if starter, ok := host.(interface{ Start(context.Context) error }); ok {
				return nil, starter.Start(startCtx)
			}
			return nil, nil
		}
		console.mu.Unlock()
		return nil, ErrConsoleIOAlreadyAcquired
	}
	input, output := console.input, console.output
	if input == nil {
		var err error
		input, err = NewTCPAudioInput()
		if err != nil {
			console.mu.Unlock()
			return nil, err
		}
		console.input = input
		if isNilInterface(console.sessionInput) {
			console.sessionInput = input
		}
	}
	if output == nil {
		var err error
		output, err = NewTCPAudioOutput(console.transport)
		if err != nil {
			console.mu.Unlock()
			return nil, err
		}
		console.output = output
		if isNilInterface(console.sessionOutput) {
			console.sessionOutput = output
		}
	}
	sessionInput, sessionOutput := console.sessionInput, console.sessionOutput
	if isNilInterface(sessionInput) {
		sessionInput = input
		console.sessionInput = input
	}
	if isNilInterface(sessionOutput) {
		sessionOutput = output
		console.sessionOutput = output
	}
	job, _ := console.job.(*agents.JobContext[UserData])
	onSimulationEnd, _ := console.simulationEnd.(agents.SimulationEndFunc[UserData])
	console.acquired = session
	console.mu.Unlock()

	previousInput := session.Input().Audio()
	previousOutput := session.Output().Audio()
	previousText := session.Output().Transcription()
	rollback := func(host *SessionHost[UserData]) error {
		var closeErr error
		if host != nil {
			if host.Started() {
				closeCtx, cancel := context.WithTimeout(context.Background(), DefaultConsoleOperationTimeout)
				closeErr = host.Close(closeCtx)
				cancel()
			} else {
				host.rollbackUnstarted()
			}
		}
		session.Input().SetAudio(previousInput)
		session.Output().SetAudio(previousOutput)
		session.Output().SetTranscription(previousText)
		console.mu.Lock()
		if console.acquired == session {
			console.acquired, console.host = nil, nil
		}
		console.mu.Unlock()
		return closeErr
	}

	session.Input().SetAudio(sessionInput)
	session.Output().SetAudio(sessionOutput)
	// Console transcription is rendered by the broker/TUI. Keeping this unset
	// matches the pinned JS/Python console contract.
	session.Output().SetTranscription(nil)
	var onTransportClosed func(error)
	if job != nil {
		onTransportClosed = func(err error) { job.Shutdown("console transport stopped: " + err.Error()) }
	}
	host, err := NewSessionHost(console.transport, SessionHostOptions[UserData]{
		AudioInput: input, AudioOutput: output, JobContext: job, OnSimulationEnd: onSimulationEnd,
		ExternalTransport: true, OnTransportClosed: onTransportClosed,
	})
	if err != nil {
		_ = rollback(nil)
		return nil, err
	}
	if err := host.RegisterSession(session); err != nil {
		_ = rollback(host)
		return nil, err
	}
	startCtx, cancel := consoleStartContext(ctx)
	err = host.Start(startCtx)
	cancel()
	if err != nil {
		_ = rollback(host)
		return nil, fmt.Errorf("voice start console session host: %w", err)
	}
	console.mu.Lock()
	if !console.enabled.Load() || console.acquired != session {
		console.mu.Unlock()
		closeCtx, closeCancel := context.WithTimeout(context.Background(), DefaultConsoleOperationTimeout)
		closeErr := host.Close(closeCtx)
		closeCancel()
		_ = rollback(nil)
		return nil, errors.Join(ErrConsoleIOClosed, closeErr)
	}
	console.host = host
	console.mu.Unlock()
	var once sync.Once
	var rollbackErr error
	return func() error {
		once.Do(func() { rollbackErr = rollback(host) })
		return rollbackErr
	}, nil
}

func consoleStartContext(parent context.Context) (context.Context, context.CancelFunc) {
	if deadline, ok := parent.Deadline(); ok && time.Until(deadline) <= DefaultConsoleOperationTimeout {
		return context.WithCancel(parent)
	}
	return context.WithTimeout(parent, DefaultConsoleOperationTimeout)
}
