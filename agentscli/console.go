// SPDX-License-Identifier: Apache-2.0

package agentscli

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strconv"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/voice"
	"github.com/livekit/agents-go/voice/recorderio"
)

// NewConsoleRunner returns the built-in in-process console runner. The
// transport is initialized only when the console command is actually invoked.
func NewConsoleRunner[UserData any](server agents.ServerOptions[UserData]) ConsoleRunner {
	return func(ctx context.Context, options ConsoleOptions) (result error) {
		host, port, err := splitConsoleAddress(options.ConnectAddress)
		if err != nil {
			return err
		}
		transport, err := voice.NewTCPSessionTransport(host, port)
		if err != nil {
			return err
		}
		input, err := voice.NewTCPAudioInput()
		if err != nil {
			return err
		}
		output, err := voice.NewTCPAudioOutput(transport)
		if err != nil {
			return err
		}
		console, err := voice.NewAgentsConsole(voice.AgentsConsoleOptions{
			Enabled: true, Record: options.Record, Transport: transport,
			AudioInput: input, AudioOutput: output,
		})
		if err != nil {
			return err
		}
		defer func() {
			closeCtx, cancel := context.WithTimeout(context.Background(), voice.DefaultConsoleOperationTimeout)
			result = errors.Join(result, console.Close(closeCtx))
			cancel()
		}()
		if options.Record {
			recorder, recorderErr := recorderio.NewRecorderIO(recorderio.RecorderOptions{
				OnError: func(recordErr error) {
					if options.Logger != nil {
						options.Logger.Error("console recording failed", "error", recordErr)
					}
				},
			})
			if recorderErr != nil {
				return recorderErr
			}
			recordedInput, recorderErr := recorder.RecordInput(input)
			if recorderErr != nil {
				return recorderErr
			}
			recordedOutput, recorderErr := recorder.RecordOutput(output)
			if recorderErr != nil {
				return recorderErr
			}
			if recorderErr = console.SetRecordingIO(recordedInput, recordedOutput, recorder); recorderErr != nil {
				return recorderErr
			}
			if recorderErr = recorder.Start(ctx, filepath.Join(console.SessionDirectory(), "audio.ogg")); recorderErr != nil {
				return fmt.Errorf("start console recording: %w", recorderErr)
			}
		}
		restore := voice.SetDefaultAgentsConsole(console)
		defer restore()
		if options.Logger != nil {
			options.Logger.Info("starting console session", "host", host, "port", port)
		}
		server.Logger = options.Logger
		return agents.RunConsoleJob(ctx, agents.ConsoleJobOptions[UserData]{
			Server: server, Record: options.Record, SessionDirectory: console.SessionDirectory(),
			Setup: func(_ context.Context, job *agents.JobContext[UserData], onSimulationEnd agents.SimulationEndFunc[UserData]) error {
				return voice.BindAgentsConsoleJob(console, job, onSimulationEnd)
			},
		})
	}
}

func splitConsoleAddress(address string) (string, int, error) {
	host, portText, err := net.SplitHostPort(address)
	if err != nil || host == "" || portText == "" {
		return "", 0, fmt.Errorf("invalid --connect-addr %q, expected host:port (IPv6 addresses must use brackets)", address)
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return "", 0, fmt.Errorf("invalid port in --connect-addr %q, expected 1-65535", address)
	}
	return host, port, nil
}
