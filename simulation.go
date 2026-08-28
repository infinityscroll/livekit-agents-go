// SPDX-License-Identifier: Apache-2.0

package agents

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"sync"

	"github.com/livekit/protocol/livekit"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type Scenario = livekit.Scenario
type ScenarioGroup = livekit.ScenarioGroup
type SimulationDispatch = livekit.SimulationDispatch
type SimulationMode = livekit.SimulationMode
type SimulationRun = livekit.SimulationRun
type SimulationRunJob = livekit.SimulationRun_Job

const (
	SimulationModeUnspecified = livekit.SimulationMode_SIMULATION_MODE_UNSPECIFIED
	SimulationModeText        = livekit.SimulationMode_SIMULATION_MODE_TEXT
	SimulationModeAudio       = livekit.SimulationMode_SIMULATION_MODE_AUDIO

	// Upper-case aliases ease mechanical TypeScript/protobuf migrations.
	SIMULATION_MODE_UNSPECIFIED = SimulationModeUnspecified
	SIMULATION_MODE_TEXT        = SimulationModeText
	SIMULATION_MODE_AUDIO       = SimulationModeAudio
)

var ErrSimulatorVerdictUnavailable = errors.New("simulator verdict is only available after the simulation completes")

// SimulationVerdict is a pass/fail result with a human-readable reason.
type SimulationVerdict struct {
	Success bool   `json:"success"`
	Reason  string `json:"reason"`
}

type ScenarioUserdata map[string]any

// ParseSimulationDispatch decodes the proto-JSON job attribute. Unknown fields
// are discarded so a newer LiveKit server can extend the message without
// breaking an older agent binary.
func ParseSimulationDispatch(raw string) (*SimulationDispatch, error) {
	dispatch := new(SimulationDispatch)
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal([]byte(raw), dispatch); err != nil {
		return nil, err
	}
	return dispatch, nil
}

// SimulationContext carries the immutable dispatch plus final verdict state
// for one simulated job. The user verdict can veto a simulator success but
// cannot turn a simulator failure into a success.
type SimulationContext[T any] struct {
	dispatch *SimulationDispatch
	job      *JobContext[T]

	mu               sync.RWMutex
	run              *SimulationRun
	simulatorVerdict *SimulationVerdict
	userVerdict      *SimulationVerdict
}

func NewSimulationContext[T any](dispatch *SimulationDispatch, job *JobContext[T]) (*SimulationContext[T], error) {
	if dispatch == nil {
		return nil, errors.New("simulation dispatch is required")
	}
	return &SimulationContext[T]{dispatch: proto.Clone(dispatch).(*SimulationDispatch), job: job}, nil
}

func (s *SimulationContext[T]) Dispatch() *SimulationDispatch {
	if s == nil || s.dispatch == nil {
		return nil
	}
	return proto.Clone(s.dispatch).(*SimulationDispatch)
}

func (s *SimulationContext[T]) Scenario() *Scenario {
	if s == nil || s.dispatch == nil || s.dispatch.Scenario == nil {
		return &Scenario{}
	}
	return proto.Clone(s.dispatch.Scenario).(*Scenario)
}

func (s *SimulationContext[T]) Mode() SimulationMode {
	if s == nil || s.dispatch == nil || s.dispatch.Mode == SimulationModeUnspecified {
		return SimulationModeText
	}
	return s.dispatch.Mode
}

func (s *SimulationContext[T]) RunID() string {
	if s == nil || s.dispatch == nil {
		return ""
	}
	return s.dispatch.SimulationRunId
}

func (s *SimulationContext[T]) JobID() string {
	if s == nil || s.dispatch == nil {
		return ""
	}
	return s.dispatch.JobId
}

func (s *SimulationContext[T]) Run() (*SimulationRun, bool) {
	if s == nil {
		return nil, false
	}
	s.mu.RLock()
	run := s.run
	if run != nil {
		run = proto.Clone(run).(*SimulationRun)
	}
	s.mu.RUnlock()
	return run, run != nil
}

func (s *SimulationContext[T]) SimulatorVerdict() (SimulationVerdict, error) {
	if s == nil {
		return SimulationVerdict{}, ErrSimulatorVerdictUnavailable
	}
	s.mu.RLock()
	verdict := s.simulatorVerdict
	s.mu.RUnlock()
	if verdict == nil {
		return SimulationVerdict{}, ErrSimulatorVerdictUnavailable
	}
	return *verdict, nil
}

func (s *SimulationContext[T]) JobContext() *JobContext[T] {
	if s == nil {
		return nil
	}
	return s.job
}

// BeginFinalize is called by the session host before the on-simulation-end
// hook. It makes the simulator verdict and run visible atomically.
func (s *SimulationContext[T]) BeginFinalize(verdict SimulationVerdict, run *SimulationRun) {
	if s == nil {
		return
	}
	s.mu.Lock()
	verdictCopy := verdict
	s.simulatorVerdict = &verdictCopy
	if run == nil {
		s.run = nil
	} else {
		s.run = proto.Clone(run).(*SimulationRun)
	}
	s.mu.Unlock()
}

// Userdata decodes the scenario's JSON object. It returns an empty map for an
// empty field and preserves integer tokens as json.Number.
func (s *SimulationContext[T]) Userdata() (ScenarioUserdata, error) {
	raw := s.Scenario().Userdata
	if raw == "" {
		return ScenarioUserdata{}, nil
	}
	decoder := json.NewDecoder(bytes.NewBufferString(raw))
	decoder.UseNumber()
	var value ScenarioUserdata
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	if value == nil {
		return nil, errors.New("simulation scenario userdata must be a JSON object")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("simulation scenario userdata contains trailing JSON")
		}
		return nil, err
	}
	return value, nil
}

// Fail records the user's veto. The last call wins.
func (s *SimulationContext[T]) Fail(reason string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.userVerdict = &SimulationVerdict{Success: false, Reason: reason}
	s.mu.Unlock()
}

func (s *SimulationContext[T]) UserVerdict() (SimulationVerdict, bool) {
	if s == nil {
		return SimulationVerdict{}, false
	}
	s.mu.RLock()
	verdict := s.userVerdict
	s.mu.RUnlock()
	if verdict == nil {
		return SimulationVerdict{}, false
	}
	return *verdict, true
}

func (s *SimulationContext[T]) EffectiveVerdict() (SimulationVerdict, error) {
	simulator, err := s.SimulatorVerdict()
	if err != nil {
		return SimulationVerdict{}, err
	}
	if user, ok := s.UserVerdict(); ok {
		if !simulator.Success || !user.Success {
			reason := user.Reason
			if !simulator.Success {
				reason = simulator.Reason
			}
			return SimulationVerdict{Success: false, Reason: reason}, nil
		}
	}
	return simulator, nil
}
