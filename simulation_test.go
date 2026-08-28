// SPDX-License-Identifier: Apache-2.0

package agents

import (
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/livekit/protocol/livekit"
)

func TestParseSimulationDispatchAndContext(t *testing.T) {
	t.Parallel()
	raw := `{
      "simulationRunId":"SR_1",
      "jobId":"AJ_1",
      "mode":"SIMULATION_MODE_AUDIO",
      "scenario":{"label":"refund","userdata":"{\"amount\":12,\"ok\":true}"},
      "futureField":true
    }`
	dispatch, err := ParseSimulationDispatch(raw)
	if err != nil {
		t.Fatal(err)
	}
	ctx, err := NewSimulationContext[struct{}](dispatch, nil)
	if err != nil {
		t.Fatal(err)
	}
	if ctx.RunID() != "SR_1" || ctx.JobID() != "AJ_1" || ctx.Mode() != SimulationModeAudio {
		t.Fatalf("unexpected dispatch: %#v", ctx.Dispatch())
	}
	userdata, err := ctx.Userdata()
	if err != nil {
		t.Fatal(err)
	}
	if userdata["amount"] != json.Number("12") || userdata["ok"] != true {
		t.Fatalf("userdata = %#v", userdata)
	}
	if _, err := ctx.SimulatorVerdict(); !errors.Is(err, ErrSimulatorVerdictUnavailable) {
		t.Fatalf("early verdict error = %v", err)
	}
	run := &SimulationRun{Id: "SR_1", Jobs: []*SimulationRunJob{{Id: "AJ_1", Label: "refund"}}}
	ctx.BeginFinalize(SimulationVerdict{Success: true, Reason: "passed"}, run)
	// BeginFinalize owns a clone rather than the caller's mutable protobuf.
	run.Id = "mutated"
	gotRun, ok := ctx.Run()
	if !ok || gotRun.Id != "SR_1" {
		t.Fatalf("run = %#v, %t", gotRun, ok)
	}
	ctx.Fail("backend state diverged")
	effective, err := ctx.EffectiveVerdict()
	if err != nil {
		t.Fatal(err)
	}
	if effective.Success || effective.Reason != "backend state diverged" {
		t.Fatalf("effective verdict = %#v", effective)
	}
}

func TestSimulationModeDefaultsToTextAndValidation(t *testing.T) {
	t.Parallel()
	dispatch, err := ParseSimulationDispatch(`{"simulationRunId":"SR_2","unknown":1}`)
	if err != nil {
		t.Fatal(err)
	}
	ctx, err := NewSimulationContext[struct{}](dispatch, nil)
	if err != nil {
		t.Fatal(err)
	}
	if ctx.Mode() != SimulationModeText {
		t.Fatalf("mode = %v", ctx.Mode())
	}
	for _, raw := range []string{`{oops`, `[1,2]`, `"string"`} {
		if _, err := ParseSimulationDispatch(raw); err == nil {
			t.Fatalf("ParseSimulationDispatch(%q) succeeded", raw)
		}
	}
	dispatch.Scenario = &Scenario{Userdata: `[1,2]`}
	bad, err := NewSimulationContext[struct{}](dispatch, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bad.Userdata(); err == nil {
		t.Fatal("Userdata accepted an array")
	}
}

func TestJobContextSimulationContextIsCachedAndConcurrent(t *testing.T) {
	t.Parallel()
	job := &livekit.Job{Attributes: map[string]string{
		AttributeSimulatorDispatch: `{"simulationRunId":"SR_3","jobId":"AJ_3"}`,
	}}
	jobCtx := &JobContext[struct{}]{info: RunningJobInfo{Job: job}}
	const readers = 32
	results := make(chan *SimulationContext[struct{}], readers)
	var wg sync.WaitGroup
	wg.Add(readers)
	for range readers {
		go func() {
			defer wg.Done()
			value, ok := jobCtx.SimulationContext()
			if !ok {
				t.Error("SimulationContext not found")
				return
			}
			results <- value
		}()
	}
	wg.Wait()
	close(results)
	var first *SimulationContext[struct{}]
	for result := range results {
		if first == nil {
			first = result
		} else if result != first {
			t.Fatal("SimulationContext returned different cached pointers")
		}
	}
}
