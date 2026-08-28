// SPDX-License-Identifier: Apache-2.0

package agents

const (
	// UserdataTimedTranscript stores timed transcripts on an AudioFrame.
	UserdataTimedTranscript = "lk.timed_transcripts"
	// UserdataTTSStartedTime stores when text was first sent to TTS.
	UserdataTTSStartedTime = "lk.tts_started_time"

	AttributeSimulator         = "lk.simulator"
	AttributeSimulatorDispatch = "lk.simulator.dispatch"
	AttributeSimulationEnabled = "lk.simulation.enabled"
	AttributeSimulationRunID   = "lk.simulation.run_id"
	AttributeSimulationJobID   = "lk.simulation.job_id"
	AttributeRedactionEnabled  = "lk.redaction.enabled"

	AttributeTranscriptionExpression = "lk.expression"
)

// FlushSentinel is the Go equivalent of the JS FlushSentinel symbol. A model
// stream emits a value with Flush=true instead of relying on an untyped marker.
type FlushSentinel struct{}
