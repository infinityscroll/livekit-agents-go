// SPDX-License-Identifier: Apache-2.0

package inference

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/infinityscroll/livekit-agents-go/ipc"
)

const (
	// VADInferenceMethod is the shared-runner method for stateful Silero VAD.
	// A runner must isolate recurrent state by StreamID and implement init,
	// predict, reset, and close operations.
	VADInferenceMethod = "lk_vad"

	VADOperationInit    VADInferenceOperation = "init"
	VADOperationPredict VADInferenceOperation = "predict"
	VADOperationReset   VADInferenceOperation = "reset"
	VADOperationClose   VADInferenceOperation = "close"

	defaultVADExecutorOperationTimeout = 5 * time.Second
)

type VADInferenceOperation string

// VADInferenceInput is the stable local-runner/IPC request. PCM contains
// little-endian 16 kHz signed PCM and is present only for predict.
type VADInferenceInput struct {
	StreamID  string                `json:"streamId"`
	Operation VADInferenceOperation `json:"operation"`
	PCM       []byte                `json:"pcm,omitempty"`
}

// VADInferenceOutput returns one probability. Init may override the standard
// 512-sample Silero window for compatible runner implementations.
type VADInferenceOutput struct {
	Probability   float64 `json:"probability"`
	WindowSamples int     `json:"windowSamples,omitempty"`
}

type executorVADPredictor struct {
	executor ipc.InferenceExecutor
	streamID string
	window   int
}

func executorVADPredictorFactory(executor ipc.InferenceExecutor) VADPredictorFactory {
	return func(ctx context.Context) (VADPredictor, error) {
		if !ipc.IsInferenceExecutor(executor) {
			return nil, ErrLocalInferenceUnavailable
		}
		predictor := &executorVADPredictor{
			executor: executor, streamID: agents.ShortUUID("vad_"), window: defaultVADWindowSamples,
		}
		result, err := executor.DoInference(ctx, VADInferenceMethod, VADInferenceInput{
			StreamID: predictor.streamID, Operation: VADOperationInit,
		})
		if err != nil {
			var unknown *ipc.UnknownMethodError
			if errors.As(err, &unknown) {
				return nil, ErrLocalInferenceUnavailable
			}
			return nil, err
		}
		if result != nil {
			output, err := decodeVADInferenceOutput(result)
			if err != nil {
				return nil, err
			}
			if output.WindowSamples > 0 {
				predictor.window = output.WindowSamples
			}
		}
		return predictor, nil
	}
}

func (p *executorVADPredictor) WindowSamples() int { return p.window }

func (p *executorVADPredictor) Predict(ctx context.Context, samples []int16) (float64, error) {
	if len(samples) != p.window {
		return 0, fmt.Errorf("inference VAD runner window has %d samples, want %d", len(samples), p.window)
	}
	pcm := make([]byte, len(samples)*2)
	for index, sample := range samples {
		binary.LittleEndian.PutUint16(pcm[index*2:], uint16(sample))
	}
	result, err := p.executor.DoInference(ctx, VADInferenceMethod, VADInferenceInput{
		StreamID: p.streamID, Operation: VADOperationPredict, PCM: pcm,
	})
	if err != nil {
		return 0, err
	}
	output, err := decodeVADInferenceOutput(result)
	if err != nil {
		return 0, err
	}
	if math.IsNaN(output.Probability) || math.IsInf(output.Probability, 0) || output.Probability < 0 || output.Probability > 1 {
		return 0, fmt.Errorf("inference VAD runner returned invalid probability %v", output.Probability)
	}
	return output.Probability, nil
}

func (p *executorVADPredictor) Reset() error {
	return p.operation(VADOperationReset)
}

func (p *executorVADPredictor) Close() error {
	return p.operation(VADOperationClose)
}

func (p *executorVADPredictor) operation(operation VADInferenceOperation) error {
	ctx, cancel := context.WithTimeout(context.Background(), defaultVADExecutorOperationTimeout)
	defer cancel()
	_, err := p.executor.DoInference(ctx, VADInferenceMethod, VADInferenceInput{
		StreamID: p.streamID, Operation: operation,
	})
	if errors.Is(err, ipc.ErrInferenceExecutorClosed) || errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func decodeVADInferenceOutput(result any) (VADInferenceOutput, error) {
	switch value := result.(type) {
	case VADInferenceOutput:
		return value, nil
	case *VADInferenceOutput:
		if value != nil {
			return *value, nil
		}
	case map[string]any:
		output := VADInferenceOutput{}
		if probability, ok := value["probability"].(float64); ok {
			output.Probability = probability
		}
		if window, ok := value["windowSamples"].(float64); ok {
			output.WindowSamples = int(window)
		} else if window, ok := value["window_samples"].(float64); ok {
			output.WindowSamples = int(window)
		}
		return output, nil
	}
	payload, err := json.Marshal(result)
	if err != nil {
		return VADInferenceOutput{}, fmt.Errorf("decode local VAD inference output: %w", err)
	}
	var output VADInferenceOutput
	if err := json.Unmarshal(payload, &output); err != nil {
		return VADInferenceOutput{}, fmt.Errorf("decode local VAD inference output: %w", err)
	}
	return output, nil
}

var _ VADPredictor = (*executorVADPredictor)(nil)
