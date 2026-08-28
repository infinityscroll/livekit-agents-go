// SPDX-License-Identifier: Apache-2.0

package inference

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	agents "github.com/livekit/agents-go"
	"github.com/livekit/agents-go/ipc"
	vadpkg "github.com/livekit/agents-go/vad"
)

func TestInferenceVADUsesExecutorFromContext(t *testing.T) {
	var mu sync.Mutex
	var operations []VADInferenceOperation
	executor := ipc.InferenceExecutorFunc(func(_ context.Context, method string, data any) (any, error) {
		if method != VADInferenceMethod {
			t.Fatalf("method = %q", method)
		}
		input, ok := data.(VADInferenceInput)
		if !ok {
			t.Fatalf("input = %T", data)
		}
		mu.Lock()
		operations = append(operations, input.Operation)
		mu.Unlock()
		switch input.Operation {
		case VADOperationInit:
			return VADInferenceOutput{WindowSamples: 512}, nil
		case VADOperationPredict:
			if len(input.PCM) != 1_024 {
				t.Fatalf("PCM bytes = %d", len(input.PCM))
			}
			return map[string]any{"probability": 0.9}, nil
		case VADOperationReset, VADOperationClose:
			return nil, nil
		default:
			return nil, errors.New("unknown operation")
		}
	})
	detector, err := NewVAD(VADOptions{MinimumSpeechDuration: 32 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx := agents.WithInferenceExecutor(t.Context(), executor)
	streamValue, err := detector.Stream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	stream := streamValue.(*InferenceVADStream)
	frame, _ := agents.NewAudioFrame(make([]int16, 512), 16_000, 1)
	if err := stream.Push(ctx, frame); err != nil {
		t.Fatal(err)
	}
	if err := stream.EndInput(); err != nil {
		t.Fatal(err)
	}
	event, err := stream.Recv(ctx)
	if err != nil || event.Type != vadpkg.InferenceDone || event.Probability != 0.9 {
		t.Fatalf("inference event = %+v, %v", event, err)
	}
	event, err = stream.Recv(ctx)
	if err != nil || event.Type != vadpkg.StartOfSpeech {
		t.Fatalf("speech event = %+v, %v", event, err)
	}
	if _, err := stream.Recv(ctx); !errors.Is(err, io.EOF) {
		t.Fatalf("terminal = %v", err)
	}
	if err := stream.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	got := append([]VADInferenceOperation(nil), operations...)
	mu.Unlock()
	want := []VADInferenceOperation{VADOperationInit, VADOperationPredict, VADOperationClose}
	if len(got) != len(want) {
		t.Fatalf("operations = %v, want %v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("operations = %v, want %v", got, want)
		}
	}
	if err := detector.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestInferenceVADUnknownExecutorMethodIsNoOp(t *testing.T) {
	executor := ipc.InferenceExecutorFunc(func(context.Context, string, any) (any, error) {
		return nil, &ipc.UnknownMethodError{Method: VADInferenceMethod}
	})
	detector, err := NewVAD(VADOptions{Executor: executor})
	if err != nil {
		t.Fatal(err)
	}
	streamValue, err := detector.Stream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	stream := streamValue.(*InferenceVADStream)
	frame, _ := agents.NewAudioFrame(make([]int16, 512), 16_000, 1)
	if err := stream.Push(t.Context(), frame); err != nil {
		t.Fatal(err)
	}
	_ = stream.EndInput()
	event, err := stream.Recv(t.Context())
	if err != nil || event.Type != vadpkg.InferenceDone || event.Probability != 0 {
		t.Fatalf("no-op event = %+v, %v", event, err)
	}
	if _, err := stream.Recv(t.Context()); !errors.Is(err, io.EOF) {
		t.Fatalf("terminal = %v", err)
	}
	_ = detector.Close(t.Context())
}

func TestInferenceVADRejectsMultipleLocalBackends(t *testing.T) {
	_, err := NewVAD(VADOptions{
		PredictorFactory: func(context.Context) (VADPredictor, error) { return nil, nil },
		Executor:         ipc.InferenceExecutorFunc(func(context.Context, string, any) (any, error) { return nil, nil }),
	})
	if err == nil {
		t.Fatal("expected mutually exclusive backend error")
	}
}
