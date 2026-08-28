// SPDX-License-Identifier: Apache-2.0

package llm

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

type weatherArgs struct {
	Location string `json:"location"`
}

func TestTypedToolExecute(t *testing.T) {
	t.Parallel()
	tool := MustTool(FunctionToolOptions[weatherArgs, string]{
		Name: "weather", Description: "look up weather",
		Parameters: json.RawMessage(`{"type":"object","properties":{"location":{"type":"string"}},"required":["location"]}`),
		Validate: func(input *weatherArgs) error {
			if input.Location == "" {
				return errors.New("location required")
			}
			return nil
		},
		Execute: func(_ context.Context, input weatherArgs, _ ToolOptions) (string, error) {
			return "sunny in " + input.Location, nil
		},
	})
	result, err := tool.Execute(context.Background(), json.RawMessage(`{"location":"Paris"}`), ToolOptions{})
	if err != nil || result != "sunny in Paris" {
		t.Fatalf("Execute() = %#v, %v", result, err)
	}
	if _, err := tool.Execute(context.Background(), json.RawMessage(`{"location":"Paris","unknown":1}`), ToolOptions{}); err == nil {
		t.Fatal("unknown argument accepted")
	}
}

func TestToolContextDeterministicNames(t *testing.T) {
	t.Parallel()
	a := MustTool(FunctionToolOptions[struct{}, struct{}]{Name: "zeta", Execute: func(context.Context, struct{}, ToolOptions) (struct{}, error) { return struct{}{}, nil }})
	b := MustTool(FunctionToolOptions[struct{}, struct{}]{Name: "alpha", Execute: func(context.Context, struct{}, ToolOptions) (struct{}, error) { return struct{}{}, nil }})
	ctx, err := NewToolContext(a, b)
	if err != nil {
		t.Fatal(err)
	}
	names := ctx.SortedToolNames()
	if len(names) != 2 || names[0] != "alpha" || names[1] != "zeta" {
		t.Fatalf("names = %v", names)
	}
}
