// SPDX-License-Identifier: Apache-2.0

package llm

import (
	"context"
	"testing"
)

func TestAsyncToolsetMarkerAndOptionsSurviveContextCopy(t *testing.T) {
	tool := MustTool(FunctionToolOptions[struct{}, string]{
		Name: "lookup",
		Execute: func(context.Context, struct{}, ToolOptions) (string, error) {
			return "ok", nil
		},
	})
	set, err := NewAsyncToolset(AsyncToolsetOptions{
		ID: "background", Tools: []Tool{tool},
		ToolHandling: &AsyncToolOptions{UpdateTemplate: "custom {message}"},
	})
	if err != nil {
		t.Fatal(err)
	}
	tools, err := NewToolContext(set)
	if err != nil {
		t.Fatal(err)
	}
	copy := tools.Copy()
	sets := copy.Toolsets()
	if len(sets) != 1 || sets[0] != set.Toolset {
		t.Fatalf("copied toolsets = %#v", sets)
	}
	marker, ok := sets[0].Async()
	if !ok || marker != set {
		t.Fatalf("async marker = %#v, %v", marker, ok)
	}
	options := marker.ToolHandling()
	if options == nil || options.UpdateTemplate != "custom {message}" {
		t.Fatalf("options = %#v", options)
	}
	options.UpdateTemplate = "mutated"
	if got := marker.ToolHandling().UpdateTemplate; got != "custom {message}" {
		t.Fatalf("ToolHandling exposed mutable storage: %q", got)
	}
	if registered, ok := copy.FunctionTool("lookup"); !ok || registered != tool {
		t.Fatalf("registered tool = %#v, %v", registered, ok)
	}
}

func TestAsyncToolsetRejectsInvalidBaseOptions(t *testing.T) {
	if _, err := NewAsyncToolset(AsyncToolsetOptions{}); err == nil {
		t.Fatal("empty async toolset id was accepted")
	}
	var nilSet *AsyncToolset
	if _, err := NewToolContext(nilSet); err == nil {
		t.Fatal("nil async toolset was accepted")
	}
}
