// SPDX-License-Identifier: Apache-2.0

package agents

import (
	"context"
	"testing"
)

func TestDefineAgentValidationAndMarker(t *testing.T) {
	t.Parallel()
	if _, err := DefineAgent(AgentDefinitionOptions[struct{}]{}); err == nil {
		t.Fatal("DefineAgent accepted a nil entrypoint")
	}
	entry := func(context.Context, *JobContext[struct{}]) error { return nil }
	definition, err := DefineAgent(AgentDefinitionOptions[struct{}]{Entrypoint: entry})
	if err != nil {
		t.Fatal(err)
	}
	if !IsAgent(definition) || IsAgent(struct{}{}) || IsAgent((*AgentDefinition[struct{}])(nil)) || IsAgent(new(AgentDefinition[struct{}])) {
		t.Fatal("agent definition marker accepted or rejected the wrong value")
	}
	if definition.Entrypoint() == nil || definition.Prewarm() != nil || definition.SimulationEnd() != nil {
		t.Fatalf("unexpected definition: %#v", definition)
	}
}
