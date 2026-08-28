// SPDX-License-Identifier: Apache-2.0

package agents

import (
	"context"
	"errors"
)

type SimulationEndFunc[T any] func(context.Context, *SimulationContext[T]) error

type AgentDefinitionOptions[T any] struct {
	Entrypoint      JobEntrypoint[T]
	Prewarm         PrewarmFunc[T]
	OnSimulationEnd SimulationEndFunc[T]
}

// AgentDefinition is the validated worker module used by agents-js
// defineAgent. Its fields are immutable after construction so it can be shared
// safely between server setup and process executors.
type AgentDefinition[T any] struct {
	entry           JobEntrypoint[T]
	prewarm         PrewarmFunc[T]
	onSimulationEnd SimulationEndFunc[T]
}

func DefineAgent[T any](options AgentDefinitionOptions[T]) (*AgentDefinition[T], error) {
	if options.Entrypoint == nil {
		return nil, errors.New("agents: agent definition requires an entrypoint")
	}
	return &AgentDefinition[T]{
		entry: options.Entrypoint, prewarm: options.Prewarm,
		onSimulationEnd: options.OnSimulationEnd,
	}, nil
}

func MustDefineAgent[T any](options AgentDefinitionOptions[T]) *AgentDefinition[T] {
	definition, err := DefineAgent(options)
	if err != nil {
		panic(err)
	}
	return definition
}

func (d *AgentDefinition[T]) Entrypoint() JobEntrypoint[T] {
	if d == nil {
		return nil
	}
	return d.entry
}

func (d *AgentDefinition[T]) Prewarm() PrewarmFunc[T] {
	if d == nil {
		return nil
	}
	return d.prewarm
}

func (d *AgentDefinition[T]) SimulationEnd() SimulationEndFunc[T] {
	if d == nil {
		return nil
	}
	return d.onSimulationEnd
}

func (*AgentDefinition[T]) agentDefinitionMarker() {}
func (d *AgentDefinition[T]) validAgentDefinition() bool {
	return d != nil && d.entry != nil
}

// IsAgent reports whether value was produced as a Go AgentDefinition. The
// private marker deliberately rejects look-alike structs.
func IsAgent(value any) bool {
	definition, ok := value.(interface {
		agentDefinitionMarker()
		validAgentDefinition() bool
	})
	return ok && definition.validAgentDefinition()
}
