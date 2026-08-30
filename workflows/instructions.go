// SPDX-License-Identifier: Apache-2.0

// Package workflows contains reusable, context-first agent workflows.
package workflows

import "github.com/infinityscroll/livekit-agents-go/llm"

// InstructionPart is one replaceable section of a built-in workflow prompt.
// TextInstruction is the convenient form for ordinary text; ModalInstruction
// preserves separate audio and text instructions.
type InstructionPart struct {
	instructions llm.Instructions
}

// TextInstruction constructs a workflow instruction section from plain text.
func TextInstruction(value string) InstructionPart {
	return InstructionPart{instructions: llm.NewInstructions(value, "")}
}

// ModalInstruction constructs a workflow instruction section from LiveKit's
// modality-aware Instructions value.
func ModalInstruction(value llm.Instructions) InstructionPart {
	return InstructionPart{instructions: value}
}

// Value returns the audio/default representation of the instruction section.
func (p InstructionPart) Value() string { return p.instructions.Value() }

// Instructions returns the modality-aware instruction value.
func (p InstructionPart) Instructions() llm.Instructions { return p.instructions }

// InstructionParts customizes sections of built-in workflow prompts. Nil
// preserves the workflow default; a pointer to TextInstruction("") removes the
// section. This distinction matches agents-js and agents-python.
type InstructionParts struct {
	Persona *InstructionPart
	Extra   *InstructionPart
}

// InstructionPartPtr is a convenience for struct literals.
func InstructionPartPtr(value InstructionPart) *InstructionPart { return &value }
