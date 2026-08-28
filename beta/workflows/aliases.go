// SPDX-License-Identifier: Apache-2.0

// Package workflows contains deprecated compatibility aliases. Import the
// stable github.com/livekit/agents-go/workflows package in new code.
package workflows

import stable "github.com/livekit/agents-go/workflows"

type Task = stable.Task
type TaskFactory = stable.TaskFactory
type TaskRegistration = stable.TaskRegistration
type TaskGroup = stable.TaskGroup
type TaskGroupOptions = stable.TaskGroupOptions
type TaskGroupResult = stable.TaskGroupResult
type TaskCompletedEvent = stable.TaskCompletedEvent
type InstructionPart = stable.InstructionPart
type InstructionParts = stable.InstructionParts
type WarmTransferResult = stable.WarmTransferResult
type WarmTransferSpeech = stable.WarmTransferSpeech
type WarmTransferInstructionConfig = stable.WarmTransferInstructionConfig
type WarmTransferBackend = stable.WarmTransferBackend
type WarmTransferDialRequest = stable.WarmTransferDialRequest
type WarmTransferTask[UserData any] = stable.WarmTransferTask[UserData]
type WarmTransferTaskOptions[UserData any] = stable.WarmTransferTaskOptions[UserData]

var NewTaskGroup = stable.NewTaskGroup
var MustTaskGroup = stable.MustTaskGroup
var TextInstruction = stable.TextInstruction
var ModalInstruction = stable.ModalInstruction
var InstructionPartPtr = stable.InstructionPartPtr
var TextSpeech = stable.TextSpeech
var SpeechFunc = stable.SpeechFunc
var FullWarmTransferInstructions = stable.FullWarmTransferInstructions
var PartialWarmTransferInstructions = stable.PartialWarmTransferInstructions
var ResolveWarmTransferInstructions = stable.ResolveWarmTransferInstructions
var ResolveHumanAgentRoomName = stable.ResolveHumanAgentRoomName

func NewWarmTransferTask[UserData any](options WarmTransferTaskOptions[UserData]) (*WarmTransferTask[UserData], error) {
	return stable.NewWarmTransferTask(options)
}

func CreateWarmTransferTask[UserData any](options WarmTransferTaskOptions[UserData]) (*WarmTransferTask[UserData], error) {
	return stable.CreateWarmTransferTask(options)
}

func MustWarmTransferTask[UserData any](options WarmTransferTaskOptions[UserData]) *WarmTransferTask[UserData] {
	return stable.MustWarmTransferTask(options)
}
