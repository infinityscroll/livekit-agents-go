// SPDX-License-Identifier: Apache-2.0

// Package beta retains the temporary agents-js compatibility surface. Stable
// workflows should be imported from github.com/livekit/agents-go/workflows.
package beta

import (
	"context"

	agents "github.com/livekit/agents-go"
	betatools "github.com/livekit/agents-go/beta/tools"
	"github.com/livekit/agents-go/llm"
	stable "github.com/livekit/agents-go/workflows"
)

type Instructions = llm.Instructions

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
type WarmTransferTask[UserData any] = stable.WarmTransferTask[UserData]
type WarmTransferTaskOptions[UserData any] = stable.WarmTransferTaskOptions[UserData]

var NewTaskGroup = stable.NewTaskGroup
var MustTaskGroup = stable.MustTaskGroup

func NewWarmTransferTask[UserData any](options WarmTransferTaskOptions[UserData]) (*WarmTransferTask[UserData], error) {
	return stable.NewWarmTransferTask(options)
}
func CreateWarmTransferTask[UserData any](options WarmTransferTaskOptions[UserData]) (*WarmTransferTask[UserData], error) {
	return stable.CreateWarmTransferTask(options)
}

type DTMFEvent = betatools.DTMFEvent
type DtmfEvent = betatools.DtmfEvent
type DTMFPublisher = betatools.DTMFPublisher
type SendDTMFOptions = betatools.SendDTMFOptions
type SendDTMFEventsToolOptions = betatools.SendDTMFEventsToolOptions
type SendDTMFEventsInput = betatools.SendDTMFEventsInput

const (
	DTMF0                = betatools.DTMF0
	DTMF1                = betatools.DTMF1
	DTMF2                = betatools.DTMF2
	DTMF3                = betatools.DTMF3
	DTMF4                = betatools.DTMF4
	DTMF5                = betatools.DTMF5
	DTMF6                = betatools.DTMF6
	DTMF7                = betatools.DTMF7
	DTMF8                = betatools.DTMF8
	DTMF9                = betatools.DTMF9
	DTMFStar             = betatools.DTMFStar
	DTMFPound            = betatools.DTMFPound
	DTMFA                = betatools.DTMFA
	DTMFB                = betatools.DTMFB
	DTMFC                = betatools.DTMFC
	DTMFD                = betatools.DTMFD
	EndCallDescription   = betatools.EndCallDescription
	END_CALL_DESCRIPTION = betatools.END_CALL_DESCRIPTION
)

var DTMFEvents = betatools.DTMFEvents
var DTMFEventCode = betatools.DTMFEventCode
var NewSendDTMFEventsTool = betatools.NewSendDTMFEventsTool
var NewSendDtmfEventsTool = betatools.NewSendDtmfEventsTool

func SendDTMFEvents(ctx context.Context, publisher DTMFPublisher, events []DTMFEvent, options SendDTMFOptions) string {
	return betatools.SendDTMFEvents(ctx, publisher, events, options)
}
func SendDtmfEvents(ctx context.Context, publisher DTMFPublisher, events []DtmfEvent, options SendDTMFOptions) string {
	return betatools.SendDtmfEvents(ctx, publisher, events, options)
}
func AdaptDTMFJobContext[UserData any](job *agents.JobContext[UserData]) DTMFPublisher {
	return betatools.AdaptDTMFJobContext(job)
}

type EndCallJob = betatools.EndCallJob
type EndCallSession = betatools.EndCallSession
type EndCallToolOutput = betatools.EndCallToolOutput
type EndCallToolCalledEvent[UserData any] = betatools.EndCallToolCalledEvent[UserData]
type EndCallToolCompletedEvent[UserData any] = betatools.EndCallToolCompletedEvent[UserData]
type EndCallToolOptions[UserData any] = betatools.EndCallToolOptions[UserData]

func CreateEndCallTool[UserData any](options EndCallToolOptions[UserData]) (*llm.Toolset, error) {
	return betatools.CreateEndCallTool(options)
}
func NewEndCallTool[UserData any](options EndCallToolOptions[UserData]) (*llm.Toolset, error) {
	return betatools.NewEndCallTool(options)
}
