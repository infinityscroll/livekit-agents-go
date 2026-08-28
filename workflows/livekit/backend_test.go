// SPDX-License-Identifier: Apache-2.0

package livekit

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/livekit/agents-go/voice/roomio"
	lksdk "github.com/livekit/server-sdk-go/v2"
)

func TestCloneRoomOptionsAreIndependent(t *testing.T) {
	enabled, disabled := true, false
	input := &roomio.RoomInputOptions{AudioEnabled: &enabled, TextEnabled: &disabled, ParticipantKinds: []lksdk.ParticipantKind{lksdk.ParticipantSIP}}
	copy := cloneRoomInputOptions(input)
	*copy.AudioEnabled = false
	copy.ParticipantKinds[0] = lksdk.ParticipantStandard
	if !*input.AudioEnabled || input.ParticipantKinds[0] != lksdk.ParticipantSIP {
		t.Fatal("input options shared mutable state")
	}
	output := &roomio.RoomOutputOptions{AudioEnabled: &enabled, TranscriptionEnabled: &disabled}
	outputCopy := cloneRoomOutputOptions(output)
	*outputCopy.TranscriptionEnabled = true
	if *output.TranscriptionEnabled {
		t.Fatal("output options shared mutable state")
	}
}

func TestBackendCloseRacesPostMergeRegistrationSafely(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	b := &LiveKitWarmTransferBackend[struct{}]{
		options: LiveKitWarmTransferBackendOptions[struct{}]{CloseTimeout: time.Second},
		ctx:     ctx, cancel: cancel, callerEvents: make(chan WarmTransferParticipantEvent), closeDone: make(chan struct{}),
	}
	var workers sync.WaitGroup
	for range 32 {
		workers.Add(1)
		go func() { defer workers.Done(); _ = b.DeleteCallerRoomOnDisconnect(t.Context(), "caller") }()
	}
	workers.Add(1)
	go func() {
		defer workers.Done()
		closeCtx, closeCancel := context.WithTimeout(t.Context(), time.Second)
		defer closeCancel()
		_ = b.Close(closeCtx)
	}()
	workers.Wait()
	closeCtx, closeCancel := context.WithTimeout(t.Context(), time.Second)
	defer closeCancel()
	if err := b.Close(closeCtx); err != nil {
		t.Fatal(err)
	}
}
