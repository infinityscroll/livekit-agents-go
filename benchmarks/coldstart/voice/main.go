// SPDX-License-Identifier: Apache-2.0

package main

import (
	"runtime"

	"github.com/livekit/agents-go/voice"
)

var linked = voice.NewAgentSession[struct{}]

func main() { runtime.KeepAlive(linked) }
