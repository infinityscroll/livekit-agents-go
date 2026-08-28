// SPDX-License-Identifier: Apache-2.0

package main

import (
	"runtime"

	agents "github.com/livekit/agents-go"
)

// Retain the representative worker constructor without starting a worker.
// runtime.KeepAlive prevents the linker from reducing this probe to an empty
// program merely because the SDK has intentionally side-effect-free init.
var linked = agents.NewAgentServer[struct{}]

func main() { runtime.KeepAlive(linked) }
