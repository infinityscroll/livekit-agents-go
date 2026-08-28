// SPDX-License-Identifier: Apache-2.0

package main

import (
	"runtime"

	"github.com/livekit/agents-go/plugins/elevenlabs"
)

var linkedSTT = elevenlabs.NewSTT
var linkedTTS = elevenlabs.NewTTS

func main() {
	runtime.KeepAlive(linkedSTT)
	runtime.KeepAlive(linkedTTS)
}
