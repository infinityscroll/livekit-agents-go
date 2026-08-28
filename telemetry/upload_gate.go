// SPDX-License-Identifier: Apache-2.0

package telemetry

import (
	"bytes"
	"log/slog"
	"sync"
)

var disabledUploadMarkers = [][]byte{
	[]byte("data recording is disabled"),
	[]byte("disabled by owner"),
}

// UploadGate is a session-generation latch that suppresses all subsequent
// Cloud telemetry after the project reports that data recording is disabled.
// Generations prevent a delayed response from a previous session disabling a
// newly started one.
type UploadGate struct {
	mu         sync.RWMutex
	disabled   bool
	generation uint64
	logger     *slog.Logger
}

func NewUploadGate(logger *slog.Logger) *UploadGate {
	if logger == nil {
		logger = slog.Default()
	}
	return &UploadGate{logger: logger}
}

var DefaultUploadGate = NewUploadGate(nil)

func (g *UploadGate) Reset() uint64 {
	if g == nil {
		return 0
	}
	g.mu.Lock()
	g.disabled = false
	g.generation++
	generation := g.generation
	g.mu.Unlock()
	return generation
}

func (g *UploadGate) Disabled() bool {
	if g == nil {
		return false
	}
	g.mu.RLock()
	disabled := g.disabled
	g.mu.RUnlock()
	return disabled
}

func (g *UploadGate) Generation() uint64 {
	if g == nil {
		return 0
	}
	g.mu.RLock()
	generation := g.generation
	g.mu.RUnlock()
	return generation
}

// Disable returns true only for the first matching disable transition.
func (g *UploadGate) Disable(generation uint64) bool {
	if g == nil {
		return false
	}
	g.mu.Lock()
	if generation != g.generation || g.disabled {
		g.mu.Unlock()
		return false
	}
	g.disabled = true
	logger := g.logger
	g.mu.Unlock()
	logger.Warn("LiveKit Cloud data recording is disabled for this project; skipping telemetry and recording uploads for this session")
	return true
}

func (g *UploadGate) IsDisabledResponse(statusCode int, body []byte) bool {
	if statusCode != 401 {
		return false
	}
	lower := bytes.ToLower(body)
	for _, marker := range disabledUploadMarkers {
		if bytes.Contains(lower, marker) {
			return true
		}
	}
	return false
}
