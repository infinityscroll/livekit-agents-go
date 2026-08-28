// SPDX-License-Identifier: Apache-2.0

package agents

import (
	"context"
	"os"
	"os/exec"
	"strings"
)

const (
	// FFmpegPathEnvironment is the TypeScript-compatible executable override.
	FFmpegPathEnvironment = "LIVEKIT_FFMPEG_PATH"
	// FFmpegPathEnv is retained as the shorter compatibility spelling.
	FFmpegPathEnv = FFmpegPathEnvironment
)

// ResolveFFmpegPath resolves LIVEKIT_FFMPEG_PATH first and then ffmpeg on
// PATH. It performs no work at package initialization and returns an empty
// string when no executable can be found.
func ResolveFFmpegPath() string {
	if configured := strings.TrimSpace(os.Getenv(FFmpegPathEnvironment)); configured != "" {
		return configured
	}
	resolved, err := exec.LookPath("ffmpeg")
	if err != nil {
		return ""
	}
	return resolved
}

// ConfigureFFmpeg mirrors agents-js's explicit setup hook. Go callers do not
// need process-global configuration, so the resolved executable is returned
// for dependency injection instead.
func ConfigureFFmpeg() string { return ResolveFFmpegPath() }

// FFmpegCommand constructs a cancellation-aware command using the standard
// SDK resolution rules. Starting the command reports the usual exec error if
// no executable is installed.
func FFmpegCommand(ctx context.Context, args ...string) *exec.Cmd {
	if ctx == nil {
		ctx = context.Background()
	}
	path := ResolveFFmpegPath()
	if path == "" {
		path = "ffmpeg"
	}
	return exec.CommandContext(ctx, path, args...)
}
