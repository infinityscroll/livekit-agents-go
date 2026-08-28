// SPDX-License-Identifier: Apache-2.0

package elevenlabs

import (
	"context"

	agents "github.com/livekit/agents-go"
)

const Version = "1.7.1"

type Plugin struct{}

func (Plugin) Title() string                       { return "ElevenLabs" }
func (Plugin) Version() string                     { return Version }
func (Plugin) Package() string                     { return "github.com/livekit/agents-go/plugins/elevenlabs" }
func (Plugin) DownloadFiles(context.Context) error { return nil }

// Register adds the plugin metadata to the process registry. Registration is
// explicit so importing this package has no startup work or global side effect.
func Register() error { return agents.RegisterPlugin(Plugin{}) }

var _ agents.Plugin = Plugin{}
