// SPDX-License-Identifier: Apache-2.0

// Command releaseversion prints the SDK version without a leading v. It keeps
// the release workflow tied to the version exported by the module.
package main

import (
	"fmt"
	"strings"

	agents "github.com/livekit/agents-go"
)

func main() {
	fmt.Print(strings.TrimPrefix(agents.Version, "v"))
}
