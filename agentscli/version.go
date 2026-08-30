// SPDX-License-Identifier: Apache-2.0

package agentscli

import (
	"fmt"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"

	agents "github.com/infinityscroll/livekit-agents-go"
	"github.com/livekit/protocol/livekit"
)

type VersionDetails struct {
	SDK            string
	WorkerProtocol int
	Go             string
	VCSRevision    string
	VCSModified    bool
	Dependencies   map[string]string
}

func VersionInfo() VersionDetails {
	result := VersionDetails{
		SDK: agents.Version, WorkerProtocol: livekit.CurrentWorkerProtocol,
		Go: runtime.Version(), Dependencies: make(map[string]string),
	}
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dependency := range info.Deps {
			switch dependency.Path {
			case "github.com/livekit/protocol", "github.com/livekit/server-sdk-go/v2":
				version := dependency.Version
				if dependency.Replace != nil {
					version = dependency.Replace.Version
					if version == "" {
						version = dependency.Replace.Path
					}
				}
				result.Dependencies[dependency.Path] = version
			}
		}
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				result.VCSRevision = setting.Value
			case "vcs.modified":
				result.VCSModified = setting.Value == "true"
			}
		}
	}
	return result
}

func FormatVersionInfo() string {
	info := VersionInfo()
	lines := []string{
		fmt.Sprintf("livekit-agents-go %s", info.SDK),
		fmt.Sprintf("worker-protocol %d", info.WorkerProtocol),
		fmt.Sprintf("go %s", info.Go),
	}
	dependencies := make([]string, 0, len(info.Dependencies))
	for module := range info.Dependencies {
		dependencies = append(dependencies, module)
	}
	sort.Strings(dependencies)
	for _, module := range dependencies {
		lines = append(lines, module+" "+info.Dependencies[module])
	}
	if info.VCSRevision != "" {
		revision := info.VCSRevision
		if info.VCSModified {
			revision += " (modified)"
		}
		lines = append(lines, "vcs "+revision)
	}
	return strings.Join(lines, "\n")
}
