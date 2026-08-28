// SPDX-License-Identifier: Apache-2.0

package agentscli

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	agents "github.com/livekit/agents-go"
)

type PluginDownloadFailure struct {
	Plugin agents.Plugin
	Err    error
}

func FormatDownloadFailureMessage(failures []PluginDownloadFailure) string {
	label := "plugins"
	if len(failures) == 1 {
		label = "plugin"
	}
	var output strings.Builder
	fmt.Fprintf(&output, "Failed to download files for %d %s:", len(failures), label)
	for _, failure := range failures {
		if failure.Plugin == nil {
			fmt.Fprintf(&output, "\n- unknown plugin: %v", failure.Err)
			continue
		}
		fmt.Fprintf(&output, "\n- %s (%s@%s): %v", failure.Plugin.Title(), failure.Plugin.Package(), failure.Plugin.Version(), failure.Err)
	}
	return output.String()
}

func DownloadPluginFiles(ctx context.Context, logger *slog.Logger, plugins []agents.Plugin) []PluginDownloadFailure {
	if ctx == nil {
		ctx = context.Background()
	}
	if logger == nil {
		logger = slog.Default()
	}
	plugins = mergePlugins(plugins)
	failures := make([]PluginDownloadFailure, 0)
	for _, plugin := range plugins {
		if err := ctx.Err(); err != nil {
			failures = append(failures, PluginDownloadFailure{Plugin: plugin, Err: context.Cause(ctx)})
			break
		}
		logger.Info("downloading plugin files", "plugin", plugin.Title(), "package", plugin.Package(), "version", plugin.Version())
		if err := plugin.DownloadFiles(ctx); err != nil {
			failures = append(failures, PluginDownloadFailure{Plugin: plugin, Err: err})
			logger.Error("failed to download plugin files", "plugin", plugin.Title(), "error", err)
			continue
		}
		logger.Info("finished downloading plugin files", "plugin", plugin.Title())
	}
	return failures
}

func mergePlugins(groups ...[]agents.Plugin) []agents.Plugin {
	byPackage := make(map[string]agents.Plugin)
	for _, group := range groups {
		for _, plugin := range group {
			if plugin == nil || plugin.Package() == "" {
				continue
			}
			if _, exists := byPackage[plugin.Package()]; !exists {
				byPackage[plugin.Package()] = plugin
			}
		}
	}
	result := make([]agents.Plugin, 0, len(byPackage))
	for _, plugin := range byPackage {
		result = append(result, plugin)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Package() < result[j].Package() })
	return result
}
