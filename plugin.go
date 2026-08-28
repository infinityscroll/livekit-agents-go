// SPDX-License-Identifier: Apache-2.0

package agents

import (
	"context"
	"fmt"
	"sync"
)

type Plugin interface {
	Title() string
	Version() string
	Package() string
	DownloadFiles(context.Context) error
}

type PluginRegisteredEvent struct{ Plugin Plugin }

var globalPlugins = struct {
	sync.RWMutex
	byPackage map[string]Plugin
	events    EventEmitter[PluginRegisteredEvent]
}{}

func RegisterPlugin(plugin Plugin) error {
	if plugin == nil || plugin.Package() == "" || plugin.Title() == "" {
		return fmt.Errorf("invalid plugin metadata")
	}
	globalPlugins.Lock()
	if _, exists := globalPlugins.byPackage[plugin.Package()]; exists {
		globalPlugins.Unlock()
		return fmt.Errorf("plugin %q already registered", plugin.Package())
	}
	if globalPlugins.byPackage == nil {
		globalPlugins.byPackage = make(map[string]Plugin)
	}
	globalPlugins.byPackage[plugin.Package()] = plugin
	globalPlugins.Unlock()
	globalPlugins.events.Emit(PluginRegisteredEvent{Plugin: plugin})
	return nil
}

func RegisteredPlugins() []Plugin {
	globalPlugins.RLock()
	plugins := make([]Plugin, 0, len(globalPlugins.byPackage))
	for _, plugin := range globalPlugins.byPackage {
		plugins = append(plugins, plugin)
	}
	globalPlugins.RUnlock()
	return plugins
}

func OnPluginRegistered(fn func(PluginRegisteredEvent)) func() {
	return globalPlugins.events.Subscribe(fn)
}
