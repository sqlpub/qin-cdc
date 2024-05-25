package outputs

import (
	"github.com/sqlpub/qin-cdc/outputs/starrocks"
	"github.com/sqlpub/qin-cdc/registry"
)

func init() {
	// registry output plugins
	registry.RegisterPlugin(registry.OutputPlugin, starrocks.PluginName, &starrocks.OutputPlugin{})
	registry.RegisterPlugin(registry.MetaPlugin, starrocks.PluginName, &starrocks.MetaPlugin{})
}
