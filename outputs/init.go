package outputs

import (
	"github.com/sqlpub/qin-cdc/outputs/doris"
	"github.com/sqlpub/qin-cdc/outputs/kafka"
	"github.com/sqlpub/qin-cdc/outputs/mysql"
	"github.com/sqlpub/qin-cdc/outputs/starrocks"
	"github.com/sqlpub/qin-cdc/registry"
)

func init() {
	// registry output plugins
	registry.RegisterPlugin(registry.OutputPlugin, starrocks.PluginName, &starrocks.OutputPlugin{})
	registry.RegisterPlugin(registry.MetaPlugin, string(registry.OutputPlugin+starrocks.PluginName), &starrocks.MetaPlugin{})

	registry.RegisterPlugin(registry.OutputPlugin, doris.PluginName, &doris.OutputPlugin{})
	registry.RegisterPlugin(registry.MetaPlugin, string(registry.OutputPlugin+doris.PluginName), &doris.MetaPlugin{})

	registry.RegisterPlugin(registry.OutputPlugin, mysql.PluginName, &mysql.OutputPlugin{})
	registry.RegisterPlugin(registry.MetaPlugin, string(registry.OutputPlugin+mysql.PluginName), &mysql.MetaPlugin{})

	registry.RegisterPlugin(registry.OutputPlugin, kafka.PluginName, &kafka.OutputPlugin{})
	registry.RegisterPlugin(registry.MetaPlugin, string(registry.OutputPlugin+kafka.PluginName), &kafka.MetaPlugin{})
}
