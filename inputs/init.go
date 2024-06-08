package inputs

import (
	"github.com/sqlpub/qin-cdc/inputs/mysql"
	"github.com/sqlpub/qin-cdc/registry"
)

func init() {
	// input mysql plugins
	registry.RegisterPlugin(registry.InputPlugin, mysql.PluginName, &mysql.InputPlugin{})
	registry.RegisterPlugin(registry.MetaPlugin, string(registry.InputPlugin+mysql.PluginName), &mysql.MetaPlugin{})
	registry.RegisterPlugin(registry.PositionPlugin, mysql.PluginName, &mysql.PositionPlugin{})
}
