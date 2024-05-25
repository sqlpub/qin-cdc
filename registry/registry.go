package registry

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/siddontang/go-log/log"
	"sync"
)

type PluginType string

const (
	InputPlugin    PluginType = "input"
	OutputPlugin   PluginType = "output"
	MetaPlugin     PluginType = "meta"
	PositionPlugin PluginType = "position"
)

type Plugin interface {
	Configure(data map[string]interface{}) error
}

var registry map[PluginType]map[string]Plugin
var mutex sync.Mutex

func init() {
	registry = make(map[PluginType]map[string]Plugin)
}

func RegisterPlugin(pluginType PluginType, name string, v Plugin) {
	mutex.Lock()
	defer mutex.Unlock()

	log.Debugf("[RegisterPlugin] type: %v, name: %v", pluginType, name)

	_, ok := registry[pluginType]
	if !ok {
		registry[pluginType] = make(map[string]Plugin)
	}

	_, ok = registry[pluginType][name]
	if ok {
		panic(fmt.Sprintf("plugin already exists, type: %v, name: %v", pluginType, name))
	}
	registry[pluginType][name] = v
}

func GetPlugin(pluginType PluginType, name string) (Plugin, error) {
	mutex.Lock()
	defer mutex.Unlock()

	if registry == nil {
		return nil, errors.Errorf("empty registry")
	}

	plugins, ok := registry[pluginType]
	if !ok {
		return nil, errors.Errorf("empty plugin type: %v, name: %v", pluginType, name)
	}
	p, ok := plugins[name]
	if !ok {
		return nil, errors.Errorf("empty plugin, type: %v, name: %v", pluginType, name)
	}
	log.Infof("load %v plugin: %v", pluginType, name)
	return p, nil
}
