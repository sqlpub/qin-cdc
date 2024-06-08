package app

import (
	"github.com/juju/errors"
	"github.com/sqlpub/qin-cdc/config"
	"github.com/sqlpub/qin-cdc/core"
	_ "github.com/sqlpub/qin-cdc/inputs"
	"github.com/sqlpub/qin-cdc/metas"
	_ "github.com/sqlpub/qin-cdc/outputs"
	"github.com/sqlpub/qin-cdc/registry"
	"github.com/sqlpub/qin-cdc/transforms"
	"sync"
)

type Server struct {
	Input        core.Input
	Output       core.Output
	Metas        *core.Metas
	Position     core.Position
	Transforms   transforms.MatcherTransforms
	InboundChan  chan *core.Msg
	OutboundChan chan *core.Msg
	sync.Mutex
}

func NewServer(conf *config.Config) (server *Server, err error) {
	server = &Server{}

	// input
	plugin, err := registry.GetPlugin(registry.InputPlugin, conf.InputConfig.Type)
	if err != nil {
		return nil, err
	}
	input, ok := plugin.(core.Input)
	if !ok {
		return nil, errors.Errorf("not a valid input type")
	}
	server.Input = input
	err = plugin.Configure(conf.InputConfig.Config)
	if err != nil {
		return nil, err
	}

	// output
	plugin, err = registry.GetPlugin(registry.OutputPlugin, conf.OutputConfig.Type)
	if err != nil {
		return nil, err
	}
	output, ok := plugin.(core.Output)
	if !ok {
		return nil, errors.Errorf("not a valid output type")
	}
	server.Output = output
	err = plugin.Configure(conf.OutputConfig.Config)
	if err != nil {
		return nil, err
	}

	// meta
	err = server.initMeta(conf)
	if err != nil {
		return nil, err
	}

	// position
	plugin, err = registry.GetPlugin(registry.PositionPlugin, conf.InputConfig.Type)
	if err != nil {
		return nil, err
	}
	position, ok := plugin.(core.Position)
	if !ok {
		return nil, errors.Errorf("not a valid position type")
	}
	server.Position = position
	err = plugin.Configure(conf.InputConfig.Config)
	if err != nil {
		return nil, err
	}

	// sync chan
	server.InboundChan = make(chan *core.Msg, 10240)
	server.OutboundChan = make(chan *core.Msg, 10240)

	// new position
	server.Position.LoadPosition(conf.Name)
	// new output
	server.Output.NewOutput(server.Metas)
	// new input
	server.Input.NewInput(server.Metas)

	return server, nil
}

func (s *Server) initMeta(conf *config.Config) (err error) {
	// Routers
	s.Metas = &core.Metas{
		Routers: &metas.Routers{},
	}
	err = s.Metas.Routers.InitRouters(conf.OutputConfig.Config)
	if err != nil {
		return err
	}

	// input meta
	plugin, err := registry.GetPlugin(registry.MetaPlugin, string(registry.InputPlugin)+conf.InputConfig.Type)
	if err != nil {
		return err
	}
	meta, ok := plugin.(core.Meta)
	if !ok {
		return errors.Errorf("not a valid meta type")
	}

	s.Metas.Input = meta
	err = plugin.Configure(conf.InputConfig.Config)
	if err != nil {
		return err
	}
	err = s.Metas.Input.LoadMeta(s.Metas.Routers.Raws)
	if err != nil {
		return err
	}

	// output meta
	plugin, err = registry.GetPlugin(registry.MetaPlugin, string(registry.OutputPlugin)+conf.OutputConfig.Type)
	if err != nil {
		return err
	}
	meta, ok = plugin.(core.Meta)
	if !ok {
		return errors.Errorf("not a valid meta type")
	}

	s.Metas.Output = meta
	err = plugin.Configure(conf.OutputConfig.Config)
	if err != nil {
		return err
	}
	err = s.Metas.Output.LoadMeta(s.Metas.Routers.Raws)
	if err != nil {
		return err
	}

	// router column mapper
	err = s.Metas.InitRouterColumnsMapper()
	if err != nil {
		return err
	}

	// trans
	s.Transforms = transforms.NewMatcherTransforms(conf.TransformsConfig, s.Metas.Routers)

	// router column mapper MapMapper
	s.Metas.InitRouterColumnsMapperMapMapper()
	return nil
}

func (s *Server) Start() {
	s.Lock()
	defer s.Unlock()

	s.Input.Start(s.Position, s.InboundChan)
	s.Position.Start()
	s.Transforms.Start(s.InboundChan, s.OutboundChan)
	s.Output.Start(s.OutboundChan, s.Position)
}

func (s *Server) Close() {
	s.Lock()
	defer s.Unlock()

	s.Input.Close()
	s.Output.Close()
	s.Position.Close()
	s.Metas.Input.Close()
	s.Metas.Output.Close()
}
