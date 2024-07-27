package mysql

import (
	"github.com/mitchellh/mapstructure"
	"github.com/siddontang/go-log/log"
	"github.com/sqlpub/qin-cdc/config"
	"github.com/sqlpub/qin-cdc/core"
)

type InputPlugin struct {
	*config.MysqlConfig
	in           chan *core.Msg
	metas        *core.Metas
	metaPlugin   *MetaPlugin
	binlogTailer *BinlogTailer
}

func (i *InputPlugin) Configure(conf map[string]interface{}) error {
	i.MysqlConfig = &config.MysqlConfig{}
	var source = conf["source"]
	if err := mapstructure.Decode(source, i.MysqlConfig); err != nil {
		return err
	}
	return nil
}

func (i *InputPlugin) NewInput(metas *core.Metas) {
	i.metas = metas
	i.metaPlugin = metas.Input.(*MetaPlugin)
}

func (i *InputPlugin) Start(pos core.Position, in chan *core.Msg) {
	i.in = in
	var positionPlugin = &PositionPlugin{}
	if err := mapstructure.Decode(pos, positionPlugin); err != nil {
		log.Fatalf("mysql position parsing failed. err: %s", err.Error())
	}

	i.binlogTailer = &BinlogTailer{}
	i.binlogTailer.New(i)
	go i.binlogTailer.Start(positionPlugin.pos)
}

func (i *InputPlugin) Close() {
	i.binlogTailer.Close()
	close(i.in)
}
