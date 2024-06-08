package mysql

import (
	"database/sql"
	"github.com/juju/errors"
	"github.com/mitchellh/mapstructure"
	"github.com/siddontang/go-log/log"
	"github.com/sqlpub/qin-cdc/config"
	"github.com/sqlpub/qin-cdc/core"
	"github.com/sqlpub/qin-cdc/metas"
	"github.com/sqlpub/qin-cdc/metrics"
	"time"
)

type OutputPlugin struct {
	*config.MysqlConfig
	Done         chan bool
	metas        *core.Metas
	msgTxnBuffer struct {
		size        int
		tableMsgMap map[string][]*core.Msg
	}
	client       *sql.DB
	lastPosition string
}

func (o *OutputPlugin) Configure(conf map[string]interface{}) error {
	o.MysqlConfig = &config.MysqlConfig{}
	var targetConf = conf["target"]
	if err := mapstructure.Decode(targetConf, o.MysqlConfig); err != nil {
		return err
	}
	return nil
}

func (o *OutputPlugin) NewOutput(metas *core.Metas) {
	o.Done = make(chan bool)
	o.metas = metas
	// options handle
	if o.MysqlConfig.Options.BatchSize == 0 {
		o.MysqlConfig.Options.BatchSize = DefaultBatchSize
	}
	if o.MysqlConfig.Options.BatchIntervalMs == 0 {
		o.MysqlConfig.Options.BatchIntervalMs = DefaultBatchIntervalMs
	}
	o.msgTxnBuffer.size = 0
	o.msgTxnBuffer.tableMsgMap = make(map[string][]*core.Msg)

	var err error
	o.client, err = getConn(o.MysqlConfig)
	if err != nil {
		log.Fatal("output config client failed. err: ", err.Error())
	}
}

func (o *OutputPlugin) Start(out chan *core.Msg, pos core.Position) {
	// first pos
	o.lastPosition = pos.Get()
	go func() {
		ticker := time.NewTicker(time.Millisecond * time.Duration(o.Options.BatchIntervalMs))
		defer ticker.Stop()
		for {
			select {
			case data := <-out:
				switch data.Type {
				case core.MsgCtl:
					o.lastPosition = data.InputContext.Pos
				case core.MsgDML:
					o.appendMsgTxnBuffer(data)
					if o.msgTxnBuffer.size >= o.MysqlConfig.Options.BatchSize {
						o.flushMsgTxnBuffer(pos)
					}
				}
			case <-ticker.C:
				o.flushMsgTxnBuffer(pos)
			case <-o.Done:
				o.flushMsgTxnBuffer(pos)
				return
			}

		}
	}()
}

func (o *OutputPlugin) Close() {
	log.Infof("output is closing...")
	close(o.Done)
	<-o.Done
	closeConn(o.client)
	log.Infof("output is closed")
}

func (o *OutputPlugin) appendMsgTxnBuffer(msg *core.Msg) {
	key := metas.GenerateMapRouterKey(msg.Database, msg.Table)
	o.msgTxnBuffer.tableMsgMap[key] = append(o.msgTxnBuffer.tableMsgMap[key], msg)
	o.msgTxnBuffer.size += 1
}

func (o *OutputPlugin) flushMsgTxnBuffer(pos core.Position) {
	defer func() {
		// flush position
		err := pos.Update(o.lastPosition)
		if err != nil {
			log.Fatalf(err.Error())
		}
	}()

	if o.msgTxnBuffer.size == 0 {
		return
	}
	// table level export
	for k, msgs := range o.msgTxnBuffer.tableMsgMap {
		columnsMapper := o.metas.Routers.Maps[k].ColumnsMapper
		targetSchema := o.metas.Routers.Maps[k].TargetSchema
		targetTable := o.metas.Routers.Maps[k].TargetTable
		err := o.execute(msgs, columnsMapper, targetSchema, targetTable)
		if err != nil {
			log.Fatalf("do %s bulk err %v", PluginName, err)
		}
	}
	o.clearMsgTxnBuffer()
}

func (o *OutputPlugin) clearMsgTxnBuffer() {
	o.msgTxnBuffer.size = 0
	o.msgTxnBuffer.tableMsgMap = make(map[string][]*core.Msg)
}

func (o *OutputPlugin) execute(msgs []*core.Msg, columnsMapper metas.ColumnsMapper, targetSchema string, targetTable string) error {
	if len(columnsMapper.PrimaryKeys) == 0 {
		return errors.Errorf("only support data has primary key")
	}

	splitMsgsList := o.splitMsgs(msgs)
	for _, splitMsgs := range splitMsgsList {
		if splitMsgs[0].DmlMsg.Action != core.DeleteAction {
			// insert and update can bulk exec
			bulkSQL, args, err := o.generateBulkInsertOnDuplicateKeyUpdateSQL(splitMsgs, columnsMapper, targetSchema, targetTable)
			err = o.executeSQL(bulkSQL, args)
			if err != nil {
				return err
			}
			log.Debugf("output %s sql: %v; args: %v", PluginName, bulkSQL, args)
		} else {
			if len(columnsMapper.PrimaryKeys) > 1 { // multi-pk single sql exec (delete from table where pk1 = ? and pk2 = ? ...)
				for _, msg := range splitMsgs {
					singleSQL, args, err := o.generateSingleDeleteSQL(msg, columnsMapper, targetSchema, targetTable)
					err = o.executeSQL(singleSQL, args)
					if err != nil {
						return err
					}
					log.Debugf("output %s sql: %v; args: %v", PluginName, singleSQL, args)
				}

			} else { // one-pk bulk sql exec (delete from table where pk in (?,? ..))
				bulkSQL, args, err := o.generateBulkDeleteSQL(splitMsgs, columnsMapper, targetSchema, targetTable)
				err = o.executeSQL(bulkSQL, args)
				if err != nil {
					return err
				}
				log.Debugf("output %s sql: %v; args: %v", PluginName, bulkSQL, args)
			}
		}
		// log.Debugf("%s bulk sync %s.%s row data num: %d", PluginName, targetSchema, targetTable, len(msgs))

		// prom write event number counter
		metrics.OpsWriteProcessed.Add(float64(len(splitMsgs)))
	}
	return nil
}

func (o *OutputPlugin) splitMsgs(msgs []*core.Msg) [][]*core.Msg {
	msgsList := make([][]*core.Msg, 0)
	tmpMsgs := make([]*core.Msg, 0)
	tmpDeleteMsgs := make([]*core.Msg, 0)
	var nextMsgAction core.ActionType
	lenMsgs := len(msgs)
	// split delete msg
	for i, msg := range msgs {
		if i < lenMsgs-1 {
			nextMsgAction = msgs[i+1].DmlMsg.Action
		} else {
			// last msg
			nextMsgAction = ""
		}

		if msg.DmlMsg.Action == core.DeleteAction {
			tmpDeleteMsgs = append(tmpDeleteMsgs, msg)
			if nextMsgAction != core.DeleteAction {
				msgsList = append(msgsList, tmpDeleteMsgs)
				tmpDeleteMsgs = make([]*core.Msg, 0)
			}
		} else {
			tmpMsgs = append(tmpMsgs, msg)
			if nextMsgAction != core.InsertAction && nextMsgAction != core.UpdateAction {
				msgsList = append(msgsList, tmpMsgs)
				tmpMsgs = make([]*core.Msg, 0)
			}
		}
	}
	return msgsList
}

func (o *OutputPlugin) executeSQL(sqlCmd string, args []interface{}) error {
	var err error
	var result sql.Result
	for i := 0; i < RetryCount; i++ {
		result, err = o.client.Exec(sqlCmd, args...)
		if err != nil {
			log.Warnf("exec data failed, err: %v, execute retry...", err.Error())
			if i+1 == RetryCount {
				break
			}
			time.Sleep(time.Duration(RetryInterval*(i+1)) * time.Second)
			continue
		}
		break
	}
	if err != nil {
		return err
	}
	if result == nil {
		return errors.Errorf("execute bulksql retry failed, result is nil")
	}
	return err
}
