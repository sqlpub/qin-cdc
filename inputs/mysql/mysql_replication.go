package mysql

import (
	"context"
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/google/uuid"
	"github.com/siddontang/go-log/log"
	"github.com/sqlpub/qin-cdc/config"
	"github.com/sqlpub/qin-cdc/core"
	"github.com/sqlpub/qin-cdc/metas"
	"regexp"
)
import "github.com/go-mysql-org/go-mysql/replication"

type BinlogTailer struct {
	*config.MysqlConfig
	syncer      *replication.BinlogSyncer
	inputPlugin *InputPlugin
	Pos         mysql.Position
	GSet        mysql.GTIDSet
}

func (b *BinlogTailer) New(inputPlugin *InputPlugin) {
	cfg := replication.BinlogSyncerConfig{
		ServerID: getServerId(inputPlugin.MysqlConfig),
		Flavor:   PluginName,
		Host:     inputPlugin.Host,
		Port:     uint16(inputPlugin.Port),
		User:     inputPlugin.UserName,
		Password: inputPlugin.Password,
		Charset:  DefaultCharset,
	}
	b.syncer = replication.NewBinlogSyncer(cfg)
	b.inputPlugin = inputPlugin
}

func (b *BinlogTailer) Start(gtid string) {
	gtidSet, err := mysql.ParseGTIDSet(PluginName, gtid)
	if err != nil {
		log.Fatalf("parse gtid %s with flavor %s failed, error: %v", gtid, PluginName, err.Error())
	}
	streamer, _ := b.syncer.StartSyncGTID(gtidSet)
	for {
		ev, err := streamer.GetEvent(context.Background())
		if err == replication.ErrSyncClosed {
			return
		}
		// ev.Dump(os.Stdout)
		switch e := ev.Event.(type) {
		case *replication.RotateEvent:
			b.handleRotateEvent(e)
		case *replication.RowsEvent:
			b.handleRowsEvent(ev)
		case *replication.XIDEvent:
			b.handleXIDEvent(ev)
		case *replication.GTIDEvent:
			b.handleGTIDEvent(e)
		case *replication.QueryEvent:
			b.handleDDLEvent(ev)
		default:
			continue
		}
	}
}

func (b *BinlogTailer) Close() {
	b.syncer.Close()
}

func (b *BinlogTailer) columnCountEqual(e *replication.RowsEvent, tableMeta *metas.Table) bool {
	return int(e.ColumnCount) == len(tableMeta.Columns)
}

func (b *BinlogTailer) handleRotateEvent(e *replication.RotateEvent) {
	b.Pos.Name = string(e.NextLogName)
	b.Pos.Pos = uint32(e.Position)
}

func (b *BinlogTailer) handleRowsEvent(ev *replication.BinlogEvent) {
	e := ev.Event.(*replication.RowsEvent)
	schemaName, tableName := string(e.Table.Schema), string(e.Table.Table)
	tableMeta, _ := b.inputPlugin.metas.Input.Get(schemaName, tableName)
	if tableMeta == nil {
		return
	}
	// column count equal check
	if !b.columnCountEqual(e, tableMeta) {
		log.Fatalf("binlog event row values length: %d != table meta columns length: %d", e.ColumnCount, len(tableMeta.Columns))
	}

	var msgs []*core.Msg
	var err error
	var actionType core.ActionType
	switch ev.Header.EventType {
	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		actionType = core.InsertAction
		msgs, err = b.inputPlugin.NewInsertMsgs(schemaName, tableName, tableMeta, e, ev.Header)
	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		actionType = core.UpdateAction
		msgs, err = b.inputPlugin.NewUpdateMsgs(schemaName, tableName, tableMeta, e, ev.Header)
	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		actionType = core.DeleteAction
		msgs, err = b.inputPlugin.NewDeleteMsgs(schemaName, tableName, tableMeta, e, ev.Header)
	default:
		log.Fatalf("unsupported event type: %s", ev.Header.EventType)
	}
	if err != nil {
		log.Fatalf("%v event handle failed: %s", actionType, err.Error())
	}
	b.inputPlugin.SendMsgs(msgs)
}

func (b *BinlogTailer) handleXIDEvent(ev *replication.BinlogEvent) {
	e := ev.Event.(*replication.XIDEvent)
	msg, err := b.inputPlugin.NewXIDMsg(e, ev.Header)
	if err != nil {
		log.Fatalf("xid event handle failed: %s", err.Error())
	}
	b.inputPlugin.SendMsg(msg)
}

func (b *BinlogTailer) handleGTIDEvent(e *replication.GTIDEvent) {
	var err error
	u, _ := uuid.FromBytes(e.SID)
	b.GSet, err = mysql.ParseMysqlGTIDSet(fmt.Sprintf("%s:%d", u.String(), e.GNO))
	if err != nil {
		log.Fatalf("gtid event handle failed: %v", err)
	}
}

func (b *BinlogTailer) handleDDLEvent(ev *replication.BinlogEvent) {
	e := ev.Event.(*replication.QueryEvent)
	schemaName := string(e.Schema)
	ddlSql := string(e.Query)
	if ddlSql == "BEGIN" {
		return
	}
	ddlStatements, err := TableDdlParser(ddlSql, schemaName)
	if err != nil {
		log.Fatalf("ddl event handle failed: %s", err.Error())
	}
	for _, ddlStatement := range ddlStatements {
		schema := ddlStatement.Schema
		name := ddlStatement.Name
		var table *metas.Table
		table, err = b.inputPlugin.metas.Input.Get(schema, name)
		if err != nil {
			log.Fatalf("ddl event handle get table meta failed: %s", err.Error())
		}

		isSyncTable := false
		isOnlineDdlTable := false
		for _, v := range b.inputPlugin.metas.Input.GetAll() {
			if schema == v.Schema && name == v.Name {
				isSyncTable = true
				break
			}

			// aliyun dms online ddl reg
			aliyunDMSOnlineDdlRegStr := fmt.Sprintf("^tp_\\d+_(ogt|del|ogl)_%s$", v.Name)
			aliyunDMSOnlineDdlReg, err := regexp.Compile(aliyunDMSOnlineDdlRegStr)
			if err != nil {
				log.Fatalf("parse aliyun dms online ddl regexp err %v", err.Error())
			}
			// aliyun dms online ddl reg2
			aliyunDMSOnlineDdlReg2Str := fmt.Sprintf("^tpa_[a-z0-9]+_%v$", v.Name)
			aliyunDMSOnlineDdlReg2, err := regexp.Compile(aliyunDMSOnlineDdlReg2Str)
			if err != nil {
				log.Fatalf("parse aliyun dms online ddl regexp err %v", err.Error())
			}
			// gh-ost online ddl reg
			ghostOnlineDdlRegStr := fmt.Sprintf("^_%s_(gho|ghc|del)$", v.Name)
			ghostOnlineDdlReg, err := regexp.Compile(ghostOnlineDdlRegStr)
			if err != nil {
				log.Fatalf("parse gh-ost online ddl regexp err %v", err.Error())
			}
			if schema == v.Schema &&
				(aliyunDMSOnlineDdlReg.MatchString(name) ||
					aliyunDMSOnlineDdlReg2.MatchString(name) ||
					ghostOnlineDdlReg.MatchString(name)) {
				isOnlineDdlTable = true
				break
			}
		}
		if isSyncTable || isOnlineDdlTable {
			if ddlStatement.IsAlterTable { // alter table
				err = TableDdlHandle(table, ddlStatement.RawSql)
				if err != nil {
					log.Fatalf("ddl event handle failed: %s", err.Error())
				}
				err = b.inputPlugin.metas.Input.Update(table)
				if err != nil {
					log.Fatalf("ddl event handle failed: %s", err.Error())
				}
			} else if ddlStatement.IsCreateTable {
				err = TableDdlHandle(table, ddlStatement.RawSql)
				if err != nil {
					log.Fatalf("ddl event handle failed: %s", err.Error())
				}
				err = b.inputPlugin.metas.Input.Add(table)
				if err != nil {
					log.Fatalf("ddl event handle failed: %s", err.Error())
				}
			} else if ddlStatement.IsDropTable { // drop table
				err = b.inputPlugin.metas.Input.Delete(schema, name)
				if err != nil {
					log.Fatalf("ddl event handle failed: %s", err.Error())
				}
			} else if ddlStatement.IsRenameTable { // rename table
				err = TableDdlHandle(table, ddlStatement.RawSql)
				if err != nil {
					log.Fatalf("ddl event handle failed: %s", err.Error())
				}
				err = b.inputPlugin.metas.Input.Update(table)
				if err != nil {
					log.Fatalf("ddl event handle failed: %s", err.Error())
				}
			} else if ddlStatement.IsTruncateTable { // truncate table
				return
			}
		}
	}
}
