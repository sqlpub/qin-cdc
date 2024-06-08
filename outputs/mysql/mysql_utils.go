package mysql

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	"github.com/siddontang/go-log/log"
	"github.com/sqlpub/qin-cdc/config"
	"github.com/sqlpub/qin-cdc/core"
	"github.com/sqlpub/qin-cdc/metas"
	"strings"
	"time"
)

const (
	PluginName                 = "mysql"
	DefaultBatchSize       int = 10240
	DefaultBatchIntervalMs int = 100
	RetryCount             int = 3
	RetryInterval          int = 5
)

func getConn(conf *config.MysqlConfig) (db *sql.DB, err error) {
	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/information_schema?charset=utf8mb4&timeout=3s",
		conf.UserName, conf.Password,
		conf.Host, conf.Port)
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		return db, err
	}
	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(2)
	return db, err
}

func closeConn(db *sql.DB) {
	if db != nil {
		_ = db.Close()
	}
}

func (o *OutputPlugin) generateBulkInsertOnDuplicateKeyUpdateSQL(msgs []*core.Msg, columnsMapper metas.ColumnsMapper, targetSchema string, targetTable string) (string, []interface{}, error) {
	pks := make(map[string]interface{}, len(columnsMapper.PrimaryKeys))
	for _, pk := range columnsMapper.PrimaryKeys {
		pks[pk] = nil
	}

	updateColumnsIdx := 0
	columnNamesAssignWithoutPks := make([]string, len(columnsMapper.MapMapper)-len(columnsMapper.PrimaryKeys))
	allColumnNamesInSQL := make([]string, 0, len(columnsMapper.MapMapper))
	allColumnPlaceHolder := make([]string, 0, len(columnsMapper.MapMapper))
	for _, sourceColumn := range columnsMapper.MapMapperOrder {
		columnNameInSQL := fmt.Sprintf("`%s`", columnsMapper.MapMapper[sourceColumn])
		allColumnNamesInSQL = append(allColumnNamesInSQL, columnNameInSQL)
		allColumnPlaceHolder = append(allColumnPlaceHolder, "?")
		_, ok := pks[sourceColumn]
		if !ok {
			columnNamesAssignWithoutPks[updateColumnsIdx] = fmt.Sprintf("%s = VALUES(%s)", columnNameInSQL, columnNameInSQL)
			updateColumnsIdx++
		}
	}
	sqlInsert := fmt.Sprintf("INSERT INTO `%s`.`%s` (%s) VALUES ",
		targetSchema,
		targetTable,
		strings.Join(allColumnNamesInSQL, ","))
	args := make([]interface{}, 0, len(columnsMapper.MapMapper)*len(msgs))
	for i, msg := range msgs {
		switch msg.DmlMsg.Action {
		case core.InsertAction, core.UpdateAction:
			for _, sourceColumn := range columnsMapper.MapMapperOrder {
				columnData := msg.DmlMsg.Data[sourceColumn]
				args = append(args, columnData)
			}
			if i == 0 {
				sqlInsert += fmt.Sprintf("(%s)", strings.Join(allColumnPlaceHolder, ","))
			} else {
				sqlInsert += fmt.Sprintf(",(%s)", strings.Join(allColumnPlaceHolder, ","))
			}
		default:
			log.Fatalf("unhandled message type: %v", msg)
		}
	}
	sqlUpdate := fmt.Sprintf("ON DUPLICATE KEY UPDATE %s", strings.Join(columnNamesAssignWithoutPks, ","))
	return fmt.Sprintf("%s %s", sqlInsert, sqlUpdate), args, nil
}

func (o *OutputPlugin) generateSingleDeleteSQL(msg *core.Msg, columnsMapper metas.ColumnsMapper, targetSchema string, targetTable string) (string, []interface{}, error) {
	pks := make(map[string]interface{}, len(columnsMapper.PrimaryKeys))
	for _, pk := range columnsMapper.PrimaryKeys {
		pks[pk] = nil
	}

	var whereSql []string
	var args []interface{}
	for sourceColumn, targetColumn := range columnsMapper.MapMapper {
		pkData, ok := pks[sourceColumn]
		if !ok {
			continue
		}
		whereSql = append(whereSql, fmt.Sprintf("`%s` = ?", targetColumn))
		args = append(args, pkData)
	}
	if len(whereSql) == 0 {
		return "", nil, errors.Errorf("where sql is empty, probably missing pk")
	}

	stmt := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s", targetSchema, targetTable, strings.Join(whereSql, " AND "))
	return stmt, args, nil
}

func (o *OutputPlugin) generateBulkDeleteSQL(msgs []*core.Msg, columnsMapper metas.ColumnsMapper, targetSchema string, targetTable string) (string, []interface{}, error) {
	pkName := columnsMapper.PrimaryKeys[0]
	var whereSql []string
	var args []interface{}
	for _, msg := range msgs {
		pkData, ok := msg.DmlMsg.Data[pkName]
		if !ok {
			continue
		}
		whereSql = append(whereSql, "?")
		args = append(args, pkData)

	}
	targetPkName := columnsMapper.MapMapper[pkName]
	if len(whereSql) == 0 {
		return "", nil, errors.Errorf("where sql is empty, probably missing pk")
	}

	stmt := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE `%s` IN (%s)", targetSchema, targetTable, targetPkName, strings.Join(whereSql, ","))
	return stmt, args, nil
}
