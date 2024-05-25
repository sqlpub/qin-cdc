package mysql

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/mitchellh/mapstructure"
	"github.com/sqlpub/qin-cdc/config"
	"github.com/sqlpub/qin-cdc/metas"
	"strings"
	"sync"
	"time"
)

type MetaPlugin struct {
	*config.MysqlConfig
	tables map[string]*metas.Table
	db     *sql.DB
	mu     sync.Mutex
}

func (m *MetaPlugin) Configure(conf map[string]interface{}) error {
	m.MysqlConfig = &config.MysqlConfig{}
	var source = conf["source"]
	if err := mapstructure.Decode(source, m.MysqlConfig); err != nil {
		return err
	}
	return nil
}

func (m *MetaPlugin) LoadMeta(routers []*metas.Router) (err error) {
	m.tables = make(map[string]*metas.Table)
	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/information_schema?charset=utf8mb4&timeout=3s",
		m.MysqlConfig.UserName, m.MysqlConfig.Password,
		m.MysqlConfig.Host, m.MysqlConfig.Port)
	m.db, err = sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	m.db.SetConnMaxLifetime(time.Minute * 3)
	m.db.SetMaxOpenConns(2)
	m.db.SetMaxIdleConns(2)
	for _, router := range routers {
		row := m.db.QueryRow(fmt.Sprintf("show create table `%s`.`%s`", router.SourceSchema, router.SourceTable))
		if row.Err() != nil {
			return err
		}
		var tableName string
		var createTableDdlStr string
		err = row.Scan(&tableName, &createTableDdlStr)
		if err != nil {
			return err
		}
		createTableDdlStr = strings.Replace(createTableDdlStr, "CREATE TABLE ", fmt.Sprintf("CREATE TABLE `%s`.", router.SourceSchema), 1)
		table := &metas.Table{}
		table, err = NewTable(createTableDdlStr)
		if err != nil {
			return err
		}
		m.tables[metas.GenerateMapRouterKey(router.SourceSchema, router.SourceTable)] = table
	}
	return nil
}

func (m *MetaPlugin) Get(schema string, tableName string) (table *metas.Table, err error) {
	key := metas.GenerateMapRouterKey(schema, tableName)
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tables[key], err
}

func (m *MetaPlugin) GetAll() map[string]*metas.Table {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tables
}

func (m *MetaPlugin) Add(newTable *metas.Table) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tables[metas.GenerateMapRouterKey(newTable.Schema, newTable.Name)] = newTable
	return nil
}

func (m *MetaPlugin) Update(newTable *metas.Table) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tables[metas.GenerateMapRouterKey(newTable.Schema, newTable.Name)] = newTable
	return nil
}

func (m *MetaPlugin) Delete(schema string, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.tables, metas.GenerateMapRouterKey(schema, name))
	return nil
}

func (m *MetaPlugin) Save() error {
	return nil
}

func (m *MetaPlugin) Close() {
	if m.db != nil {
		_ = m.db.Close()
	}
}
