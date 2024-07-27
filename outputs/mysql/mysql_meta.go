package mysql

import (
	"database/sql"
	"fmt"
	"github.com/mitchellh/mapstructure"
	"github.com/sqlpub/qin-cdc/config"
	"github.com/sqlpub/qin-cdc/metas"
	"strings"
	"sync"
)

type MetaPlugin struct {
	*config.MysqlConfig
	tables        map[string]*metas.Table
	tablesVersion map[string]*metas.Table
	db            *sql.DB
	mu            sync.Mutex
}

func (m *MetaPlugin) Configure(conf map[string]interface{}) error {
	m.MysqlConfig = &config.MysqlConfig{}
	var target = conf["target"]
	if err := mapstructure.Decode(target, m.MysqlConfig); err != nil {
		return err
	}
	return nil
}

func (m *MetaPlugin) LoadMeta(routers []*metas.Router) (err error) {
	m.tables = make(map[string]*metas.Table)
	m.tablesVersion = make(map[string]*metas.Table)
	m.db, err = getConn(m.MysqlConfig)
	if err != nil {
		return err
	}
	for _, router := range routers {
		row := m.db.QueryRow(fmt.Sprintf("show create table `%s`.`%s`", router.TargetSchema, router.TargetTable))
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
		table, err = metas.NewTable(createTableDdlStr)
		if err != nil {
			return err
		}
		err = m.Add(table)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *MetaPlugin) GetMeta(router *metas.Router) (table interface{}, err error) {
	return m.Get(router.SourceSchema, router.SourceTable)
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

func (m *MetaPlugin) GetVersion(schema string, tableName string, version uint) (table *metas.Table, err error) {
	key := metas.GenerateMapRouterVersionKey(schema, tableName, version)
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tablesVersion[key], err
}

func (m *MetaPlugin) GetVersions(schema string, tableName string) []*metas.Table {
	m.mu.Lock()
	defer m.mu.Unlock()
	tables := make([]*metas.Table, 0)
	for k, table := range m.tablesVersion {
		s, t, _ := metas.SplitMapRouterVersionKey(k)
		if schema == s && tableName == t {
			tables = append(tables, table)
		}
	}
	return tables
}

func (m *MetaPlugin) Add(newTable *metas.Table) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tables[metas.GenerateMapRouterKey(newTable.Schema, newTable.Name)] = newTable
	m.tablesVersion[metas.GenerateMapRouterVersionKey(newTable.Schema, newTable.Name, newTable.Version)] = newTable
	return nil
}

func (m *MetaPlugin) Update(newTable *metas.Table) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	newTable.Version += 1
	m.tables[metas.GenerateMapRouterKey(newTable.Schema, newTable.Name)] = newTable
	m.tablesVersion[metas.GenerateMapRouterVersionKey(newTable.Schema, newTable.Name, newTable.Version)] = newTable
	return nil
}

func (m *MetaPlugin) Delete(schema string, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.tables, metas.GenerateMapRouterKey(schema, name))
	for _, table := range m.GetVersions(schema, name) {
		delete(m.tablesVersion, metas.GenerateMapRouterVersionKey(schema, name, table.Version))
	}
	return nil
}

func (m *MetaPlugin) Save() error {
	return nil
}

func (m *MetaPlugin) Close() {
	closeConn(m.db)
}
