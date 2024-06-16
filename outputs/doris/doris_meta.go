package doris

import (
	"database/sql"
	"fmt"
	"github.com/juju/errors"
	"github.com/mitchellh/mapstructure"
	"github.com/sqlpub/qin-cdc/config"
	"github.com/sqlpub/qin-cdc/metas"
	"sync"
	"time"
)

type MetaPlugin struct {
	*config.DorisConfig
	tables        map[string]*metas.Table
	tablesVersion map[string]*metas.Table
	db            *sql.DB
	mu            sync.Mutex
}

func (m *MetaPlugin) Configure(conf map[string]interface{}) error {
	m.DorisConfig = &config.DorisConfig{}
	var target = conf["target"]
	if err := mapstructure.Decode(target, m.DorisConfig); err != nil {
		return err
	}
	return nil
}

func (m *MetaPlugin) LoadMeta(routers []*metas.Router) (err error) {
	m.tables = make(map[string]*metas.Table)
	m.tablesVersion = make(map[string]*metas.Table)
	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/information_schema?charset=utf8mb4&timeout=3s&interpolateParams=true",
		m.DorisConfig.UserName, m.DorisConfig.Password,
		m.DorisConfig.Host, m.DorisConfig.Port)
	m.db, err = sql.Open("mysql", dsn)
	if err != nil {
		return err
	}
	m.db.SetConnMaxLifetime(time.Minute * 3)
	m.db.SetMaxOpenConns(2)
	m.db.SetMaxIdleConns(2)
	err = m.db.Ping()
	if err != nil {
		return err
	}
	for _, router := range routers {
		rows, err := m.db.Query("select "+
			"column_name,column_default,is_nullable,data_type,column_type,column_key "+
			"from information_schema.columns "+
			"where table_schema = ? and table_name = ? "+
			"order by ordinal_position", router.TargetSchema, router.TargetTable)
		if err != nil {
			return err
		}
		table := &metas.Table{
			Schema: router.TargetSchema,
			Name:   router.TargetTable,
		}
		for rows.Next() {
			var columnName, isNullable, dataType, columnType, columnKey string
			var columnDefault sql.NullString
			err = rows.Scan(&columnName, &columnDefault, &isNullable, &dataType, &columnType, &columnKey)
			if err != nil {
				return err
			}
			var column metas.Column
			column.Name = columnName
			column.RawType = columnType
			switch dataType {
			case "tinyint", "smallint", "mediumint", "int", "bigint":
				column.Type = metas.TypeNumber
			case "float", "double":
				column.Type = metas.TypeFloat
			case "enum":
				column.Type = metas.TypeEnum
			case "set":
				column.Type = metas.TypeSet
			case "datetime":
				column.Type = metas.TypeDatetime
			case "timestamp":
				column.Type = metas.TypeTimestamp
			case "date":
				column.Type = metas.TypeDate
			case "time":
				column.Type = metas.TypeTime
			case "bit":
				column.Type = metas.TypeBit
			case "json":
				column.Type = metas.TypeJson
			case "decimal":
				column.Type = metas.TypeDecimal
			default:
				column.Type = metas.TypeString
			}
			if columnKey == "PRI" {
				column.IsPrimaryKey = true
			}
			table.Columns = append(table.Columns, column)
		}
		if table.Columns == nil {
			return errors.Errorf("load meta %s.%s not found", router.TargetSchema, router.TargetTable)
		}
		err = m.Add(table)
		if err != nil {
			return err
		}
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
	if m.db != nil {
		_ = m.db.Close()
	}
}
