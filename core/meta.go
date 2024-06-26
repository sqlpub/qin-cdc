package core

import (
	"github.com/juju/errors"
	"github.com/sqlpub/qin-cdc/metas"
)

type Meta interface {
	LoadMeta(routers []*metas.Router) error
	Get(schema string, tableName string) (*metas.Table, error)
	GetAll() map[string]*metas.Table
	GetVersion(schema string, tableName string, version uint) (*metas.Table, error)
	Add(*metas.Table) error
	Update(newTable *metas.Table) error
	Delete(string, string) error
	Save() error
	Close()
}

type Metas struct {
	Input   Meta
	Output  Meta
	Routers *metas.Routers
}

func (m *Metas) InitRouterColumnsMapper() error {
	// router column mapper
	for _, router := range m.Routers.Raws {
		table, err := m.Input.Get(router.SourceSchema, router.SourceTable)
		if err != nil {
			return err
		}
		if table == nil {
			return errors.Errorf("get input table meta failed, err: %s.%s not found", router.SourceSchema, router.SourceTable)
		}
		for _, column := range table.Columns {
			router.ColumnsMapper.SourceColumns = append(router.ColumnsMapper.SourceColumns, column.Name)
			if column.IsPrimaryKey {
				router.ColumnsMapper.PrimaryKeys = append(router.ColumnsMapper.PrimaryKeys, column.Name)
			}
		}
		table, err = m.Output.Get(router.TargetSchema, router.TargetTable)
		if err != nil {
			return err
		}
		if table == nil {
			return errors.Errorf("get output table meta failed, err: %s.%s not found", router.TargetSchema, router.TargetTable)
		}
		for _, column := range table.Columns {
			router.ColumnsMapper.TargetColumns = append(router.ColumnsMapper.TargetColumns, column.Name)
		}
	}
	return nil
}

func (m *Metas) InitRouterColumnsMapperMapMapper() {
	// router column mapper MapMapper
	for _, router := range m.Routers.Raws {
		mapMapper := make(map[string]string)
		mapMapperOrder := make([]string, 0)
		// user config output.config.routers.columns-mapper.map-mapper
		if len(router.ColumnsMapper.MapMapper) > 0 {
			for i, column := range router.ColumnsMapper.SourceColumns {
				mapMapper[column] = router.ColumnsMapper.TargetColumns[i]
				mapMapperOrder = append(mapMapperOrder, column)
			}
		} else {
			for _, column := range router.ColumnsMapper.SourceColumns {
				// same name mapping
				for _, targetColumn := range router.ColumnsMapper.TargetColumns {
					if column == targetColumn {
						mapMapper[column] = targetColumn
						mapMapperOrder = append(mapMapperOrder, column)
						break
					}
				}
			}
		}
		router.ColumnsMapper.MapMapper = mapMapper
		router.ColumnsMapper.MapMapperOrder = mapMapperOrder
	}
}
