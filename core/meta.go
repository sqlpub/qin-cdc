package core

import (
	"github.com/juju/errors"
	"github.com/sqlpub/qin-cdc/metas"
)

type InputMeta interface {
	LoadMeta(routers []*metas.Router) error
	GetMeta(*metas.Router) (*metas.Table, error)
	// GetAll() map[string]*metas.Table
	GetVersion(schema string, tableName string, version uint) (*metas.Table, error)
	// Add(*metas.Table) error
	// Update(newTable *metas.Table) error
	// Delete(string, string) error
	Save() error
	Close()
}

type OutputMeta interface {
	LoadMeta(routers []*metas.Router) error
	GetMeta(*metas.Router) (interface{}, error)
	// GetAll() map[string]*metas.Table
	// GetVersion(schema string, tableName string, version uint) (*metas.Table, error)
	// Add(*metas.Table) error
	// Update(newTable *metas.Table) error
	// Delete(string, string) error
	Save() error
	Close()
}

type Metas struct {
	Input   InputMeta
	Output  OutputMeta
	Routers *metas.Routers
}

func (m *Metas) InitRouterColumnsMapper() error {
	// router column mapper
	for _, router := range m.Routers.Raws {
		inputTable, err := m.Input.GetMeta(router)
		if err != nil {
			return err
		}
		if inputTable == nil {
			return errors.Errorf("get input meta failed, err: %s.%s not found", router.SourceSchema, router.SourceTable)
		}
		for _, column := range inputTable.Columns {
			router.ColumnsMapper.SourceColumns = append(router.ColumnsMapper.SourceColumns, column.Name)
			if column.IsPrimaryKey {
				router.ColumnsMapper.PrimaryKeys = append(router.ColumnsMapper.PrimaryKeys, column.Name)
			}
		}
		metaObj, err := m.Output.GetMeta(router)
		if err != nil {
			return err
		}

		outputTable, ok := metaObj.(*metas.Table)
		if ok {
			if outputTable == nil {
				return errors.Errorf("get output meta failed, err: %s.%s not found", router.TargetSchema, router.TargetTable)
			}
			for _, column := range outputTable.Columns {
				router.ColumnsMapper.TargetColumns = append(router.ColumnsMapper.TargetColumns, column.Name)
			}
		} else {
			// target == source
			for _, column := range inputTable.Columns {
				router.ColumnsMapper.TargetColumns = append(router.ColumnsMapper.TargetColumns, column.Name)
			}
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
