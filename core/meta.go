package core

import (
	"github.com/sqlpub/qin-cdc/metas"
)

type Meta interface {
	LoadMeta(routers []*metas.Router) error
	Get(schema string, tableName string) (*metas.Table, error)
	GetAll() map[string]*metas.Table
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
