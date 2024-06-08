package metas

import (
	"github.com/mitchellh/mapstructure"
	"github.com/siddontang/go-log/log"
	"strconv"
	"strings"
)

type Router struct {
	SourceSchema  string `mapstructure:"source-schema"`
	SourceTable   string `mapstructure:"source-table"`
	TargetSchema  string `mapstructure:"target-schema"`
	TargetTable   string `mapstructure:"target-table"`
	ColumnsMapper ColumnsMapper
}

type ColumnsMapper struct {
	PrimaryKeys    []string // source
	SourceColumns  []string
	TargetColumns  []string
	MapMapper      map[string]string
	MapMapperOrder []string
}

type Routers struct {
	Raws []*Router
	Maps map[string]*Router
}

var MapRouterKeyDelimiter = ":"

func (r *Routers) InitRouters(config map[string]interface{}) error {
	r.initRaws(config)
	r.initMaps()
	return nil
}

func (r *Routers) initRaws(config map[string]interface{}) {
	configRules := config["routers"]
	err := mapstructure.Decode(configRules, &r.Raws)
	if err != nil {
		log.Fatal("output.config.routers config parsing failed. err: ", err.Error())
	}
}

func (r *Routers) initMaps() {
	if len(r.Raws) == 0 {
		log.Fatal("routers config cannot be empty")
	}
	r.Maps = make(map[string]*Router)
	for _, router := range r.Raws {
		r.Maps[GenerateMapRouterKey(router.SourceSchema, router.SourceTable)] = router
	}
}

func GenerateMapRouterKey(schema string, table string) string {
	return schema + MapRouterKeyDelimiter + table
}

func GenerateMapRouterVersionKey(schema string, table string, version uint) string {
	return schema + MapRouterKeyDelimiter + table + MapRouterKeyDelimiter + strconv.Itoa(int(version))
}

func SplitMapRouterKey(key string) (schema string, table string) {
	splits := strings.Split(key, MapRouterKeyDelimiter)
	return splits[0], splits[1]
}

func SplitMapRouterVersionKey(key string) (schema string, table string, version uint) {
	splits := strings.Split(key, MapRouterKeyDelimiter)
	tmpVersion, _ := strconv.Atoi(splits[2])
	return splits[0], splits[1], uint(tmpVersion)
}
