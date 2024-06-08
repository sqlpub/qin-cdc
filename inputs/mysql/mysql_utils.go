package mysql

import (
	"database/sql"
	"fmt"
	"github.com/siddontang/go-log/log"
	"github.com/sqlpub/qin-cdc/config"
	"github.com/sqlpub/qin-cdc/metas"
	"math/rand"
	"time"
)

const (
	PluginName            = "mysql"
	DefaultCharset string = "utf8"
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

func getServerId(conf *config.MysqlConfig) uint32 {
	serverId := conf.Options.ServerId
	if serverId > 0 {
		return uint32(serverId)
	} else if serverId < 0 {
		log.Fatalf("options server-id: %v is invalid, must be greater than 0", serverId)
	}
	// default generate rand value [1001, 2000]
	return uint32(rand.New(rand.NewSource(time.Now().Unix())).Intn(1000)) + 1001
}

func deserialize(raw interface{}, column metas.Column) interface{} {
	if raw == nil {
		return nil
	}

	ret := raw
	if column.RawType == "text" || column.RawType == "json" {
		_, ok := raw.([]uint8)
		if ok {
			ret = string(raw.([]uint8))
		}
	}
	return ret
}
