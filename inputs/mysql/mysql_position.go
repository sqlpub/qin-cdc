package mysql

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/mitchellh/mapstructure"
	"github.com/siddontang/go-log/log"
	"github.com/sqlpub/qin-cdc/config"
	bolt "go.etcd.io/bbolt"
	"sync"
	"time"
)

type PositionPlugin struct {
	*config.MysqlConfig
	name       string
	metaDb     *bolt.DB
	bucketName string
	pos        string
	stop       chan struct{}
	done       chan struct{}
	mu         sync.Mutex
}

func (p *PositionPlugin) Configure(conf map[string]interface{}) error {
	p.MysqlConfig = &config.MysqlConfig{}
	var source = conf["source"]
	if err := mapstructure.Decode(source, p.MysqlConfig); err != nil {
		return err
	}
	p.bucketName = "position"
	p.stop = make(chan struct{})
	p.done = make(chan struct{})
	return nil
}

func (p *PositionPlugin) LoadPosition(name string) string {
	p.name = name
	var err error
	p.metaDb, err = bolt.Open("meta.db", 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		log.Fatal(err)
	}
	p.pos = p.getPosFromMetaDb() // meta.db
	if p.pos != "" {
		return p.pos
	}
	p.pos = p.getPosFromConfig() // config file
	if p.pos != "" {
		return p.pos
	}
	p.pos = p.getPosFromSource() // from db get now position
	return p.pos
}

func (p *PositionPlugin) Start() {
	go p.timerSave()
}

func (p *PositionPlugin) Update(v string) error {
	// only updating memory variables is not persistent
	// select save func persistent
	p.mu.Lock()
	defer p.mu.Unlock()
	if v == "" {
		return errors.Errorf("empty value")
	}
	p.pos = v
	return nil
}

func (p *PositionPlugin) Save() error {
	// persistent save pos
	p.mu.Lock()
	defer p.mu.Unlock()
	err := p.metaDb.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(p.bucketName))
		if b == nil {
			return fmt.Errorf("bucket:%s does not exist", p.bucketName)
		}
		err := b.Put([]byte(p.name), []byte(p.pos))
		return err
	})
	return err
}

func (p *PositionPlugin) Get() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pos
}

func (p *PositionPlugin) Close() {
	close(p.stop)
	<-p.done
	if p.metaDb != nil {
		err := p.metaDb.Close()
		if err != nil {
			log.Errorf("close metaDb conn failed: %s", err.Error())
		}
	}
}

func (p *PositionPlugin) getPosFromMetaDb() string {
	var pos []byte
	err := p.metaDb.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(p.bucketName))
		if err != nil {
			return err
		}
		pos = b.Get([]byte(p.name))
		return nil
	})
	if err != nil {
		log.Fatalf("from metaDb get position: %s", err.Error())
	}
	return string(pos)
}

func (p *PositionPlugin) getPosFromConfig() string {
	if pos, ok := p.MysqlConfig.Options["start-gtid"]; ok {
		return fmt.Sprintf("%v", pos)
	}
	return ""
}

func (p *PositionPlugin) getPosFromSource() string {
	db, err := getConn(p.MysqlConfig)
	if err != nil {
		log.Fatalf("conn db failed, %s", err.Error())
		return ""
	}
	defer closeConn(db)
	var gtidMode string
	err = db.QueryRow(`select @@GLOBAL.gtid_mode`).Scan(&gtidMode)
	if err != nil {
		log.Fatalf("query gtid_mode failed, %s", err.Error())
	}
	if gtidMode != "ON" {
		log.Fatalf("gtid_mode is not enabled")
	}
	var nowGtid string
	err = db.QueryRow(`SELECT @@GLOBAL.gtid_executed`).Scan(&nowGtid)
	if err != nil {
		log.Fatalf("query now gitd value failed, %s", err.Error())
	}
	return nowGtid
}

func (p *PositionPlugin) timerSave() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := p.Save(); err != nil {
				log.Fatalf("timer save position failed: %s", err.Error())
			}
			// log.Debugf("timer save position: %s", p.pos)
		case <-p.stop:
			if err := p.Save(); err != nil {
				log.Fatalf("timer save position failed: %s", err.Error())
			}
			log.Infof("last save position: %v", p.pos)
			p.done <- struct{}{}
			return

		}
	}
}
