package kafka

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/juju/errors"
	"github.com/mitchellh/hashstructure/v2"
	"github.com/siddontang/go-log/log"
	"github.com/sqlpub/qin-cdc/config"
	"github.com/sqlpub/qin-cdc/core"
	"github.com/sqlpub/qin-cdc/metas"
	"strings"
	"time"
)

type formatType string

var inputSequence uint64

const (
	PluginName                        = "kafka"
	DefaultBatchSize       int        = 10240
	DefaultBatchIntervalMs int        = 100
	RetryCount             int        = 3
	RetryInterval          int        = 5
	defaultJson            formatType = "json"
	aliyunDtsCanal         formatType = "aliyun_dts_canal"
)

func getProducer(conf *config.KafkaConfig) (producer *kafka.Producer, err error) {
	kafkaConf := &kafka.ConfigMap{
		"api.version.request": "true",
		"message.max.bytes":   1000000,
		"linger.ms":           10,
		"retries":             30,
		"retry.backoff.ms":    1000,
		"acks":                "1"}
	err = kafkaConf.SetKey("bootstrap.servers", strings.Join(conf.Brokers, ","))
	if err != nil {
		return nil, err
	}
	err = kafkaConf.SetKey("security.protocol", "plaintext")
	if err != nil {
		return nil, err
	}
	producer, err = kafka.NewProducer(kafkaConf)
	return producer, err
}

func closeProducer(producer *kafka.Producer) {
	if producer != nil {
		producer.Close()
	}
}

type formatInterface interface {
	formatMsg(event *core.Msg, table *metas.Table) interface{}
}

func (o *OutputPlugin) initFormatPlugin(outputFormat string) {
	// init kafka format handle func
	outputFormatType := formatType(fmt.Sprintf("%v", outputFormat))
	switch outputFormatType {
	case defaultJson:
		o.formatInterface = &defaultJsonFormat{}
	case aliyunDtsCanal:
		o.formatInterface = &aliyunDtsCanalFormat{}
	default:
		log.Fatalf("Unknown format type: %v", outputFormatType)
	}
}

type defaultJsonFormat struct{}

type kafkaDefaultMsg struct {
	Database string                 `json:"database"`
	Table    string                 `json:"table"`
	Type     core.ActionType        `json:"type"`
	Ts       uint32                 `json:"ts"`
	Data     map[string]interface{} `json:"data"`
	Old      map[string]interface{} `json:"old"`
}

func (djf *defaultJsonFormat) formatMsg(event *core.Msg, table *metas.Table) interface{} {
	kMsg := &kafkaDefaultMsg{
		Database: event.Database,
		Table:    event.Table,
		Type:     event.DmlMsg.Action,
		Ts:       uint32(event.Timestamp.Unix()),
		Data:     event.DmlMsg.Data,
		Old:      event.DmlMsg.Old,
	}
	return kMsg
}

type aliyunDtsCanalFormat struct{}

type kafkaMsgForAliyunDtsCanal struct {
	Database  string                   `json:"database"`
	Table     string                   `json:"table"`
	Type      string                   `json:"type"`
	Es        uint64                   `json:"es"` // source write datetime
	Ts        uint64                   `json:"ts"` // target write datetime
	Data      []map[string]interface{} `json:"data"`
	Old       []map[string]interface{} `json:"old"`
	SqlType   map[string]interface{}   `json:"sqlType"`
	MysqlType map[string]interface{}   `json:"mysqlType"`
	ServerId  string                   `json:"serverId"`
	Sql       string                   `json:"sql"`
	PkNames   []string                 `json:"pkNames"`
	IsDdl     bool                     `json:"isDdl"`
	Id        uint64                   `json:"id"`
	Gtid      *string                  `json:"gtid"`
}

func (adc *aliyunDtsCanalFormat) formatMsg(event *core.Msg, table *metas.Table) interface{} {
	datas := make([]map[string]interface{}, 0)
	datas = append(datas, event.DmlMsg.Data)
	var olds []map[string]interface{}
	if event.DmlMsg.Old != nil {
		olds = make([]map[string]interface{}, 0)
		olds = append(olds, event.DmlMsg.Old)
	}
	sqlType := make(map[string]interface{})
	mysqlType := make(map[string]interface{})
	for _, column := range table.Columns {
		if event.DmlMsg.Data[column.Name] != nil {
			event.DmlMsg.Data[column.Name] = fmt.Sprintf("%v", event.DmlMsg.Data[column.Name]) // to string
		}
		if event.DmlMsg.Old[column.Name] != nil {
			event.DmlMsg.Old[column.Name] = fmt.Sprintf("%v", event.DmlMsg.Old[column.Name]) // to string
		}
		switch column.Type {
		case metas.TypeNumber: // tinyint, smallint, int, bigint, year
			if strings.HasPrefix(column.RawType, "smallint") {
				sqlType[column.Name] = 2
				mysqlType[column.Name] = "smallint"
				continue
			} else if strings.HasPrefix(column.RawType, "tinyint") {
				sqlType[column.Name] = 1
				mysqlType[column.Name] = "tinyint"
				continue
			} else if strings.HasPrefix(column.RawType, "mediumint") {
				sqlType[column.Name] = 9
				mysqlType[column.Name] = "mediumint"
				continue
			} else if strings.HasPrefix(column.RawType, "bigint") {
				sqlType[column.Name] = 8
				mysqlType[column.Name] = "bigint"
				continue
			} else if strings.HasPrefix(column.RawType, "year") {
				mysqlType[column.Name] = "year"
				continue
			} else {
				sqlType[column.Name] = 3
				mysqlType[column.Name] = "int"
				continue
			}
		case metas.TypeFloat: // float, double
			if strings.HasPrefix(column.RawType, "float") {
				sqlType[column.Name] = 4
				mysqlType[column.Name] = "float"
				continue
			} else if strings.HasPrefix(column.RawType, "double") {
				sqlType[column.Name] = 5
				mysqlType[column.Name] = "double"
				continue
			}
		case metas.TypeEnum: // enum
			sqlType[column.Name] = 247
			mysqlType[column.Name] = "enum"
			continue
		case metas.TypeSet: // set
			sqlType[column.Name] = 248
			mysqlType[column.Name] = "set"
			continue
		case metas.TypeString: // other
			if strings.HasSuffix(column.RawType, "text") {
				sqlType[column.Name] = 15
				mysqlType[column.Name] = "text"
				continue
			} else if strings.HasPrefix(column.RawType, "char") {
				sqlType[column.Name] = 254
				mysqlType[column.Name] = "char"
				continue
			} else {
				sqlType[column.Name] = 253
				mysqlType[column.Name] = "varchar"
				continue
			}
		case metas.TypeDatetime: // datetime
			sqlType[column.Name] = 12
			mysqlType[column.Name] = "datetime"
			continue
		case metas.TypeTimestamp: // timestamp
			sqlType[column.Name] = 7
			mysqlType[column.Name] = "timestamp"
			continue
		case metas.TypeDate: // date
			sqlType[column.Name] = 10
			mysqlType[column.Name] = "date"
			continue
		case metas.TypeTime: // time
			sqlType[column.Name] = 11
			mysqlType[column.Name] = "time"
			continue
		case metas.TypeBit: // bit
			sqlType[column.Name] = 16
			mysqlType[column.Name] = "bit"
			continue
		case metas.TypeJson: // json
			sqlType[column.Name] = 245
			mysqlType[column.Name] = "json"
			continue
		case metas.TypeDecimal: // decimal
			sqlType[column.Name] = 246
			mysqlType[column.Name] = "decimal"
			continue
		case metas.TypeBinary:
			sqlType[column.Name] = 252
			if strings.HasPrefix(column.RawType, "binary") {
				mysqlType[column.Name] = "binary"
			} else {
				mysqlType[column.Name] = "blob"
			}
			continue
		default:
			sqlType[column.Name] = column.Type
			mysqlType[column.Name] = column.RawType
		}
	}

	pkNames := make([]string, 0)
	for _, primaryKeyColumn := range table.PrimaryKeyColumns {
		pkNames = append(pkNames, primaryKeyColumn.Name)
	}
	inputSequence++
	kMsg := &kafkaMsgForAliyunDtsCanal{
		Database:  event.Database,
		Table:     event.Table,
		Type:      strings.ToUpper(string(event.DmlMsg.Action)),
		Es:        uint64(event.Timestamp.UnixMilli()),
		Ts:        uint64(time.Now().UnixMilli()),
		Data:      datas,
		Old:       olds,
		SqlType:   sqlType,
		MysqlType: mysqlType,
		ServerId:  "",
		Sql:       "",
		PkNames:   pkNames,
		IsDdl:     false,
		Id:        inputSequence,
		Gtid:      nil,
	}
	return kMsg
}

func DataHash(key interface{}) (string, uint64, error) {
	hash, err := hashstructure.Hash(key, hashstructure.FormatV2, nil)
	if err != nil {
		return "", 0, err
	}
	return fmt.Sprint(key), hash, nil
}

func GenPrimaryKeys(pkColumns []metas.Column, rowData map[string]interface{}) (map[string]interface{}, error) {
	pks := make(map[string]interface{})
	for i := 0; i < len(pkColumns); i++ {
		pkName := pkColumns[i].Name
		pks[pkName] = rowData[pkName]
		if pks[pkName] == nil {
			return nil, errors.Errorf("primary key nil, pkName: %v, data: %v", pkName, rowData)
		}
	}
	return pks, nil
}
