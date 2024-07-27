package kafka

import (
	"fmt"
	gokafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/goccy/go-json"
	"github.com/mitchellh/mapstructure"
	"github.com/siddontang/go-log/log"
	"github.com/sqlpub/qin-cdc/config"
	"github.com/sqlpub/qin-cdc/core"
	"github.com/sqlpub/qin-cdc/metas"
	"github.com/sqlpub/qin-cdc/metrics"
	"strconv"
	"time"
)

type OutputPlugin struct {
	*config.KafkaConfig
	Done            chan bool
	metas           *core.Metas
	formatInterface formatInterface
	msgTxnBuffer    struct {
		size        int
		tableMsgMap map[string][]*core.Msg
	}
	client       *gokafka.Producer
	lastPosition string
}

func (o *OutputPlugin) Configure(conf map[string]interface{}) error {
	o.KafkaConfig = &config.KafkaConfig{}
	var targetConf = conf["target"]
	if err := mapstructure.Decode(targetConf, o.KafkaConfig); err != nil {
		return err
	}
	o.initFormatPlugin(fmt.Sprintf("%v", o.Options.OutputFormat))
	return nil
}

func (o *OutputPlugin) NewOutput(metas *core.Metas) {
	o.Done = make(chan bool)
	o.metas = metas
	// options handle
	if o.KafkaConfig.Options.BatchSize == 0 {
		o.KafkaConfig.Options.BatchSize = DefaultBatchSize
	}
	if o.KafkaConfig.Options.BatchIntervalMs == 0 {
		o.KafkaConfig.Options.BatchIntervalMs = DefaultBatchIntervalMs
	}

	o.msgTxnBuffer.size = 0
	o.msgTxnBuffer.tableMsgMap = make(map[string][]*core.Msg)

	var err error
	o.client, err = getProducer(o.KafkaConfig)
	if err != nil {
		log.Fatal("output config client failed. err: ", err.Error())
	}
}

func (o *OutputPlugin) Start(out chan *core.Msg, pos core.Position) {
	// first pos
	o.lastPosition = pos.Get()
	go func() {
		ticker := time.NewTicker(time.Millisecond * time.Duration(o.Options.BatchIntervalMs))
		defer ticker.Stop()
		for {
			select {
			case data := <-out:
				switch data.Type {
				case core.MsgCtl:
					o.lastPosition = data.InputContext.Pos
				case core.MsgDML:
					o.appendMsgTxnBuffer(data)
					if o.msgTxnBuffer.size >= o.KafkaConfig.Options.BatchSize {
						o.flushMsgTxnBuffer(pos)
					}
				}
			case e := <-o.client.Events():
				switch ev := e.(type) {
				case *gokafka.Message:
					m := ev
					if m.TopicPartition.Error != nil {
						log.Fatalf("Delivery failed: %v", m.TopicPartition.Error)
					}
					_, ok := m.Opaque.(*core.Msg)
					if !ok {
						log.Fatalf("kafka send failed to get meta data")
					}
				case gokafka.Error:
					log.Fatalf("kafka producer failed, err: %v", ev)
				default:
					log.Infof("Ignored event: %s", ev)
				}
			case <-ticker.C:
				o.flushMsgTxnBuffer(pos)
			case <-o.Done:
				o.flushMsgTxnBuffer(pos)
				return
			}

		}
	}()
}

func (o *OutputPlugin) Close() {
	log.Infof("output is closing...")
	close(o.Done)
	<-o.Done
	closeProducer(o.client)
	log.Infof("output is closed")
}

func (o *OutputPlugin) appendMsgTxnBuffer(msg *core.Msg) {
	key := metas.GenerateMapRouterVersionKey(msg.Database, msg.Table, msg.DmlMsg.TableVersion)
	o.msgTxnBuffer.tableMsgMap[key] = append(o.msgTxnBuffer.tableMsgMap[key], msg)
	o.msgTxnBuffer.size += 1
}

func (o *OutputPlugin) flushMsgTxnBuffer(pos core.Position) {
	defer func() {
		// flush position
		err := pos.Update(o.lastPosition)
		if err != nil {
			log.Fatalf(err.Error())
		}
	}()

	if o.msgTxnBuffer.size == 0 {
		return
	}
	// table level send
	for k, msgs := range o.msgTxnBuffer.tableMsgMap {
		// columnsMapper := o.metas.Routers.Maps[k].ColumnsMapper
		schemaName, tableName, version := metas.SplitMapRouterVersionKey(k)
		dmlTopic := o.metas.Routers.Maps[metas.GenerateMapRouterKey(schemaName, tableName)].DmlTopic
		table, err := o.metas.Input.GetVersion(schemaName, tableName, version)
		if err != nil {
			log.Fatalf("get input table meta failed, err: %v", err.Error())
		}
		err = o.execute(msgs, table, dmlTopic)
		if err != nil {
			log.Fatalf("output %s send err %v", PluginName, err)
		}
	}
	o.clearMsgTxnBuffer()
}

func (o *OutputPlugin) clearMsgTxnBuffer() {
	o.msgTxnBuffer.size = 0
	o.msgTxnBuffer.tableMsgMap = make(map[string][]*core.Msg)
}

func (o *OutputPlugin) execute(msgs []*core.Msg, table *metas.Table, dmlTopic string) error {
	for _, msg := range msgs {
		formatMsg := o.formatInterface.formatMsg(msg, table)
		bFormatMsg, err := json.Marshal(formatMsg)
		if err != nil {
			return err
		}
		pksData, err := GenPrimaryKeys(table.PrimaryKeyColumns, msg.DmlMsg.Data)
		if err != nil {
			return err
		}
		_, dataHash, err := DataHash(pksData)
		if err != nil {
			return err
		}
		kPartition := dataHash % uint64(o.PartitionNum)
		kKey := strconv.FormatUint(dataHash, 10)

		kMsg := gokafka.Message{
			TopicPartition: gokafka.TopicPartition{Topic: &dmlTopic, Partition: int32(kPartition)},
			Key:            []byte(kKey),
			Value:          bFormatMsg,
			Opaque:         msg,
		}

		err = o.send(&kMsg)
		if err != nil {
			return err
		}
		log.Debugf("output %s msg: %v", PluginName, string(bFormatMsg))
		// prom write event number counter
		metrics.OpsWriteProcessed.Add(1)
	}
	return nil
}

func (o *OutputPlugin) send(message *gokafka.Message) error {
	var err error
	for i := 0; i < RetryCount; i++ {
		err = o.client.Produce(message, nil)
		if err != nil {
			log.Warnf("kafka send data failed, err: %v, start retry...", err.Error())
			if i+1 == RetryCount {
				break
			}
			time.Sleep(time.Duration(RetryInterval*(i+1)) * time.Second)
			continue
		}
		break
	}
	if err != nil {
		return err
	}
	return err
}
