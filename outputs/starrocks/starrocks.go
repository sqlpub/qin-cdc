package starrocks

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/mitchellh/mapstructure"
	"github.com/siddontang/go-log/log"
	"github.com/sqlpub/qin-cdc/config"
	"github.com/sqlpub/qin-cdc/core"
	"github.com/sqlpub/qin-cdc/metas"
	"github.com/sqlpub/qin-cdc/metrics"
	"io"
	"net/http"
	"strings"
	"time"
)

type OutputPlugin struct {
	*config.StarrocksConfig
	Done         chan bool
	metas        *core.Metas
	msgTxnBuffer struct {
		size        int
		tableMsgMap map[string][]*core.Msg
	}
	client       *http.Client
	transport    *http.Transport
	lastPosition string
}

func (o *OutputPlugin) Configure(conf map[string]interface{}) error {
	o.StarrocksConfig = &config.StarrocksConfig{}
	var targetConf = conf["target"]
	if err := mapstructure.Decode(targetConf, o.StarrocksConfig); err != nil {
		return err
	}
	return nil
}

func (o *OutputPlugin) NewOutput(metas *core.Metas) {
	o.Done = make(chan bool)
	o.metas = metas
	// options handle
	if o.StarrocksConfig.Options.BatchSize == 0 {
		o.StarrocksConfig.Options.BatchSize = DefaultBatchSize
	}
	if o.StarrocksConfig.Options.BatchIntervalMs == 0 {
		o.StarrocksConfig.Options.BatchIntervalMs = DefaultBatchIntervalMs
	}
	o.msgTxnBuffer.size = 0
	o.msgTxnBuffer.tableMsgMap = make(map[string][]*core.Msg)

	o.transport = &http.Transport{}
	o.client = &http.Client{
		Transport: o.transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			req.Header.Add("Authorization", "Basic "+o.auth())
			// log.Debugf("重定向请求到be: %v", req.URL)
			return nil // return nil nil回重定向。
		},
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
					if o.msgTxnBuffer.size >= o.StarrocksConfig.Options.BatchSize {
						o.flushMsgTxnBuffer(pos)
					}
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
	log.Infof("output is closed")
}

func (o *OutputPlugin) appendMsgTxnBuffer(msg *core.Msg) {
	key := metas.GenerateMapRouterKey(msg.Database, msg.Table)
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
	// table level export
	for k, msgs := range o.msgTxnBuffer.tableMsgMap {
		columnsMapper := o.metas.Routers.Maps[k].ColumnsMapper
		targetSchema := o.metas.Routers.Maps[k].TargetSchema
		targetTable := o.metas.Routers.Maps[k].TargetTable
		err := o.execute(msgs, columnsMapper, targetSchema, targetTable)
		if err != nil {
			log.Fatalf("do %s bulk err %v", PluginName, err)
		}
	}
	o.clearMsgTxnBuffer()
}

func (o *OutputPlugin) clearMsgTxnBuffer() {
	o.msgTxnBuffer.size = 0
	o.msgTxnBuffer.tableMsgMap = make(map[string][]*core.Msg)
}

func (o *OutputPlugin) execute(msgs []*core.Msg, columnsMapper metas.ColumnsMapper, targetSchema string, targetTable string) error {
	if len(msgs) == 0 {
		return nil
	}
	var jsonList []string

	jsonList = o.generateJson(msgs)
	for _, s := range jsonList {
		log.Debugf("%s load %s.%s row data: %v", PluginName, targetSchema, targetTable, s)
	}
	log.Debugf("%s bulk load %s.%s row data num: %d", PluginName, targetSchema, targetTable, len(jsonList))
	var err error
	for i := 0; i < RetryCount; i++ {
		err = o.sendData(jsonList, columnsMapper, targetSchema, targetTable)
		if err != nil {
			log.Warnf("send data failed, err: %v, execute retry...", err.Error())
			if i+1 == RetryCount {
				break
			}
			time.Sleep(time.Duration(RetryInterval*(i+1)) * time.Second)
			continue
		}
		break
	}
	return err
}

func (o *OutputPlugin) sendData(content []string, columnsMapper metas.ColumnsMapper, targetSchema string, targetTable string) error {
	loadUrl := fmt.Sprintf("http://%s:%d/api/%s/%s/_stream_load",
		o.Host, o.LoadPort, targetSchema, targetTable)
	newContent := `[` + strings.Join(content, ",") + `]`
	req, _ := http.NewRequest("PUT", loadUrl, strings.NewReader(newContent))

	// req.Header.Add
	req.Header.Add("Authorization", "Basic "+o.auth())
	req.Header.Add("Expect", "100-continue")
	req.Header.Add("strict_mode", "true")
	// req.Header.Add("label", "39c25a5c-7000-496e-a98e-348a264c81de")
	req.Header.Add("format", "json")
	req.Header.Add("strip_outer_array", "true")

	var columnArray []string
	for _, column := range columnsMapper.SourceColumns {
		columnArray = append(columnArray, column)
	}
	columnArray = append(columnArray, DeleteColumn)
	columns := fmt.Sprintf("%s, __op = %s", strings.Join(columnArray, ","), DeleteColumn)
	req.Header.Add("columns", columns)

	response, err := o.client.Do(req)
	if err != nil {
		return err
	}
	defer func(Body io.ReadCloser) {
		_ = Body.Close()
	}(response.Body)
	returnMap, err := o.parseResponse(response)
	if err != nil {
		return err
	}
	if returnMap["Status"] != "Success" {
		message := returnMap["Message"]
		errorUrl := returnMap["ErrorURL"]
		errorMsg := message.(string) +
			fmt.Sprintf(", targetTable: %s.%s", targetSchema, targetTable) +
			fmt.Sprintf(", visit ErrorURL to view error details, ErrorURL: %s", errorUrl)
		return errors.New(errorMsg)
	}
	// prom write event number counter
	numberLoadedRows := returnMap["NumberLoadedRows"]
	metrics.OpsWriteProcessed.Add(numberLoadedRows.(float64))
	return nil
}
