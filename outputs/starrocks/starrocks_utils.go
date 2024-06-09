package starrocks

import (
	"encoding/base64"
	"github.com/goccy/go-json"
	"github.com/siddontang/go-log/log"
	"github.com/sqlpub/qin-cdc/core"
	"io"
	"net/http"
)

const (
	PluginName                    = "starrocks"
	DefaultBatchSize       int    = 10240
	DefaultBatchIntervalMs int    = 3000
	DeleteColumn           string = "_delete_sign_"
	RetryCount             int    = 3
	RetryInterval          int    = 5
)

func (o *OutputPlugin) auth() string {
	s := o.UserName + ":" + o.Password
	b := []byte(s)

	sEnc := base64.StdEncoding.EncodeToString(b)
	return sEnc
}

func (o *OutputPlugin) parseResponse(response *http.Response) (map[string]interface{}, error) {
	var result map[string]interface{}
	body, err := io.ReadAll(response.Body)
	if err == nil {
		err = json.Unmarshal(body, &result)
	}

	return result, err
}

func (o *OutputPlugin) generateJson(msgs []*core.Msg) []string {
	var jsonList []string

	for _, event := range msgs {
		switch event.DmlMsg.Action {
		case core.InsertAction:
			// 增加虚拟列，标识操作类型 (stream load opType：UPSERT 0，DELETE：1)
			event.DmlMsg.Data[DeleteColumn] = 0
			b, _ := json.Marshal(event.DmlMsg.Data)
			jsonList = append(jsonList, string(b))
		case core.UpdateAction:
			// 增加虚拟列，标识操作类型 (stream load opType：UPSERT 0，DELETE：1)
			event.DmlMsg.Data[DeleteColumn] = 0
			b, _ := json.Marshal(event.DmlMsg.Data)
			jsonList = append(jsonList, string(b))
		case core.DeleteAction: // starrocks2.4版本只支持primary key模型load delete
			// 增加虚拟列，标识操作类型 (stream load opType：UPSERT 0，DELETE：1)
			event.DmlMsg.Data[DeleteColumn] = 1
			b, _ := json.Marshal(event.DmlMsg.Data)
			jsonList = append(jsonList, string(b))
		case core.ReplaceAction: // for mongo
			// 增加虚拟列，标识操作类型 (stream load opType：UPSERT 0，DELETE：1)
			event.DmlMsg.Data[DeleteColumn] = 0
			b, _ := json.Marshal(event.DmlMsg.Data)
			jsonList = append(jsonList, string(b))
		default:
			log.Fatalf("unhandled message type: %v", event)
		}
	}
	return jsonList
}
