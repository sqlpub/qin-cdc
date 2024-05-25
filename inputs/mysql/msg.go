package mysql

import (
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/sqlpub/qin-cdc/core"
	"github.com/sqlpub/qin-cdc/metas"
	"time"
)

func (i *InputPlugin) NewInsertMsgs(schemaName string, tableName string, tableMeta *metas.Table, evs *replication.RowsEvent, header *replication.EventHeader) (msgs []*core.Msg, err error) {
	// new insert msgs
	msgs = make([]*core.Msg, len(evs.Rows))
	for index, row := range evs.Rows {
		data := make(map[string]interface{})
		for columnIndex, value := range row {
			data[tableMeta.Columns[columnIndex].Name] = deserialize(value, tableMeta.Columns[columnIndex])
		}
		msg := &core.Msg{
			Database:  schemaName,
			Table:     tableName,
			Type:      core.MsgDML,
			DmlMsg:    &core.DMLMsg{Action: core.InsertAction, Data: data},
			Timestamp: time.Unix(int64(header.Timestamp), 0),
		}
		msgs[index] = msg
	}
	return msgs, nil
}

func (i *InputPlugin) NewUpdateMsgs(schemaName string, tableName string, tableMeta *metas.Table, evs *replication.RowsEvent, header *replication.EventHeader) (msgs []*core.Msg, err error) {
	// new update msgs
	for index := 0; index < len(evs.Rows); index += 2 {
		oldDataRow := evs.Rows[index]
		newDataRow := evs.Rows[index+1]

		data := make(map[string]interface{})
		old := make(map[string]interface{})

		for columnIndex := 0; columnIndex < len(newDataRow); columnIndex++ {
			data[tableMeta.Columns[columnIndex].Name] = deserialize(newDataRow[columnIndex], tableMeta.Columns[columnIndex])
			old[tableMeta.Columns[columnIndex].Name] = deserialize(oldDataRow[columnIndex], tableMeta.Columns[columnIndex])
		}
		msg := &core.Msg{
			Database:  schemaName,
			Table:     tableName,
			Type:      core.MsgDML,
			DmlMsg:    &core.DMLMsg{Action: core.UpdateAction, Data: data, Old: old},
			Timestamp: time.Unix(int64(header.Timestamp), 0),
		}
		msgs = append(msgs, msg)

	}
	return msgs, nil
}

func (i *InputPlugin) NewDeleteMsgs(schemaName string, tableName string, tableMeta *metas.Table, evs *replication.RowsEvent, header *replication.EventHeader) (msgs []*core.Msg, err error) {
	// new delete msgs
	msgs = make([]*core.Msg, len(evs.Rows))
	for index, row := range evs.Rows {
		data := make(map[string]interface{})
		for columnIndex, value := range row {
			data[tableMeta.Columns[columnIndex].Name] = deserialize(value, tableMeta.Columns[columnIndex])
		}
		msg := &core.Msg{
			Database:  schemaName,
			Table:     tableName,
			Type:      core.MsgDML,
			DmlMsg:    &core.DMLMsg{Action: core.DeleteAction, Data: data},
			Timestamp: time.Unix(int64(header.Timestamp), 0),
		}
		msgs[index] = msg
	}
	return msgs, nil
}

func (i *InputPlugin) NewXIDMsg(ev *replication.XIDEvent, header *replication.EventHeader) (msg *core.Msg, err error) {
	// new xid msg
	msg = &core.Msg{
		Type:      core.MsgCtl,
		Timestamp: time.Unix(int64(header.Timestamp), 0),
	}
	msg.InputContext.Pos = ev.GSet.String()
	return msg, nil
}

func (i *InputPlugin) SendMsgs(msgs []*core.Msg) {
	for _, msg := range msgs {
		i.SendMsg(msg)
	}
}

func (i *InputPlugin) SendMsg(msg *core.Msg) {
	i.in <- msg
}
