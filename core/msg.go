package core

import (
	"fmt"
	"github.com/goccy/go-json"
	"github.com/sqlpub/qin-cdc/metas"
	"time"
)

type MsgType string
type ActionType string
type DDLActionType string

const (
	MsgDML MsgType = "dml"
	MsgDDL MsgType = "ddl"
	MsgCtl MsgType = "ctl"

	InsertAction  ActionType = "insert"
	UpdateAction  ActionType = "update"
	DeleteAction  ActionType = "delete"
	ReplaceAction ActionType = "replace"

	CreateAction   DDLActionType = "create"
	AlterAction    DDLActionType = "alter"
	RenameAction   DDLActionType = "rename"
	DropAction     DDLActionType = "drop"
	TruncateAction DDLActionType = "Truncate"
)

type Msg struct {
	Database     string
	Table        string
	Type         MsgType
	DmlMsg       *DMLMsg
	Timestamp    time.Time
	InputContext struct {
		Pos string
	}
}

type DMLMsg struct {
	Action       ActionType
	Data         map[string]interface{}
	Old          map[string]interface{}
	TableVersion uint
}

type DDLMsg struct {
	Action       DDLActionType
	NewTable     metas.Table
	DdlStatement metas.DdlStatement
}

func (m *Msg) ToString() string {
	switch m.Type {
	case MsgDML:
		marshal, _ := json.Marshal(m.DmlMsg)
		return fmt.Sprintf("msg event: %s %s.%s %v", m.DmlMsg.Action, m.Database, m.Table, string(marshal))
	case MsgDDL:
	case MsgCtl:
		marshal, _ := json.Marshal(m.InputContext)
		return fmt.Sprintf("msg event: %s %v", m.Type, string(marshal))
	default:
		return fmt.Sprintf("msg event: %s %v", m.DmlMsg.Action, m)
	}
	return ""
}
