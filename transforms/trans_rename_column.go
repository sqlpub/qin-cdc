package transforms

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/sqlpub/qin-cdc/core"
	"github.com/sqlpub/qin-cdc/utils"
)

const RenameColumnTransName = "rename-column"

type RenameColumnTrans struct {
	name        string
	matchSchema string
	matchTable  string
	columns     []string
	renameAs    []string
}

func (rct *RenameColumnTrans) NewTransform(config map[string]interface{}) error {
	columns, ok := config["columns"]
	if !ok {
		return errors.Trace(errors.New("'columns' is not configured"))
	}
	renameAs, ok := config["rename-as"]
	if !ok {
		return errors.Trace(errors.New("'rename-as' is not configured"))
	}

	c, ok := utils.CastToSlice(columns)
	if !ok {
		return errors.Trace(errors.New("'columns' should be an array"))
	}

	columnsString, err := utils.CastSliceInterfaceToSliceString(c)
	if err != nil {
		return errors.Trace(errors.New("'columns' should be an array of string"))
	}

	ra, ok := utils.CastToSlice(renameAs)
	if !ok {
		return errors.Trace(errors.New("'rename-as' should be an array"))
	}

	renameAsString, err := utils.CastSliceInterfaceToSliceString(ra)
	if err != nil {
		return errors.Trace(errors.New("'cast-as' should be an array of string"))
	}

	if len(c) != len(ra) {
		return errors.Trace(errors.New("'columns' should have the same length of 'rename-as'"))
	}

	rct.name = RenameColumnTransName
	rct.matchSchema = fmt.Sprintf("%v", config["match-schema"])
	rct.matchTable = fmt.Sprintf("%v", config["match-table"])
	rct.columns = columnsString
	rct.renameAs = renameAsString
	return nil
}

func (rct *RenameColumnTrans) Transform(msg *core.Msg) bool {
	if rct.matchSchema == msg.Database && rct.matchTable == msg.Table {
		for i, column := range rct.columns {
			value := FindColumn(msg.DmlMsg.Data, column)
			if value != nil {
				renameAsColumn := rct.renameAs[i]
				msg.DmlMsg.Data[renameAsColumn] = msg.DmlMsg.Data[column]
				delete(msg.DmlMsg.Data, column)
			}
		}
	}
	return false
}
