package transforms

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/sqlpub/qin-cdc/core"
	"github.com/sqlpub/qin-cdc/utils"
)

const DeleteColumnTransName = "delete-column"

type DeleteColumnTrans struct {
	name        string
	matchSchema string
	matchTable  string
	columns     []string
}

func (dct *DeleteColumnTrans) NewTransform(config map[string]interface{}) error {
	columns := config["columns"]
	c, ok := utils.CastToSlice(columns)
	if !ok {
		return errors.Trace(errors.New("'column' should be an array"))
	}

	columnsString, err := utils.CastSliceInterfaceToSliceString(c)
	if err != nil {
		return errors.Trace(errors.New("'column' should be an array of string"))
	}
	dct.name = DeleteColumnTransName
	dct.matchSchema = fmt.Sprintf("%v", config["match-schema"])
	dct.matchTable = fmt.Sprintf("%v", config["match-table"])
	dct.columns = columnsString
	return nil
}

func (dct *DeleteColumnTrans) Transform(msg *core.Msg) bool {
	if dct.matchSchema == msg.Database && dct.matchTable == msg.Table {
		for _, column := range dct.columns {
			value := FindColumn(msg.DmlMsg.Data, column)
			if value != nil {
				delete(msg.DmlMsg.Data, column)
			}
		}
	}
	return false
}
