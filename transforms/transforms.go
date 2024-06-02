package transforms

import (
	"github.com/siddontang/go-log/log"
	"github.com/sqlpub/qin-cdc/config"
	"github.com/sqlpub/qin-cdc/core"
	"github.com/sqlpub/qin-cdc/metas"
	"github.com/sqlpub/qin-cdc/metrics"
)

type MatcherTransforms []core.Transform

func NewMatcherTransforms(transConfigs []config.TransformConfig, routers *metas.Routers) (matcher MatcherTransforms) {
	for _, tc := range transConfigs {
		switch typ := tc.Type; typ {
		case RenameColumnTransName:
			rct := &RenameColumnTrans{}
			if err := rct.NewTransform(tc.Config); err != nil {
				log.Fatal(err)
			}
			// rename router mapper column name to new column name
			for _, router := range routers.Raws {
				if router.SourceSchema == rct.matchSchema && router.SourceTable == rct.matchTable {
					for i, column := range rct.columns {
						for i2, sourceColumn := range router.ColumnsMapper.SourceColumns {
							if sourceColumn == column {
								router.ColumnsMapper.SourceColumns[i2] = rct.renameAs[i]
							}
						}
					}
				}
			}
			log.Infof("load transform: %s", RenameColumnTransName)
			matcher = append(matcher, rct)
		case DeleteColumnTransName:
			dct := &DeleteColumnTrans{}
			if err := dct.NewTransform(tc.Config); err != nil {
				log.Fatal(err)
			}
			// delete router mapper column name
			for _, router := range routers.Raws {
				if router.SourceSchema == dct.matchSchema && router.SourceTable == dct.matchTable {
					for _, column := range dct.columns {
						sourceColumns := make([]string, len(router.ColumnsMapper.SourceColumns))
						copy(sourceColumns, router.ColumnsMapper.SourceColumns)
						for i2, sourceColumn := range sourceColumns {
							if sourceColumn == column {
								router.ColumnsMapper.SourceColumns = append(router.ColumnsMapper.SourceColumns[:i2], router.ColumnsMapper.SourceColumns[i2+1:]...)
							}
						}
					}
				}
			}
			log.Infof("load transform: %s", DeleteColumnTransName)
			matcher = append(matcher, dct)
		default:
			log.Warnf("transform: %s unhandled will not take effect", typ)
		}
	}
	return matcher
}

func (m MatcherTransforms) IterateTransforms(msg *core.Msg) bool {
	for _, trans := range m {
		if trans.Transform(msg) {
			log.Debugf("transform msg %v", msg.DmlMsg.Data)
			return true
		}
	}
	return false
}

func (m MatcherTransforms) Start(in chan *core.Msg, out chan *core.Msg) {
	go func() {
		for data := range in {
			log.Debugf(data.ToString())
			if !m.IterateTransforms(data) {
				out <- data
				handleMetrics(data)
			}
		}
	}()
}

func handleMetrics(data *core.Msg) {
	if data.Type == core.MsgDML {
		metrics.OpsReadProcessed.Inc()
	}
}
