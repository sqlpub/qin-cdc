package metas

type ColumnType = int

const (
	TypeNumber    ColumnType = iota + 1 // tinyint, smallint, mediumint, int, bigint, year
	TypeFloat                           // float, double
	TypeEnum                            // enum
	TypeSet                             // set
	TypeString                          // other
	TypeDatetime                        // datetime
	TypeTimestamp                       // timestamp
	TypeDate                            // date
	TypeTime                            // time
	TypeBit                             // bit
	TypeJson                            // json
	TypeDecimal                         // decimal
	TypeBinary                          // binary
)

type Table struct {
	Schema            string
	Name              string
	Comment           string
	Columns           []Column
	PrimaryKeyColumns []Column
}
type Column struct {
	Name         string
	Type         ColumnType
	RawType      string
	Comment      string
	IsPrimaryKey bool
}

type DdlStatement struct {
	Schema        string
	Name          string
	RawSql        string
	IsAlterTable  bool
	IsCreateTable bool
	CreateTable   struct {
		IsLikeCreateTable bool
		ReferTable        struct {
			Schema string
			Name   string
		}
		IsSelectCreateTable bool
		SelectRawSql        string
	}
	IsDropTable     bool
	IsRenameTable   bool
	IsTruncateTable bool
}
