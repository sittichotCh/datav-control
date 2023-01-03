package dbcontrol

import "time"

type DateTime time.Time

func (t *DateTime) String() string {
	return t.String()
}

func (t *DateTime) Millisecond() int64 {
	return t.Millisecond()
}

type migrateDatabaseConfig struct {
	Path             string
	DatabaseProtocol string
	DatabaseUrl      string
	DatabaseId       string
	DatabasePassword string
	DatabaseSchema   string
}

type changelog struct {
	Id          string       `json:"id"`
	Description string       `json:"description"`
	Author      string       `json:"author"`
	CreateTable *createTable `json:"createTable"`
	AlterTable  *alterTable  `json:"alterTable"`
}

type alterTable struct {
	TableName string `json:"tableName"`
}

type createTable struct {
	TableName   string   `json:"tableName"`
	Columns     []column `json:"columns"`
	PrimaryKeys []string `json:"primaryKeys"`
}

type column struct {
	ColumnName   string `json:"columnName"`
	ColumnType   string `json:"columnType"`
	NonNullable  bool   `json:"nonNullable"`
	DefaultValue string `json:"defaultValue"`
}

type changelogEntity struct {
	Id            *string
	Description   *string
	ExecutionTime *time.Time
	Md5Sum        *string
	QueryLog      *string
}
