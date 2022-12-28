package db_table

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"

	_ "github.com/taosdata/driver-go/v3/taosSql"
)

func NewDatabaseAndTable(dbUri string) (*DatabaseAndTable, error) {
	conn, err := sql.Open("taosSql", dbUri)
	return &DatabaseAndTable{conn: conn}, err
}

type DatabaseAndTable struct {
	conn *sql.DB
}

func (m *DatabaseAndTable) CreateDB(ctx context.Context, param DBParam) error {
	ql, err := createDBSql(param)
	if err != nil {
		return err
	}
	_, err = m.conn.ExecContext(ctx, ql)
	return err
}

func (m *DatabaseAndTable) CreateSTableBySql(ctx context.Context, db string, sql string) error {
	_, err := m.conn.ExecContext(ctx, fmt.Sprintf("use %s", db))
	if err != nil {
		return err
	}
	_, err = m.conn.ExecContext(ctx, sql)
	return err
}

func (m *DatabaseAndTable) CreateSTable(ctx context.Context, param STableParam) error {
	return m.CreateSTableBySql(ctx, param.DBName, createSTableSql(param))
}

func (m *DatabaseAndTable) CreateTableBySql(ctx context.Context, db string, sql string) error {
	_, err := m.conn.ExecContext(ctx, fmt.Sprintf("use %s", db))
	if err != nil {
		return err
	}
	_, err = m.conn.ExecContext(ctx, sql)
	return err
}

func (m *DatabaseAndTable) CreateTable(ctx context.Context, param TableParam) error {
	tableSql := createTableSql(param)
	err := m.CreateTableBySql(ctx, param.DBName, tableSql)
	if err != nil {
		log.Printf("## create table by sql-[%s] error %v", tableSql, err)
	}
	return err
}

type DBParam struct {
	DBName             string // dbname
	Buffer             int    // 一个 VNODE 写入内存池大小, MB，默认为 96，
	CacheModel         string // 是否在内存中缓存子表的最近数据, 默认为 none, last_row/last_value/both
	CacheSize          int    // 每个 vnode 中用于缓存子表最近数据的内存大小, MB, [1, 65536]
	Comp               int    // 数据库文件压缩标志位, 缺省值为 2，取值范围为 [0, 2]
	Duration           string // 数据文件存储数据的时间跨度
	WALFsyncPeriod     int    // WAL落盘的周期。默认为 3000
	MaxRows            int    // 文件块中记录的最大条数，默认为 4096 条
	MinRows            int    // 文件块中记录的最小条数，默认为 100 条
	Keep               int    // 数据文件保存的天数，缺省值为 3650, 取值范围 [1, 365000], 且必须大于或等于 DURATION 参数值
	Pages              int    // 缓存页个数, 默认为 256，最小 64
	PageSize           int    // 一个 VNODE 中元数据存储引擎的页大小，单位为 KB，默认为 4 KB。范围为 1 到 16384，即 1 KB 到 16 MB
	Precision          string // 数据库的时间戳精度
	Replica            int    // 数据库副本数，取值为 1 或 3，默认为 1
	Retentions         string // 聚合周期和保存时长
	WALLevel           int    // WAL 级别，默认为 1
	VGroups            int    // 数据库中初始 vgroup 的数目
	SingleTable        int    // 数据库中是否只可以创建一个超级表
	WalRetentionPeriod *int   // wal 文件的额外保留策略
	WALRetentionSize   *int   // wal 文件的额外保留策略
	WALRollPeriod      *int   // wal 文件切换时长，单位为 s
	WALSegmentSize     *int   // wal 单个文件大小，单位为 KB
}

func (*DBParam) check() error {
	// todo
	return nil
}

func createDBSql(param DBParam) (string, error) {
	if err := param.check(); err != nil {
		return "", err
	}
	var buffer bytes.Buffer

	buffer.WriteString(fmt.Sprintf("create database if not exists `%s` ", param.DBName))
	if param.Buffer > 0 {
		buffer.WriteString(fmt.Sprintf("buffer %d ", param.Buffer))
	}
	if len(param.CacheModel) > 0 {
		buffer.WriteString(fmt.Sprintf("cachemodel %s ", param.CacheModel))
	}
	if param.CacheSize > 0 {
		buffer.WriteString(fmt.Sprintf("cachesize %d ", param.CacheSize))
	}
	if param.Comp > 0 {
		buffer.WriteString(fmt.Sprintf("comp %d ", param.Comp))
	}
	if len(param.Duration) > 0 {
		buffer.WriteString(fmt.Sprintf("duration %s ", param.Duration))
	}
	if param.WALFsyncPeriod > 0 {
		buffer.WriteString(fmt.Sprintf("wal_fsync_period %d ", param.WALFsyncPeriod))
	}
	if param.MaxRows > 0 {
		buffer.WriteString(fmt.Sprintf("maxrows %d ", param.MaxRows))
	}
	if param.MinRows > 0 {
		buffer.WriteString(fmt.Sprintf("minrows %d ", param.MinRows))
	}
	if param.Keep > 0 {
		buffer.WriteString(fmt.Sprintf("keep %d ", param.Keep))
	}
	if param.Pages > 0 {
		buffer.WriteString(fmt.Sprintf("pages %d ", param.Pages))
	}
	if param.PageSize > 0 {
		buffer.WriteString(fmt.Sprintf("pagesize %d ", param.PageSize))
	}
	if len(param.Precision) > 0 {
		buffer.WriteString(fmt.Sprintf("precision %s ", param.Precision))
	}
	if param.Replica > 0 {
		buffer.WriteString(fmt.Sprintf("replica %d ", param.Replica))
	}
	if len(param.Retentions) > 0 {
		buffer.WriteString(fmt.Sprintf("retentions %s ", param.Retentions))
	}
	if param.WALLevel > 0 {
		buffer.WriteString(fmt.Sprintf("wal_level %d ", param.WALLevel))
	}
	if param.VGroups > 0 {
		buffer.WriteString(fmt.Sprintf("vgroups %d ", param.VGroups))
	}
	if param.SingleTable == 1 {
		buffer.WriteString(fmt.Sprintf("single_stable %d ", param.SingleTable))
	}
	if param.WalRetentionPeriod != nil {
		buffer.WriteString(fmt.Sprintf("wal_retention_period %d ", *param.WalRetentionPeriod))
	}
	if param.WALRetentionSize != nil {
		buffer.WriteString(fmt.Sprintf("wal_retention_size %d ", *param.WALRetentionSize))
	}
	if param.WALRollPeriod != nil {
		buffer.WriteString(fmt.Sprintf("wal_roll_period %d ", param.WALRollPeriod))
	}
	if param.WALSegmentSize != nil {
		buffer.WriteString(fmt.Sprintf("wal_segment_size %d ", param.WALSegmentSize))
	}
	return strings.Trim(buffer.String(), " "), nil
}

type TableColumn struct {
	ColumnName string
	ColumnType string
}

type TagValue struct {
	TagName      string
	TagValue     any
	TagValueType string
}

type STableParam struct {
	DBName     string
	STableName string
	Columns    []TableColumn
	Tags       []TableColumn
	Comment    string
	Watermark  []string
	MaxDelay   []string
	RollUp     []string
	Sma        []string
	TTL        int
}

type TableParam struct {
	DBName     string
	STableName string
	TableName  string
	Columns    []TableColumn
	TagValues  []TagValue
	Comment    string
	Watermark  []string
	MaxDelay   []string
	RollUp     []string
	Sma        []string
	TTL        int
}

func createSTableSql(param STableParam) string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("create stable if not exists `%s` (", param.STableName))

	for _, col := range param.Columns {
		buffer.WriteString(fmt.Sprintf("`%s` %s, ", col.ColumnName, col.ColumnType))
	}
	buffer.Truncate(buffer.Len() - 2)
	buffer.WriteString(") ")

	if len(param.Tags) > 0 {
		buffer.WriteString("tags (")

		for _, tag := range param.Tags {
			buffer.WriteString(fmt.Sprintf("`%s` %s, ", tag.ColumnName, tag.ColumnType))
		}
		buffer.Truncate(buffer.Len() - 2)
		buffer.WriteString(") ")
	}

	if len(param.Comment) > 0 {
		buffer.WriteString(fmt.Sprintf("comment '%s' ", param.Comment))
	}
	if len(param.Watermark) > 0 {
		buffer.WriteString(fmt.Sprintf("watermark %s ", strings.Join(param.Watermark, ",")))
	}
	if len(param.MaxDelay) > 0 {
		buffer.WriteString(fmt.Sprintf("max_delay %s ", strings.Join(param.MaxDelay, ",")))
	}
	if len(param.RollUp) > 0 {
		buffer.WriteString(fmt.Sprintf("rollup %s ", strings.Join(param.RollUp, ",")))
	}
	if len(param.Sma) > 0 {
		buffer.WriteString(fmt.Sprintf("sma %s ", strings.Join(param.Sma, ",")))
	}
	if param.TTL > 0 {
		buffer.WriteString(fmt.Sprintf("ttl %d ", param.TTL))
	}

	return strings.Trim(buffer.String(), " ")
}

func createTableSql(param TableParam) string {
	var buffer bytes.Buffer
	var tagBuffer bytes.Buffer
	var tagValueBuffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("create table if not exists `%s` ", param.TableName))
	if len(param.STableName) > 0 {
		buffer.WriteString(fmt.Sprintf("using `%s` ", param.STableName))
	}

	if len(param.TagValues) > 0 {
		tagBuffer.WriteString("(")
		tagValueBuffer.WriteString("(")
		for _, tv := range param.TagValues {
			if tv.TagValue == nil || tv.TagValue == "" {
				continue
			}

			tagBuffer.WriteString(tv.TagName)
			tagBuffer.WriteString(", ")

			if strings.HasPrefix(tv.TagValueType, "binary") || strings.HasPrefix(tv.TagValueType, "nchar") ||
				strings.HasPrefix(tv.TagValueType, "varchar") || strings.HasPrefix(tv.TagValueType, "json") {
				tagValueBuffer.WriteString(fmt.Sprintf("'%s'", tv.TagValue))
			} else {
				tagValueBuffer.WriteString(fmt.Sprintf("%s", tv.TagValue))
			}
			tagValueBuffer.WriteString(", ")
		}
		tagBuffer.Truncate(tagBuffer.Len() - 2)
		tagBuffer.WriteString(")")
		tagValueBuffer.Truncate(tagValueBuffer.Len() - 2)
		tagValueBuffer.WriteString(")")

		buffer.WriteString(tagBuffer.String())
		buffer.WriteString(" tags ")
		buffer.WriteString(tagValueBuffer.String())
	}
	if len(param.Columns) > 0 {
		buffer.WriteString("(")
		for _, col := range param.Columns {
			buffer.WriteString(fmt.Sprintf("`%s` %s, ", col.ColumnName, col.ColumnType))
		}
		buffer.Truncate(buffer.Len() - 2)
		buffer.WriteString(") ")
	}

	if len(param.Comment) > 0 {
		buffer.WriteString(fmt.Sprintf("comment '%s' ", param.Comment))
	}
	if len(param.Watermark) > 0 {
		buffer.WriteString(fmt.Sprintf("watermark %s ", strings.Join(param.Watermark, ",")))
	}
	if len(param.MaxDelay) > 0 {
		buffer.WriteString(fmt.Sprintf("max_delay %s ", strings.Join(param.MaxDelay, ",")))
	}
	if len(param.RollUp) > 0 {
		buffer.WriteString(fmt.Sprintf("rollup %s ", strings.Join(param.RollUp, ",")))
	}
	if len(param.Sma) > 0 {
		buffer.WriteString(fmt.Sprintf("sma %s ", strings.Join(param.Sma, ",")))
	}
	if param.TTL > 0 {
		buffer.WriteString(fmt.Sprintf("ttl %d ", param.TTL))
	}

	return strings.Trim(buffer.String(), " ")
}
