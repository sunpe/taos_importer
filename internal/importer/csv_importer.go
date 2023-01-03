package importer

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"taos_importer/internal/common"
	"taos_importer/internal/config"
	"taos_importer/internal/field"
	"time"

	"github.com/taosdata/driver-go/v3/af"
	common2 "github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/param"
	_ "github.com/taosdata/driver-go/v3/taosSql"
)

type CsvImporter struct {
	conn        *af.Connector
	db          string
	table       string
	columns     []config.Column
	concurrent  int
	batchSize   int
	precision   int
	columnTypes *param.ColumnType
	insertSql   string
	locker      sync.Mutex
	extractor   *field.Extractor

	// aggregate
	Total      atomic.Int64
	ErrorCount atomic.Int64
	Start      time.Time
	End        time.Time
}

func NewCsvImporter(conf config.Config, table string) (importer *CsvImporter, err error) {
	conn, err := af.Open(conf.TDEngine.Host, conf.TDEngine.User, conf.TDEngine.Password, conf.DB.Name, conf.TDEngine.Port)
	if err != nil {
		return nil, err
	}
	importer = &CsvImporter{
		conn:       conn,
		db:         conf.DB.Name,
		table:      table,
		columns:    conf.STable.Columns,
		concurrent: conf.Concurrent,
		batchSize:  conf.BatchSize,
	}
	importer.extractor = field.NewExtractor(&importer.locker)
	importer.precision = dbPrecision(conf.DB.Precision)
	importer.insertSql = importer.stmtSql()
	importer.columnTypes, err = importer.columnType()
	return importer, err
}

func (c *CsvImporter) Import(ctx context.Context, csvPath string) (err error) {
	defer func() {
		_ = c.conn.Close()
	}()

	ch, err := common.ReadCsv(csvPath)
	if err != nil {
		return err
	}
	c.Start = time.Now()

	var wait sync.WaitGroup
	for i := 0; i < c.concurrent; i++ {
		wait.Add(1)
		go c.doImport(ctx, ch, &wait)
	}
	wait.Wait()
	c.End = time.Now()

	return
}

func (c *CsvImporter) doImport(ctx context.Context, ch chan map[string]string, wait *sync.WaitGroup) {
	defer wait.Done()

	tickerDuration := 200 * time.Millisecond
	ticker := time.NewTicker(tickerDuration)
	defer ticker.Stop()

	lines := make([]map[string]any, 0, c.batchSize)

	for {
		select {
		case <-ctx.Done():
			// todo
		case data, ok := <-ch:
			if !ok { // channel is closed
				c.do(ctx, lines)
				return
			}

			lines = append(lines, common.StrMap2AnyMap(data))
			if len(lines) >= c.batchSize {
				c.do(ctx, lines)
				lines = make([]map[string]any, 0, c.batchSize)
			}
		case <-ticker.C:
			c.do(ctx, lines)
			lines = make([]map[string]any, 0, c.batchSize)
		}
	}
}

func (c *CsvImporter) do(_ context.Context, lines []map[string]any) {
	if len(lines) == 0 {
		return
	}

	stmt := c.conn.InsertStmt()
	err := stmt.Prepare(c.insertSql)
	if err != nil {
		log.Printf("## prepare sql %s error %v", c.insertSql, err)
		return
	}
	defer func() { _ = stmt.Close() }()

	c.Total.Add(int64(len(lines)))
	params, err := c.params(lines)

	if err != nil {
		c.ErrorCount.Add(int64(len(lines)))
		log.Println("## parse params error ", err)
		return
	}

	if err = stmt.BindParam(params, c.columnTypes); err != nil {
		c.ErrorCount.Add(int64(len(lines)))
		log.Println("## bind params error ", c.table, err)
		return
	}
	if err = stmt.AddBatch(); err != nil {
		c.ErrorCount.Add(int64(len(lines)))
		log.Println("## add batch error ", err)
		return
	}
	if err = stmt.Execute(); err != nil {
		c.ErrorCount.Add(int64(len(lines)))
		log.Println("## insert data error ", err)
		return
	}
}

func (c *CsvImporter) stmtSql() string {
	var buffer bytes.Buffer
	buffer.WriteString(fmt.Sprintf("insert into %s.%s values (", c.db, c.table))
	for range c.columns {
		buffer.WriteString("?, ")
	}
	buffer.Truncate(buffer.Len() - 2)
	buffer.WriteString(")")
	return buffer.String()
}

func (c *CsvImporter) columnType() (*param.ColumnType, error) {
	columnType := param.NewColumnType(len(c.columns))

	for _, column := range c.columns {
		if column.Type == common.TypeTimeStamp {
			columnType.AddTimestamp()
		}
		if column.Type == common.TypeInt {
			columnType.AddInt()
		}
		if column.Type == common.TypeIntUnSigned {
			columnType.AddUInt()
		}
		if column.Type == common.TypeBigInt {
			columnType.AddBigint()
		}
		if column.Type == common.TypeBigIntUnsigned {
			columnType.AddUBigint()
		}
		if column.Type == common.TypeFloat {
			columnType.AddFloat()
		}
		if column.Type == common.TypeDouble {
			columnType.AddDouble()
		}
		if strings.HasPrefix(column.Type, common.TypeBinary) {
			length, err := getFieldLength(column.Type, common.TypeBinary)
			if err != nil {
				return nil, fmt.Errorf("column %s type error. %v", column.Field, err)
			}
			columnType.AddBinary(length)
		}
		if column.Type == common.TypeSmallInt {
			columnType.AddSmallint()
		}
		if column.Type == common.TypeSmallIntUnSigned {
			columnType.AddUSmallint()
		}
		if column.Type == common.TypeTinyInt {
			columnType.AddTinyint()
		}
		if column.Type == common.TypeTinyIntUnsigned {
			columnType.AddUTinyint()
		}
		if column.Type == common.TypeBool {
			columnType.AddBool()
		}
		if strings.HasPrefix(column.Type, common.TypeNchar) {
			length, err := getFieldLength(column.Type, common.TypeNchar)
			if err != nil {
				return nil, fmt.Errorf("column %s type error. %v", column.Field, err)
			}
			columnType.AddNchar(length)
		}
		if strings.HasPrefix(column.Type, common.TypeJson) {
			length, err := getFieldLength(column.Type, common.TypeJson)
			if err != nil {
				return nil, fmt.Errorf("column %s type error. %v", column.Field, err)
			}
			columnType.AddJson(length)
		}
		if strings.HasPrefix(column.Type, common.TypeVarchar) {
			length, err := getFieldLength(column.Type, common.TypeVarchar)
			if err != nil {
				return nil, fmt.Errorf("column %s type error. %v", column.Field, err)
			}
			columnType.AddBinary(length)
		}
	}

	return columnType, nil
}

func (c *CsvImporter) params(lines []map[string]any) (params []*param.Param, err error) {
	params = make([]*param.Param, 0, len(c.columns))

	for _, column := range c.columns {
		source := column.Source
		if len(source) == 0 {
			return nil, fmt.Errorf("column-[%s] source is null", column.Field)
		}

		p := param.NewParam(len(lines))
		for _, line := range lines {

			value, err := c.extractor.Extract(source, line)
			if err != nil {
				return nil, err
			}
			if value == nil {
				p.AddNull()
				continue
			}

			if column.Type == common.TypeTimeStamp {
				v, err := common.Time(value)
				if err != nil {
					return nil, err
				}
				p.AddTimestamp(v, c.precision)
			}

			if column.Type == common.TypeInt || column.Type == common.TypeIntUnSigned || column.Type == common.TypeBigInt ||
				column.Type == common.TypeBigIntUnsigned || column.Type == common.TypeSmallInt || column.Type == common.TypeSmallIntUnSigned ||
				column.Type == common.TypeTinyInt || column.Type == common.TypeTinyIntUnsigned {
				v, err := common.Int(value)
				if err != nil {
					return nil, err
				}
				switch column.Type {
				case common.TypeInt:
					p.AddInt(v)
				case common.TypeIntUnSigned:
					p.AddUInt(uint(v))
				case common.TypeBigInt:
					p.AddBigint(v)
				case common.TypeBigIntUnsigned:
					p.AddUBigint(uint(v))
				case common.TypeSmallInt:
					p.AddSmallint(v)
				case common.TypeSmallIntUnSigned:
					p.AddUSmallint(uint(v))
				case common.TypeTinyInt:
					p.AddTinyint(v)
				case common.TypeTinyIntUnsigned:
					p.AddUTinyint(uint(v))
				}
			}

			if column.Type == common.TypeFloat {
				v, err := common.Float32(value)
				if err != nil {
					return nil, err
				}
				p.AddFloat(v)
			}
			if column.Type == common.TypeDouble {
				v, err := common.Float64(value)
				if err != nil {
					return nil, err
				}
				p.AddDouble(v)
			}

			if column.Type == common.TypeBool {
				v, err := common.Bool(value)
				if err != nil {
					return nil, err
				}
				p.AddBool(v)
			}

			if strings.HasPrefix(column.Type, common.TypeBinary) {
				p.AddBinary([]byte(common.String(value)))
			}
			if strings.HasPrefix(column.Type, common.TypeNchar) {
				p.AddNchar(common.String(value))
			}
			if strings.HasPrefix(column.Type, common.TypeJson) {
				p.AddJson([]byte(common.String(value)))
			}
			if strings.HasPrefix(column.Type, common.TypeVarchar) {
				p.AddBinary([]byte(common.String(value)))
			}
		}
		params = append(params, p)
	}

	return
}

func getFieldLength(fieldType string, baseType string) (int, error) {
	t := strings.Trim(fieldType, baseType)
	t = strings.TrimLeft(t, "(")
	t = strings.TrimRight(t, ")")
	return strconv.Atoi(t)
}

func dbPrecision(p string) int {
	if p == "ns" {
		return common2.PrecisionNanoSecond
	}
	if p == "us" {
		return common2.PrecisionMicroSecond
	}
	return common2.PrecisionMilliSecond
}
