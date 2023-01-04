package field

import (
	"bytes"
	"context"
	"go/ast"
	"strings"
	"sync"
	"taos_importer/internal/common"
	"time"

	"github.com/allegro/bigcache/v3"
)

func (e *Extractor) leftPad(args []ast.Expr, data map[string]any) (any, error) {
	str, padStr, toLength, err := e.padParam(args, data)
	if err != nil {
		return nil, err
	}

	if len(str) >= toLength {
		return str, nil
	}

	needPad := toLength - len(str)
	var buffer bytes.Buffer
	for i := 0; i < needPad; i++ {
		buffer.WriteString(padStr)
	}
	buffer.WriteString(str)

	return buffer.String(), nil
}

func (e *Extractor) rightPad(args []ast.Expr, data map[string]any) (any, error) {
	str, padStr, toLength, err := e.padParam(args, data)
	if err != nil {
		return nil, err
	}

	if len(str) >= toLength {
		return str, nil
	}

	needPad := toLength - len(str)
	var buffer bytes.Buffer
	buffer.WriteString(str)
	for i := 0; i < needPad; i++ {
		buffer.WriteString(padStr)
	}

	return buffer.String(), nil
}

func (e *Extractor) subStr(args []ast.Expr, data map[string]any) (any, error) {
	if len(args) != 3 {
		return nil, illegalParams
	}

	strArg, err := e.eval(args[0], data)
	if err != nil {
		return nil, err
	}
	startArg, err := e.eval(args[1], data)
	if err != nil {
		return nil, err
	}
	endArg, err := e.eval(args[2], data)
	if err != nil {
		return nil, err
	}

	str := common.String(strArg)
	start, err := common.Int(startArg)
	if err != nil {
		return nil, err
	}
	end, err := common.Int(endArg)
	if err != nil {
		return nil, err
	}

	if start > end || start > len(str) || end > len(str) {
		return nil, illegalParams
	}

	return str[start:end], nil
}

func (e *Extractor) contact(args []ast.Expr, data map[string]any) (any, error) {
	ss := make([]string, 0, len(args))

	for _, arg := range args {
		strArg, err := e.eval(arg, data)
		if err != nil {
			return nil, err
		}
		ss = append(ss, common.String(strArg))
	}

	return strings.Join(ss, ""), nil
}

func (e *Extractor) indexOf(args []ast.Expr, data map[string]any) (any, error) {
	if len(args) != 2 {
		return nil, illegalParams
	}

	strArg, err := e.eval(args[0], data)
	if err != nil {
		return nil, err
	}
	subStrArg, err := e.eval(args[1], data)
	if err != nil {
		return nil, err
	}

	str := common.String(strArg)
	sub := common.String(subStrArg)

	return strings.Index(str, sub), nil
}

func (e *Extractor) dateParse(args []ast.Expr, data map[string]any) (any, error) {
	if len(args) != 3 {
		return nil, illegalParams
	}

	dateArg, err := e.eval(args[0], data)
	if err != nil {
		return nil, err
	}
	formatArg, err := e.eval(args[1], data)
	if err != nil {
		return nil, err
	}
	locationArg, err := e.eval(args[2], data)
	if err != nil {
		return nil, err
	}
	location, err := time.LoadLocation(common.String(locationArg))
	if err != nil {
		return nil, err
	}

	date := common.String(dateArg)
	format := common.String(formatArg) // YYYY MM DD 格式

	return parseDate(format, date, location)
}

var usedDatetime *datetimeCache
var avoidDatetimeLocker sync.Mutex

func (e *Extractor) avoidDatetimeConflict(args []ast.Expr, data map[string]any) (any, error) {
	if len(args) != 3 {
		return nil, illegalParams
	}
	dateArg, err := e.eval(args[0], data)
	if err != nil {
		return nil, err
	}
	date, err := common.Time(dateArg)
	if err != nil {
		return nil, err
	}

	if usedDatetime == nil {
		avoidDatetimeLocker.Lock()
		defer avoidDatetimeLocker.Unlock()

		if usedDatetime == nil {
			durationArg, err := e.eval(args[1], data) // cache duration
			if err != nil {
				return nil, err
			}
			precisionArg, err := e.eval(args[2], data) //timestamp precision
			if err != nil {
				return nil, err
			}

			duration, err := common.Int64(durationArg)
			if err != nil {
				return nil, err
			}
			precision := common.String(precisionArg)

			usedDatetime = newDatetimeCache(time.Duration(duration)*time.Millisecond, precision, e.locker)
		}
	}

	date = usedDatetime.cacheAndGet(date)

	return date, nil
}

func (e *Extractor) padParam(args []ast.Expr, data map[string]any) (string, string, int, error) {
	if len(args) != 3 {
		return "", "", 0, illegalParams
	}
	strArg, err := e.eval(args[0], data)
	if err != nil {
		return "", "", 0, err
	}
	padStrArg, err := e.eval(args[1], data)
	if err != nil {
		return "", "", 0, err
	}
	toLengthArg, err := e.eval(args[2], data)
	if err != nil {
		return "", "", 0, err
	}

	str := common.String(strArg)
	padStr := common.String(padStrArg)
	toLength, err := common.Int(toLengthArg)
	return str, padStr, toLength, err
}

func parseDate(format, date string, location *time.Location) (time.Time, error) {
	if strings.Contains(format, "YYYY") {
		format = strings.ReplaceAll(format, "YYYY", "2006")
	}
	if strings.Contains(format, "yyyy") {
		format = strings.ReplaceAll(format, "yyyy", "2006")
	}
	if strings.Contains(format, "MM") {
		format = strings.ReplaceAll(format, "MM", "01")
	}
	if strings.Contains(format, "DD") {
		format = strings.ReplaceAll(format, "DD", "02")
	}
	if strings.Contains(format, "dd") {
		format = strings.ReplaceAll(format, "dd", "02")
	}
	if strings.Contains(format, "HH") {
		format = strings.ReplaceAll(format, "HH", "15")
	}
	if strings.Contains(format, "hh") {
		format = strings.ReplaceAll(format, "hh", "15")
	}
	if strings.Contains(format, "mm") {
		format = strings.ReplaceAll(format, "mm", "04")
	}
	if strings.Contains(format, "ss") {
		format = strings.ReplaceAll(format, "ss", "05")
	}
	// 纳秒
	format, date = nsFormatStyle(format, date, "SSSSSSSSS", "000000000")

	// 微秒
	format, date = nsFormatStyle(format, date, "SSSSSS", "000000")

	// 毫秒
	format, date = nsFormatStyle(format, date, "SSS", "000")

	return time.ParseInLocation(format, date, location)
}

func nsFormatStyle(format, date string, old, new string) (string, string) {
	if index := strings.Index(format, old); index > 0 {
		format = strings.ReplaceAll(format, old, new)
		if format[index-1:index] != "." {
			format = format[:index] + "." + format[index:]
			date = date[:index] + "." + date[index:]
		}
	}
	return format, date
}

type datetimeCache struct {
	data      *bigcache.BigCache
	precision string
	lock      sync.Locker
}

func newDatetimeCache(duration time.Duration, precision string, locker sync.Locker) *datetimeCache {
	cacheConf := bigcache.DefaultConfig(duration)
	cacheConf.CleanWindow = 10 * duration
	cacheConf.Verbose = false
	cache, _ := bigcache.New(context.Background(), cacheConf)

	return &datetimeCache{data: cache, precision: precision, lock: locker}
}

func (c *datetimeCache) cacheAndGet(x time.Time) time.Time {
	c.lock.Lock()
	defer c.lock.Unlock()

	ts := c.get(x)
	key := common.String(ts)
	for {
		if _, err := c.data.Get(key); err == bigcache.ErrEntryNotFound {
			break
		}

		ts, x = c.addAndGet(x)
		key = common.String(ts)
	}

	_ = c.data.Set(key, []byte{})
	return x
}

func (c *datetimeCache) get(ts time.Time) int64 {
	if c.precision == "ns" {
		return ts.UnixNano()
	} else if c.precision == "us" {
		return ts.UnixMicro()
	}
	return ts.UnixMilli()
}

func (c *datetimeCache) addAndGet(ts time.Time) (int64, time.Time) {
	if c.precision == "ns" {
		ts = ts.Add(time.Nanosecond)
		return ts.UnixNano(), ts
	} else if c.precision == "us" {
		ts = ts.Add(time.Microsecond)
		return ts.UnixMicro(), ts
	}

	ts = ts.Add(time.Millisecond)
	return ts.UnixMilli(), ts
}
