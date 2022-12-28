package field

import (
	"bytes"
	"go/ast"
	"sort"
	"strings"
	"sync"
	"taos_importer/internal/common"
	"time"
)

func init() {
	funcMap["left_pad"] = leftPad
	funcMap["right_pad"] = rightPad
	funcMap["date_parse"] = dateParse
	funcMap["avoid_datetime_conflict"] = avoidDatetimeConflict
	funcMap["sub_str"] = subStr
	funcMap["contact"] = contact
	funcMap["index_of"] = indexOf
}

var funcMap = map[string]func(args []ast.Expr, data map[string]any) (any, error){}

func leftPad(args []ast.Expr, data map[string]any) (any, error) {
	str, padStr, toLength, err := _padParam(args, data)
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

func rightPad(args []ast.Expr, data map[string]any) (any, error) {
	str, padStr, toLength, err := _padParam(args, data)
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

func subStr(args []ast.Expr, data map[string]any) (any, error) {
	if len(args) != 3 {
		return nil, illegalParams
	}

	strArg, err := eval(args[0], data)
	if err != nil {
		return nil, err
	}
	startArg, err := eval(args[1], data)
	if err != nil {
		return nil, err
	}
	endArg, err := eval(args[2], data)
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

func contact(args []ast.Expr, data map[string]any) (any, error) {
	strs := make([]string, 0, len(args))

	for _, arg := range args {
		strArg, err := eval(arg, data)
		if err != nil {
			return nil, err
		}
		strs = append(strs, common.String(strArg))
	}

	return strings.Join(strs, ""), nil
}

func indexOf(args []ast.Expr, data map[string]any) (any, error) {
	if len(args) != 2 {
		return nil, illegalParams
	}

	strArg, err := eval(args[0], data)
	if err != nil {
		return nil, err
	}
	subStrArg, err := eval(args[1], data)
	if err != nil {
		return nil, err
	}

	str := common.String(strArg)
	sub := common.String(subStrArg)

	return strings.Index(str, sub), nil
}

func dateParse(args []ast.Expr, data map[string]any) (any, error) {
	if len(args) != 2 {
		return nil, illegalParams
	}

	dateArg, err := eval(args[0], data)
	if err != nil {
		return nil, err
	}
	formatArg, err := eval(args[1], data)
	if err != nil {
		return nil, err
	}

	date := common.String(dateArg)
	format := common.String(formatArg) // YYYY MM DD 格式

	return _parseDate(format, date)
}

var usedDatetime *datetimeCache

func avoidDatetimeConflict(args []ast.Expr, data map[string]any) (any, error) {
	if len(args) != 2 {
		return nil, illegalParams
	}
	dateArg, err := eval(args[0], data)
	if err != nil {
		return nil, err
	}

	if usedDatetime == nil {
		cacheSizeArg, err := eval(args[1], data)
		if err != nil {
			return nil, err
		}
		cacheSize, err := common.Int(cacheSizeArg)
		if err != nil {
			return nil, err
		}
		usedDatetime = newDatetimeCache(cacheSize)
	}

	date, err := common.Time(dateArg)
	if err != nil {
		return nil, err
	}

	for {
		unixMicro := date.UnixNano()
		if exist := usedDatetime.exists(unixMicro); !exist {
			usedDatetime.cache(unixMicro)
			break
		}
		date = date.Add(time.Microsecond)
	}

	return date, nil
}

func _padParam(args []ast.Expr, data map[string]any) (string, string, int, error) {
	if len(args) != 3 {
		return "", "", 0, illegalParams
	}
	strArg, err := eval(args[0], data)
	if err != nil {
		return "", "", 0, err
	}
	padStrArg, err := eval(args[1], data)
	if err != nil {
		return "", "", 0, err
	}
	toLengthArg, err := eval(args[2], data)
	if err != nil {
		return "", "", 0, err
	}

	str := common.String(strArg)
	padStr := common.String(padStrArg)
	toLength, err := common.Int(toLengthArg)
	return str, padStr, toLength, err
}

func _parseDate(format, date string) (time.Time, error) {
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
	format, date = _nsFormatStyle(format, date, "SSSSSSSSS", "000000000")

	// 微秒
	format, date = _nsFormatStyle(format, date, "SSSSSS", "000000")

	// 毫秒
	format, date = _nsFormatStyle(format, date, "SSS", "000")

	return time.Parse(format, date)
}

func _nsFormatStyle(format, date string, old, new string) (string, string) {
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
	sync.RWMutex

	data []int64
	size int
}

func newDatetimeCache(size int) *datetimeCache {
	s := make([]int64, 0, size+2)
	c := datetimeCache{data: s, size: size}
	return &c
}

func (c *datetimeCache) cache(x int64) {
	c.Lock()
	c.Unlock()

	if len(c.data) >= c.size {
		c.data = c.data[1:]
	}
	c.data = append(c.data, x)
	sort.Sort(c)
}

func (c *datetimeCache) exists(x int64) bool {
	c.RLock()
	defer c.RUnlock()

	i, j := 0, len(c.data)
	for i < j {
		h := int(uint(i+j) >> 1)
		if c.data[h] == x {
			return true
		}
		if c.data[h] > x {
			j = h
		} else {
			i = h + 1
		}
	}

	return false
}

func (c *datetimeCache) Len() int {
	return len(c.data)
}

func (c *datetimeCache) Less(i, j int) bool {
	return c.data[i] < c.data[j]
}

func (c *datetimeCache) Swap(i, j int) {
	c.data[i], c.data[j] = c.data[j], c.data[i]
}
