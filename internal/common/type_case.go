package common

import (
	"fmt"
	"strconv"
	"time"
)

func String(a any) string {
	switch a := a.(type) {
	case string:
		return a
	case []byte:
		return string(a)
	case int:
		return strconv.Itoa(a)
	case int64:
		return strconv.FormatInt(a, 10)
	case float64:
		return strconv.FormatFloat(a, 'f', -1, 64)
	default:
		return fmt.Sprintf("%v", a)
	}
}

func Int(a any) (int, error) {
	switch a := a.(type) {
	case int:
		return a, nil
	case int32:
		return int(a), nil
	case int64:
		return int(a), nil
	case float32:
		return int(a), nil
	case float64:
		return int(a), nil
	case string:
		return strconv.Atoi(a)
	default:
		return 0, fmt.Errorf("unknown type %T ", a)
	}
}

func Int64(a any) (int64, error) {
	switch a := a.(type) {
	case int:
		return int64(a), nil
	case int32:
		return int64(a), nil
	case int64:
		return a, nil
	case float32:
		return int64(a), nil
	case float64:
		return int64(a), nil
	case string:
		return strconv.ParseInt(a, 10, 64)
	default:
		return 0, fmt.Errorf("unknown type %T ", a)
	}
}

func Float32(a any) (float32, error) {
	switch a := a.(type) {
	case int:
		return float32(a), nil
	case int32:
		return float32(a), nil
	case int64:
		return float32(a), nil
	case float32:
		return a, nil
	case float64:
		return float32(a), nil
	case string:
		f, err := strconv.ParseFloat(a, 64)
		return float32(f), err
	default:
		return 0, fmt.Errorf("unknown type %T ", a)
	}
}

func Float64(a any) (float64, error) {
	switch a := a.(type) {
	case int:
		return float64(a), nil
	case int32:
		return float64(a), nil
	case int64:
		return float64(a), nil
	case float32:
		return float64(a), nil
	case float64:
		return a, nil
	case string:
		return strconv.ParseFloat(a, 64)
	default:
		return 0, fmt.Errorf("unknown type %T ", a)
	}
}

func Bool(a any) (bool, error) {
	switch a := a.(type) {
	case bool:
		return a, nil
	case int:
		return a == 1, nil
	case int64:
		return a == 1, nil
	case string:
		return strconv.ParseBool(a)
	default:
		return false, fmt.Errorf("unknown type %T ", a)
	}
}

func Time(a any) (time.Time, error) {
	switch a := a.(type) {
	case time.Time:
		return a, nil
	default:
		return time.Now(), fmt.Errorf("unknown type %T ", a)
	}
}

func StrMap2AnyMap(m map[string]string) (am map[string]any) {
	am = make(map[string]any, len(m))
	for k, v := range m {
		am[k] = v
	}
	return
}
