package field

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"sync"
	"taos_importer/internal/common"
)

var emptyExpression = errors.New("field is nil")
var illegalParams = errors.New("illegal params")

var defaultLocker sync.Mutex
var DefaultExtractor = NewExtractor(&defaultLocker)

func NewExtractor(locker sync.Locker) *Extractor {
	e := &Extractor{
		locker: locker,
	}
	e.funcMap = map[string]func(args []ast.Expr, data map[string]any) (any, error){
		"left_pad":                e.leftPad,
		"right_pad":               e.rightPad,
		"date_parse":              e.dateParse,
		"avoid_datetime_conflict": e.avoidDatetimeConflict,
		"sub_str":                 e.subStr,
		"contact":                 e.contact,
		"index_of":                e.indexOf,
	}

	return e
}

type Extractor struct {
	exprCache sync.Map // cache expression parsed result, key is expression, value is ast.Expr
	funcMap   map[string]func(args []ast.Expr, data map[string]any) (any, error)
	locker    sync.Locker
}

func (e *Extractor) Extract(expression string, data map[string]any) (any, error) {
	expr, err := e.parseExpression(expression)
	if err != nil {
		return nil, err
	}

	return e.eval(expr, data)
}

func (e *Extractor) parseExpression(expression string) (expr ast.Expr, err error) {
	if len(expression) == 0 {
		return nil, emptyExpression
	}

	if exp, ok := e.exprCache.Load(expression); ok {
		expr = exp.(ast.Expr)
		return
	}

	expr, err = parser.ParseExpr(expression)
	if err != nil {
		return nil, err
	}
	e.exprCache.Store(expression, expr)
	return
}

func (e *Extractor) eval(expr ast.Expr, data map[string]any) (any, error) {
	switch expr := expr.(type) {
	case *ast.BasicLit: // base type
		return e.basicLit(expr)
	case *ast.BinaryExpr: // binary field
		return e.evalForBinaryExpr(expr, data)
	case *ast.CallExpr:
		return e.evalForFunc(expr.Fun.(*ast.Ident).Name, expr.Args, data)
	case *ast.ParenExpr: // parenthesized field
		return e.eval(expr.X, data)
	case *ast.UnaryExpr: // unary field
		return e.evalForUnaryExpr(expr, data)
	case *ast.Ident: // identifier
		return e.evalForIdent(expr, data)
	default:
		return nil, fmt.Errorf("unknown ast node type [%s]", expr)
	}
}

func (e *Extractor) basicLit(lit *ast.BasicLit) (value any, err error) {
	switch lit.Kind {
	case token.INT:
		value, err = strconv.ParseInt(lit.Value, 10, 64)
	case token.FLOAT:
		value, err = strconv.ParseFloat(lit.Value, 64)
	case token.STRING:
		value, err = strconv.Unquote(lit.Value)
	default:
		err = fmt.Errorf("unknown lit type [%s]", lit.Kind) // token.CHAR token.IMAG
	}
	return value, err
}

func (e *Extractor) evalForBinaryExpr(expr *ast.BinaryExpr, data map[string]any) (any, error) {
	x, xErr := e.eval(expr.X, data)
	if xErr != nil {
		return nil, xErr
	}
	if x == nil {
		return nil, fmt.Errorf("x [%v] is nil", x)
	}
	y, yErr := e.eval(expr.Y, data)

	if yErr != nil {
		return nil, yErr
	}
	if x == nil {
		return nil, fmt.Errorf("y [%v] is nil", y)
	}
	switch x := x.(type) {
	case int, int32, int64:
		xInt, err := common.Int64(x)
		if err != nil {
			return nil, err
		}
		yInt, err := common.Int64(y)
		if err != nil {
			return nil, err
		}
		return evalForNum[int64](xInt, yInt, expr.Op)
	case float32, float64:
		xFloat, err := common.Float64(x)
		if err != nil {
			return nil, err
		}
		yFloat, err := common.Float64(y)
		if err != nil {
			return nil, err
		}
		return evalForNum[float64](xFloat, yFloat, expr.Op)
	case string:
		xString := common.String(x)
		yString := common.String(y)

		switch expr.Op {
		case token.EQL:
			return xString == yString, nil
		case token.NEQ:
			return xString != yString, nil
		case token.ADD:
			return xString + yString, nil
		default:
			return nil, fmt.Errorf("unsupported operator: [%s]", expr.Op)
		}
	case bool:
		xb, errX := common.Bool(x)
		yb, errY := common.Bool(y)
		if errX != nil || errY != nil {
			return nil, fmt.Errorf("eval field [%v %s %v] failed", x, expr.Op, y)
		}
		switch expr.Op {
		case token.LAND:
			return xb && yb, nil
		case token.LOR:
			return xb || yb, nil
		case token.EQL:
			return xb == yb, nil
		case token.NEQ:
			return xb != yb, nil
		default:
			return nil, fmt.Errorf("unsupported operator: [%s]", expr.Op)
		}
	default:
		return nil, fmt.Errorf("unknown operation [%s]", expr.Op)
	}
}

func (e *Extractor) evalForFunc(funcName string, args []ast.Expr, data map[string]any) (any, error) {
	handler, ok := e.funcMap[funcName]
	if !ok {
		return nil, fmt.Errorf("unknown func %s", funcName)
	}
	return handler(args, data)
}

func evalForNum[T int | int32 | int64 | float32 | float64](x, y T, op token.Token) (any, error) {
	switch op {
	case token.EQL:
		return x == y, nil
	case token.NEQ:
		return x != y, nil
	case token.GTR:
		return x > y, nil
	case token.LSS:
		return x < y, nil
	case token.GEQ:
		return x >= y, nil
	case token.LEQ:
		return x <= y, nil
	case token.ADD:
		return x + y, nil
	case token.SUB:
		return x - y, nil
	case token.MUL:
		return x * y, nil
	case token.QUO:
		if y == 0 {
			return 0, nil
		}
		return x / y, nil
	default:
		return nil, fmt.Errorf("unsupported operator for number: [%s]", op)
	}
}

func (e *Extractor) evalForUnaryExpr(expr *ast.UnaryExpr, data map[string]any) (any, error) {
	x, err := e.eval(expr.X, data)
	if err != nil {
		return nil, err
	}
	if x == nil {
		return nil, fmt.Errorf("x [%v] is nil", x)
	}
	if b, ok := x.(bool); ok && expr.Op == token.NOT {
		return !b, nil
	}
	return nil, fmt.Errorf("unknown unary field [%v]", expr)
}

func (e *Extractor) evalForIdent(expr *ast.Ident, data map[string]any) (any, error) {
	if expr.Name == "true" { // true
		return true, nil
	}
	if expr.Name == "false" { // false
		return false, nil
	}
	return data[expr.Name], nil
}
