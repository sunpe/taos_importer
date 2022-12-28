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

var expressionCache sync.Map // cache expression parsed result, key is expression, value is ast.Expr

func Extract(expression string, data map[string]any) (any, error) {
	expr, err := parseExpression(expression)
	if err != nil {
		return nil, err
	}

	return eval(expr, data)
}

func parseExpression(expression string) (expr ast.Expr, err error) {
	if len(expression) == 0 {
		return nil, emptyExpression
	}

	if exp, ok := expressionCache.Load(expression); ok {
		expr = exp.(ast.Expr)
		return
	}

	expr, err = parser.ParseExpr(expression)
	if err != nil {
		return nil, err
	}
	expressionCache.Store(expression, expr)
	return
}

func eval(expr ast.Expr, data map[string]any) (any, error) {
	switch expr := expr.(type) {
	case *ast.BasicLit: // base type
		return basicLit(expr)
	case *ast.BinaryExpr: // binary field
		return evalForBinaryExpr(expr, data)
	case *ast.CallExpr:
		return evalForFunc(expr.Fun.(*ast.Ident).Name, expr.Args, data)
	case *ast.ParenExpr: // parenthesized field
		return eval(expr.X, data)
	case *ast.UnaryExpr: // unary field
		return evalForUnaryExpr(expr, data)
	case *ast.Ident: // identifier
		return evalForIdent(expr, data)
	default:
		return nil, fmt.Errorf("unknown ast node type [%s]", expr)
	}
}

func basicLit(lit *ast.BasicLit) (value any, err error) {
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

func evalForBinaryExpr(expr *ast.BinaryExpr, data map[string]any) (any, error) {
	x, xErr := eval(expr.X, data)
	if xErr != nil {
		return nil, xErr
	}
	if x == nil {
		return nil, fmt.Errorf("x [%v] is nil", x)
	}
	y, yErr := eval(expr.Y, data)

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

func evalForFunc(funcName string, args []ast.Expr, data map[string]any) (any, error) {
	handler, ok := funcMap[funcName]
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

func evalForUnaryExpr(expr *ast.UnaryExpr, data map[string]any) (any, error) {
	x, err := eval(expr.X, data)
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

func evalForIdent(expr *ast.Ident, data map[string]any) (any, error) {
	if expr.Name == "true" { // true
		return true, nil
	}
	if expr.Name == "false" { // false
		return false, nil
	}
	return data[expr.Name], nil
}
