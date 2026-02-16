package compiler

import (
	"fmt"
	"strings"

	"syndrql-training/ir"
)

// CompileExpr compiles an expression tree to a SyndrQL string.
func CompileExpr(e *ir.Expr) string {
	if e == nil {
		return ""
	}

	switch e.Type {
	case "literal":
		return compileLiteral(e.Value)
	case "identifier":
		return e.Name
	case "qualified":
		return fmt.Sprintf(`"%s"."%s"`, e.Bundle, e.Name)
	case "binary":
		return compileBinary(e)
	case "not":
		return "NOT " + CompileExpr(e.Left)
	case "isNull":
		return CompileExpr(e.Left) + " IS NULL"
	case "isNotNull":
		return CompileExpr(e.Left) + " IS NOT NULL"
	case "in":
		return compileIn(e, "IN")
	case "notIn":
		return compileIn(e, "NOT IN")
	case "function":
		return compileFunction(e)
	case "parameter":
		return fmt.Sprintf("$%d", e.Index)
	case "exists":
		if e.Query != nil {
			inner := CompileSelect(e.Query)
			// Remove trailing semicolon from inner SELECT
			inner = strings.TrimSuffix(inner, ";")
			return "EXISTS (" + inner + ")"
		}
		return "EXISTS"
	default:
		return ""
	}
}

func compileLiteral(v *ir.Value) string {
	if v == nil {
		return "NULL"
	}
	switch v.Type {
	case "string":
		if s, ok := v.Raw.(string); ok {
			return fmt.Sprintf(`"%s"`, s)
		}
		return `""`
	case "int":
		if f, ok := v.Raw.(float64); ok {
			return fmt.Sprintf("%d", int64(f))
		}
		return "0"
	case "float":
		if f, ok := v.Raw.(float64); ok {
			// Format without trailing zeros but ensure decimal point
			s := fmt.Sprintf("%g", f)
			return s
		}
		return "0"
	case "bool":
		if b, ok := v.Raw.(bool); ok {
			if b {
				return "true"
			}
			return "false"
		}
		return "false"
	case "null":
		return "NULL"
	default:
		return "NULL"
	}
}

func compileBinary(e *ir.Expr) string {
	left := CompileExpr(e.Left)
	right := CompileExpr(e.Right)

	// Parenthesize nested binary expressions for AND/OR precedence
	if needsParens(e.Left, e.Operator) {
		left = "(" + left + ")"
	}
	if needsParens(e.Right, e.Operator) {
		right = "(" + right + ")"
	}

	return left + " " + e.Operator + " " + right
}

func needsParens(child *ir.Expr, parentOp string) bool {
	if child == nil || child.Type != "binary" {
		return false
	}
	// OR inside AND needs parentheses
	if parentOp == "AND" && child.Operator == "OR" {
		return true
	}
	return false
}

func compileIn(e *ir.Expr, keyword string) string {
	left := CompileExpr(e.Left)
	var values []string
	for _, arg := range e.Args {
		values = append(values, CompileExpr(arg))
	}
	return left + " " + keyword + " (" + strings.Join(values, ", ") + ")"
}

func compileFunction(e *ir.Expr) string {
	var args []string
	for _, arg := range e.Args {
		args = append(args, CompileExpr(arg))
	}

	name := e.Name
	if e.IsBuiltIn {
		name = "F:" + name
	}

	return name + "(" + strings.Join(args, ", ") + ")"
}

// CompileSelectFieldExpr compiles a SELECT field expression.
// This handles special cases like * and aggregates.
func CompileSelectFieldExpr(e *ir.Expr) string {
	if e == nil {
		return "*"
	}
	return CompileExpr(e)
}
