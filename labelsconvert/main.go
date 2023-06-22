package main

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"

	"golang.org/x/tools/go/ast/astutil"
)

func exprIsLabelsDotLabels(expr ast.Expr) bool {
	sel, ok := expr.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	if sel.Sel.Name != "Labels" {
		return false
	}
	x, ok := sel.X.(*ast.Ident)
	if !ok {
		return false
	}
	if x.Name != "labels" {
		return false
	}
	return true
}

func convertAst(fset *token.FileSet, node ast.Node) {
	// Traverse the AST and convert any labels.Labels to labels.FromStrings
	astutil.Apply(node, func(c *astutil.Cursor) bool {
		printError := func(format string, args ...any) {
			position := fset.Position(c.Node().Pos())
			fmt.Fprintf(os.Stderr, "%s:%d: ", position.Filename, position.Line)
			fmt.Fprintf(os.Stderr, format, args...)
			fmt.Fprintln(os.Stderr)
		}

		// Check if the node is a composite literal expression with a type named "Labels"
		clExpr, ok := c.Node().(*ast.CompositeLit)
		if !ok {
			return true
		}

		nodeType := clExpr.Type
		if nodeType == nil {
			parentComposite, ok := c.Parent().(*ast.CompositeLit)
			if !ok {
				return true
			}
			if arrayType, ok := parentComposite.Type.(*ast.ArrayType); !ok {
				return true
			} else if !exprIsLabelsDotLabels(arrayType.Elt) {
				return true
			}
		} else {
			// Otherwise we look for 'labels.Labels' directly
			switch expr := nodeType.(type) {
			case *ast.SelectorExpr:
				if !exprIsLabelsDotLabels(expr) {
					return true
				}
			default:
				return true
			}
		}
		// We're going to rewrite the struct initializer as a function call.
		call := &ast.CallExpr{
			Fun:    &ast.Ident{Name: "labels.FromStrings", NamePos: clExpr.Pos()},
			Rparen: clExpr.Rbrace,
		}
		// Each element in the Labels is a Label
		for _, elem := range clExpr.Elts {
			comp, ok := elem.(*ast.CompositeLit)
			if !ok {
				printError("expected CompositeLit; got %#v", elem)
				continue // TODO error handling
			}

			// Within each Label should be two key-value pairs.
			var lName, lValue ast.Expr
			for _, elem := range comp.Elts {
				kvPair, ok := elem.(*ast.KeyValueExpr)
				if !ok {
					return true
				}
				switch kvPair.Key.(*ast.Ident).Name {
				case "Name":
					lName = kvPair.Value
				case "Value":
					lValue = kvPair.Value
				default:
					printError("unexpected key: %q", kvPair.Key.(*ast.Ident).Name)
				}
			}
			if lName == nil {
				printError("Label name not found")
				return true
			} else if lValue == nil {
				printError("Label value not found")
				return true
			}
			call.Args = append(call.Args, lName, lValue)
		}

		if len(call.Args) == 0 { // Special-case empty labels.
			call.Fun = &ast.Ident{Name: "labels.EmptyLabels", NamePos: clExpr.Pos()}
		}
		c.Replace(call)

		return true
	}, nil)
}

func main() {
	fset := token.NewFileSet()
	for arg := 1; arg < len(os.Args); arg++ {
		fileName := os.Args[arg]

		node, err := parser.ParseFile(fset, fileName, nil, parser.ParseComments)
		if err != nil {
			fatal("error while parsing: %v", err)
		}

		convertAst(fset, node)

		// Format the resulting AST as Go source code
		var buf bytes.Buffer
		if err := format.Node(&buf, fset, node); err != nil {
			fatal("error while formatting: %v", err)
		}

		os.WriteFile(fileName, buf.Bytes(), 0)
	}
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format, args...)
	fmt.Fprintln(os.Stderr)
	os.Exit(1)
}
