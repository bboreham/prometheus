package main

import (
	"bytes"
	"fmt"
	"go/format"
	"go/parser"
	"go/token"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSimple(t *testing.T) {
	input := `package log

import "testing"

func TestParseLabels(t *testing.T) {
	for _, tc := range []struct {
		desc   string
		input  string
		output labels.Labels
	}{
		{
			desc:   "basic",
			input:  ` + "`" + `{job="foo"}` + "`" + `,
			output: []labels.Label{{Name: "job", Value: "foo"}},
		},
		{
			desc:   "strip empty label value",
			input:  ` + "`" + `{job="foo", bar=""}` + "`" + `,
			output: []labels.Label{{Name: "job", Value: "foo"}},
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			got, _ := ParseLabels(tc.input)
			require.Equal(t, tc.output, got)
		})
	}
}
`
	fset := token.NewFileSet()
	node, err := parser.ParseFile(fset, "test.go", input, parser.ParseComments)
	require.NoError(t, err)
	convertAst(fset, node)

	// Format the resulting AST as Go source code
	var buf bytes.Buffer
	require.NoError(t, format.Node(&buf, fset, node))
	fmt.Print(buf.String())
}

func TestLabelsSlice(t *testing.T) {
	input := `package log

import (
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/grafana/loki/pkg/logqlmodel"
)

func TestDropLabelsPipeline(t *testing.T) {
	tests := []struct {
		name       string
		stages     []Stage
		lines      [][]byte
		wantLine   [][]byte
		wantLabels []labels.Labels
	}{
		{
			"drop __error__",
			[]Stage{
				NewLogfmtParser(),
				NewJSONParser(),
				NewDropLabels([]DropLabel{
					{
						nil,
						"__error__",
					},
					{
						nil,
						"__error_details__",
					},
				}),
			},
			[][]byte{
				[]byte("level=info ts=2020-10-18T18:04:22.147378997Z caller=metrics.go:81 status=200"),
			},
			[][]byte{
				[]byte("level=info ts=2020-10-18T18:04:22.147378997Z caller=metrics.go:81 status=200"),
			},
			[]labels.Labels{
				{
					{Name: "level", Value: "info"},
					{Name: "ts", Value: "2020-10-18T18:04:22.147378997Z"},
					{Name: "caller", Value: "metrics.go:81"},
					{Name: "status", Value: "200"},
				},
				{
					{Name: "app", Value: "foo"},
					{Name: "namespace", Value: "prod"},
					{Name: "pod_uuid", Value: "foo"},
					{Name: "pod_deployment_ref", Value: "foobar"},
				},
			},
		},
	}
}
	`
	fset := token.NewFileSet()
	node, err := parser.ParseFile(fset, "test.go", input, parser.ParseComments)
	require.NoError(t, err)
	convertAst(fset, node)
}
