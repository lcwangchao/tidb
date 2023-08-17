// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"
	"github.com/pingcap/tidb/sessionctx/variable/proto"
	"io"
	"os"
	"reflect"
	"strings"
	"text/template"
)

var sessionVarAliasGetterSetterTpl = template.Must(template.New("").Parse(`
func (s *SessionSysVarAlias) Get{{.AliasField.Name}}() {{.AliasField.Type}} {
	return s.{{.AliasField.Name}}
}

{{ if .Setter -}}
func (s *SessionSysVarAlias) Set{{.AliasField.Name}}(val {{.AliasField.Type}}) {
	s.{{.AliasField.Name}} = val
}

{{end -}}
func (s *SessionSysVarAlias) Set{{.AliasField.Name}}WithVarStr(val string) error {
	s.{{.AliasField.Name}} = {{.StrCastValExpr}}
	return nil
}

func (s *SessionSysVarAlias) Get{{.AliasField.Name}}AsVarStr() (string, error) {
	return {{.ValCastStrExpr}}, nil
}
`))

var sessionSysVarTpl = template.Must(template.New("").Parse(`	{
		Scope: {{- if .GlobalScope }} ScopeGlobal | ScopeSession {{- else }} ScopeSession {{- end }},
		Name: {{ .VarCamelCaseName }},
		Type: {{.Type}},
		Value: {{.DefValCastStrExpr}},
{{- if .Setter }}
		GetSession: func(s *SessionVars) (string, error) {
			return s.sysVarAlias.Get{{.AliasField.Name}}AsVarStr()
		},
{{- end }}
		SetSession: func(s *SessionVars, val string) error {
			return s.sysVarAlias.Set{{.AliasField.Name}}WithVarStr(val)
		},
	},
`))

type tplVarWrapper struct {
	*proto.SessionSystemVarDefItem
	ValCastStrExpr    string
	StrCastValExpr    string
	DefValCastStrExpr string
}

func newVarWrapper(item *proto.SessionSystemVarDefItem) *tplVarWrapper {
	wrapper := &tplVarWrapper{
		SessionSystemVarDefItem: item,
		ValCastStrExpr:          fmt.Sprintf("s.%s", item.AliasField.Name),
		StrCastValExpr:          "val",
		DefValCastStrExpr:       fmt.Sprintf("Def%s", item.AliasField.Name),
	}
	castStrFunc := ""
	switch item.AliasField.Type.Kind() {
	case reflect.Bool:
		castStrFunc = "BoolToOnOff(%s)"
		wrapper.StrCastValExpr = "TiDBOptOn(val)"
	case reflect.Int:
		castStrFunc = "strconv.Itoa(%s)"
		wrapper.StrCastValExpr = fmt.Sprintf("TidbOptInt(val, Def%s)", item.AliasField.Name)
	case reflect.Int64:
		castStrFunc = "strconv.FormatInt(%s, 10)"
		wrapper.StrCastValExpr = fmt.Sprintf("TidbOptInt64(val, Def%s)", item.AliasField.Name)
	}

	if castStrFunc != "" {
		wrapper.DefValCastStrExpr = fmt.Sprintf(castStrFunc, "Def"+wrapper.AliasField.Name)
		wrapper.ValCastStrExpr = fmt.Sprintf(castStrFunc, wrapper.ValCastStrExpr)
	}

	return wrapper
}

func main() {
	spec, err := proto.GetSessionSystemVarsSpec()
	if err != nil {
		panic(err)
	}
	generateSessionAliasFile(spec)
	generateSysVarsFile(spec)
}

func mustWrite(w io.StringWriter, format string, args ...any) {
	if _, err := w.WriteString(fmt.Sprintf(format, args...)); err != nil {
		panic(err)
	}
}

func mustWriteFile(path string, sb *strings.Builder) {
	if err := os.WriteFile(path, []byte(sb.String()), 0644); err != nil {
		panic(err)
	}
}

func generateSessionAliasFile(spec *proto.SessionSystemVarDefsSpec) {
	w := &strings.Builder{}
	mustWrite(w, `package variable

import (
	"strconv"

	"github.com/pingcap/tidb/sessionctx/variable/proto"
)

var _ SessionSysVarAliasReaderMutator = &SessionSysVarAlias{}
var _ aliasInnerOperator = &SessionSysVarAlias{}

type SessionSysVarAliasReaderMutator interface {
	SessionSysVarAliasReader
	SessionSysVarAliasMutator
}

type SessionSysVarAliasReader interface {
`)
	for _, item := range spec.Items {
		mustWrite(w, "\tGet%s() %s\n", item.AliasField.Name, item.AliasField.Type)
	}
	mustWrite(w, "}\n")
	mustWrite(w, `
type SessionSysVarAliasMutator interface {
`)
	for _, item := range spec.Items {
		if item.Setter {
			mustWrite(w, "\tSet%s(%s)\n", item.AliasField.Name, item.AliasField.Type)
		}
	}
	mustWrite(w, "}\n")

	mustWrite(w, `
type aliasInnerOperator interface {
`)
	for _, item := range spec.Items {
		mustWrite(w, "\tSet%sWithVarStr(string) error\n", item.AliasField.Name)
		mustWrite(w, "\tGet%sAsVarStr() (string, error)\n", item.AliasField.Name)
	}
	mustWrite(w, "}\n")

	mustWrite(w, `
type SessionSysVarAlias proto.SessionSystemVarDefs

func (s *SessionSysVarAlias) clone() *SessionSysVarAlias {
	var cloned SessionSysVarAlias
	cloned = *s
	return &cloned
}
`)
	for _, item := range spec.Items {
		if err := sessionVarAliasGetterSetterTpl.Execute(w, newVarWrapper(item)); err != nil {
			panic(err)
		}
	}
	mustWriteFile("./sessionctx/variable/session_alias_generated.go", w)
}

func generateSysVarsFile(spec *proto.SessionSystemVarDefsSpec) {
	w := &strings.Builder{}
	mustWrite(w, `package variable

import "strconv"
`)

	mustWrite(w, `
const (
`)
	for _, item := range spec.Items {
		mustWrite(w, "\t%s = \"%s\"\n", item.VarCamelCaseName, item.Var)
	}
	mustWrite(w, ")\n")

	mustWrite(w, `
const (
`)
	for _, item := range spec.Items {
		mustWrite(w, "\tDef%s %s = %s\n", item.AliasField.Name, item.AliasField.Type.String(), item.DefValue)
	}
	mustWrite(w, ")\n")

	mustWrite(w, `
var generatedSessionSysVars = []*SysVar{
`)
	for _, item := range spec.Items {
		if err := sessionSysVarTpl.Execute(w, newVarWrapper(item)); err != nil {
			panic(err)
		}
	}
	mustWrite(w, "}\n")
	mustWriteFile("./sessionctx/variable/sysvar_generated.go", w)
}
