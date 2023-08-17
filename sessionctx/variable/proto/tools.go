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

package proto

import (
	"fmt"
	"github.com/pingcap/errors"
	"reflect"
	"strings"

	"golang.org/x/text/language"

	"golang.org/x/text/cases"
)

type SessionSystemVarDefsSpec struct {
	Items []*SessionSystemVarDefItem
}

type SessionSystemVarDefItem struct {
	Var              string
	Type             string
	VarCamelCaseName string
	GlobalScope      bool
	DefValue         string
	Min              string
	Max              string
	IsNumber         bool
	AliasField       struct {
		Name string
		Type reflect.Type
	}
	Setter bool
}

var camelReplaces = map[string]string{
	"tidb":  "TiDB",
	"utf8":  "UTF8",
	"utf16": "UTF16",
	"utf32": "UTF32",
	"dml":   "DML",
	"ip":    "IP",
	"sql":   "SQL",
	"json":  "JSON",
}

func toCamelCase(s string) string {
	var sb strings.Builder
	title := cases.Title(language.AmericanEnglish)
	for _, part := range strings.Split(s, "_") {
		part = strings.ToLower(part)
		word, ok := camelReplaces[part]
		if !ok {
			word = title.String(part)
		}
		sb.WriteString(word)
	}
	return sb.String()
}

func GetSessionSystemVarsSpec() (*SessionSystemVarDefsSpec, error) {
	var spec SessionSystemVarDefsSpec
	tp := reflect.TypeOf(SessionSystemVarDefs{})
	spec.Items = make([]*SessionSystemVarDefItem, 0, tp.NumField())
	for i := 0; i < tp.NumField(); i++ {
		f := tp.Field(i)
		var item SessionSystemVarDefItem
		item.AliasField.Name = f.Name
		item.AliasField.Type = f.Type
		tag := f.Tag.Get("sys_var")
		segments := strings.Split(tag, ",")
		item.GlobalScope = true

		if f.Type.Kind() == reflect.Bool {
			item.Type = "TypeBool"
		}

		for j, seg := range segments {
			seg = strings.TrimSpace(seg)
			if j == 0 {
				item.Var = seg
				item.VarCamelCaseName = toCamelCase(seg)
				continue
			}

			if j == 1 {
				if len(seg) > 1 && seg[0] == '\'' && seg[len(seg)-1] == '\'' {
					seg = fmt.Sprintf("\"%s\"", seg[1:len(seg)-1])
				}
				item.DefValue = seg
				continue
			}

			kv := strings.SplitN(seg, "=", 2)
			key := kv[0]
			value := ""
			if len(kv) > 1 {
				value = kv[1]
			}

			switch key {
			case "session_only":
				item.GlobalScope = false
			case "str":
				if !reflect.TypeOf("").ConvertibleTo(f.Type) {
					return nil, errors.Errorf("invalid var %q spec: string cannot be assigned to %q", item.Var, tp.Kind())
				}
				item.Type = "TypeStr"
			case "bool":
				if f.Type.Kind() != reflect.Bool {
					return nil, errors.Errorf("invalid var %q spec: bool cannot be assigned to %q", item.Var, tp.Kind())
				}
			case "unsigned":
				if !reflect.TypeOf(uint64(0)).ConvertibleTo(f.Type) {
					return nil, errors.Errorf("invalid var %q spec: unsigned cannot be assigned to %q", item.Var, tp.Kind())
				}
				item.Type = "TypeUnsigned"
			case "min":
				item.Min = value
			case "max":
				item.Max = value
			case "range":
				minVal := ""
				maxVal := ""
				switch value {
				case "int32":
					minVal = "math.MinInt32"
					maxVal = "math.MaxInt32"
				}

				if item.Min == "" {
					item.Min = minVal
				}

				if item.Max == "" {
					item.Max = maxVal
				}
			case "setter":
				item.Setter = true
			}
		}

		if item.Type == "" {
			return nil, errors.Errorf("invalid var %q spec: type should be specified", item.Var)
		}

		spec.Items = append(spec.Items, &item)
	}
	return &spec, nil
}
