// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package extensionctx

import (
	"fmt"
	"strings"

	"github.com/pingcap/tidb/expression"
	plannercore "github.com/pingcap/tidb/planner/core"

	"github.com/pingcap/tidb/extensions"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
)

func (s *seContext) CreateStmtEventContextWithRawSQL(sql string) extensions.StmtEventContext {
	return &stmtEventContext{
		SessionContext: s,
		originalSQL:    sql,
	}
}

func (s *seContext) CreateStmtEventContextWithStmt(stmt ast.StmtNode) extensions.StmtEventContext {
	var arguments []interface{}
	if execStmt, ok := stmt.(*ast.ExecuteStmt); ok {
		preparedStmt, _ := plannercore.GetPreparedStmt(execStmt, s.GetSessionVars())
		if preparedStmt != nil {
			stmt = preparedStmt.PreparedAst.Stmt
		}

		if len(execStmt.UsingVars) > 0 {
			arguments = make([]interface{}, 0, len(execStmt.UsingVars))
			for _, useVar := range execStmt.UsingVars {
				arguments = append(arguments, useVar)
			}
		} else if execStmt.BinaryArgs != nil {
			binaryArgs := execStmt.BinaryArgs.([]expression.Expression)
			if len(binaryArgs) > 0 {
				arguments = make([]interface{}, 0, len(binaryArgs))
				for _, arg := range binaryArgs {
					arguments = append(arguments, arg)
				}
			}
		}
	}

	return &stmtEventContext{
		SessionContext: s,
		stmt:           stmt,
		rawArguments:   arguments,
	}
}

type stmtEventContext struct {
	extensions.SessionContext
	stmt         ast.StmtNode
	rawArguments []interface{}
	originalSQL  string
	arguments    []string
	digest       struct {
		normalized string
		digest     *parser.Digest
	}
	err error
}

func (sc *stmtEventContext) OriginalSQL() string {
	if sc.originalSQL == "" {
		sc.originalSQL = sc.stmt.Text()
	}
	return sc.originalSQL
}

func (sc *stmtEventContext) StmtArguments() []string {
	if len(sc.rawArguments) == 0 {
		return nil
	}

	if len(sc.arguments) == 0 {
		sc.arguments = make([]string, 0, len(sc.rawArguments))
		for _, rawArg := range sc.rawArguments {
			switch arg := rawArg.(type) {
			case ast.Node:
				var sb strings.Builder
				ctx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
				_ = arg.Restore(ctx)
				sc.arguments = append(sc.arguments, sb.String())
			case fmt.Stringer:
				sc.arguments = append(sc.arguments, arg.String())
			default:
				sc.arguments = append(sc.arguments, "unknown")
			}
		}
	}

	return sc.arguments
}

func (sc *stmtEventContext) StmtDigest() (string, *parser.Digest) {
	if sc.digest.normalized == "" {
		sc.digest.normalized, sc.digest.digest = parser.NormalizeDigest(sc.OriginalSQL())
	}
	return sc.digest.normalized, sc.digest.digest
}

func (sc *stmtEventContext) Error() error {
	return sc.err
}

func (sc *stmtEventContext) SetError(err error) {
	sc.err = err
}
