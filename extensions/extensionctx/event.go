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
	"context"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/extensions"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/terror"
	plannercore "github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
)

func (s *seContext) CreateStmtEventContextWithRawSQL(sql string) extensions.StmtEventContext {
	return &stmtEventContext{
		seContext:   s,
		rawStmtText: sql,
	}
}

func (s *seContext) CreateStmtEventContextWithStmt(stmt ast.StmtNode) extensions.StmtEventContext {
	stmtEventCtx := &stmtEventContext{
		seContext: s,
		stmt:      stmt,
	}

	if execStmt, ok := stmt.(*ast.ExecuteStmt); ok {
		stmtEventCtx.executeStmt = execStmt
		stmtEventCtx.prepared, _ = plannercore.GetPreparedStmt(execStmt, s.GetSessionVars())
	}
	return stmtEventCtx
}

type stmtEventContext struct {
	*seContext
	stmt        ast.StmtNode
	rawStmtText string

	stmtCtx *stmtctx.StatementContext
	err     error

	executeStmt *ast.ExecuteStmt
	prepared    *plannercore.PlanCacheStmt
	argsText    string
}

func (sc *stmtEventContext) getStmtCtx() *stmtctx.StatementContext {
	if stmtCtx := sc.GetSessionVars().StmtCtx; stmtCtx != nil && !stmtCtx.Expired {
		return stmtCtx
	}

	if sc.stmtCtx == nil {
		stmtCtx := &stmtctx.StatementContext{}
		if sc.prepared != nil {
			stmtCtx.OriginalSQL = sc.prepared.StmtText
			stmtCtx.InitSQLDigest(sc.prepared.NormalizedSQL, sc.prepared.SQLDigest)
		} else if sc.stmt != nil {
			stmtCtx.OriginalSQL = sc.stmt.Text()
		} else {
			stmtCtx.OriginalSQL = sc.rawStmtText
		}
		sc.stmtCtx = stmtCtx
	}

	return sc.stmtCtx
}

func (sc *stmtEventContext) OriginalSQL() string {
	return sc.getStmtCtx().OriginalSQL
}

func (sc *stmtEventContext) SQLDigest() (normalized string, sqlDigest *parser.Digest) {
	return sc.getStmtCtx().SQLDigest()
}

var planBuilderPool = sync.Pool{
	New: func() interface{} {
		return plannercore.NewPlanBuilder()
	},
}

func (sc *stmtEventContext) ArgumentsText() string {
	executeStmt := sc.executeStmt
	if executeStmt == nil {
		return ""
	}

	if executeStmt.BinaryArgs == nil && len(executeStmt.UsingVars) == 0 {
		return ""
	}

	if sc.argsText != "" {
		return sc.argsText
	}

	sessVars := sc.GetSessionVars()
	stmtCtx := sessVars.StmtCtx
	var datums []types.Datum
	if stmtCtx != nil && !stmtCtx.Expired && len(sessVars.PreparedParams) == len(executeStmt.UsingVars) {
		sc.argsText = types.DatumsToStrNoErr(sessVars.PreparedParams)
		return sc.argsText
	}

	planBuilder := planBuilderPool.Get().(*plannercore.PlanBuilder)
	defer planBuilderPool.Put(planBuilder.ResetForReuse())
	planBuilder.Init(sc, sessiontxn.GetTxnManager(sc).GetTxnInfoSchema(), nil)

	plan, err := planBuilder.Build(context.TODO(), sc.executeStmt)
	if err != nil {
		terror.Log(errors.Trace(err))
		return ""
	}

	datums = make([]types.Datum, 0, len(executeStmt.UsingVars))
	for _, exp := range plan.(*plannercore.Execute).Params {
		datum, err := exp.Eval(chunk.Row{})
		if err != nil {
			terror.Log(errors.Trace(err))
			return ""
		}
		datums = append(datums, datum)
	}

	sc.argsText = types.DatumsToStrNoErr(datums)
	return sc.argsText
}

func (sc *stmtEventContext) Error() error {
	return sc.err
}

func (sc *stmtEventContext) SetError(err error) {
	sc.err = err
}
