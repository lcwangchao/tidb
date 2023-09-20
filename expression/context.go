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

package expression

import (
	"context"
	"fmt"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/mathutil"
	"time"
)

type EvalContext struct {
	SQLMode                    mysql.SQLMode
	StrictSQLMode              bool
	Killed                     *uint32
	EnableVectorizedExpression bool
	StmtCtx                    *stmtctx.StatementContext
	Location                   *time.Location
}

func NewDefaultEvalContext() *EvalContext {
	return &EvalContext{
		Location: time.Local,
	}
}

func NewEvalContext(sctx sessionctx.Context) *EvalContext {
	return &EvalContext{
		SQLMode:                    sctx.GetSessionVars().SQLMode,
		StrictSQLMode:              sctx.GetSessionVars().StrictSQLMode,
		EnableVectorizedExpression: sctx.GetSessionVars().EnableVectorizedExpression,
		Killed:                     &sctx.GetSessionVars().Killed,
		StmtCtx:                    sctx.GetSessionVars().StmtCtx,
		Location:                   sctx.GetSessionVars().Location(),
	}
}

type ExprContext struct {
	sctx                        sessionctx.Context
	stmtCtx                     *stmtctx.StatementContext
	sqlMode                     func() *mysql.SQLMode
	GetSystemVar                func(name string) (string, bool)
	GetSessionOrGlobalSystemVar func(ctx context.Context, name string) (string, error)
	GlobalVarsAccessor          variable.GlobalVarAccessor
	CurrentUser                 *auth.UserIdentity
	CurrentDB                   string
	ConnectionID                uint64
	ResourceGroupName           string
	ActiveRoles                 []*auth.RoleIdentity
	IsDDLOwner                  func() bool
	LastFoundRows               uint64
	SetLastInsertID             func(insertID uint64)
	Value                       func(key fmt.Stringer) interface{}
	SetValue                    func(key fmt.Stringer, value interface{})
	InfoSchema                  interface{}
	MaxExecutionTime            uint64
	SequenceState               *variable.SequenceState
	GetAdvisoryLock             func(string, int64) error
	IsUsedAdvisoryLock          func(string) uint64
	ReleaseAdvisoryLock         func(string) bool
	ReleaseAllAdvisoryLocks     func() int
	SetStringUserVar            func(name string, strVal string, collation string)
	SetUserVarVal               func(name string, dt types.Datum)
	GetUserVarVal               func(name string) (types.Datum, bool)
	CurrInsertValues            chunk.Row
	PlanCacheParams             *variable.PlanCacheParamList
	GetCharsetInfo              func() (charset, collation string)
	GetStore                    func() kv.Storage
	ConnectionInfo              *variable.ConnectionInfo
	Rng                         *mathutil.MysqlRng
	SysdateIsNow                bool
	GetPrivilegeManager         func() privilege.Manager
	NoopFuncsMode               int
	EnableVectorizedExpression  bool
	AllocPlanColumnID           func() int64
	RetrieveSQLDigest           func(ctx context.Context, digests []interface{}) (map[string]string, error)
	GetAllowInSubqToJoinAndAgg  func() bool
	DefaultCollationForUTF8MB4  string
}

func NewExprContext(sctx sessionctx.Context) *ExprContext {
	sessVars := sctx.GetSessionVars()
	return &ExprContext{
		sctx:                        sctx,
		stmtCtx:                     sessVars.StmtCtx,
		GetSystemVar:                sessVars.GetSystemVar,
		GetSessionOrGlobalSystemVar: sessVars.GetSessionOrGlobalSystemVar,
		GlobalVarsAccessor:          sessVars.GlobalVarsAccessor,
		CurrentUser:                 sessVars.User,
		CurrentDB:                   sessVars.CurrentDB,
		ConnectionID:                sessVars.ConnectionID,
		ResourceGroupName:           sessVars.ResourceGroupName,
		ActiveRoles:                 sessVars.ActiveRoles,
		IsDDLOwner:                  sctx.IsDDLOwner,
		LastFoundRows:               sessVars.LastFoundRows,
		SetLastInsertID:             sessVars.SetLastInsertID,
		Value:                       sctx.Value,
		SetValue:                    sctx.SetValue,
		MaxExecutionTime:            sessVars.MaxExecutionTime,
		SequenceState:               sessVars.SequenceState,
		GetAdvisoryLock:             sctx.GetAdvisoryLock,
		IsUsedAdvisoryLock:          sctx.IsUsedAdvisoryLock,
		ReleaseAdvisoryLock:         sctx.ReleaseAdvisoryLock,
		ReleaseAllAdvisoryLocks:     sctx.ReleaseAllAdvisoryLocks,
		SetStringUserVar:            sessVars.SetStringUserVar,
		SetUserVarVal:               sessVars.SetUserVarVal,
		GetUserVarVal:               sessVars.GetUserVarVal,
		CurrInsertValues:            sessVars.CurrInsertValues,
		PlanCacheParams:             sessVars.PlanCacheParams,
		GetCharsetInfo:              sessVars.GetCharsetInfo,
		GetStore:                    sctx.GetStore,
		ConnectionInfo:              sessVars.ConnectionInfo,
		Rng:                         sessVars.Rng,
		SysdateIsNow:                sessVars.SysdateIsNow,
		NoopFuncsMode:               sessVars.NoopFuncsMode,
		EnableVectorizedExpression:  sessVars.EnableVectorizedExpression,
		GetPrivilegeManager: func() privilege.Manager {
			return privilege.GetPrivilegeManager(sctx)
		},
		AllocPlanColumnID: sessVars.AllocPlanColumnID,
		RetrieveSQLDigest: func(ctx context.Context, digests []interface{}) (map[string]string, error) {
			retriever := NewSQLDigestTextRetriever()
			for _, item := range digests {
				if item != nil {
					digest, ok := item.(string)
					if ok {
						retriever.SQLDigestsMap[digest] = ""
					}
				}
			}

			if err := retriever.RetrieveGlobal(ctx, sctx); err != nil {
				return nil, err
			}

			return retriever.SQLDigestsMap, nil
		},
		GetAllowInSubqToJoinAndAgg: sessVars.GetAllowInSubqToJoinAndAgg,
		DefaultCollationForUTF8MB4: sessVars.DefaultCollationForUTF8MB4,
	}
}

func (c *ExprContext) EvalCtx() *EvalContext {
	return &EvalContext{}
}

func (c *ExprContext) StmtCtx() *stmtctx.StatementContext {
	return c.stmtCtx
}

func (c *ExprContext) SQLMode() mysql.SQLMode {
	return c.SQLMode()
}

func (c *ExprContext) ConstItemCtx() *stmtctx.StatementContext {
	return c.stmtCtx
}

func (c *ExprContext) HashCodeCtx() *stmtctx.StatementContext {
	return c.stmtCtx
}

func (c *ExprContext) AppendWarning(err error) {
	c.stmtCtx.AppendWarning(err)
}

func (c *ExprContext) WarningCount() uint16 {
	return c.stmtCtx.WarningCount()
}

func (c *ExprContext) HandleTruncate(err error) error {
	return c.stmtCtx.HandleTruncate(err)
}

func (c *ExprContext) TruncateWarnings(start int) []stmtctx.SQLWarn {
	return c.stmtCtx.TruncateWarnings(start)
}

func (c *ExprContext) SetSkipPlanCache(reason error) {
	c.stmtCtx.SetSkipPlanCache(reason)
}

func (c *ExprContext) UseCache() bool {
	return c.stmtCtx.UseCache
}

func (c *ExprContext) PointExec() bool {
	return c.stmtCtx.PointExec
}

func (c *ExprContext) GetStaleTSO() (uint64, error) {
	return c.stmtCtx.GetStaleTSO()
}

func (c *ExprContext) GetSessionCtx() sessionctx.Context {
	return c.sctx
}
