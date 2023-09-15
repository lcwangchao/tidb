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
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"time"
)

type ExprContext struct {
	sctx                        sessionctx.Context
	StmtCtx                     *stmtctx.StatementContext
	SQLMode                     mysql.SQLMode
	StrictSQLMode               bool
	GetSystemVar                func(name string) (string, bool)
	Location                    func() *time.Location
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
}

func NewExprContext(sctx sessionctx.Context) *ExprContext {
	sessVars := sctx.GetSessionVars()
	return &ExprContext{
		sctx:                        sctx,
		StmtCtx:                     sessVars.StmtCtx,
		SQLMode:                     sessVars.SQLMode,
		StrictSQLMode:               sessVars.StrictSQLMode,
		GetSystemVar:                sessVars.GetSystemVar,
		Location:                    sessVars.Location,
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
	}
}

func (c *ExprContext) GetSessionCtx() sessionctx.Context {
	return c.sctx
}
