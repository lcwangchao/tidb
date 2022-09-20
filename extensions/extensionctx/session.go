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
	"github.com/pingcap/tidb/extensions"
	"github.com/pingcap/tidb/parser/auth"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
)

type seContext struct {
	sessionctx.Context
}

func NewSessionContext(sctx sessionctx.Context) extensions.SessionContext {
	return &seContext{
		Context: sctx,
	}
}

func (s *seContext) GetConnectionInfo() *variable.ConnectionInfo {
	return s.GetSessionVars().ConnectionInfo
}

func (s *seContext) GetSessionOrGlobalSystemVar(name string) (string, error) {
	return s.GetSessionVars().GetSessionOrGlobalSystemVar(name)
}

func (s *seContext) GetGlobalSysVar(name string) (string, error) {
	return s.GetSessionVars().GetGlobalSystemVar(name)
}

func (s *seContext) GetPrivilegeManager() extensions.PrivilegeManager {
	return privilege.GetPrivilegeManager(s)
}

func (s *seContext) GetUser() *auth.UserIdentity {
	return s.GetSessionVars().User
}
