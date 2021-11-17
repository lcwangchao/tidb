// Copyright 2021 PingCAP, Inc.
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

package executor_test

import (
	"fmt"
	"testing"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/planner"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

func TestGetTSOVerySlow(t *testing.T) {
	store, _, clean := testkit.CreateMockStoreAndDomain(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("create table t(id int primary key, c int unique)")
	tk.MustExec("insert into t values(1, 11)")

	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/planner/prepareTsSlow", fmt.Sprintf("return(%d)", tk.Session().GetSessionVars().ConnectionID)))
	defer func() {
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/planner/prepareTsSlow"))
	}()

	beginStmtReply := make(chan chan interface{})
	tk.Session().SetValue(planner.SlowTSOChannel, beginStmtReply)

	go func() {
		// waiting for begin stmt is ready to get tso
		ddlReply := <-beginStmtReply
		// do some ddl
		tk2 := testkit.NewTestKit(t, store)
		tk2.MustExec("use test")
		tk2.MustExec("alter table t drop index c")
		// notify begin stmt to continue get tso
		ddlReply <- struct{}{}
	}()

	tk.MustExec("begin")
	tk.MustQuery("select * from t").Check(testkit.Rows("1 11"))
	// this will fail
	tk.MustQuery("select * from t where c=11").Check(testkit.Rows("1 11"))
}
