// Copyright 2026 PingCAP, Inc.
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

package writetest

import (
	"fmt"
	"io"
	"testing"

	"github.com/pingcap/tidb/pkg/executor"
	"github.com/pingcap/tidb/pkg/lightning/mydump"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/testkit"
)

func queryMLogRows(tk *testkit.TestKit, baseTable string) *testkit.Result {
	return tk.MustQuery(fmt.Sprintf(
		"select a, b, `_MLOG$_DML_TYPE`, `_MLOG$_OLD_NEW` from `%s` order by _tidb_rowid",
		"$mlog$"+baseTable,
	))
}

func TestMaterializedViewLogWriteDMLSync(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int primary key, a int, b int, c int)")
	tk.MustExec("create materialized view log on t (a, b)")

	tk.MustExec("insert into t values (1, 10, 20, 30)")
	queryMLogRows(tk, "t").Check(testkit.Rows("10 20 I 1"))

	tk.MustExec("delete from `$mlog$t`")
	tk.MustExec("update t set c = 31 where id = 1")
	tk.MustQuery("select count(*) from `$mlog$t`").Check(testkit.Rows("0"))

	tk.MustExec("update t set a = 11 where id = 1")
	queryMLogRows(tk, "t").Check(testkit.Rows(
		"10 20 U -1",
		"11 20 U 1",
	))

	tk.MustExec("delete from `$mlog$t`")
	tk.MustExec("update t set id = 2 where id = 1")
	tk.MustQuery("select count(*) from `$mlog$t`").Check(testkit.Rows("0"))

	tk.MustExec("update t set id = 3, a = 12 where id = 2")
	queryMLogRows(tk, "t").Check(testkit.Rows(
		"11 20 U -1",
		"12 20 U 1",
	))

	tk.MustExec("delete from `$mlog$t`")
	tk.MustExec("delete from t where id = 3")
	queryMLogRows(tk, "t").Check(testkit.Rows("12 20 D -1"))
}

func TestMaterializedViewLogWriteInsertOnDuplicateHandleChange(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t_dup")
	tk.MustExec("create table t_dup (id int primary key, a int, b int)")
	tk.MustExec("create materialized view log on t_dup (a, b)")

	tk.MustExec("insert into t_dup values (1, 100, 200)")
	tk.MustExec("delete from `$mlog$t_dup`")

	// Trigger UPDATE path of INSERT ... ON DUPLICATE KEY UPDATE and change the handle.
	tk.MustExec("insert into t_dup values (1, 101, 200) on duplicate key update id = 2, a = values(a)")
	queryMLogRows(tk, "t_dup").Check(testkit.Rows(
		"100 200 U -1",
		"101 200 U 1",
	))
}

func TestMaterializedViewLogWriteReplace(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t_replace")
	tk.MustExec("create table t_replace (id int primary key, a int, b int)")
	tk.MustExec("create materialized view log on t_replace (a, b)")

	tk.MustExec("insert into t_replace values (1, 10, 20)")
	tk.MustExec("delete from `$mlog$t_replace`")

	tk.MustExec("replace into t_replace values (1, 30, 40)")
	queryMLogRows(tk, "t_replace").Check(testkit.Rows(
		"10 20 R -1",
		"30 40 R 1",
	))
}

func TestMaterializedViewLogWriteLoadData(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t_load")
	tk.MustExec("create table t_load (id int primary key, a int, b int)")
	tk.MustExec("create materialized view log on t_load (a, b)")

	data := "1\t10\t20\n2\t11\t21\n"
	ctx := tk.Session().(sessionctx.Context)
	ctx.SetValue(
		executor.LoadDataReaderBuilderKey,
		executor.LoadDataReaderBuilder(func(_ string) (io.ReadCloser, error) {
			return mydump.NewStringReader(data), nil
		}),
	)

	tk.MustExec("load data local infile '/tmp/nonexistence.csv' into table t_load")
	queryMLogRows(tk, "t_load").Check(testkit.Rows(
		"10 20 L 1",
		"11 21 L 1",
	))
}

func TestMaterializedViewLogWritePartitionTable(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")

	tk.MustExec("drop table if exists t_part")
	tk.MustExec(`
		create table t_part (
			id int primary key,
			a int,
			b int
		)
		partition by hash(id) partitions 2
	`)
	tk.MustExec("create materialized view log on t_part (a, b)")

	tk.MustExec("insert into t_part values (1, 10, 20)")
	tk.MustExec("delete from `$mlog$t_part`")

	tk.MustExec("insert into t_part values (1, 11, 20) on duplicate key update id = 3, a = values(a)")
	queryMLogRows(tk, "t_part").Check(testkit.Rows(
		"10 20 U -1",
		"11 20 U 1",
	))
}
