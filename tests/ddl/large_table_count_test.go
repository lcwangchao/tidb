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

package ddl

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/tidb/types"

	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
)

const createTplTable = `create table customer (
		c_id int not null,
		c_d_id smallint not null,
		c_w_id smallint not null,
		c_first varchar(16),
		c_middle char(8),
		c_last varchar(16),
		c_street_1 varchar(20),
		c_street_2 varchar(20),
		c_city varchar(20),
		c_state char(8),
		c_zip char(9),
		c_phone char(16),
		c_since datetime,
		c_credit char(8),
		c_credit_lim bigint,
		c_discount decimal(12,2),
		c_balance decimal(12,2),
		c_ytd_payment decimal(12,2),
		c_payment_cnt smallint,
		c_delivery_cnt smallint,
		c_data text,
		PRIMARY KEY(c_w_id, c_d_id, c_id))
`

func BenchmarkLargeTableCountStatsMem(b *testing.B) {
	db, err := sql.Open("mysql", "root@tcp(127.0.0.1:4000)/test")
	defer func() {
		require.NoError(b, db.Close())
	}()
	require.NoError(b, err)
	conn, err := db.Conn(context.TODO())
	defer func() {
		require.NoError(b, conn.Close())
	}()
	_, err = conn.ExecContext(context.TODO(), "create database if not exists btest")
	require.NoError(b, err)
	_, err = conn.ExecContext(context.TODO(), "drop table if exists customer")
	require.NoError(b, err)
	_, err = conn.ExecContext(context.TODO(), createTplTable)
	require.NoError(b, err)

	store, do := testkit.CreateMockStoreAndDomain(b)
	tk := testkit.NewTestKit(b, store)
	tk.MustExec("use test")
	tk.MustExec(createTplTable)
	tbl, err := do.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("customer"))
	require.NoError(b, err)
	tblInfo := tbl.Meta()

	tblCnt := 1000
	tblNames := make([]string, 0, tblCnt)
	for i := 1; i <= tblCnt; i++ {
		tblNames = append(tblNames, fmt.Sprintf("btest.customer%04d", i))
	}

	for _, name := range tblNames {
		_, err = conn.ExecContext(context.TODO(), fmt.Sprintf("create table if not exists %s like test.customer", name))
		require.NoError(b, err)
		_, err = conn.ExecContext(context.TODO(), fmt.Sprintf("delete from %s where 1", name))
		require.NoError(b, err)
	}

	batch := make([]string, 0, 1024)
	insertCnt := 1024
	for i := 1; i <= insertCnt; i++ {
		values := make([]string, 0, len(tblInfo.Columns))
		for _, c := range tblInfo.Columns {
			switch c.FieldType.EvalType() {
			case types.ETInt, types.ETReal, types.ETDecimal:
				values = append(values, fmt.Sprintf("%d", i))
			case types.ETString:
				values = append(values, fmt.Sprintf("'s%d'", i))
			case types.ETDatetime:
				values = append(values, fmt.Sprintf("'20%02d-%02d-%02d 12:00:00.%d'", i%23, i%12+1, i%28+1, i))
			default:
				values = append(values, "NULL")
			}
		}
		batch = append(batch, fmt.Sprintf("(%s)", strings.Join(values, ", ")))
		if i == insertCnt || len(batch) == cap(batch) {
			for _, name := range tblNames {
				insertSQL := fmt.Sprintf("insert into %s values\n  %s;", name, strings.Join(batch, ",\n  "))
				_, err = conn.ExecContext(context.TODO(), insertSQL)
				require.NoError(b, err)
			}
			batch = batch[:0]
		}
	}

	for _, name := range tblNames {
		_, err = conn.ExecContext(context.TODO(), "analyze table "+name)
		require.NoError(b, err)
	}
}

func BenchmarkLargeTableCountMetaMem(b *testing.B) {
	store, do := testkit.CreateMockStoreAndDomain(b)
	tk := testkit.NewTestKit(b, store)
	tk.MustExec("use test")
	tk.MustExec(createTplTable)
	tpl, err := do.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("customer"))
	tplJson, err := json.Marshal(tpl.Meta())
	require.NoError(b, err)

	tbls := make([]*model.TableInfo, 0, b.N)
	var memStart, memEnd runtime.MemStats

	runtime.GC()
	runtime.ReadMemStats(&memStart)

	b.ResetTimer()
	startTime := time.Now()
	for i := 1; i <= cap(tbls); i++ {
		var tblInfo model.TableInfo
		require.NoError(b, json.Unmarshal(tplJson, &tblInfo))
		tblInfo.Name = model.NewCIStr(fmt.Sprintf("customer_%06d", i))
		tbls = append(tbls, &tblInfo)
	}

	builder, err := infoschema.NewBuilder(store, nil).InitWithDBInfos(
		[]*model.DBInfo{
			{ID: 1, Name: model.NewCIStr("test"), Tables: tbls},
		},
		nil,
		1,
	)
	require.NoError(b, err)
	is := builder.Build()
	b.StopTimer()

	runtime.GC()
	runtime.ReadMemStats(&memEnd)

	interval := time.Now().Sub(startTime)
	b.ReportMetric(interval.Seconds(), "meta-build-interval-sec")
	b.ReportMetric(float64(memEnd.HeapInuse-memStart.HeapInuse), "meta-alloc-bytes")
	// keep information schema not gc
	fmt.Println(is.SchemaMetaVersion())
}

func BenchmarkLargeTableCountApplyDiff(b *testing.B) {
	store, do := testkit.CreateMockStoreAndDomain(b)
	tk := testkit.NewTestKit(b, store)
	tk.MustExec("use test")
	tk.MustExec(createTplTable)
	schema, ok := do.InfoSchema().SchemaByName(model.NewCIStr("test"))
	require.True(b, ok)
	tpl, err := do.InfoSchema().TableByName(model.NewCIStr("test"), model.NewCIStr("customer"))
	require.NoError(b, err)

	tbls := make([]*model.TableInfo, 0, 10)
	for i := 1; i <= cap(tbls); i++ {
		tblInfo := tpl.Meta().Clone()
		tblInfo.Name = model.NewCIStr(fmt.Sprintf("customer_%06d", i))
		tbls = append(tbls, tblInfo)
	}

	builder, err := infoschema.NewBuilder(store, nil).InitWithDBInfos(
		[]*model.DBInfo{
			{ID: schema.ID, Name: model.NewCIStr("test"), Tables: tbls},
		},
		nil,
		1,
	)
	require.NoError(b, err)
	is := builder.Build()

	b.ResetTimer()
	ctx := kv.WithInternalSourceType(context.TODO(), kv.InternalTxnDDL)
	for i := 0; i < b.N; i++ {
		err = kv.RunInNewTxn(ctx, store, false, func(ctx context.Context, txn kv.Transaction) error {
			m := meta.NewMeta(txn)
			_, err = infoschema.NewBuilder(store, nil).InitWithOldInfoSchema(is).ApplyDiff(m, &model.SchemaDiff{
				Type: model.ActionCreateTable, SchemaID: schema.ID, TableID: tpl.Meta().ID,
			})
			return err
		})
		require.NoError(b, err)
	}
}
