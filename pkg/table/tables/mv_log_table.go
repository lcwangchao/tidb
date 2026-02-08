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

package tables

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/table"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
)

// MVLogDMLType is the DML type stored in `_MLOG$_DML_TYPE`.
type MVLogDMLType string

const (
	// MVLogDMLTypeInsert means the log row comes from INSERT.
	MVLogDMLTypeInsert MVLogDMLType = "I"
	// MVLogDMLTypeDelete means the log row comes from DELETE.
	MVLogDMLTypeDelete MVLogDMLType = "D"
	// MVLogDMLTypeUpdate means the log row comes from UPDATE.
	MVLogDMLTypeUpdate MVLogDMLType = "U"
	// MVLogDMLTypeReplace means the log row comes from REPLACE.
	MVLogDMLTypeReplace MVLogDMLType = "R"
	// MVLogDMLTypeLoadData means the log row comes from LOAD DATA.
	MVLogDMLTypeLoadData MVLogDMLType = "L"
)

const (
	mvLogOldRowMarker int64 = -1
	mvLogNewRowMarker int64 = 1
)

// WrapTableWithMaterializedViewLog wraps a base table with MV-log writing capability.
func WrapTableWithMaterializedViewLog(
	baseTbl table.Table,
	logTbl table.Table,
	defaultDMLType MVLogDMLType,
) (table.Table, error) {
	wrapped, err := newMaterializedViewLogTable(baseTbl, logTbl, defaultDMLType)
	if err != nil {
		return nil, err
	}
	if p := baseTbl.GetPartitionedTable(); p != nil {
		return &materializedViewLogPartitionedTable{
			materializedViewLogTable: wrapped,
			partitionedTable:         p,
		}, nil
	}
	if physical, ok := baseTbl.(table.PhysicalTable); ok {
		return &materializedViewLogPhysicalTable{
			materializedViewLogTable: wrapped,
			physicalTable:            physical,
		}, nil
	}
	return wrapped, nil
}

type materializedViewLogTable struct {
	table.Table

	logTable       table.Table
	loggedBaseCols []*table.Column

	defaultDMLType MVLogDMLType

	// Used for UPDATE-like flows that are implemented as RemoveRecord + AddRecord.
	pendingUpdateOldRows [][]types.Datum
}

func newMaterializedViewLogTable(
	baseTbl table.Table,
	logTbl table.Table,
	defaultDMLType MVLogDMLType,
) (*materializedViewLogTable, error) {
	if baseTbl == nil {
		return nil, errors.New("base table is nil")
	}
	if logTbl == nil {
		return nil, errors.New("materialized view log table is nil")
	}

	logTblInfo := logTbl.Meta()
	logTblName := "<unknown>"
	if logTblInfo != nil {
		logTblName = logTblInfo.Name.O
	}
	if logTblInfo == nil || logTblInfo.MaterializedViewLog == nil {
		return nil, errors.Errorf("table %s is not a materialized view log table", logTblName)
	}
	if logTblInfo.MaterializedViewLog.BaseTableID != baseTbl.Meta().ID {
		return nil, errors.Errorf(
			"materialized view log table %s does not belong to base table %s",
			logTblInfo.Name.O,
			baseTbl.Meta().Name.O,
		)
	}

	baseCols := make([]*table.Column, 0, len(logTblInfo.MaterializedViewLog.Columns))
	for _, colName := range logTblInfo.MaterializedViewLog.Columns {
		col := table.FindColLowerCase(baseTbl.Cols(), colName.L)
		if col == nil {
			return nil, errors.Errorf(
				"column %s not found on base table %s",
				colName.O,
				baseTbl.Meta().Name.O,
			)
		}
		baseCols = append(baseCols, col)
	}

	return &materializedViewLogTable{
		Table:          baseTbl,
		logTable:       logTbl,
		loggedBaseCols: baseCols,
		defaultDMLType: defaultDMLType,
	}, nil
}

func (t *materializedViewLogTable) cloneWithBase(baseTbl table.Table) *materializedViewLogTable {
	return &materializedViewLogTable{
		Table:          baseTbl,
		logTable:       t.logTable,
		loggedBaseCols: t.loggedBaseCols,
		defaultDMLType: t.defaultDMLType,
	}
}

func (t *materializedViewLogTable) AddRecord(
	ctx table.MutateContext,
	txn kv.Transaction,
	r []types.Datum,
	opts ...table.AddRecordOption,
) (recordID kv.Handle, err error) {
	opt := table.NewAddRecordOpt(opts...)
	recordID, err = t.Table.AddRecord(ctx, txn, r, opts...)
	if err != nil {
		return recordID, err
	}

	newVals, err := t.extractLoggedValuesFromRow(r)
	if err != nil {
		return recordID, err
	}

	mutateCtx := opt.Ctx()
	pessimisticLazyCheck := opt.PessimisticLazyDupKeyCheck()

	if opt.IsUpdate() {
		oldVals, ok := t.popPendingUpdateOldRow()
		if !ok {
			return recordID, errors.New("missing old row for update log")
		}
		changed, err := loggedValuesChanged(ctx, oldVals, newVals)
		if err != nil {
			return recordID, err
		}
		if !changed {
			return recordID, nil
		}
		if err := t.appendLogRow(ctx, txn, oldVals, MVLogDMLTypeUpdate, mvLogOldRowMarker, mutateCtx, pessimisticLazyCheck); err != nil {
			return recordID, err
		}
		if err := t.appendLogRow(ctx, txn, newVals, MVLogDMLTypeUpdate, mvLogNewRowMarker, mutateCtx, pessimisticLazyCheck); err != nil {
			return recordID, err
		}
		return recordID, nil
	}

	if err := t.appendLogRow(ctx, txn, newVals, t.defaultDMLType, mvLogNewRowMarker, mutateCtx, pessimisticLazyCheck); err != nil {
		return recordID, err
	}
	return recordID, nil
}

func (t *materializedViewLogTable) UpdateRecord(
	ctx table.MutateContext,
	txn kv.Transaction,
	h kv.Handle,
	currData []types.Datum,
	newData []types.Datum,
	touched []bool,
	opts ...table.UpdateRecordOption,
) error {
	oldVals, err := t.extractLoggedValuesFromRow(currData)
	if err != nil {
		return err
	}
	newVals, err := t.extractLoggedValuesFromRow(newData)
	if err != nil {
		return err
	}
	changed, err := loggedValuesChanged(ctx, oldVals, newVals)
	if err != nil {
		return err
	}

	if err := t.Table.UpdateRecord(ctx, txn, h, currData, newData, touched, opts...); err != nil {
		return err
	}
	if !changed {
		return nil
	}

	opt := table.NewUpdateRecordOpt(opts...)
	mutateCtx := opt.Ctx()
	pessimisticLazyCheck := opt.PessimisticLazyDupKeyCheck()
	if err := t.appendLogRow(ctx, txn, oldVals, MVLogDMLTypeUpdate, mvLogOldRowMarker, mutateCtx, pessimisticLazyCheck); err != nil {
		return err
	}
	return t.appendLogRow(ctx, txn, newVals, MVLogDMLTypeUpdate, mvLogNewRowMarker, mutateCtx, pessimisticLazyCheck)
}

func (t *materializedViewLogTable) RemoveRecord(
	ctx table.MutateContext,
	txn kv.Transaction,
	h kv.Handle,
	r []types.Datum,
	opts ...table.RemoveRecordOption,
) error {
	oldVals, err := t.extractLoggedValuesFromRow(r)
	if err != nil {
		return err
	}

	if err := t.Table.RemoveRecord(ctx, txn, h, r, opts...); err != nil {
		return err
	}

	switch t.defaultDMLType {
	case MVLogDMLTypeUpdate, MVLogDMLTypeInsert:
		t.pushPendingUpdateOldRow(oldVals)
		return nil
	default:
		return t.appendLogRow(
			ctx,
			txn,
			oldVals,
			t.defaultDMLType,
			mvLogOldRowMarker,
			nil,
			table.DupKeyCheckInAcquireLock,
		)
	}
}

func (t *materializedViewLogTable) extractLoggedValuesFromRow(row []types.Datum) ([]types.Datum, error) {
	values := make([]types.Datum, len(t.loggedBaseCols))
	for i, col := range t.loggedBaseCols {
		if col.Offset < 0 || col.Offset >= len(row) {
			return nil, errors.Errorf(
				"row length %d is too short for column %s (offset %d)",
				len(row),
				col.Name.O,
				col.Offset,
			)
		}
		row[col.Offset].Copy(&values[i])
	}
	return values, nil
}

func loggedValuesChanged(ctx table.MutateContext, oldVals, newVals []types.Datum) (bool, error) {
	if len(oldVals) != len(newVals) {
		return true, nil
	}
	typeCtx := ctx.GetExprCtx().GetEvalCtx().TypeCtx()
	for i := range oldVals {
		cmp, err := oldVals[i].Compare(typeCtx, &newVals[i], collate.GetBinaryCollator())
		if err != nil {
			return false, err
		}
		if cmp != 0 {
			return true, nil
		}
	}
	return false, nil
}

func (t *materializedViewLogTable) appendLogRow(
	ctx table.MutateContext,
	txn kv.Transaction,
	loggedVals []types.Datum,
	dmlType MVLogDMLType,
	oldNew int64,
	mutateCtx context.Context,
	pessimisticLazyCheck table.PessimisticLazyDupKeyCheckMode,
) error {
	logRow := make([]types.Datum, 0, len(loggedVals)+2)
	logRow = append(logRow, loggedVals...)
	logRow = append(logRow, types.NewStringDatum(string(dmlType)), types.NewIntDatum(oldNew))

	logOpts := []table.AddRecordOption{
		table.DupKeyCheckSkip,
		pessimisticLazyCheck,
	}
	if mutateCtx != nil {
		logOpts = append(logOpts, table.WithCtx(mutateCtx))
	}
	_, err := t.logTable.AddRecord(ctx, txn, logRow, logOpts...)
	return err
}

func (t *materializedViewLogTable) pushPendingUpdateOldRow(row []types.Datum) {
	t.pendingUpdateOldRows = append(t.pendingUpdateOldRows, row)
}

func (t *materializedViewLogTable) popPendingUpdateOldRow() ([]types.Datum, bool) {
	if len(t.pendingUpdateOldRows) == 0 {
		return nil, false
	}
	row := t.pendingUpdateOldRows[0]
	t.pendingUpdateOldRows = t.pendingUpdateOldRows[1:]
	return row, true
}

type materializedViewLogPhysicalTable struct {
	*materializedViewLogTable
	physicalTable table.PhysicalTable
}

func (t *materializedViewLogPhysicalTable) GetPhysicalID() int64 {
	return t.physicalTable.GetPhysicalID()
}

type materializedViewLogPartitionedTable struct {
	*materializedViewLogTable
	partitionedTable table.PartitionedTable
}

func (t *materializedViewLogPartitionedTable) GetPartition(physicalID int64) table.PhysicalTable {
	p := t.partitionedTable.GetPartition(physicalID)
	if p == nil {
		return nil
	}
	cloned := t.cloneWithBase(p)
	return &materializedViewLogPhysicalTable{
		materializedViewLogTable: cloned,
		physicalTable:            p,
	}
}

func (t *materializedViewLogPartitionedTable) GetPartitionByRow(
	ctx expression.EvalContext,
	row []types.Datum,
) (table.PhysicalTable, error) {
	p, err := t.partitionedTable.GetPartitionByRow(ctx, row)
	if err != nil {
		return nil, err
	}
	cloned := t.cloneWithBase(p)
	return &materializedViewLogPhysicalTable{
		materializedViewLogTable: cloned,
		physicalTable:            p,
	}, nil
}

func (t *materializedViewLogPartitionedTable) GetPartitionIdxByRow(ctx expression.EvalContext, row []types.Datum) (int, error) {
	return t.partitionedTable.GetPartitionIdxByRow(ctx, row)
}

func (t *materializedViewLogPartitionedTable) GetAllPartitionIDs() []int64 {
	return t.partitionedTable.GetAllPartitionIDs()
}

func (t *materializedViewLogPartitionedTable) GetPartitionColumnIDs() []int64 {
	return t.partitionedTable.GetPartitionColumnIDs()
}

func (t *materializedViewLogPartitionedTable) GetPartitionColumnNames() []pmodel.CIStr {
	return t.partitionedTable.GetPartitionColumnNames()
}

func (t *materializedViewLogPartitionedTable) CheckForExchangePartition(
	ctx expression.EvalContext,
	pi *model.PartitionInfo,
	r []types.Datum,
	partID int64,
	ntID int64,
) error {
	return t.partitionedTable.CheckForExchangePartition(ctx, pi, r, partID, ntID)
}
