// Copyright 2019 PingCAP, Inc.
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
	"slices"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/printer"
)

func (b *builtinDatabaseSig) vectorized() bool {
	return true
}

// evalString evals a builtinDatabaseSig.
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html
func (b *builtinDatabaseSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()

	currentDB := b.ctx.CurrentDB
	result.ReserveString(n)
	if currentDB == "" {
		for i := 0; i < n; i++ {
			result.AppendNull()
		}
	} else {
		for i := 0; i < n; i++ {
			result.AppendString(currentDB)
		}
	}
	return nil
}

func (b *builtinConnectionIDSig) vectorized() bool {
	return true
}

func (b *builtinConnectionIDSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	connectionID := int64(b.ctx.ConnectionID)
	result.ResizeInt64(n, false)
	i64s := result.Int64s()
	for i := 0; i < n; i++ {
		i64s[i] = connectionID
	}
	return nil
}

func (b *builtinTiDBVersionSig) vectorized() bool {
	return true
}

func (b *builtinTiDBVersionSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ReserveString(n)
	info := printer.GetTiDBInfo()
	for i := 0; i < n; i++ {
		result.AppendString(info)
	}
	return nil
}

func (b *builtinRowCountSig) vectorized() bool {
	return true
}

// evalInt evals ROW_COUNT().
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_row-count
func (b *builtinRowCountSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ResizeInt64(n, false)
	i64s := result.Int64s()
	res := b.ctx.StmtCtx.PrevAffectedRows
	for i := 0; i < n; i++ {
		i64s[i] = res
	}
	return nil
}

func (b *builtinCurrentUserSig) vectorized() bool {
	return true
}

// evalString evals a builtinCurrentUserSig.
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_current-user
func (b *builtinCurrentUserSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()

	result.ReserveString(n)
	if b.ctx.CurrentUser == nil {
		return errors.Errorf("Missing session variable when eval builtin")
	}
	for i := 0; i < n; i++ {
		result.AppendString(b.ctx.CurrentUser.String())
	}
	return nil
}

func (b *builtinCurrentResourceGroupSig) vectorized() bool {
	return true
}

func (b *builtinCurrentResourceGroupSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ReserveString(n)
	for i := 0; i < n; i++ {
		result.AppendString(b.ctx.ResourceGroupName)
	}
	return nil
}

func (b *builtinCurrentRoleSig) vectorized() bool {
	return true
}

// evalString evals a builtinCurrentUserSig.
// See https://dev.mysql.com/doc/refman/8.0/en/information-functions.html#function_current-role
func (b *builtinCurrentRoleSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()

	if b.ctx.ActiveRoles == nil {
		return errors.Errorf("Missing session variable when eval builtin")
	}

	result.ReserveString(n)
	if len(b.ctx.ActiveRoles) == 0 {
		for i := 0; i < n; i++ {
			result.AppendString("NONE")
		}
		return nil
	}

	sortedRes := make([]string, 0, 10)
	for _, r := range b.ctx.ActiveRoles {
		sortedRes = append(sortedRes, r.String())
	}
	slices.Sort(sortedRes)
	res := strings.Join(sortedRes, ",")
	for i := 0; i < n; i++ {
		result.AppendString(res)
	}
	return nil
}

func (b *builtinUserSig) vectorized() bool {
	return true
}

// evalString evals a builtinUserSig.
// See https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_user
func (b *builtinUserSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	if b.ctx.CurrentUser == nil {
		return errors.Errorf("Missing session variable when eval builtin")
	}

	result.ReserveString(n)
	for i := 0; i < n; i++ {
		result.AppendString(b.ctx.CurrentUser.LoginString())
	}
	return nil
}

func (b *builtinTiDBIsDDLOwnerSig) vectorized() bool {
	return true
}

func (b *builtinTiDBIsDDLOwnerSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	var res int64
	if b.ctx.IsDDLOwner() {
		res = 1
	}
	result.ResizeInt64(n, false)
	i64s := result.Int64s()
	for i := 0; i < n; i++ {
		i64s[i] = res
	}
	return nil
}

func (b *builtinFoundRowsSig) vectorized() bool {
	return true
}

func (b *builtinFoundRowsSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	lastFoundRows := int64(b.ctx.LastFoundRows)
	n := input.NumRows()
	result.ResizeInt64(n, false)
	i64s := result.Int64s()
	for i := range i64s {
		i64s[i] = lastFoundRows
	}
	return nil
}

func (b *builtinBenchmarkSig) vectorized() bool {
	return b.constLoopCount > 0
}

func (b *builtinBenchmarkSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	loopCount := b.constLoopCount
	arg := b.args[1]
	evalType := arg.GetType().EvalType()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)

	var k int64
	switch evalType {
	case types.ETInt:
		for ; k < loopCount; k++ {
			if err = arg.VecEvalInt(input, buf); err != nil {
				return err
			}
		}
	case types.ETReal:
		for ; k < loopCount; k++ {
			if err = arg.VecEvalReal(input, buf); err != nil {
				return err
			}
		}
	case types.ETDecimal:
		for ; k < loopCount; k++ {
			if err = arg.VecEvalDecimal(input, buf); err != nil {
				return err
			}
		}
	case types.ETString:
		for ; k < loopCount; k++ {
			if err = arg.VecEvalString(input, buf); err != nil {
				return err
			}
		}
	case types.ETDatetime, types.ETTimestamp:
		for ; k < loopCount; k++ {
			if err = arg.VecEvalTime(input, buf); err != nil {
				return err
			}
		}
	case types.ETDuration:
		for ; k < loopCount; k++ {
			if err = arg.VecEvalDuration(input, buf); err != nil {
				return err
			}
		}
	case types.ETJson:
		for ; k < loopCount; k++ {
			if err = arg.VecEvalJSON(input, buf); err != nil {
				return err
			}
		}
	default: // Should never go into here.
		return errors.Errorf("EvalType %v not implemented for builtin BENCHMARK()", evalType)
	}

	// Return value of BENCHMARK() is always 0.
	// even if args[1].IsNull(i)
	result.ResizeInt64(n, false)

	return nil
}

func (b *builtinLastInsertIDSig) vectorized() bool {
	return true
}

func (b *builtinLastInsertIDSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ResizeInt64(n, false)
	i64s := result.Int64s()
	res := int64(b.ctx.StmtCtx.PrevLastInsertID)
	for i := 0; i < n; i++ {
		i64s[i] = res
	}
	return nil
}

func (b *builtinLastInsertIDWithIDSig) vectorized() bool {
	return true
}

func (b *builtinLastInsertIDWithIDSig) vecEvalInt(input *chunk.Chunk, result *chunk.Column) error {
	if err := b.args[0].VecEvalInt(input, result); err != nil {
		return err
	}
	i64s := result.Int64s()
	for i := len(i64s) - 1; i >= 0; i-- {
		if !result.IsNull(i) {
			b.ctx.SetLastInsertID(uint64(i64s[i]))
			break
		}
	}
	return nil
}

func (b *builtinVersionSig) vectorized() bool {
	return true
}

func (b *builtinVersionSig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	result.ReserveString(n)
	for i := 0; i < n; i++ {
		result.AppendString(mysql.ServerVersion)
	}
	return nil
}

func (b *builtinTiDBDecodeKeySig) vectorized() bool {
	return true
}

func (b *builtinTiDBDecodeKeySig) vecEvalString(input *chunk.Chunk, result *chunk.Column) error {
	n := input.NumRows()
	buf, err := b.bufAllocator.get()
	if err != nil {
		return err
	}
	defer b.bufAllocator.put(buf)
	if err := b.args[0].VecEvalString(input, buf); err != nil {
		return err
	}
	result.ReserveString(n)
	decode := func(ctx *ExprContext, s string) string { return s }
	if fn := b.ctx.Value(TiDBDecodeKeyFunctionKey); fn != nil {
		decode = fn.(func(ctx *ExprContext, s string) string)
	}
	for i := 0; i < n; i++ {
		if buf.IsNull(i) {
			result.AppendNull()
			continue
		}
		result.AppendString(decode(b.ctx, buf.GetString(i)))
	}
	return nil
}
