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

package executor

import (
	"context"

	"github.com/pingcap/tidb/extensions"
	"github.com/pingcap/tidb/extensions/extensionctx"
	"github.com/pingcap/tidb/util/chunk"
)

// ExtensionCmdExec executes extension command
type ExtensionCmdExec struct {
	baseExecutor
	done    bool
	handler extensions.ExtensionCmdHandler
}

func (e *ExtensionCmdExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if e.done {
		return nil
	}

	cmdCtx := extensionctx.NewCmdContext(
		ctx,
		extensionctx.NewSessionContext(e.ctx),
	)

	if err := e.handler.ExecuteCmd(cmdCtx, req); err != nil {
		return err
	}

	e.done = true
	return nil
}
