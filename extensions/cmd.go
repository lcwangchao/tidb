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

package extensions

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/util/chunk"
)

type ExtensionCmdHandler interface {
	OutputColumnsNum() int
	BuildOutputSchema(addColumn func(tableName, name string, tp byte, size int))
	ExecuteCmd(ctx context.Context, chk *chunk.Chunk) error
}

func WithHandleCommand(fn func(ast.ExtensionCmdNode) (ExtensionCmdHandler, error)) ExtensionOption {
	return func(ext *extensionManifest) {
		ext.handleCommand = fn
	}
}

func (e *Extensions) CreateExtensionCmdHandler(node ast.ExtensionCmdNode) (ExtensionCmdHandler, error) {
	errorMsg := "no matched extension found"
	if e == nil {
		return nil, errors.New(errorMsg)
	}

	for _, item := range e.items {
		if fn := item.handleCommand; fn != nil {
			handler, err := fn(node)
			if err != nil {
				return nil, err
			}

			if handler != nil {
				return handler, nil
			}
		}
	}

	return nil, errors.New(errorMsg)
}
