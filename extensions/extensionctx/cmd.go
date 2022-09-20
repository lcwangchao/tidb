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
	"context"

	"github.com/pingcap/tidb/extensions"
)

type cmdContext struct {
	context.Context
	extensions.SessionContext
}

func NewCmdContext(ctx context.Context, sctx extensions.SessionContext) extensions.CmdContext {
	return &cmdContext{
		Context:        ctx,
		SessionContext: sctx,
	}
}
