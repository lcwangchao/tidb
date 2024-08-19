// Copyright 2024 PingCAP, Inc.
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

package contextopt

import (
	"github.com/pingcap/tidb/pkg/expression/context"
	exprctx "github.com/pingcap/tidb/pkg/expression/context"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
)

var _ context.OptionalEvalPropProvider = &UserVarsPropProvider{}

// UserVarsPropProvider is used to provide user variables
type UserVarsPropProvider struct {
	vars *variable.UserVars
}

// NewUserVarsPropProvider creates a new `UserVarsPropProvider`
func NewUserVarsPropProvider(vars *variable.UserVars) *UserVarsPropProvider {
	return &UserVarsPropProvider{
		vars: vars,
	}
}

// GetUserVars returns the `UserVars`
func (p *UserVarsPropProvider) GetUserVars() *variable.UserVars {
	return p.vars
}

// Desc returns the description for the property key.
func (*UserVarsPropProvider) Desc() *context.OptionalEvalPropDesc {
	return context.OptPropUserVars.Desc()
}

type UserVarsReader struct{}

// RequiredOptionalEvalProps implements the RequireOptionalEvalProps interface.
func (UserVarsReader) RequiredOptionalEvalProps() exprctx.OptionalEvalPropKeySet {
	return exprctx.OptPropUserVars.AsPropKeySet()
}

// GetUserVars returns the `UserVars`
func (UserVarsReader) GetUserVars(ctx exprctx.EvalContext) (*variable.UserVars, error) {
	prop, err := getPropProvider[*UserVarsPropProvider](ctx, exprctx.OptPropUserVars)
	if err != nil {
		return nil, err
	}
	return prop.vars, nil
}
