// Copyright 2023 PingCAP, Inc.
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

package types

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/errno"
	"time"
)

type flags uint16

const (
	FlagIgnoreTruncateErr flags = 1 << iota
	FlagTruncateAsWarning
	FlagClipNegativeToZero
	FlagIgnoreOverflowError
	FlagOverflowAsWarning
	FlagIgnoreZeroDateErr
	FlagZeroDateAsWarning
	FlagIgnoreZeroInDateErr
	FlagZeroInDateAsWarning
	FlagIgnoreInvalidDateErr
	FlagInvalidDateAsWarning
	FlagSkipASCIICheck
	FlagSkipUTF8Check
	FlagSkipUTF8MB4Check
)

func (f flags) Flags() flags {
	return f
}

func (f flags) TruncateAsError() bool {
	return f&(FlagIgnoreTruncateErr|FlagTruncateAsWarning) == 0
}

func (f flags) TruncateAsWarning() bool {
	return f&FlagTruncateAsWarning != 0
}

func (f flags) ClipNegativeToZero() bool {
	return f&FlagClipNegativeToZero != 0
}

func (f flags) OverflowAsError() bool {
	return f&(FlagIgnoreOverflowError|FlagOverflowAsWarning) == 0
}

func (f flags) OverflowAsWarning() bool {
	return f&FlagOverflowAsWarning != 0
}

func (f flags) ZeroDateAsError() bool {
	return f&(FlagIgnoreZeroDateErr|FlagZeroDateAsWarning) == 0
}

func (f flags) ZeroDateAsWarning() bool {
	return f&FlagZeroDateAsWarning != 0
}

func (f flags) ZeroInDateAsError() bool {
	return f&(FlagIgnoreZeroInDateErr|FlagZeroInDateAsWarning) != 0
}

func (f flags) ZeroInDateAsWarning() bool {
	return f&FlagZeroInDateAsWarning != 0
}

func (f flags) InvalidDateAsError() bool {
	return f&(FlagIgnoreInvalidDateErr|FlagInvalidDateAsWarning) == 0
}

func (f flags) InvalidDateAsWarning() bool {
	return f&FlagInvalidDateAsWarning != 0
}

func (f flags) SkipASCIICheck() bool {
	return f&FlagSkipASCIICheck != 0
}

func (f flags) SkipUTF8Check() bool {
	return f&FlagSkipUTF8Check != 0
}

func (f flags) SkipUTF8MB4Check() bool {
	return f&FlagSkipUTF8MB4Check != 0
}

var (
	DefaultFlags = flags(0)
	DefaultCtx   = ContextWithFlags(DefaultFlags, time.UTC)
)

type Context struct {
	flags
	loc         *time.Location
	warningFunc func(error)
}

func ContextWithFlags(flags flags, loc *time.Location) Context {
	return Context{flags: flags, loc: loc}
}

func ContextWithWarningFunc(flags flags, loc *time.Location, fn func(error)) Context {
	return Context{flags: flags, loc: loc, warningFunc: fn}
}

func (ctx Context) appendWarning(err error) {
	if fn := ctx.warningFunc; fn != nil {
		fn(err)
	}
}

func (ctx Context) Loc() *time.Location {
	return ctx.loc
}

func (ctx Context) HandleTruncate(err error) error {
	// TODO: At present we have not checked whether the error can be ignored or treated as warning.
	// We will do that later, and then append WarnDataTruncated instead of the error itself.
	if err == nil {
		return nil
	}

	asWarning, asError := ctx.TruncateAsWarning(), ctx.TruncateAsError()
	if !asWarning && !asError {
		return nil
	}

	err = errors.Cause(err)
	if e, ok := err.(*errors.Error); !ok ||
		(e.Code() != errno.ErrTruncatedWrongValue &&
			e.Code() != errno.ErrDataTooLong &&
			e.Code() != errno.ErrTruncatedWrongValueForField &&
			e.Code() != errno.ErrWarnDataOutOfRange &&
			e.Code() != errno.ErrDataOutOfRange &&
			e.Code() != errno.ErrBadNumber &&
			e.Code() != errno.ErrWrongValueForType &&
			e.Code() != errno.ErrDatetimeFunctionOverflow &&
			e.Code() != errno.WarnDataTruncated &&
			e.Code() != errno.ErrIncorrectDatetimeValue) {
		return err
	}

	if asWarning {
		ctx.appendWarning(err)
	}

	if asError {
		return err
	}

	return nil
}
