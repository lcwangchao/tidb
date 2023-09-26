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

type Flags uint16

const (
	FlagIgnoreTruncateErr Flags = 1 << iota
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

func (f Flags) TruncateAsError() bool {
	return f&(FlagIgnoreTruncateErr|FlagTruncateAsWarning) == 0
}

func (f Flags) TruncateAsWarning() bool {
	return f&FlagTruncateAsWarning != 0
}

func (f Flags) ClipNegativeToZero() bool {
	return f&FlagClipNegativeToZero != 0
}

func (f Flags) OverflowAsError() bool {
	return f&(FlagIgnoreOverflowError|FlagOverflowAsWarning) == 0
}

func (f Flags) OverflowAsWarning() bool {
	return f&FlagOverflowAsWarning != 0
}

func (f Flags) ZeroDateAsError() bool {
	return f&(FlagIgnoreZeroDateErr|FlagZeroDateAsWarning) == 0
}

func (f Flags) ZeroDateAsWarning() bool {
	return f&FlagZeroDateAsWarning != 0
}

func (f Flags) ZeroInDateAsError() bool {
	return f&(FlagIgnoreZeroInDateErr|FlagZeroInDateAsWarning) != 0
}

func (f Flags) ZeroInDateAsWarning() bool {
	return f&FlagZeroInDateAsWarning != 0
}

func (f Flags) InvalidDateAsError() bool {
	return f&(FlagIgnoreInvalidDateErr|FlagInvalidDateAsWarning) == 0
}

func (f Flags) InvalidDateAsWarning() bool {
	return f&FlagInvalidDateAsWarning != 0
}

func (f Flags) SkipASCIICheck() bool {
	return f&FlagSkipASCIICheck != 0
}

func (f Flags) SkipUTF8Check() bool {
	return f&FlagSkipUTF8Check != 0
}

func (f Flags) SkipUTF8MB4Check() bool {
	return f&FlagSkipUTF8MB4Check != 0
}

type ValContext struct {
	Flags
	TimeZone    *time.Location
	WarningFunc func(error)
}

func DefaultValContext() ValContext {
	return ValContext{TimeZone: time.Local}
}

func (ctx ValContext) AppendWarning(err error) {
	if fn := ctx.WarningFunc; fn != nil {
		fn(err)
	}
}

func (ctx ValContext) HandleTruncate(err error) error {
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
		ctx.AppendWarning(err)
	}

	if asError {
		return err
	}

	return nil
}
