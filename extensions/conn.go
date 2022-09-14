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
	"fmt"
	"strings"

	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/sessionctx/variable"
)

type ConnHandler interface {
	CreateConnEventListener() ConnEventListener
	CreateStmtEventListener() StmtEventListener
}

type ConnEventTp int8

const (
	ConnEstablished ConnEventTp = iota
	ConnAuthenticated
	ConnRejected
	ConnReset
	ConnDisconnect
)

type ConnEvent struct {
	Tp ConnEventTp
	*variable.ConnectionInfo
}

type ConnEventListener interface {
	OnConnEvent(event *ConnEvent)
}

type StmtEventTp int8

const (
	StmtParserError StmtEventTp = iota
	StmtStart
	StmtEnd
)

type StmtEvent struct {
	Tp   StmtEventTp
	Conn *variable.ConnectionInfo
	*StmtContext
}

type StmtEventListener interface {
	OnStmtEvent(event *StmtEvent)
}

type StmtContext struct {
	stmt         ast.StmtNode
	rawArguments []interface{}
	originalSQL  string
	arguments    []string
	digest       struct {
		normalized string
		digest     *parser.Digest
	}
	err error
}

func NewStmtContextWithNode(stmt ast.StmtNode, arguments []interface{}) *StmtContext {
	return &StmtContext{
		stmt:         stmt,
		rawArguments: arguments,
	}
}

func NewStmtContextWithRawText(rawText string) *StmtContext {
	return &StmtContext{
		originalSQL: rawText,
	}
}

func (sc *StmtContext) OriginalSQL() string {
	if sc.originalSQL == "" {
		sc.originalSQL = sc.stmt.Text()
	}
	return sc.originalSQL
}

func (sc *StmtContext) StmtArguments() []string {
	if len(sc.rawArguments) == 0 {
		return nil
	}

	if len(sc.arguments) == 0 {
		sc.arguments = make([]string, 0, len(sc.rawArguments))
		for _, rawArg := range sc.rawArguments {
			switch arg := rawArg.(type) {
			case ast.Node:
				var sb strings.Builder
				ctx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
				_ = arg.Restore(ctx)
				sc.arguments = append(sc.arguments, sb.String())
			case fmt.Stringer:
				sc.arguments = append(sc.arguments, arg.String())
			default:
				sc.arguments = append(sc.arguments, "unknown")
			}
		}
	}

	return sc.arguments
}

func (sc *StmtContext) StmtDigest() (string, *parser.Digest) {
	if sc.digest.normalized == "" {
		sc.digest.normalized, sc.digest.digest = parser.NormalizeDigest(sc.OriginalSQL())
	}
	return sc.digest.normalized, sc.digest.digest
}

func (sc *StmtContext) Error() error {
	return sc.err
}

func (sc *StmtContext) SetError(err error) {
	sc.err = err
}

type ConnExtensions struct {
	*Extensions

	connListeners []ConnEventListener
	stmtListeners []StmtEventListener
}

func (e *ConnExtensions) ListenConnEvents() bool {
	return len(e.connListeners) > 0
}

func (e *ConnExtensions) OnConnEstablished(host string) {
	e.onConnEvent(&ConnEvent{
		Tp: ConnEstablished,
		ConnectionInfo: &variable.ConnectionInfo{
			Host: host,
		},
	})
}

func (e *ConnExtensions) OnConnRejected(connInfo *variable.ConnectionInfo) {
	e.onConnEvent(&ConnEvent{
		Tp:             ConnRejected,
		ConnectionInfo: connInfo,
	})
}

func (e *ConnExtensions) OnConnAuthenticated(connInfo *variable.ConnectionInfo) {
	e.onConnEvent(&ConnEvent{
		Tp:             ConnAuthenticated,
		ConnectionInfo: connInfo,
	})
}

func (e *ConnExtensions) OnConnReset(connInfo *variable.ConnectionInfo) {
	e.onConnEvent(&ConnEvent{
		Tp:             ConnReset,
		ConnectionInfo: connInfo,
	})
}

func (e *ConnExtensions) OnConnDisconnect(connInfo *variable.ConnectionInfo) {
	e.onConnEvent(&ConnEvent{
		Tp:             ConnDisconnect,
		ConnectionInfo: connInfo,
	})
}

func (e *ConnExtensions) ListenStmtEvents() bool {
	return len(e.stmtListeners) != 0
}

func (e *ConnExtensions) OnStmtParseError(connInfo *variable.ConnectionInfo, rawText string) {
	e.onStmtEvent(&StmtEvent{
		Tp:          StmtParserError,
		Conn:        connInfo,
		StmtContext: NewStmtContextWithRawText(rawText),
	})
}

func (e *ConnExtensions) OnStmtStart(connInfo *variable.ConnectionInfo, sc *StmtContext) {
	e.onStmtEvent(&StmtEvent{
		Tp:          StmtStart,
		Conn:        connInfo,
		StmtContext: sc,
	})
}

func (e *ConnExtensions) OnStmtEnd(connInfo *variable.ConnectionInfo, sc *StmtContext) {
	e.onStmtEvent(&StmtEvent{
		Tp:          StmtEnd,
		Conn:        connInfo,
		StmtContext: sc,
	})
}

func (e *ConnExtensions) onConnEvent(event *ConnEvent) {
	for _, l := range e.connListeners {
		l.OnConnEvent(event)
	}
}

func (e *ConnExtensions) onStmtEvent(event *StmtEvent) {
	for _, l := range e.stmtListeners {
		l.OnStmtEvent(event)
	}
}

func WithHandleConnect(fn func() (ConnHandler, error)) ExtensionOption {
	return func(ext *extensionManifest) {
		ext.handleConnect = fn
	}
}

func (e *Extensions) CreateConnExtensions() *ConnExtensions {
	connExtensions := &ConnExtensions{
		Extensions: e,
	}

	if e == nil {
		return connExtensions
	}

	for _, item := range e.items {
		handler, err := item.handleConnect()
		if err != nil {
			return nil
		}

		connExtensions.connListeners = append(connExtensions.connListeners, handler.CreateConnEventListener())
		connExtensions.stmtListeners = append(connExtensions.stmtListeners, handler.CreateStmtEventListener())
	}

	return connExtensions
}
