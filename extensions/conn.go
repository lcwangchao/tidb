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
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/sessionctx/variable"
)

type ConnHandler interface {
	CreateConnEventListener() ConnEventListener
	CreateStmtEventListener() StmtEventListener
}

type ConnEventTp int8

const (
	Connected ConnEventTp = iota
	ConnAuthenticated
	ConnRejected
	ConnReset
	ConnDisconnect
)

type ConnEventListener interface {
	OnConnEvent(tp ConnEventTp, connInfo *variable.ConnectionInfo)
}

type StmtEventTp int8

const (
	StmtParserError StmtEventTp = iota
	StmtStart
	StmtEnd
)

type StmtEventContext interface {
	SessionContext
	OriginalSQL() string
	StmtDigest() (string, *parser.Digest)
	StmtArguments() []string
	Error() error
	SetError(err error)
}

type StmtEventListener interface {
	OnStmtEvent(tp StmtEventTp, stmt StmtEventContext)
}

type ConnExtensions struct {
	extensions *Extensions

	seCtx         SessionContext
	stmtEventCtx  StmtEventContext
	connListeners []ConnEventListener
	stmtListeners []StmtEventListener
}

func (e *ConnExtensions) CreateExtensionCmdHandler(node ast.ExtensionCmdNode) (ExtensionCmdHandler, error) {
	errorMsg := "no matched extension found"
	if e == nil {
		return nil, errors.New(errorMsg)
	}

	for _, item := range e.extensions.items {
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

func (e *ConnExtensions) SetSessionContext(seCtx SessionContext) {
	if e == nil {
		return
	}
	e.seCtx = seCtx
}

func (e *ConnExtensions) GetSessionContext() SessionContext {
	if e == nil {
		return nil
	}
	return e.seCtx
}

func (e *ConnExtensions) OnConnected(host string) {
	if e == nil || len(e.connListeners) == 0 {
		return
	}
	e.onConnEvent(Connected, &variable.ConnectionInfo{
		Host: host,
	})
}

func (e *ConnExtensions) OnConnRejected(connInfo *variable.ConnectionInfo) {
	if e == nil || len(e.connListeners) == 0 {
		return
	}
	e.onConnEvent(ConnRejected, connInfo)
}

func (e *ConnExtensions) OnConnAuthenticated(connInfo *variable.ConnectionInfo) {
	if e == nil || len(e.connListeners) == 0 {
		return
	}
	e.onConnEvent(ConnAuthenticated, connInfo)
}

func (e *ConnExtensions) OnConnReset(connInfo *variable.ConnectionInfo) {
	if e == nil || len(e.connListeners) == 0 {
		return
	}
	e.onConnEvent(ConnRejected, connInfo)
}

func (e *ConnExtensions) OnConnDisconnect(connInfo *variable.ConnectionInfo) {
	if e == nil || len(e.connListeners) == 0 {
		return
	}
	e.onConnEvent(ConnDisconnect, connInfo)
}

func (e *ConnExtensions) OnStmtParseError(rawText string) {
	if e == nil || len(e.stmtListeners) == 0 {
		return
	}
	e.onStmtEvent(StmtParserError, e.seCtx.CreateStmtEventContextWithRawSQL(rawText))
}

func (e *ConnExtensions) OnStmtStart(stmt ast.StmtNode) {
	if e == nil || len(e.stmtListeners) == 0 {
		return
	}
	e.stmtEventCtx = e.seCtx.CreateStmtEventContextWithStmt(stmt)
	e.onStmtEvent(StmtStart, e.stmtEventCtx)
}

func (e *ConnExtensions) OnStmtEnd(err error) {
	if e == nil || len(e.stmtListeners) == 0 {
		return
	}
	defer func() {
		e.stmtEventCtx = nil
	}()

	e.stmtEventCtx.SetError(err)
	e.onStmtEvent(StmtEnd, e.stmtEventCtx)
}

func (e *ConnExtensions) onConnEvent(tp ConnEventTp, connInfo *variable.ConnectionInfo) {
	for _, l := range e.connListeners {
		l.OnConnEvent(tp, connInfo)
	}
}

func (e *ConnExtensions) onStmtEvent(tp StmtEventTp, stmt StmtEventContext) {
	for _, l := range e.stmtListeners {
		l.OnStmtEvent(tp, stmt)
	}
}

func WithHandleConnect(fn func() (ConnHandler, error)) ExtensionOption {
	return func(ext *extensionManifest) {
		ext.handleConnect = fn
	}
}

func (e *Extensions) CreateConnExtensions() *ConnExtensions {
	if e == nil {
		return nil
	}

	var connExtensions *ConnExtensions
	for _, item := range e.items {
		handler, err := item.handleConnect()
		if err != nil {
			return nil
		}

		if connExtensions == nil {
			connExtensions = &ConnExtensions{
				extensions: extensions,
			}
		}

		connExtensions.connListeners = append(connExtensions.connListeners, handler.CreateConnEventListener())
		connExtensions.stmtListeners = append(connExtensions.stmtListeners, handler.CreateStmtEventListener())
	}

	return connExtensions
}
