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

package extension

import (
	"crypto/tls"
	"strings"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

var (
	// ErrAccessDenied is returned when access is denied.
	ErrAccessDenied = dbterror.ClassPrivilege.NewStd(mysql.ErrAccessDenied)
	// ErrPasswordFormat is returned when the password is not in the proper format.
	ErrPasswordFormat = dbterror.ClassExecutor.NewStd(mysql.ErrPasswordFormat)
)

// MysqlAuthInfo is information about a mysql user authentication
type MysqlAuthInfo struct {
	ConnectionID         uint64
	UserName             string
	HostName             string
	AuthenticationString string
	TLSConnState         *tls.ConnectionState
	UserData             []byte
	ClientPlugin         string
	authCommand          func(byte, []byte) error
}

// NeedAuthCommand indicates each command need to be authenticated too
func (i *MysqlAuthInfo) NeedAuthCommand(fn func(byte, []byte) error) {
	i.authCommand = fn
}

func (i *MysqlAuthInfo) GetAuthCommand() func(byte, []byte) error {
	return i.authCommand
}

// ErrAccessDenied returns an ErrAccessDenied error
func (i *MysqlAuthInfo) ErrAccessDenied() error {
	hasPassWord := "YES"
	if len(i.AuthenticationString) == 0 {
		hasPassWord = "NO"
	}
	return ErrAccessDenied.GenWithStackByArgs(i.UserName, i.HostName, hasPassWord)
}

// MysqlAuthPlugin is used to register mysql authenticate plugin
type MysqlAuthPlugin struct {
	// Name is the plugin name
	Name string
	// AuthenticateUser authenticates a user
	AuthenticateUser func(*MysqlAuthInfo) error
	// GenerateAuthString generates the digest from the password
	GenerateAuthString func(string) (string, error)
	// ValidateAuthString validates the password digest
	ValidateAuthString func(string) bool
	// SwitchClientPlugin indicates whether the client plugin should be switched
	SwitchClientPlugin func(connectionID uint64, clientPlugin string) string
}

type MysqlAuthPluginHandle struct {
	plugin *MysqlAuthPlugin
}

func (h *MysqlAuthPluginHandle) AuthenticateUser(info *MysqlAuthInfo) error {
	return h.plugin.AuthenticateUser(info)
}

func (h *MysqlAuthPluginHandle) GenerateAuthString(input string) (string, error) {
	if fn := h.plugin.GenerateAuthString; fn != nil {
		return fn(input)
	}
	return input, nil
}

func (h *MysqlAuthPluginHandle) ValidateAuthString(input string) bool {
	if fn := h.plugin.ValidateAuthString; fn != nil {
		return fn(input)
	}

	return true
}

func (h *MysqlAuthPluginHandle) SwitchClientPlugin(connectionID uint64, clientPlugin string) string {
	if fn := h.plugin.SwitchClientPlugin; fn != nil {
		return strings.ToLower(fn(connectionID, clientPlugin))
	}
	return h.plugin.Name
}

// MysqlAuthPlugins is a list of MysqlAuthPlugin
type MysqlAuthPlugins []*MysqlAuthPlugin

func (ps MysqlAuthPlugins) HasPlugin(plugin string) (ok bool) {
	_, ok = ps.find(plugin)
	return
}

func (ps MysqlAuthPlugins) Plugin(plugin string) (*MysqlAuthPluginHandle, bool) {
	if ps == nil {
		return nil, false
	}

	if pl, ok := ps.find(plugin); ok {
		return &MysqlAuthPluginHandle{plugin: pl}, true
	}

	return nil, false
}

func (ps MysqlAuthPlugins) find(name string) (*MysqlAuthPlugin, bool) {
	for _, pl := range ps {
		if strings.ToLower(pl.Name) == strings.ToLower(name) {
			return pl, true
		}
	}
	return nil, false
}
