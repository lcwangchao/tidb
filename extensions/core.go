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
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/sessionctx/variable"
)

type ExtensionOption func(ext *extensionManifest)

func WithHandleConnect(fn func() (*ConnHandler, error)) ExtensionOption {
	return func(ext *extensionManifest) {
		ext.handleConnect = fn
	}
}

func WithDynamicPrivileges(privileges []string) ExtensionOption {
	return func(ext *extensionManifest) {
		ext.dynPrivileges = privileges
	}
}

func WithSysVariables(vars []*variable.SysVar) ExtensionOption {
	return func(ext *extensionManifest) {
		ext.sysVariables = vars
	}
}

func WithHandleCommand(fn func(ast.ExtensionCmdNode) (ExtensionCmdHandler, error)) ExtensionOption {
	return func(ext *extensionManifest) {
		ext.handleCommand = fn
	}
}

var extensions *Extensions
var inited bool
var lock sync.RWMutex

func Get() (*Extensions, error) {
	lock.RLock()
	defer lock.RUnlock()
	if extensions == nil {
		inited = true
		return extensions, nil
	}

	if !inited {
		return nil, errors.New("extensions not inited")
	}
	return extensions, nil
}

func Init() error {
	lock.Lock()
	defer lock.Unlock()
	if inited {
		return nil
	}

	if extensions == nil {
		inited = true
		return nil
	}

	var allInited []*extensionManifest
	for _, ext := range extensions.items {
		if err := ext.init(); err != nil {
			for _, e := range allInited {
				e.deInit(e.dynPrivileges, e.sysVariables)
			}
			return err
		}
		allInited = append(allInited, ext)
	}

	inited = true
	return nil
}

func Register(name string, opts ...ExtensionOption) error {
	lock.Lock()
	defer lock.Unlock()
	if inited {
		return errors.New("extensions has been inited")
	}

	ext := newExtension(name, opts...)
	newExtensions, err := extensions.addExtension(ext)
	if err != nil {
		return err
	}

	extensions = newExtensions
	return nil
}

func Clear() {
	lock.Lock()
	defer lock.Unlock()

	for _, ext := range extensions.items {
		ext.deInit(ext.dynPrivileges, ext.sysVariables)
	}

	extensions = nil
	inited = false
}

type extensionManifest struct {
	name          string
	dynPrivileges []string
	sysVariables  []*variable.SysVar
	handleCommand func(ast.ExtensionCmdNode) (ExtensionCmdHandler, error)
	handleConnect func() (*ConnHandler, error)
}

func newExtension(name string, opts ...ExtensionOption) *extensionManifest {
	extension := &extensionManifest{name: name}
	for _, opt := range opts {
		opt(extension)
	}
	return extension
}

func (e *extensionManifest) init() error {
	var initedPrivs []string
	var initedSysVars []*variable.SysVar

	for _, priv := range e.dynPrivileges {
		if err := RegisterDynamicPrivilege(priv); err != nil {
			e.deInit(initedPrivs, initedSysVars)
			return err
		}
		initedPrivs = append(initedPrivs, priv)
	}

	for _, sysVar := range e.sysVariables {
		if v := variable.GetSysVar(sysVar.Name); v != nil {
			e.deInit(initedPrivs, initedSysVars)
			return errors.Errorf("sys var exists: %s", sysVar.Name)
		}
		initedSysVars = append(initedSysVars, sysVar)
		variable.RegisterSysVar(sysVar)
	}

	return nil
}

func (e *extensionManifest) deInit(dynPrivs []string, sysVars []*variable.SysVar) {
	for _, priv := range dynPrivs {
		RemoveDynamicPrivilege(priv)
	}

	for _, sysVar := range sysVars {
		variable.UnregisterSysVar(sysVar.Name)
	}
}

type Extensions struct {
	items map[string]*extensionManifest
}

func (e *Extensions) copy() *Extensions {
	newExtensions := &Extensions{
		items: make(map[string]*extensionManifest),
	}

	if e != nil {
		for name, item := range e.items {
			newExtensions.items[name] = item
		}
	}

	return newExtensions
}

func (e *Extensions) addExtension(extension *extensionManifest) (*Extensions, error) {
	if extension == nil {
		return nil, errors.Errorf("extension is nil")
	}

	cp := e.copy()
	if _, ok := cp.items[extension.name]; ok {
		return nil, errors.Errorf("extension with name '%s' has been registered", extension.name)
	}

	cp.items[extension.name] = extension
	return cp, nil
}

var RegisterDynamicPrivilege func(string) error
var RemoveDynamicPrivilege func(string) bool
