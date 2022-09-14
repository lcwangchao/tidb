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
)

type ExtensionOption func(ext *extensionManifest)

type extensionManifest struct {
	name          string
	handleCommand func(ast.ExtensionCmdNode) (ExtensionCmdHandler, error)
	handleConnect func() (ConnHandler, error)
}

func newExtension(name string, opts ...ExtensionOption) *extensionManifest {
	extension := &extensionManifest{name: name}
	for _, opt := range opts {
		opt(extension)
	}
	return extension
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

var extensions *Extensions
var lock sync.RWMutex

func Get() *Extensions {
	lock.RLock()
	defer lock.RUnlock()
	return extensions
}

func Register(name string, opts ...ExtensionOption) error {
	lock.Lock()
	defer lock.Unlock()
	newExtensions, err := extensions.addExtension(newExtension(name, opts...))
	if err != nil {
		return err
	}

	extensions = newExtensions
	return nil
}

func Clear() {
	lock.Lock()
	defer lock.Unlock()
	extensions = nil
}
