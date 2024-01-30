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

package authdemo

import (
	"encoding/json"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/tidb/pkg/extension"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
)

type authenticateJSON struct {
	Expire int64  `json:"expire"`
	Hash   string `json:"hash"`
}

func (obj *authenticateJSON) CheckPwd(info *extension.MysqlAuthInfo) error {
	if obj.Expire > 0 && time.Now().Unix() > obj.Expire {
		return info.ErrAccessDenied()
	}

	ok, err := auth.CheckHashingPassword([]byte(obj.Hash), string(info.UserData), mysql.AuthCachingSha2Password)
	if err != nil {
		return err
	}

	if !ok {
		return info.ErrAccessDenied()
	}

	return nil
}

type authSessionHandler struct {
	expire  *time.Time
	expires *sync.Map
}

func newAuthSessionHandler(expires *sync.Map) *authSessionHandler {
	h := &authSessionHandler{
		expires: expires,
	}
	return h
}

func (h *authSessionHandler) onConnEvent(tp extension.ConnEventTp, info *extension.ConnEventInfo) {
	switch tp {
	case extension.ConnHandshakeAccepted, extension.ConnReset:
		if v, ok := h.expires.LoadAndDelete(info.ConnectionInfo.ConnectionID); ok {
			h.expire = v.(*time.Time)
		}
	default:
	}
}

func (h *authSessionHandler) interceptBeforeCommand(cmd byte, _ []byte) error {
	if h.expire == nil || cmd == mysql.ComQuit {
		return nil
	}

	if h.expire.Before(time.Now()) {
		return extension.ErrAccessDenied.FastGen("Access denied; connection expired.")
	}

	return nil
}

func (h *authSessionHandler) Handler() *extension.SessionHandler {
	return &extension.SessionHandler{
		OnConnectionEvent:      h.onConnEvent,
		InterceptBeforeCommand: h.interceptBeforeCommand,
	}
}

func init() {
	var expires sync.Map
	err := extension.Register(
		"my_auth",
		extension.WithMysqlAuthPlugins([]*extension.MysqlAuthPlugin{
			{
				Name: "my_auth",
				AuthenticateUser: func(info *extension.MysqlAuthInfo) error {
					var obj authenticateJSON
					if err := json.Unmarshal([]byte(info.AuthenticationString), &obj); err != nil {
						return err
					}
					if err := obj.CheckPwd(info); err != nil {
						return err
					}

					if obj.Expire > 0 {
						expire := time.Unix(obj.Expire, 0)
						expires.Store(info.ConnectionID, &expire)
					}

					return nil
				},
				SwitchClientPlugin: func(_ uint64, clientPlugin string) string {
					return mysql.AuthCachingSha2Password
				},
				GenerateAuthString: func(pwd string) (string, error) {
					var obj authenticateJSON
					if parts := strings.SplitN(pwd, ",", 2); len(parts) > 1 {
						pwd = parts[0]
						expire, err := strconv.ParseInt(parts[1], 10, 64)
						if err != nil {
							return "", extension.ErrPasswordFormat
						}
						obj.Expire = time.Now().Unix() + expire
					}

					obj.Hash = auth.NewHashPassword(pwd, mysql.AuthCachingSha2Password)
					bs, err := json.Marshal(obj)
					if err != nil {
						return "", err
					}
					return string(bs), nil
				},
				ValidateAuthString: func(s string) bool {
					var obj authenticateJSON
					if err := json.Unmarshal([]byte(s), &obj); err != nil {
						return false
					}
					return len(obj.Hash) == mysql.SHAPWDHashLen
				},
			},
		}),
		extension.WithSessionHandlerFactory(func() *extension.SessionHandler {
			return newAuthSessionHandler(&expires).Handler()
		}),
	)
	terror.MustNil(err)
}
