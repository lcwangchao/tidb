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

package proto

type SessionSystemVarDefs struct {
	MaxChunkSize    int    `sys_var:"tidb_max_chunk_size,1024,unsigned,setter,range=int32,min=32"`
	DMLBatchSize    int    `sys_var:"tidb_dml_batch_size,0,unsigned,setter,range=int32"`
	SkipUTF8Check   bool   `sys_var:"tidb_skip_utf8_check,false,setter"`
	BatchInsert     bool   `sys_var:"tidb_batch_insert,false,session_only"`
	MPPStoreFailTTL string `sys_var:"tidb_mpp_store_fail_ttl,'60s',str"`
}
