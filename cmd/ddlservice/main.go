// Copyright 2022 PingCAP, Inc.
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

package main

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl/ddlservice"
	"github.com/pingcap/tidb/ddl/ddlservice/cluster"
	"github.com/pingcap/tidb/parser/terror"
	kvstore "github.com/pingcap/tidb/store"
	"github.com/pingcap/tidb/store/driver"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/keepalive"
)

func main() {
	ctx := context.TODO()
	cfg := config.GetGlobalConfig()

	err := kvstore.Register("tikv", driver.TiKVDriver{})
	terror.MustNil(err)

	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:        []string{"127.0.0.1:2379"},
		AutoSyncInterval: 30 * time.Second,
		DialTimeout:      5 * time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithConnectParams(grpc.ConnectParams{Backoff: backoff.DefaultConfig}),
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:    time.Duration(cfg.TiKVClient.GrpcKeepAliveTime) * time.Second,
				Timeout: time.Duration(cfg.TiKVClient.GrpcKeepAliveTimeout) * time.Second,
			}),
		},
	})
	terror.MustNil(err)

	session, err := concurrency.NewSession(etcdCli)
	terror.MustNil(err)

	n := ddlservice.NewServiceNode(session, uuid.New().String(), cluster.NewClusterDDLTask)
	n.Start()

	err = ddlservice.DelegateClusterDDL(ctx, etcdCli, &ddlservice.ClusterInfo{
		ID:        "cluster1",
		StoreAddr: "tikv://127.0.0.1:3379",
	})
	terror.MustNil(err)

	<-ctx.Done()
}
