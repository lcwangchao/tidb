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

package cluster

import (
	"context"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/ddl/ddlservice"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/session"
	kvstore "github.com/pingcap/tidb/store"
	"github.com/pingcap/tidb/util/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

type clusterDDLTask struct {
	ctx         context.Context
	clusterID   string
	nodeID      string
	serviceEtcd *clientv3.Client
	do          atomic.Value
	cancel      func()
}

func NewClusterDDLTask(taskCtx *ddlservice.ClusterDDLTaskContext) ddlservice.ClusterDDLTask {
	ctx, cancel := context.WithCancel(taskCtx.Context)
	task := &clusterDDLTask{
		ctx:         ctx,
		clusterID:   taskCtx.ClusterID,
		nodeID:      taskCtx.NodeID,
		serviceEtcd: taskCtx.ServiceEtcd,
		cancel:      cancel,
	}
	go task.taskLoop()
	return task
}

func (t *clusterDDLTask) ClusterID() string {
	return t.clusterID
}

func (t *clusterDDLTask) Stop() {
	t.cancel()
}

func (t *clusterDDLTask) IsDDLOwner() bool {
	if do := t.domain(); do != nil {
		return do.DDL().OwnerManager().IsOwner()
	}
	return false
}

func (t *clusterDDLTask) domain() *domain.Domain {
	if do, ok := t.do.Load().(*domain.Domain); ok {
		return do
	}
	return nil
}

func (t *clusterDDLTask) taskLoop() {
	defer func() {
		if do := t.domain(); do != nil {
			do.Close()
		}
	}()

	t.ensureDomain()
	ticker := time.Tick(time.Second * 10)
loop:
	for {
		select {
		case <-t.ctx.Done():
			break loop
		case <-ticker:
			t.ensureDomain()
		}
	}
}

func (t *clusterDDLTask) ensureDomain() {
	if t.domain() != nil {
		return
	}

	var err error
	defer func() {
		if err != nil {
			logutil.BgLogger().Error(
				"create delegate ddl cluster domain failed",
				zap.Error(err),
				zap.String("nodeID", t.nodeID),
				zap.String("clusterID", t.clusterID),
			)
		}
	}()

	var info *ddlservice.ClusterInfo
	info, err = ddlservice.GetClusterInfo(t.ctx, t.serviceEtcd, t.clusterID)
	if info == nil {
		err = errors.New("cannot get cluster info")
		return
	}

	logutil.BgLogger().Info("new delegate ddl cluster domain",
		zap.String("clusterID", t.clusterID),
		zap.String("clusterAddr", info.StoreAddr),
	)

	store, err := kvstore.New(info.StoreAddr)
	if err != nil {
		return
	}

	do, err := domain.StartDDLDomain(store, time.Second*45, t.nodeID, session.CreateSessionWithDomainFunc(store))
	if err != nil {
		return
	}

	t.do.Store(do)
	time.Sleep(time.Second)
	go t.retireNonServiceOwnerLoop(do.GetEtcdClient())
}

func (t *clusterDDLTask) retireNonServiceOwnerLoop(etcd *clientv3.Client) {
	ticker := time.Tick(time.Second * 10)
loop:
	for {
		select {
		case <-t.ctx.Done():
			break loop
		case <-ticker:
			t.retireNonServiceOwner(etcd)
		}
	}
}

func (t *clusterDDLTask) retireNonServiceOwner(etcd *clientv3.Client) {
	resp, err := etcd.Get(t.ctx, ddl.DDLOwnerKey, clientv3.WithPrefix())
	if err != nil {
		logutil.BgLogger().Error("failed to get all ddl owner keys",
			zap.Error(err),
			zap.String("clusterID", t.clusterID),
			zap.Strings("etcdEndPoints", etcd.Endpoints()),
		)
		return
	}

	for _, kv := range resp.Kvs {
		ownerID := string(kv.Value)
		if strings.HasPrefix(ownerID, ddlservice.DDLNodeIDPrefix) {
			continue
		}

		lockKey := string(kv.Key)
		logutil.BgLogger().Info("non service ddl lock detected, delete it",
			zap.String("lockKey", lockKey),
			zap.String("lockOwnerID", ownerID))
		_, err = etcd.Delete(t.ctx, lockKey)
		if err != nil {
			logutil.BgLogger().Error("failed to retire ddl lock which is not in ddl service",
				zap.Error(err),
				zap.String("clusterID", t.clusterID),
				zap.Strings("etcdEndPoints", etcd.Endpoints()),
				zap.String("lockKey", lockKey),
				zap.String("lockOwnerID", ownerID),
			)
		}
	}
}
