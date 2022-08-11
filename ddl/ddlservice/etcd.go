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

package ddlservice

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pingcap/errors"
	ddlutil "github.com/pingcap/tidb/ddl/util"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

const (
	DDLNodeIDPrefix           = "ddl-svc-"
	nodeStatusesKeyPrefix     = "/tidb/ddl_service/node_statuses"
	coordinatorElectionPrefix = "/tidb/ddl_service/coordinator/leader"
	clusterAssignKeyPrefix    = "/tidb/ddl_service/cluster_assigns"
	clusterInfoKeyPrefix      = "/tidb/ddl_service/cluster_infos"
	etcdOpRetryCnt            = 3
)

type EtcdMetaStore struct {
	etcdCli *clientv3.Client
}

func NewEtcdMetaStore(etcdCli *clientv3.Client) *EtcdMetaStore {
	return &EtcdMetaStore{
		etcdCli: etcdCli,
	}
}

func (s *EtcdMetaStore) CreateWorkerNodeSession(ctx context.Context, nodeID string, ttl int64) (WorkerNodeSession, error) {
	if nodeID == "" {
		return nil, errors.New("nodeID is empty")
	}

	leaseID := clientv3.NoLease
	if ttl > 0 {
		resp, err := s.etcdCli.Grant(ctx, ttl)
		if err != nil {
			return nil, err
		}
		leaseID = resp.ID
	}

	etcdSession, err := concurrency.NewSession(s.etcdCli, concurrency.WithLease(leaseID))
	if err != nil {
		return nil, err
	}

	err = updateWorkerNodeStatus(ctx, etcdSession, &NodeStatus{
		ID: nodeID,
	})

	if err != nil {
		return nil, err
	}

	return &EtcdWorkerNodeSession{
		nodeID:      nodeID,
		etcdSession: etcdSession,
		store:       s,
	}, nil
}

func (s *EtcdMetaStore) GetAllWorkNodeStatuses(ctx context.Context) (map[string]*NodeStatus, error) {
	resp, err := s.etcdCli.Get(ctx, nodeStatusesKeyPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	nodes := make(map[string]*NodeStatus)
	for _, kv := range resp.Kvs {
		var status NodeStatus
		if err = json.Unmarshal(kv.Value, &status); err != nil {
			return nil, err
		}
		nodes[status.ID] = &status
	}
	return nodes, nil
}

func (s *EtcdMetaStore) WatchNodeStatuses(ctx context.Context) <-chan struct{} {
	return mapChanToEmpty(s.etcdCli.Watch(ctx, nodeStatusesKeyPrefix, clientv3.WithPrefix()))
}

func (s *EtcdMetaStore) DelegateClusterDDL(ctx context.Context, cluster *ClusterInfo) error {
	if cluster.ID == "" {
		return errors.New("cluster id is empty")
	}

	jsonStr, err := json.Marshal(cluster)
	if err != nil {
		return err
	}

	return ddlutil.PutKVToEtcd(
		ctx,
		s.etcdCli,
		etcdOpRetryCnt,
		fmt.Sprintf("%s/%s", clusterInfoKeyPrefix, cluster.ID),
		string(jsonStr),
	)
}

func (s *EtcdMetaStore) RemoveClusterDDLDelegation(ctx context.Context, clusterID string) error {
	if clusterID == "" {
		return errors.New("cluster id is empty")
	}

	_, err := s.etcdCli.Delete(ctx, fmt.Sprintf("%s/%s", clusterInfoKeyPrefix, clusterID))
	return err
}

func (s *EtcdMetaStore) GetClusterInfoList(ctx context.Context) ([]*ClusterInfo, error) {
	resp, err := s.etcdCli.Get(ctx, clusterInfoKeyPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	infos := make([]*ClusterInfo, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var info ClusterInfo
		if err = json.Unmarshal(kv.Value, &info); err != nil {
			return nil, err
		}
		infos = append(infos, &info)
	}
	return infos, nil
}

func (s *EtcdMetaStore) GetClusterInfo(ctx context.Context, clusterID string) (*ClusterInfo, error) {
	if clusterID == "" {
		return nil, errors.New("cluster id is empty")
	}

	resp, err := s.etcdCli.Get(ctx, fmt.Sprintf("%s/%s", clusterInfoKeyPrefix, clusterID))
	if err != nil {
		return nil, err
	}

	if len(resp.Kvs) == 0 {
		return nil, nil
	}

	var info ClusterInfo
	if err = json.Unmarshal(resp.Kvs[0].Value, &info); err != nil {
		return nil, err
	}

	return &info, nil
}

func (s *EtcdMetaStore) WatchClusters(ctx context.Context) <-chan struct{} {
	return mapChanToEmpty(s.etcdCli.Watch(ctx, clusterInfoKeyPrefix, clientv3.WithPrefix()))
}

func (s *EtcdMetaStore) GetAllDDLOwnerAssignments(ctx context.Context) (map[string]string, error) {
	resp, err := s.etcdCli.Get(ctx, clusterAssignKeyPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	assignments := make(map[string]string)
	for _, kv := range resp.Kvs {
		clusterID := strings.TrimPrefix(string(kv.Key), clusterAssignKeyPrefix+"/")
		nodeID := string(kv.Value)
		assignments[clusterID] = nodeID
	}
	return assignments, nil
}

func (s *EtcdMetaStore) DeleteDDLOwnerAssignment(ctx context.Context, clusterID string) error {
	if clusterID == "" {
		return errors.New("cluster id is empty")
	}

	key := fmt.Sprintf("%s/%s", clusterAssignKeyPrefix, clusterID)
	cmp := clientv3.Compare(clientv3.CreateRevision(key), "!=", 0)
	_, err := s.etcdCli.Txn(ctx).If(cmp).Then(clientv3.OpDelete(key)).Commit()
	return err
}

func (s *EtcdMetaStore) SetDDLOwnerAssignment(ctx context.Context, clusterID string, nodeID string) error {
	if clusterID == "" {
		return errors.New("cluster id is empty")
	}

	key := fmt.Sprintf("%s/%s", clusterAssignKeyPrefix, clusterID)
	_, err := s.etcdCli.Put(ctx, key, nodeID)
	return err
}

func (s *EtcdMetaStore) WatchDDLOwnerAssignments(ctx context.Context) <-chan struct{} {
	return mapChanToEmpty(s.etcdCli.Watch(ctx, clusterAssignKeyPrefix, clientv3.WithPrefix()))
}

func (s *EtcdMetaStore) GetServiceLeaderID(ctx context.Context) (string, error) {
	resp, err := s.etcdCli.Get(ctx, coordinatorElectionPrefix, clientv3.WithFirstCreate()...)
	if err != nil {
		return "", err
	}

	if len(resp.Kvs) == 0 {
		return "", nil
	}

	return string(resp.Kvs[0].Value), nil
}

type EtcdWorkerNodeSession struct {
	nodeID      string
	etcdSession *concurrency.Session
	store       *EtcdMetaStore
}

func (s *EtcdWorkerNodeSession) ElectLeader(ctx context.Context) (<-chan struct{}, error) {
	election := concurrency.NewElection(s.etcdSession, coordinatorElectionPrefix)
	if err := election.Campaign(ctx, s.nodeID); err != nil {
		return nil, err
	}

	resp, err := election.Leader(ctx)
	if err != nil {
		return nil, err
	}

	if len(resp.Kvs) == 0 {
		return nil, errors.New("coordinator campaign failed, leader is empty")
	}

	key := string(resp.Kvs[0].Key)
	leaderID := string(resp.Kvs[0].Value)
	if leaderID != s.nodeID {
		return nil, errors.Errorf("coordinator campaign failed, leader not match, leader: %s, nodeID: %s", leaderID, s.nodeID)
	}

	watchCh := s.etcdSession.Client().Watch(ctx, key)
	return mapChanToEmpty(watchCh), nil
}

func (s *EtcdWorkerNodeSession) UpdateWorkerNodeStatus(ctx context.Context, status *NodeStatus) error {
	if status == nil || status.ID != s.nodeID {
		return errors.New("node id is empty")
	}
	return updateWorkerNodeStatus(ctx, s.etcdSession, status)
}

func (s *EtcdWorkerNodeSession) MetaStore() MetaStore {
	return s.store
}

func (s *EtcdWorkerNodeSession) Close() error {
	return s.etcdSession.Close()
}

func (s *EtcdWorkerNodeSession) Done() <-chan struct{} {
	return s.etcdSession.Done()
}

func updateWorkerNodeStatus(ctx context.Context, etcdSession *concurrency.Session, status *NodeStatus) error {
	value, err := json.Marshal(status)
	if err != nil {
		return err
	}

	return ddlutil.PutKVToEtcd(
		ctx,
		etcdSession.Client(),
		1,
		fmt.Sprintf("%s/%s", nodeStatusesKeyPrefix, status.ID),
		string(value),
		clientv3.WithLease(etcdSession.Lease()),
	)
}

func mapChan[I any, O any](source <-chan I, mapFunc func(I) O) <-chan O {
	ch := make(chan O)
	go func() {
		for obj := range source {
			ch <- mapFunc(obj)
		}
		close(ch)
	}()
	return ch
}

func mapChanToEmpty[I any](source <-chan I) <-chan struct{} {
	return mapChan(source, func(i I) struct{} {
		return struct{}{}
	})
}
