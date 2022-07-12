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

package ddlservice

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	ddlutil "github.com/pingcap/tidb/ddl/util"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

const (
	DDLNodeIDPrefix           = "ddl-svc-"
	nodeKeyPrefix             = "/tidb/ddl_service/nodes"
	nodeStatusesKeyPrefix     = "/tidb/ddl_service/node_statuses"
	coordinatorElectionPrefix = "/tidb/ddl_service/coordinator/leader"
	clusterAssignKeyPrefix    = "/tidb/ddl_service/cluster_assigns"
	clusterInfoKeyPrefix      = "/tidb/ddl_service/cluster_infos"
	etcdOpRetryCnt            = 3
)

type ClusterInfo struct {
	ID        string `json:"id"`
	StoreAddr string `json:"store_addr"`
}

type NodeInfo struct {
	ID string `json:"id"`
}

type NodeClusterTaskStatus struct {
	ID       string `json:"id"`
	DDLOwner bool   `json:"ddl_owner"`
}

type NodeStatus struct {
	ID           string                            `json:"id"`
	ClusterTasks map[string]*NodeClusterTaskStatus `json:"cluster_tasks"`
}

func RegisterNode(ctx context.Context, etcdSession *concurrency.Session, id string) error {
	return ddlutil.PutKVToEtcd(
		ctx,
		etcdSession.Client(),
		etcdOpRetryCnt,
		fmt.Sprintf("%s/%s", nodeKeyPrefix, id),
		id,
		clientv3.WithLease(etcdSession.Lease()),
	)
}

func UpdateNodeStatus(ctx context.Context, etcdSession *concurrency.Session, status *NodeStatus) error {
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

func GetNodeStatuses(ctx context.Context, etcdCli *clientv3.Client) (map[string]*NodeStatus, error) {
	resp, err := etcdCli.Get(ctx, nodeStatusesKeyPrefix, clientv3.WithPrefix())
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

func DelegateClusterDDL(ctx context.Context, etcdCli *clientv3.Client, cluster *ClusterInfo) error {
	jsonStr, err := json.Marshal(cluster)
	if err != nil {
		return err
	}

	return ddlutil.PutKVToEtcd(
		ctx,
		etcdCli,
		etcdOpRetryCnt,
		fmt.Sprintf("%s/%s", clusterInfoKeyPrefix, cluster.ID),
		string(jsonStr),
	)
}

func RemoveCluster(ctx context.Context, etcdCli *clientv3.Client, clusterID string) error {
	_, err := etcdCli.Delete(ctx, fmt.Sprintf("%s/%s", clusterInfoKeyPrefix, clusterID))
	return err
}

func GetClusterInfo(ctx context.Context, etcdCli *clientv3.Client, id string) (*ClusterInfo, error) {
	resp, err := etcdCli.Get(ctx, fmt.Sprintf("%s/%s", clusterInfoKeyPrefix, id))
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

func GetAssignmentMap(ctx context.Context, etcdCli *clientv3.Client) (map[string]string, error) {
	resp, err := etcdCli.Get(ctx, clusterAssignKeyPrefix, clientv3.WithPrefix())
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

func DeleteAssignment(ctx context.Context, etcdCli *clientv3.Client, clusterID string) error {
	key := fmt.Sprintf("%s/%s", clusterAssignKeyPrefix, clusterID)
	cmp := clientv3.Compare(clientv3.CreateRevision(key), "!=", 0)
	_, err := etcdCli.Txn(ctx).If(cmp).Then(clientv3.OpDelete(key)).Commit()
	return err
}

func SetAssignment(ctx context.Context, etcdCli *clientv3.Client, clusterID string, nodeID string) error {
	key := fmt.Sprintf("%s/%s", clusterAssignKeyPrefix, clusterID)
	_, err := etcdCli.Put(ctx, key, nodeID)
	return err
}

func ListClusterInfos(ctx context.Context, etcdCli *clientv3.Client) ([]*ClusterInfo, error) {
	resp, err := etcdCli.Get(ctx, clusterInfoKeyPrefix, clientv3.WithPrefix())
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

func ListNodes(ctx context.Context, etcdCli *clientv3.Client) ([]*NodeInfo, error) {
	resp, err := etcdCli.Get(ctx, nodeKeyPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	nodes := make([]*NodeInfo, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		nodeID := string(kv.Value)
		nodes = append(nodes, &NodeInfo{
			ID: nodeID,
		})
	}
	return nodes, nil
}

func GetServiceLeaderID(ctx context.Context, etcdCli *clientv3.Client) (string, error) {
	resp, err := etcdCli.Get(ctx, coordinatorElectionPrefix, clientv3.WithFirstCreate()...)
	if err != nil {
		return "", err
	}

	if len(resp.Kvs) == 0 {
		return "", nil
	}

	return string(resp.Kvs[0].Value), nil
}

type ClusterDDLTaskContext struct {
	context.Context
	NodeID      string
	ClusterID   string
	ServiceEtcd *clientv3.Client
}

type ClusterDDLTask interface {
	Stop()
	ClusterID() string
	IsDDLOwner() bool
}

type CreateClusterDDLTaskFunc func(*ClusterDDLTaskContext) ClusterDDLTask
