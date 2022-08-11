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

type ClusterDDLTaskContext struct {
	context.Context
	NodeID    string
	ClusterID string
	Store     MetaStore
}

type MetaStore interface {
	CreateWorkerNodeSession(ctx context.Context, nodeID string, ttl int64) (WorkerNodeSession, error)
	GetAllWorkNodeStatuses(ctx context.Context) (map[string]*NodeStatus, error)
	WatchNodeStatuses(ctx context.Context) <-chan struct{}

	DelegateClusterDDL(ctx context.Context, cluster *ClusterInfo) error
	RemoveClusterDDLDelegation(ctx context.Context, clusterID string) error
	GetClusterInfoList(ctx context.Context) ([]*ClusterInfo, error)
	GetClusterInfo(ctx context.Context, clusterID string) (*ClusterInfo, error)
	WatchClusters(ctx context.Context) <-chan struct{}

	GetAllDDLOwnerAssignments(ctx context.Context) (map[string]string, error)
	DeleteDDLOwnerAssignment(ctx context.Context, clusterID string) error
	SetDDLOwnerAssignment(ctx context.Context, clusterID string, nodeID string) error
	WatchDDLOwnerAssignments(ctx context.Context) <-chan struct{}

	GetServiceLeaderID(ctx context.Context) (string, error)
}

type WorkerNodeSession interface {
	ElectLeader(ctx context.Context) (<-chan struct{}, error)
	UpdateWorkerNodeStatus(ctx context.Context, status *NodeStatus) error
	MetaStore() MetaStore
	Close() error
	Done() <-chan struct{}
}
