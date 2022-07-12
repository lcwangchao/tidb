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
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

type ServiceNode struct {
	ctx          context.Context
	id           string
	etcdSession  *concurrency.Session
	cancel       func()
	once         sync.Once
	taskFunc     CreateClusterDDLTaskFunc
	tasks        atomic.Value
	taskStatuses atomic.Value
}

func NewServiceNode(etcdSession *concurrency.Session, ddlID string, taskFunc CreateClusterDDLTaskFunc) *ServiceNode {
	id := DDLNodeIDPrefix + ddlID
	ctx, cancel := context.WithCancel(context.Background())
	return &ServiceNode{
		ctx:         ctx,
		id:          id,
		etcdSession: etcdSession,
		cancel:      cancel,
		taskFunc:    taskFunc,
	}
}

func (n *ServiceNode) Start() {
	n.once.Do(func() {
		go n.campaignLoop()
		go n.nodeLoop()
	})
}

func (n *ServiceNode) Stop() {
	n.cancel()
}

func (n *ServiceNode) getTasks() map[string]ClusterDDLTask {
	if tasks, ok := n.tasks.Load().(map[string]ClusterDDLTask); ok {
		return tasks
	}
	return nil
}

func (n *ServiceNode) getTaskStatuses() map[string]*NodeClusterTaskStatus {
	if statuses, ok := n.taskStatuses.Load().(map[string]*NodeClusterTaskStatus); ok {
		return statuses
	}
	return nil
}

func (n *ServiceNode) updateClusterDDLTasks() {
	tasks := n.getTasks()
	assignments, err := GetAssignmentMap(n.ctx, n.etcdSession.Client())
	if err != nil {
		logutil.BgLogger().Error("setup cluster ddl tasks error", zap.Error(err))
		return
	}

	newTasks := make(map[string]ClusterDDLTask)
	for clusterID, nodeID := range assignments {
		if nodeID != n.id {
			continue
		}

		if task, ok := tasks[clusterID]; ok {
			newTasks[clusterID] = task
			continue
		}

		newTasks[clusterID] = n.taskFunc(&ClusterDDLTaskContext{
			Context:     n.ctx,
			NodeID:      n.id,
			ClusterID:   clusterID,
			ServiceEtcd: n.etcdSession.Client(),
		})
	}

	for _, task := range tasks {
		if _, ok := newTasks[task.ClusterID()]; !ok {
			task.Stop()
		}
	}
	n.tasks.Store(newTasks)
	n.updateNodeStatus()
}

func (n *ServiceNode) campaignLoop() {
	election := concurrency.NewElection(n.etcdSession, coordinatorElectionPrefix)
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-n.etcdSession.Done():
			return
		default:
		}
		n.campaignAndCoordinate(election)
	}
}

func (n *ServiceNode) campaignAndCoordinate(election *concurrency.Election) {
	var wg util.WaitGroupWrapper
	leaderCtx, cancel := context.WithCancel(n.ctx)
	defer func() {
		cancel()
		wg.Wait()
	}()

	if err := election.Campaign(n.ctx, n.id); err != nil {
		logutil.BgLogger().Error("coordinator campaign failed", zap.Error(err), zap.String("nodeID", n.id))
		return
	}

	resp, err := election.Leader(n.ctx)
	if err != nil {
		logutil.BgLogger().Error("coordinator campaign failed, leader cannot be fetched", zap.Error(err), zap.String("nodeID", n.id))
		return
	}

	if len(resp.Kvs) == 0 {
		logutil.BgLogger().Error("coordinator campaign failed, leader is empty")
		return
	}

	key := string(resp.Kvs[0].Key)
	leaderID := string(resp.Kvs[0].Value)
	if leaderID != n.id {
		logutil.BgLogger().Error("coordinator campaign failed, leader not match", zap.String("leader", leaderID), zap.String("nodeID", n.id))
		return
	}

	wg.Add(1)
	go func() {
		defer wg.Add(-1)
		n.coordinateLoop(leaderCtx)
	}()

	watchCh := n.etcdSession.Client().Watch(n.ctx, key)
	for {
		select {
		case resp, ok := <-watchCh:
			if !ok {
				return
			}
			if resp.Canceled {
				return
			}

			for _, ev := range resp.Events {
				if ev.Type == mvccpb.DELETE {
					return
				}
			}
		case <-n.etcdSession.Done():
			return
		case <-n.ctx.Done():
			return
		}
	}
}

func (n *ServiceNode) coordinateLoop(ctx context.Context) {
	watchClusters := n.etcdSession.Client().Watch(ctx, clusterInfoKeyPrefix, clientv3.WithPrefix())
	watchNodes := n.etcdSession.Client().Watch(ctx, nodeKeyPrefix, clientv3.WithPrefix())
	n.assignAllClusters(ctx)

	ticker := time.Tick(time.Minute)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker:
			n.assignAllClusters(ctx)
		case <-watchClusters:
			n.assignAllClusters(ctx)
		case <-watchNodes:
			n.assignAllClusters(ctx)
		}
	}
}

func (n *ServiceNode) assignAllClusters(ctx context.Context) {
	var err error
	defer func() {
		if err != nil {
			logutil.BgLogger().Error("assign clusters failed", zap.Error(err), zap.String("nodeID", n.id))
		}
	}()

	etcdCli := n.etcdSession.Client()
	clusters := make(map[string]*ClusterInfo)
	clusterList, err := ListClusterInfos(ctx, etcdCli)
	if err != nil {
		logutil.BgLogger().Error("assign clusters failed", zap.Error(err), zap.String("coordinator", n.id))
		return
	}
	for _, cluster := range clusterList {
		clusters[cluster.ID] = cluster
	}

	nodes := make(map[string]*NodeInfo)
	nodeList, err := ListNodes(ctx, etcdCli)
	if err != nil {
		logutil.BgLogger().Error("assign clusters failed", zap.Error(err), zap.String("coordinator", n.id))
		return
	}
	for _, node := range nodeList {
		nodes[node.ID] = node
	}

	assignments, err := GetAssignmentMap(ctx, etcdCli)
	if err != nil {
		logutil.BgLogger().Error("assign clusters failed", zap.Error(err), zap.String("coordinator", n.id))
		return
	}

	minClusterPerNode := len(clusters) / len(nodes)
	maxClusterPerNode := minClusterPerNode
	if len(clusters)%len(nodes) != 0 {
		maxClusterPerNode++
	}

	needDeleteClusters := make([]string, 0)
	orphanClusters := make([]string, 0)
	newNodeAssignmentList := make(map[string][]string)
	for clusterID, nodeID := range assignments {
		if _, ok := clusters[clusterID]; !ok {
			needDeleteClusters = append(needDeleteClusters, clusterID)
			continue
		}

		if _, ok := nodes[nodeID]; !ok {
			orphanClusters = append(orphanClusters, clusterID)
			continue
		}

		if l := newNodeAssignmentList[nodeID]; len(l) < maxClusterPerNode {
			newNodeAssignmentList[nodeID] = append(l, clusterID)
		} else {
			orphanClusters = append(orphanClusters, clusterID)
		}
	}

	for clusterID := range clusters {
		if _, ok := assignments[clusterID]; !ok {
			orphanClusters = append(orphanClusters, clusterID)
		}
	}

	for nodeID := range nodes {
		if _, ok := newNodeAssignmentList[nodeID]; !ok {
			newNodeAssignmentList[nodeID] = nil
		}
	}

	for nodeID, l := range newNodeAssignmentList {
		if len(orphanClusters) == 0 {
			break
		}

		if free := minClusterPerNode - len(l); free > 0 {
			if free >= len(orphanClusters) {
				l = append(l, orphanClusters...)
				orphanClusters = nil
			} else {
				l = append(l, orphanClusters[0:free]...)
				orphanClusters = orphanClusters[free:]
			}
		}
		newNodeAssignmentList[nodeID] = l
	}

	for nodeID, l := range newNodeAssignmentList {
		if len(orphanClusters) == 0 {
			break
		}
		if len(l) < maxClusterPerNode {
			l = append(l, orphanClusters[0])
			orphanClusters = orphanClusters[1:]
		}
		newNodeAssignmentList[nodeID] = l
	}

	for _, clusterID := range needDeleteClusters {
		logutil.BgLogger().Info("delete cluster assignment",
			zap.String("coordinator", n.id),
			zap.String("clusterID", clusterID),
		)
		if err = DeleteAssignment(ctx, etcdCli, clusterID); err != nil {
			logutil.BgLogger().Error(
				"delete assignment failed",
				zap.Error(err),
				zap.String("coordinator", n.id),
				zap.String("clusterID", clusterID),
			)
		}
	}

	for nodeID, l := range newNodeAssignmentList {
		for _, clusterID := range l {
			oldNodeID := assignments[clusterID]
			if oldNodeID == nodeID {
				continue
			}

			logutil.BgLogger().Info("make a new assignment",
				zap.String("clusterID", clusterID),
				zap.String("oldNodeID", oldNodeID),
				zap.String("newNodeID", nodeID),
			)

			if err = SetAssignment(ctx, etcdCli, clusterID, nodeID); err != nil {
				logutil.BgLogger().Error(
					"delete assignment failed",
					zap.Error(err),
					zap.String("coordinator", n.id),
					zap.String("nodeID", nodeID),
					zap.String("clusterID", clusterID),
				)
			}
		}
	}
}

func (n *ServiceNode) registerSelf() bool {
	if err := RegisterNode(n.ctx, n.etcdSession, n.id); err != nil {
		logutil.BgLogger().Error("register ddl service node failed", zap.Error(err), zap.String("nodeID", n.id))
		return false
	}
	return true
}

func (n *ServiceNode) updateNodeStatus() {
	tasks := n.getTasks()
	oldTaskStatuses := n.getTaskStatuses()
	newTaskStatuses := make(map[string]*NodeClusterTaskStatus)
	needUpdate := oldTaskStatuses == nil || len(oldTaskStatuses) != len(tasks)
	for _, task := range tasks {
		status := &NodeClusterTaskStatus{
			ID:       task.ClusterID(),
			DDLOwner: task.IsDDLOwner(),
		}
		newTaskStatuses[status.ID] = status
		if needUpdate {
			continue
		}

		if oldStatus, ok := oldTaskStatuses[status.ID]; ok && *oldStatus == *status {
			continue
		}

		needUpdate = true
	}

	if !needUpdate {
		return
	}

	logutil.BgLogger().Error(
		"update node status",
		zap.String("nodeID", n.id),
	)

	status := &NodeStatus{
		ID:           n.id,
		ClusterTasks: newTaskStatuses,
	}

	if err := UpdateNodeStatus(n.ctx, n.etcdSession, status); err != nil {
		logutil.BgLogger().Error(
			"update node status failed",
			zap.Error(err),
			zap.String("nodeID", n.id),
		)
	} else {
		n.taskStatuses.Store(newTaskStatuses)
	}
}

func (n *ServiceNode) nodeLoop() {
	nodeRegistered := n.registerSelf()
	lastRegisterSelf := time.Now()

	var lastUpdateTasks time.Time
	if nodeRegistered {
		n.updateClusterDDLTasks()
		lastUpdateTasks = time.Now()
	}

	ticker := time.Tick(time.Second)
	watch := n.etcdSession.Client().Watch(n.ctx, clusterAssignKeyPrefix, clientv3.WithPrefix())
loop:
	for {
		select {
		case <-ticker:
			if !nodeRegistered && time.Since(lastRegisterSelf) > time.Second*10 {
				nodeRegistered = n.registerSelf()
			}

			if !nodeRegistered {
				break
			}

			n.updateNodeStatus()
			if time.Since(lastUpdateTasks) > time.Minute {
				n.updateClusterDDLTasks()
				lastUpdateTasks = time.Now()
			}
		case <-watch:
			if !nodeRegistered {
				break
			}

			n.updateClusterDDLTasks()
			lastUpdateTasks = time.Now()
		case <-n.ctx.Done():
			break loop
		case <-n.etcdSession.Done():
			n.cancel()
			break loop
		}
	}

	if err := n.etcdSession.Close(); err != nil {
		logutil.BgLogger().Error("close session failed", zap.Error(err), zap.String("nodeID", n.id))
	}

	for _, task := range n.getTasks() {
		task.Stop()
	}
}
