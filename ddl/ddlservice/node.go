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
	"go.uber.org/zap"
)

type ClusterDDLTask interface {
	Stop()
	ClusterID() string
	IsDDLOwner() bool
}

type CreateClusterDDLTaskFunc func(*ClusterDDLTaskContext) ClusterDDLTask

type ServiceNode struct {
	ctx          context.Context
	id           string
	store        MetaStore
	nodeSession  WorkerNodeSession
	cancel       func()
	once         sync.Once
	taskFunc     CreateClusterDDLTaskFunc
	tasks        atomic.Value
	taskStatuses atomic.Value
}

func NewServiceNode(store MetaStore, ddlID string, taskFunc CreateClusterDDLTaskFunc) *ServiceNode {
	id := DDLNodeIDPrefix + ddlID
	ctx, cancel := context.WithCancel(context.Background())
	return &ServiceNode{
		ctx:      ctx,
		id:       id,
		store:    store,
		cancel:   cancel,
		taskFunc: taskFunc,
	}
}

func (n *ServiceNode) Start() error {
	nodeSess, err := n.store.CreateWorkerNodeSession(n.ctx, n.id, 0)
	if err != nil {
		return err
	}

	n.nodeSession = nodeSess
	n.once.Do(func() {
		go n.campaignLoop()
		go n.nodeLoop()
	})
	return nil
}

func (n *ServiceNode) Stop() {
	n.cancel()
}

func (n *ServiceNode) GetNodeSession() WorkerNodeSession {
	return n.nodeSession
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
	assignments, err := n.store.GetAllDDLOwnerAssignments(n.ctx)
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
			Context:   n.ctx,
			NodeID:    n.id,
			ClusterID: clusterID,
			Store:     n.store,
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
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-n.nodeSession.Done():
			return
		default:
		}
		n.campaignAndCoordinate()
	}
}

func (n *ServiceNode) campaignAndCoordinate() {
	var wg util.WaitGroupWrapper
	leaderCtx, cancel := context.WithCancel(n.ctx)
	defer func() {
		cancel()
		wg.Wait()
	}()

	ch, err := n.nodeSession.ElectLeader(n.ctx)
	if err != nil {
		logutil.BgLogger().Error("coordinator campaign failed", zap.Error(err), zap.String("nodeID", n.id))
		return
	}

	wg.Add(1)
	go func() {
		defer wg.Add(-1)
		n.coordinateLoop(leaderCtx)
	}()

	for {
		select {
		case <-ch:
			return
		case <-n.nodeSession.Done():
			return
		case <-n.ctx.Done():
			return
		}
	}
}

func (n *ServiceNode) coordinateLoop(ctx context.Context) {
	watchClusters := n.nodeSession.MetaStore().WatchClusters(ctx)
	watchNodes := n.nodeSession.MetaStore().WatchNodeStatuses(ctx)
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

	clusters := make(map[string]*ClusterInfo)
	clusterList, err := n.store.GetClusterInfoList(n.ctx)
	if err != nil {
		logutil.BgLogger().Error("assign clusters failed", zap.Error(err), zap.String("coordinator", n.id))
		return
	}
	for _, cluster := range clusterList {
		clusters[cluster.ID] = cluster
	}

	nodeList, err := n.store.GetAllWorkNodeStatuses(n.ctx)
	if err != nil {
		logutil.BgLogger().Error("assign clusters failed", zap.Error(err), zap.String("coordinator", n.id))
		return
	}

	assignments, err := n.store.GetAllDDLOwnerAssignments(ctx)
	if err != nil {
		logutil.BgLogger().Error("assign clusters failed", zap.Error(err), zap.String("coordinator", n.id))
		return
	}

	minClusterPerNode := len(clusters) / len(nodeList)
	maxClusterPerNode := minClusterPerNode
	if len(clusters)%len(nodeList) != 0 {
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

		if _, ok := nodeList[nodeID]; !ok {
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

	for nodeID := range nodeList {
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
		if err = n.store.DeleteDDLOwnerAssignment(ctx, clusterID); err != nil {
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

			if err = n.store.SetDDLOwnerAssignment(ctx, clusterID, nodeID); err != nil {
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

	logutil.BgLogger().Info(
		"update node status",
		zap.String("nodeID", n.id),
	)

	status := &NodeStatus{
		ID:           n.id,
		ClusterTasks: newTaskStatuses,
	}

	if err := n.nodeSession.UpdateWorkerNodeStatus(n.ctx, status); err != nil {
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
	ticker := time.Tick(time.Minute)
	watch := n.nodeSession.MetaStore().WatchDDLOwnerAssignments(n.ctx)
loop:
	for {
		select {
		case <-ticker:
			n.updateClusterDDLTasks()
		case <-watch:
			n.updateClusterDDLTasks()
		case <-n.ctx.Done():
			break loop
		case <-n.nodeSession.Done():
			n.cancel()
			break loop
		}
	}

	if err := n.nodeSession.Close(); err != nil {
		logutil.BgLogger().Error("close session failed", zap.Error(err), zap.String("nodeID", n.id))
	}

	for _, task := range n.getTasks() {
		task.Stop()
	}
}
