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
	"sync"
	"time"

	"github.com/pingcap/errors"
)

type MockMetaStore struct {
	mu             sync.Mutex
	nodes          map[string]*MockWorkerNodeSession
	clusters       map[string]*ClusterInfo
	assignments    map[string]string
	leaderSession  *MockWorkerNodeSession
	nodeStatusesCh chan struct{}
	clustersCh     chan struct{}
	assignmentsCh  chan struct{}
}

func NewMockMetaStore() *MockMetaStore {
	return &MockMetaStore{
		nodes:          make(map[string]*MockWorkerNodeSession),
		clusters:       make(map[string]*ClusterInfo),
		assignments:    make(map[string]string),
		nodeStatusesCh: make(chan struct{}),
		clustersCh:     make(chan struct{}),
		assignmentsCh:  make(chan struct{}),
	}
}

func (s *MockMetaStore) GetServiceLeaderID(_ context.Context) (string, error) {
	if s.leaderSession != nil {
		return s.leaderSession.nodeID, nil
	}
	return "", nil
}

func (s *MockMetaStore) CreateWorkerNodeSession(_ context.Context, nodeID string, _ int64) (WorkerNodeSession, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if nodeID == "" {
		return nil, errors.New("nodeID is empty")
	}

	se := &MockWorkerNodeSession{
		nodeID: nodeID,
		store:  s,
		status: &NodeStatus{
			ID: nodeID,
		},
	}

	se.electLeader = func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.leaderSession == se {
			return true
		}

		if s.leaderSession == nil {
			s.leaderSession = se
			return true
		}

		return false
	}

	se.close = func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.leaderSession == se {
			s.leaderSession = nil
		}
	}

	s.nodes[se.nodeID] = se
	select {
	case s.nodeStatusesCh <- struct{}{}:
	default:
	}
	return se, nil
}

func (s *MockMetaStore) GetAllWorkNodeStatuses(_ context.Context) (map[string]*NodeStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	statuses := make(map[string]*NodeStatus)
	for _, session := range s.nodes {
		obj, err := deepCopy(session.status)
		if err != nil {
			return nil, err
		}
		statuses[obj.ID] = obj
	}

	return statuses, nil
}

func (s *MockMetaStore) WatchNodeStatuses(_ context.Context) <-chan struct{} {
	return s.nodeStatusesCh
}

func (s *MockMetaStore) DelegateClusterDDL(_ context.Context, cluster *ClusterInfo) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if cluster.ID == "" {
		return errors.New("cluster id is empty")
	}

	obj, err := deepCopy(cluster)
	if err != nil {
		return err
	}
	s.clusters[cluster.ID] = obj
	select {
	case s.clustersCh <- struct{}{}:
	default:
	}
	return nil
}

func (s *MockMetaStore) RemoveClusterDDLDelegation(_ context.Context, clusterID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if clusterID == "" {
		return errors.New("clusterID is empty")
	}
	delete(s.clusters, clusterID)
	select {
	case s.clustersCh <- struct{}{}:
	default:
	}
	return nil
}

func (s *MockMetaStore) GetClusterInfoList(_ context.Context) ([]*ClusterInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ret := make([]*ClusterInfo, 0, len(s.clusters))
	for _, info := range s.clusters {
		obj, err := deepCopy(info)
		if err != nil {
			return nil, err
		}
		ret = append(ret, obj)
	}
	return ret, nil
}

func (s *MockMetaStore) GetClusterInfo(_ context.Context, clusterID string) (*ClusterInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if clusterID == "" {
		return nil, errors.New("clusterID is empty")
	}

	if info, ok := s.clusters[clusterID]; ok {
		return deepCopy(info)
	}
	return nil, nil
}

func (s *MockMetaStore) WatchClusters(_ context.Context) <-chan struct{} {
	return s.clustersCh
}

func (s *MockMetaStore) GetAllDDLOwnerAssignments(_ context.Context) (map[string]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return deepCopy(s.assignments)
}

func (s *MockMetaStore) DeleteDDLOwnerAssignment(_ context.Context, clusterID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if clusterID == "" {
		return errors.New("clusterID is empty")
	}
	delete(s.assignments, clusterID)
	select {
	case s.assignmentsCh <- struct{}{}:
	default:
	}
	return nil
}

func (s *MockMetaStore) SetDDLOwnerAssignment(_ context.Context, clusterID string, nodeID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if clusterID == "" {
		return errors.New("clusterID is empty")
	}
	s.assignments[clusterID] = nodeID
	select {
	case s.assignmentsCh <- struct{}{}:
	default:
	}
	return nil
}

func (s *MockMetaStore) WatchDDLOwnerAssignments(_ context.Context) <-chan struct{} {
	return s.assignmentsCh
}

type MockWorkerNodeSession struct {
	mu          sync.Mutex
	nodeID      string
	store       *MockMetaStore
	status      *NodeStatus
	electLeader func() bool
	close       func()
	leaderCh    chan struct{}
	doneCh      chan struct{}
	closed      bool
}

func (s *MockWorkerNodeSession) MetaStore() MetaStore {
	return s.store
}

func (s *MockWorkerNodeSession) Done() <-chan struct{} {
	return s.doneCh
}

func (s *MockWorkerNodeSession) UpdateWorkerNodeStatus(_ context.Context, status *NodeStatus) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return errors.New("closed")
	}

	if status.ID != s.nodeID {
		return errors.Errorf("node id is different(%s != %s)", status.ID, s.nodeID)
	}

	obj, err := deepCopy(status)
	if err != nil {
		return err
	}

	s.status = obj
	return nil
}

func (s *MockWorkerNodeSession) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.close()
	close(s.leaderCh)
	close(s.doneCh)
	s.leaderCh = nil
	s.store.nodeStatusesCh <- struct{}{}
	return nil
}

func (s *MockWorkerNodeSession) ElectLeader(ctx context.Context) (<-chan struct{}, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, errors.New("destroyed")
	}

	if s.leaderCh != nil {
		return s.leaderCh, nil
	}

	ticker := time.NewTicker(time.Second)
	for !s.electLeader() {
		select {
		case <-s.Done():
			return nil, errors.New("node session is closed")
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			break
		}
	}

	s.leaderCh = make(chan struct{})
	return s.leaderCh, nil
}

func deepCopy[T any](o T) (T, error) {
	var ret T
	bs, err := json.Marshal(o)
	if err != nil {
		return ret, nil
	}

	if err = json.Unmarshal(bs, &ret); err != nil {
		return ret, nil
	}
	return ret, nil
}
