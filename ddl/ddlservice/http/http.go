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

package http

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pingcap/tidb/ddl/ddlservice"
	"github.com/pingcap/tidb/domain"
)

type httpError struct {
	statusCode int
	msg        string
}

func (e *httpError) StatusCode() int {
	return e.statusCode
}

func (e *httpError) Error() string {
	return e.msg
}

func BadRequest(msg string) *httpError {
	return &httpError{
		statusCode: http.StatusBadRequest,
		msg:        msg,
	}
}

type httpContext struct {
	context.Context
	ddlservice.MetaStore
	ddlservice.WorkerNodeSession

	dom *domain.Domain
	r   *http.Request
	w   http.ResponseWriter
}

func (ctx *httpContext) Done() <-chan struct{} {
	return ctx.Context.Done()
}

func (ctx *httpContext) writeError(err error) {
	switch e := err.(type) {
	case *httpError:
		ctx.w.WriteHeader(e.StatusCode())
	default:
		ctx.w.WriteHeader(http.StatusInternalServerError)
	}
	_, err = ctx.w.Write([]byte(err.Error()))
}

func (ctx *httpContext) unmarshalJsonRequest(v any) error {
	data, err := ioutil.ReadAll(ctx.r.Body)
	if err != nil {
		return err
	}

	if err = json.Unmarshal(data, v); err != nil {
		return BadRequest(err.Error())
	}

	return nil
}

type AddClusterRequest struct {
	ClusterID string `json:"cluster_id"`
	StoreAddr string `json:"store_addr"`
}

func (r *AddClusterRequest) ClusterInfo() (*ddlservice.ClusterInfo, error) {
	if r.ClusterID == "" {
		return nil, BadRequest("cluster_id is empty")
	}

	if r.StoreAddr == "" {
		return nil, BadRequest("store_addr is empty")
	}

	return &ddlservice.ClusterInfo{
		ID:        r.ClusterID,
		StoreAddr: r.StoreAddr,
	}, nil
}

func RegisterCluster(ctx *httpContext) (any, error) {
	var req AddClusterRequest
	if err := ctx.unmarshalJsonRequest(&req); err != nil {
		return nil, err
	}

	clusterInfo, err := req.ClusterInfo()
	if err != nil {
		return nil, err
	}

	return nil, ctx.DelegateClusterDDL(ctx, clusterInfo)
}

func UnregisterCluster(ctx *httpContext) (any, error) {
	vars := mux.Vars(ctx.r)
	clusterID := vars["clusterID"]
	if clusterID == "" {
		return nil, BadRequest("clusterID is empty")
	}

	return nil, ctx.RemoveClusterDDLDelegation(ctx, clusterID)
}

type ClusterScheduleInfo struct {
	Assigned string `json:"assigned"`
	Located  string `json:"located"`
	DDLOwner bool   `json:"ddl_owner,omitempty"`
}

type ListRegisterClusterItem struct {
	ID        string               `json:"id"`
	StoreAddr string               `json:"store_addr"`
	Schedule  *ClusterScheduleInfo `json:"schedule"`
}

func ListRegisters(ctx *httpContext) (any, error) {
	clusters, err := ctx.GetClusterInfoList(ctx)
	if err != nil {
		return nil, err
	}

	assignments, err := ctx.GetAllDDLOwnerAssignments(ctx)
	if err != nil {
		return nil, err
	}

	statuses, err := ctx.GetAllWorkNodeStatuses(ctx)
	if err != nil {
		return nil, err
	}

	clusterSchedule := make(map[string]*ClusterScheduleInfo)
	for nodeID, status := range statuses {
		for _, task := range status.ClusterTasks {
			clusterSchedule[task.ID] = &ClusterScheduleInfo{
				Located:  nodeID,
				DDLOwner: task.DDLOwner,
			}
		}
	}

	for clusterID, assignment := range assignments {
		if schedule, ok := clusterSchedule[clusterID]; ok {
			schedule.Assigned = assignment
		} else {
			clusterSchedule[clusterID] = &ClusterScheduleInfo{
				Assigned: assignments[clusterID],
			}
		}
	}

	items := make([]*ListRegisterClusterItem, 0, len(clusters))
	for _, cluster := range clusters {
		item := &ListRegisterClusterItem{
			ID:        cluster.ID,
			StoreAddr: cluster.StoreAddr,
			Schedule:  clusterSchedule[cluster.ID],
		}
		items = append(items, item)
	}

	return items, nil
}

type ListServiceNodeItem struct {
	ID          string                   `json:"id"`
	Leader      bool                     `json:"leader,omitempty"`
	Assignments []*ClusterAssignmentInfo `json:"assignments"`
	ToRemove    []string                 `json:"to_remove,omitempty"`
}

type ClusterAssignmentInfo struct {
	ClusterID string `json:"cluster_id"`
	StoreAddr string `json:"store_addr"`
	Scheduled bool   `json:"scheduled"`
	DDLOwner  bool   `json:"ddl_owner"`
}

func ListServiceNodes(ctx *httpContext) (any, error) {
	assignments, err := ctx.GetAllDDLOwnerAssignments(ctx)
	if err != nil {
		return nil, err
	}

	clusters, err := ctx.GetClusterInfoList(ctx)
	if err != nil {
		return nil, err
	}

	nodeStatuses, err := ctx.GetAllWorkNodeStatuses(ctx)
	if err != nil {
		return nil, err
	}

	nodeAssignments := make(map[string][]*ClusterAssignmentInfo)
	for _, cluster := range clusters {
		nodeID, ok := assignments[cluster.ID]
		if !ok {
			continue
		}
		list := append(nodeAssignments[nodeID], &ClusterAssignmentInfo{
			ClusterID: cluster.ID,
			StoreAddr: cluster.StoreAddr,
		})
		nodeAssignments[nodeID] = list
	}

	leaderID, err := ctx.GetServiceLeaderID(ctx)
	if err != nil {
		return nil, err
	}

	items := make([]*ListServiceNodeItem, 0, len(nodeStatuses))
	for _, node := range nodeStatuses {
		item := &ListServiceNodeItem{
			ID:          node.ID,
			Leader:      leaderID == node.ID,
			Assignments: []*ClusterAssignmentInfo{},
		}

		assignmentList := nodeAssignments[node.ID]
		if len(assignmentList) > 0 {
			item.Assignments = assignmentList
		}

		clusterStatuses := make(map[string]*ddlservice.NodeClusterTaskStatus)
		for id, status := range node.ClusterTasks {
			clusterStatuses[id] = status
		}

		for _, assignment := range item.Assignments {
			if clusterStatus, ok := clusterStatuses[assignment.ClusterID]; ok {
				assignment.Scheduled = true
				assignment.DDLOwner = clusterStatus.DDLOwner
				delete(clusterStatuses, assignment.ClusterID)
			}
		}

		for clusterID := range clusterStatuses {
			item.ToRemove = append(item.ToRemove, clusterID)
		}

		items = append(items, item)
	}
	return items, nil
}

func createHandleFunc(router *mux.Router, dom *domain.Domain) func(path string, fn func(*httpContext) (any, error)) *mux.Route {
	node := dom.GetDDLServiceNode()
	if node == nil {
		return nil
	}

	nodeSession := node.GetNodeSession()
	return func(path string, fn func(*httpContext) (any, error)) *mux.Route {
		r := router.PathPrefix("/ddlservice").Subrouter()
		return r.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
			ctx := &httpContext{
				MetaStore:         nodeSession.MetaStore(),
				WorkerNodeSession: nodeSession,
				Context:           r.Context(),
				dom:               dom,
				r:                 r,
				w:                 w,
			}

			entity, err := fn(ctx)
			if err != nil {
				ctx.writeError(err)
				return
			}

			var respBody []byte
			switch ent := entity.(type) {
			case string:
				respBody = []byte(ent)
			case nil:
				break
			default:
				if respBody, err = json.MarshalIndent(ent, "", "  "); err != nil {
					ctx.writeError(err)
					return
				}
				w.Header().Set("Content-Type", "application/json")
			}

			w.WriteHeader(http.StatusOK)
			if len(respBody) > 0 {
				_, _ = w.Write(respBody)
			}
		})
	}
}

func Handle(router *mux.Router, dom *domain.Domain) {
	handleFunc := createHandleFunc(router, dom)
	if handleFunc == nil {
		return
	}

	handleFunc("/registers", RegisterCluster).Methods("POST")
	handleFunc("/registers/{clusterID}", UnregisterCluster).Methods("DELETE")
	handleFunc("/registers", ListRegisters).Methods("GET")
	handleFunc("/nodes", ListServiceNodes).Methods("GET")
}
