// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/meta"
)

func onCreateRPSGame(d *ddlCtx, t *meta.Meta, job *model.Job) (ver int64, _ error) {
	gameInfo := &model.RPSGameInfo{}
	if err := job.DecodeArgs(gameInfo); err != nil {
		// Invalid arguments, cancel this job.
		job.State = model.JobStateCancelled
		return ver, errors.Trace(err)
	}

	gameInfo.State = model.StateNone
	if err := checkRPSGameNotExists(d, t, gameInfo); err != nil {
		if infoschema.ErrGameExists.Equal(err) {
			// The game already exists, can't create it, we should cancel this job now.
			job.State = model.JobStateCancelled
		}
		return ver, errors.Trace(err)
	}

	ver, err := updateSchemaVersion(t, job)
	if err != nil {
		return ver, errors.Trace(err)
	}

	switch gameInfo.State {
	case model.StateNone:
		// none -> public
		gameInfo.State = model.StatePublic
		if err = t.CreateRPSGame(gameInfo); err != nil {
			return ver, errors.Trace(err)
		}

		// Finish this job.
		job.State = model.JobStateDone
		job.SchemaState = gameInfo.State
		return ver, nil
	default:
		// We can't enter here.
		return ver, errors.Errorf("invalid game state %v", gameInfo.State)
	}
}

func checkRPSGameNotExists(_ *ddlCtx, t *meta.Meta, gameInfo *model.RPSGameInfo) error {
	games, err := t.ListRPSGames()
	if err != nil {
		return errors.Trace(err)
	}

	for _, game := range games {
		if game.Name.L == gameInfo.Name.L {
			return infoschema.ErrGameExists.GenWithStackByArgs(gameInfo.Name)
		}
	}

	return nil
}
