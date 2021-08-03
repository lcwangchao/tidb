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

package executor

import (
	"context"
	"fmt"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/game"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/util/chunk"
)

// ActionRPSGameExec executes RPG Game actions
type ActionRPSGameExec struct {
	baseExecutor
	gameInfo *model.RPSGameInfo
	action   ast.RPSGameAction
}

// Next implements the Executor interface.
func (e *ActionRPSGameExec) Next(_ context.Context, _ *chunk.Chunk) error {
	g, err := e.ctx.GetInfoSchema().(infoschema.InfoSchema).RPSGameByName(e.gameInfo.Name)
	if err != nil {
		return err
	}

	currentRound := g.Status().CurrentRound
	computerShow, roundResult, err := g.DoAction(e.action)
	if err != nil {
		return err
	}

	youShowText, err := game.GetRPSActionName(e.action)
	if err != nil {
		return err
	}

	computerShowText, err := game.GetRPSActionName(computerShow)
	if err != nil {
		return err
	}

	resultText := ""
	switch roundResult {
	case game.RPSRoundResultWin:
		resultText = "You WIN for this round."
	case game.RPSRoundResultLose:
		resultText = "You LOSE for this round."
	case game.RPSRoundResultDraw:
		resultText = "It's a draw, please try again."
	}

	finalResult := g.Status().FinalResult
	switch finalResult {
	case game.RPSFinalResultWin:
		resultText = fmt.Sprintf("%s You are the final WINNER!", resultText)
	case game.RPSFinalResultLose:
		resultText = fmt.Sprintf("%s Sorry computer is the final winner.", resultText)
	}

	stmtCtx := e.ctx.GetSessionVars().StmtCtx
	stmtCtx.SetMessage(fmt.Sprintf("%s Round: %d, %s VS %s", resultText, currentRound, youShowText, computerShowText))
	return nil
}
