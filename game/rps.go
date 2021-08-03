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

package game

import (
	"math/rand"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
)

// RPSGameRoundResult is the round result of the RPS Game
type RPSGameRoundResult int

const (
	// RPSRoundResultDraw mean the round result is a draw
	RPSRoundResultDraw RPSGameRoundResult = iota
	// RPSRoundResultWin mean you win this round
	RPSRoundResultWin
	// RPSRoundResultLose mean you lose this round
	RPSRoundResultLose
)

const (
	// RPSFinalResultNone mean the final result is unknown yet
	RPSFinalResultNone RPSGameFinalResult = iota
	// RPSFinalResultWin mean you win this game
	RPSFinalResultWin
	// RPSFinalResultLose mean you lose this game
	RPSFinalResultLose
	// RPSFinalResultAbort mean you abort this game
	RPSFinalResultAbort
)

// RPSGameFinalResult is the final result of the RPS Game
type RPSGameFinalResult int

func (r RPSGameFinalResult) String() string {
	switch r {
	case RPSFinalResultNone:
		return "N/A"
	case RPSFinalResultWin:
		return "Win"
	case RPSFinalResultLose:
		return "Lose"
	case RPSFinalResultAbort:
		return "Abort"
	}

	return ""
}

// RPSGameStatus provides status info of RPS Game
type RPSGameStatus struct {
	FinalResult  RPSGameFinalResult
	TotalRound   int
	CurrentRound int
	TotalWin     int
	TotalLose    int
}

// RPSGame provides RPS Game info
type RPSGame struct {
	meta         *model.RPSGameInfo
	finalResult  RPSGameFinalResult
	totalRound   int
	currentRound int
	totalWin     int
	totalLose    int
}

// NewRPSGame creates a new RPSGame
func NewRPSGame(meta *model.RPSGameInfo) *RPSGame {
	return &RPSGame{
		meta:         meta,
		finalResult:  RPSFinalResultNone,
		totalRound:   3,
		currentRound: 1,
		totalWin:     0,
		totalLose:    0,
	}
}

// Meta returns RPS Game instance
func (r *RPSGame) Meta() *model.RPSGameInfo {
	return r.meta
}

// Status return the status of RPS Game
func (r *RPSGame) Status() *RPSGameStatus {
	return &RPSGameStatus{
		FinalResult:  r.finalResult,
		TotalRound:   r.totalRound,
		CurrentRound: r.currentRound,
		TotalWin:     r.totalWin,
		TotalLose:    r.totalLose,
	}
}

var winMap = [][]RPSGameRoundResult{
	ast.RPSGameActionShowRock: {
		ast.RPSGameActionShowRock:     RPSRoundResultDraw,
		ast.RPSGameActionShowPaper:    RPSRoundResultLose,
		ast.RPSGameActionShowScissors: RPSRoundResultWin,
	},
	ast.RPSGameActionShowPaper: {
		ast.RPSGameActionShowRock:     RPSRoundResultWin,
		ast.RPSGameActionShowPaper:    RPSRoundResultDraw,
		ast.RPSGameActionShowScissors: RPSRoundResultLose,
	},
	ast.RPSGameActionShowScissors: {
		ast.RPSGameActionShowRock:     RPSRoundResultLose,
		ast.RPSGameActionShowPaper:    RPSRoundResultWin,
		ast.RPSGameActionShowScissors: RPSRoundResultDraw,
	},
}

// DoAction do an action for RPSGame
func (r *RPSGame) DoAction(action ast.RPSGameAction) (ast.RPSGameAction, RPSGameRoundResult, error) {
	if int(action) >= len(winMap) {
		return 0, 0, errors.New("Invalid rps game action")
	}

	if r.finalResult != RPSFinalResultNone {
		return 0, 0, ErrGameNotActive.GenWithStackByArgs(r.meta.Name)
	}

	computerShow := r.computerShow()
	result := winMap[action][computerShow]
	switch result {
	case RPSRoundResultWin:
		r.currentRound += 1
		r.totalWin += 1
	case RPSRoundResultLose:
		r.currentRound += 1
		r.totalLose += 1
	}

	if r.totalWin > r.totalRound/2 {
		r.finalResult = RPSFinalResultWin
		return computerShow, result, nil
	}

	if r.totalLose > r.totalRound/2 {
		r.finalResult = RPSFinalResultLose
		return computerShow, result, nil
	}

	return computerShow, result, nil
}

func (r *RPSGame) computerShow() ast.RPSGameAction {
	switch rand.Int() % 3 {
	case 0:
		return ast.RPSGameActionShowRock
	case 1:
		return ast.RPSGameActionShowPaper
	default:
		return ast.RPSGameActionShowScissors
	}
}

// GetRPSActionName returns the name for ast.RPSGameAction
func GetRPSActionName(action ast.RPSGameAction) (string, error) {
	switch action {
	case ast.RPSGameActionShowRock:
		return "Rock", nil
	case ast.RPSGameActionShowPaper:
		return "Paper", nil
	case ast.RPSGameActionShowScissors:
		return "Scissors", nil
	default:
		return "", errors.New("Invalid ast.RPSGameAction")
	}
}
