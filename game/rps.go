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
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/sessionctx"
)

// RPSGameInfo provides meta data describing a RPS game.
type RPSGameInfo struct {
	Name model.CIStr
}

const (
	// RPSFinalResultNone mean the final result is unknown yet
	RPSFinalResultNone = iota
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
	meta         *RPSGameInfo
	finalResult  RPSGameFinalResult
	totalRound   int
	currentRound int
	totalWin     int
	totalLose    int
}

// NewRPSGame creates a new RPSGame
func NewRPSGame(meta *RPSGameInfo) *RPSGame {
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
func (r *RPSGame) Meta() *RPSGameInfo {
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

// RPSGames manages a collection of RPS games.
type RPSGames struct {
	games map[string]*RPSGame
}

func newRPSGames() *RPSGames {
	return &RPSGames{
		games: make(map[string]*RPSGame),
	}
}

// GetRPSGames from session
func GetRPSGames(ctx sessionctx.Context) *RPSGames {
	sessionVars := ctx.GetSessionVars()
	if sessionVars.RPSGames == nil {
		sessionVars.RPSGames = newRPSGames()
	}

	return sessionVars.RPSGames.(*RPSGames)
}

// GameByName get the RPSGameInfo by name
func (s *RPSGames) GameByName(name model.CIStr) (*RPSGame, error) {
	if game, exist := s.games[name.L]; exist {
		return game, nil
	}

	return nil, ErrGameNotExists.GenWithStackByArgs(name)
}

// AddGame add a new game
func (s *RPSGames) AddGame(game *RPSGame) error {
	meta := game.Meta()
	if _, exist := s.games[meta.Name.L]; exist {
		return ErrGameExists.GenWithStackByArgs(meta.Name)
	}

	s.games[meta.Name.L] = game
	return nil
}

// AllGames return all RPS Games
func (s *RPSGames) AllGames() []*RPSGame {
	games := make([]*RPSGame, 0, len(s.games))
	for _, g := range s.games {
		games = append(games, g)
	}
	return games
}
