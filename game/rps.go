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
)

// RPSGameInfo provides meta data describing a RPS game.
type RPSGameInfo struct {
	Name model.CIStr
}

// RPSGames manages a collection of RPS games.
type RPSGames struct {
	games map[string]*RPSGameInfo
}

// NewRPSGames create a new RPSGames
func NewRPSGames() *RPSGames {
	return &RPSGames{
		games: make(map[string]*RPSGameInfo),
	}
}

// GameByName get the RPSGameInfo by name
func (s *RPSGames) GameByName(name model.CIStr) (*RPSGameInfo, error) {
	if game, exist := s.games[name.L]; exist {
		return game, nil
	}

	return nil, ErrGameNotExists.GenWithStackByArgs(name)
}

// AddGame add a new game
func (s *RPSGames) AddGame(game *RPSGameInfo) error {
	if _, exist := s.games[game.Name.L]; exist {
		return ErrGameExists.GenWithStackByArgs(game.Name)
	}

	s.games[game.Name.L] = game
	return nil
}
