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

package game_test

import (
	"testing"

	"github.com/pingcap/tidb/game"
	"github.com/pingcap/tidb/util/testkit"

	. "github.com/pingcap/check"
	"github.com/pingcap/parser"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/tikv/client-go/v2/testutils"
)

func TestRPSGame(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct {
	cluster testutils.Cluster
	store   kv.Storage
	domain  *domain.Domain
	*parser.Parser
	ctx *mock.Context
}

func (s *testSuite) SetUpSuite(c *C) {
	testleak.BeforeTest()

	store, err := mockstore.NewMockStore(
		mockstore.WithClusterInspector(func(c testutils.Cluster) {
			mockstore.BootstrapWithSingleStore(c)
		}),
	)
	c.Assert(err, IsNil)
	s.store = store
	session.DisableStats4Test()

	s.domain, err = session.BootstrapSession(s.store)
	c.Assert(err, IsNil)
}

func (s *testSuite) TearDownSuite(c *C) {
	s.domain.Close()
	err := s.store.Close()
	c.Assert(err, IsNil)
}

func (s *testSuite) TestCreateAndShowCreate(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("CREATE RPS GAME game1")
	tk.MustQuery("SHOW CREATE RPS GAME game1").Check(testkit.Rows("game1 CREATE RPS GAME `game1`"))

	err := tk.QueryToErr("SHOW CREATE RPS GAME game2")
	c.Assert(game.ErrGameNotExists.Equal(err), IsTrue)
}
