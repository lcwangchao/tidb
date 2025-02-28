package internalsession

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/session/txninfo"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
)

type SessionContext interface {
	sessionctx.Context
	TxnInfo() *txninfo.TxnInfo
}

type CloseableSessionContext interface {
	SessionContext
	Close()
}

type session struct {
	mu   sync.Mutex
	sctx SessionContext
	// owner the current owner of the session, in most times, only the owner the session can use the session.
	owner *Session
	// inuse is the number of goroutines that are using the session.
	inuse uint64
	// inuseUnsafe is the number of goroutines that are using the session in an un-thread-safe way.
	inuseUnsafe uint64
	// destroyPending indicates whether the session is pending to be destroyed.
	// If `Destroy` is called but the session is still inuse,
	//	it will postpone the destroy operation when `inuse` becomes 0 (the last finished goroutine will do it).
	destroyPending bool
}

func newInternalSession(sctx SessionContext) *session {
	return &session{sctx: sctx}
}

func NewSession(sctx SessionContext) *Session {
	intest.AssertNotNil(sctx, "resource should not be nil")
	internal := newInternalSession(sctx)
	internal.owner = &Session{SessionContext: sctx, internal: internal}
	return internal.owner
}

func (s *session) Attach() (se *Session, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.owner != nil {
		return nil, errors.New("session has already been attached to an owner")
	}
	s.owner = &Session{SessionContext: s.sctx, internal: s}
	return s.owner, nil
}

func (s *session) Detached() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.owner == nil
}

func (s *session) Detach(owner *Session) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOwner(owner); err != nil {
		return err
	}

	if s.inuse > 0 {
		return errors.New("session is still in use")
	}

	s.owner = nil
	return nil
}

func (s *session) Destroy(owner *Session, skipOwnerCheck bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !skipOwnerCheck && owner != s.owner {
		return
	}

	if s.inuse > 0 {
		s.destroyPending = true
		return
	}

	s.doDestroy()
}

func (s *session) RollbackDirty(owner *Session) error {
	return s.WithInuse(owner, false, func(sctx SessionContext) error {
		sctx.RollbackTxn(context.Background())
		return nil
	})
}

func (s *session) enterInuse(owner *Session, threadSafe bool) (SessionContext, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkOwner(owner); err != nil {
		return nil, err
	}

	if s.sctx == nil || s.destroyPending {
		return nil, errors.New("session is destroyed or pending destroyed")
	}

	s.inuse++
	if !threadSafe {
		if s.inuseUnsafe > 0 {
			return nil, errors.New("session is being used by another un-thread-safe operation")
		}
		s.inuseUnsafe++
	}

	return s.sctx, nil
}

func (s *session) exitInuse(owner *Session, threadSafe bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	intest.Assert(s.owner == owner)
	s.inuse--
	if !threadSafe {
		s.inuseUnsafe--
	}

	if s.destroyPending && s.inuse == 0 {
		s.doDestroy()
	}
}

func (s *session) WithInuse(owner *Session, threadSafe bool, fn func(SessionContext) error) (err error) {
	sctx, err := s.enterInuse(owner, threadSafe)
	if err != nil {
		return err
	}
	defer s.exitInuse(owner, threadSafe)
	return fn(sctx)
}

func (s *session) checkOwner(owner *Session) error {
	if s.owner != owner {
		return errors.New("session does not belongs to the specified owner")
	}
	return nil
}

func (s *session) doDestroy() {
	if s.sctx == nil {
		return
	}

	defer func() {
		s.sctx = nil
		s.destroyPending = false
		s.owner = nil
	}()

	if c, ok := s.sctx.(CloseableSessionContext); ok {
		c.Close()
	}
}

type Session struct {
	SessionContext
	internal *session
}

func (s *Session) Destroy() {
	s.internal.Destroy(s, false)
}

func (s *Session) RollbackDirty() error {
	return s.internal.RollbackDirty(s)
}

func withInuse1[T any](s *Session, threadSafe bool, fn func(SessionContext) (T, error)) (r T, _ error) {
	err := s.internal.WithInuse(s, threadSafe, func(sctx SessionContext) (err error) {
		r, err = fn(sctx)
		return
	})
	return r, err
}

func withInuse2[T1 any, T2 any](s *Session, threadSafe bool, fn func(SessionContext) (T1, T2, error)) (r1 T1, r2 T2, _ error) {
	err := s.internal.WithInuse(s, threadSafe, func(sctx SessionContext) (err error) {
		r1, r2, err = fn(sctx)
		return
	})
	return r1, r2, err
}

func (s *Session) WithSessionVars(fn func(vars *variable.SessionVars) error) error {
	return s.internal.WithInuse(s, false, func(sctx SessionContext) error {
		return fn(sctx.GetSessionVars())
	})
}

func (s *Session) ParseWithParams(ctx context.Context, sql string, args ...any) (ast.StmtNode, error) {
	return withInuse1(s, false, func(sctx SessionContext) (ast.StmtNode, error) {
		return sctx.GetRestrictedSQLExecutor().ParseWithParams(ctx, sql, args...)
	})
}

func (s *Session) ExecRestrictedStmt(ctx context.Context, stmt ast.StmtNode, opts ...sqlexec.OptionFuncAlias) ([]chunk.Row, []*resolve.ResultField, error) {
	return withInuse2(s, false, func(sctx SessionContext) ([]chunk.Row, []*resolve.ResultField, error) {
		return sctx.GetRestrictedSQLExecutor().ExecRestrictedStmt(ctx, stmt, opts...)
	})
}

func (s *Session) ExecRestrictedSQL(ctx context.Context, opts []sqlexec.OptionFuncAlias, sql string, args ...any) ([]chunk.Row, []*resolve.ResultField, error) {
	return withInuse2(s, false, func(sctx SessionContext) ([]chunk.Row, []*resolve.ResultField, error) {
		return sctx.GetRestrictedSQLExecutor().ExecRestrictedSQL(ctx, opts, sql, args...)
	})
}

// GetRestrictedSQLExecutor returns the sqlexec.RestrictedSQLExecutor.
func (s *Session) GetRestrictedSQLExecutor() sqlexec.RestrictedSQLExecutor {
	return s
}

func (s *Session) Execute(ctx context.Context, sql string) ([]sqlexec.RecordSet, error) {
	return withInuse1(s, false, func(sctx SessionContext) ([]sqlexec.RecordSet, error) {
		return sctx.GetSQLExecutor().Execute(ctx, sql)
	})
}

func (s *Session) ExecuteInternal(ctx context.Context, sql string, args ...any) (sqlexec.RecordSet, error) {
	return withInuse1(s, false, func(sctx SessionContext) (sqlexec.RecordSet, error) {
		return sctx.GetSQLExecutor().ExecuteInternal(ctx, sql, args...)
	})
}

func (s *Session) ExecuteStmt(ctx context.Context, stmtNode ast.StmtNode) (sqlexec.RecordSet, error) {
	return withInuse1(s, false, func(sctx SessionContext) (sqlexec.RecordSet, error) {
		return sctx.GetSQLExecutor().ExecuteStmt(ctx, stmtNode)
	})
}

func (s *Session) GetSQLExecutor() sqlexec.SQLExecutor {
	return s
}

func (s *Session) TxnInfo() *txninfo.TxnInfo {
	info, err := withInuse1(s, true, func(sctx SessionContext) (*txninfo.TxnInfo, error) {
		return sctx.TxnInfo(), nil
	})

	if err != nil {
		info = nil
	}
	return info
}
