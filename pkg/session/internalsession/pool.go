package internalsession

import (
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/intest"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/pingcap/tidb/pkg/parser/terror"
)

type PoolFactory func() (CloseableSessionContext, error)

type poolOptions struct {
	getSessionManager func() util.SessionManager
}

type PoolOption func(*poolOptions)

func WithGetSessionManager(fn func() util.SessionManager) PoolOption {
	return func(o *poolOptions) {
		o.getSessionManager = fn
	}
}

// Pool is a recyclable resource pool for the session.
type Pool struct {
	capacity  int
	resources chan *session
	poolOptions
	factory PoolFactory
	mu      struct {
		sync.RWMutex
		closed bool
	}
}

// NewPool creates a new session pool with the given capacity and factory function.
func NewPool(capacity int, factory PoolFactory, options ...PoolOption) *Pool {
	var opts poolOptions
	for _, opt := range options {
		opt(&opts)
	}

	if opts.getSessionManager == nil {
		opts.getSessionManager = func() util.SessionManager { return nil }
	}

	return &Pool{
		resources:   make(chan *session, capacity),
		poolOptions: opts,
		factory:     factory,
	}
}

func (p *Pool) getInternal() (*session, error) {
	select {
	case internal, ok := <-p.resources:
		if !ok {
			return nil, errors.New("session pool closed")
		}
		return internal, nil
	default:
		resource, err := p.factory()
		if err != nil {
			return nil, err
		}
		return newInternalSession(resource), nil
	}
}

// Get gets a session from the session pool.
func (p *Pool) Get() (*Session, error) {
	internal, err := p.getInternal()
	if err != nil {
		return nil, err
	}

	success := false
	defer func() {
		if !success {
			internal.Destroy(nil, true)
		}
	}()

	if err = internal.RollbackDirty(nil); err != nil {
		return nil, err
	}

	se, err := internal.Attach()
	if err != nil {
		return nil, err
	}

	if manager := p.getSessionManager(); manager != nil {
		defer func() {
			if !success {
				manager.DeleteInternalSession(se)
			}
		}()
		manager.StoreInternalSession(se)
	}
	success = true
	return se, err
}

// Put puts the session back to the pool.
func (p *Pool) Put(se *Session) {
	internal := se.internal
	success, detached := false, false
	defer func() {
		intest.AssertFunc(internal.Detached)
		intest.Assert(success)
		if !success {
			internal.Destroy(se, detached)
		}
	}()

	if manager := p.getSessionManager(); manager != nil {
		manager.DeleteInternalSession(se)
	}

	if err := internal.Detach(se); err != nil {
		terror.Log(err)
		internal.Destroy(se, false)
		return
	}
	detached = true

	if err := internal.RollbackDirty(nil); err != nil {
		terror.Log(err)
		return
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.mu.closed {
		return
	}

	select {
	case p.resources <- internal:
		success = true
	default:
	}
}

// Close closes the pool to release all resources.
func (p *Pool) Close() {
	p.mu.Lock()
	if p.mu.closed {
		p.mu.Unlock()
		return
	}
	p.mu.closed = true
	close(p.resources)
	p.mu.Unlock()

	for r := range p.resources {
		r.Destroy(nil, true)
	}
}
