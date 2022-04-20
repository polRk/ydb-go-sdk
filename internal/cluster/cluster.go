package cluster

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/ctxbalancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer/multi"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/cluster/entry"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/repeater"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

const (
	MaxGetConnTimeout = 10 * time.Second
)

var (
	// ErrClusterClosed returned when requested on a closed cluster.
	ErrClusterClosed = xerrors.Wrap(fmt.Errorf("cluster closed"))

	// ErrClusterEmpty returned when no connections left in cluster.
	ErrClusterEmpty = xerrors.Wrap(fmt.Errorf("cluster empty"))
)

type Cluster struct {
	sync.RWMutex

	config   config.Config
	pool     conn.Pool
	balancer balancer.Balancer

	index     map[string]entry.Entry
	endpoints map[uint32]conn.Conn // only one endpoint by node ID

	done chan struct{}
}

func (c *Cluster) isClosed() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

// Pessimize connection in underling pool
func (c *Cluster) Pessimize(ctx context.Context, cc conn.Conn, cause error) {
	c.pool.Pessimize(ctx, cc, cause)
}

type crudOptionsHolder struct {
	withLock bool
}

type CrudOption func(h *crudOptionsHolder)

func WithoutLock() CrudOption {
	return func(h *crudOptionsHolder) {
		h.withLock = false
	}
}

func parseOptions(opts ...CrudOption) *crudOptionsHolder {
	h := &crudOptionsHolder{
		withLock: true,
	}
	for _, o := range opts {
		o(h)
	}
	return h
}

type Getter interface {
	// Get returns next available connection.
	// It returns error on given deadline cancellation or when cluster become closed.
	Get(ctx context.Context) (cc conn.Conn, err error)
}

type Inserter interface {
	// Insert inserts endpoint to cluster
	Insert(ctx context.Context, endpoint endpoint.Endpoint, opts ...CrudOption)
}

type Remover interface {
	// Remove removes endpoint from cluster
	Remove(ctx context.Context, endpoint endpoint.Endpoint, opts ...CrudOption)
}

type Explorer interface {
	SetExplorer(repeater repeater.Repeater)
	Force()
}

func New(
	ctx context.Context,
	config config.Config,
	pool conn.Pool,
	endpoints []endpoint.Endpoint,
) *Cluster {
	onDone := trace.DriverOnClusterInit(config.Trace(), &ctx)
	defer func() {
		onDone(pool.Take(ctx))
	}()

	conns := make([]conn.Conn, 0, len(endpoints))
	for _, endpoint := range endpoints {
		conns = append(conns, pool.Get(endpoint))
	}

	clusterBalancer := multi.Balancer(
		// check conn from context at first place
		multi.WithBalancer(ctxbalancer.Balancer(conns), func(cc conn.Conn) bool {
			return true
		}),

		// then use common balancers from config
		multi.WithBalancer(config.Balancer().Create(conns), func(cc conn.Conn) bool {
			return true
		}),
	)

	return &Cluster{
		done:      make(chan struct{}),
		config:    config,
		index:     make(map[string]entry.Entry),
		endpoints: make(map[uint32]conn.Conn),
		pool:      pool,
		balancer:  clusterBalancer,
	}
}

func (c *Cluster) Close(ctx context.Context) (err error) {
	defer close(c.done)

	onDone := trace.DriverOnClusterClose(c.config.Trace(), &ctx)
	defer func() {
		onDone(err)
	}()

	c.RWMutex.Lock()
	defer c.RWMutex.Unlock()

	var issues []error
	if len(c.index) > 0 {
		issues = append(issues, fmt.Errorf(
			"non empty index after remove all entries: %v",
			func() (endpoints []string) {
				for e := range c.index {
					endpoints = append(endpoints, e)
				}
				return endpoints
			}(),
		))
	}

	if len(c.endpoints) > 0 {
		issues = append(issues, fmt.Errorf(
			"non empty nodes after remove all entries: %v",
			func() (nodes []uint32) {
				for e := range c.endpoints {
					nodes = append(nodes, e)
				}
				return nodes
			}(),
		))
	}

	if err = c.pool.Release(ctx); err != nil {
		issues = append(issues, err)
	}

	if len(issues) > 0 {
		return xerrors.WithStackTrace(xerrors.NewWithIssues("cluster closed with issues", issues...))
	}

	return nil
}

func (c *Cluster) get(ctx context.Context) (cc conn.Conn, err error) {
	for {
		select {
		case <-c.done:
			return nil, xerrors.WithStackTrace(ErrClusterClosed)
		case <-ctx.Done():
			return nil, xerrors.WithStackTrace(ctx.Err())
		default:
			cc = c.config.Balancer().Next(nil)
			if cc == nil {
				return nil, xerrors.WithStackTrace(ErrClusterEmpty)
			}
			if err = cc.Ping(ctx); err == nil {
				return cc, nil
			}
		}
	}
}

// Get returns next available connection.
// It returns error on given deadline cancellation or when cluster become closed.
func (c *Cluster) Get(ctx context.Context) (cc conn.Conn, err error) {
	var cancel context.CancelFunc
	// without client context deadline lock limited on MaxGetConnTimeout
	// cluster endpoints cannot be updated at this time
	ctx, cancel = context.WithTimeout(ctx, MaxGetConnTimeout)
	defer cancel()

	if c.isClosed() {
		return nil, xerrors.WithStackTrace(ErrClusterClosed)
	}

	onDone := trace.DriverOnClusterGet(c.config.Trace(), &ctx)
	defer func() {
		if err != nil {
			onDone(nil, err)
		} else {
			onDone(cc.Endpoint().Copy(), nil)
		}
	}()

	// wait lock for read during Get
	c.RWMutex.RLock()
	defer c.RWMutex.RUnlock()

	if e, ok := ctxbalancer.ContextEndpoint(ctx); ok {
		cc, ok = c.endpoints[e.NodeID()]
		if ok && cc.IsState(
			conn.Created,
			conn.Online,
			conn.Offline,
		) {
			if err = cc.Ping(ctx); err == nil {
				return cc, nil
			}
		}
	}

	return c.get(ctx)
}
