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
	config   config.Config
	pool     conn.Pool
	balancer balancer.Balancer

	conns     []conn.Conn
	index     map[string]entry.Entry
	endpoints map[uint32]conn.Conn // only one endpoint by node ID

	m    sync.Mutex
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
	close(c.done)

	onDone := trace.DriverOnClusterClose(c.config.Trace(), &ctx)
	defer func() {
		onDone(err)
	}()

	c.m.Lock()
	defer c.m.Unlock()

	var issues []error

	for _, conn := range c.conns {
		if err := conn.Release(ctx); err != nil {
			issues = append(issues, err)
		}
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
			cc = c.config.Balancer().Next(ctx)
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
	if c.isClosed() {
		return nil, xerrors.WithStackTrace(ErrClusterClosed)
	}

	var cancel context.CancelFunc
	// without client context deadline lock limited on MaxGetConnTimeout
	// cluster endpoints cannot be updated at this time
	ctx, cancel = context.WithTimeout(ctx, MaxGetConnTimeout)
	defer cancel()

	onDone := trace.DriverOnClusterGet(c.config.Trace(), &ctx)
	defer func() {
		if err != nil {
			onDone(nil, err)
		} else {
			onDone(cc.Endpoint().Copy(), nil)
		}
	}()

	return c.get(ctx)
}
