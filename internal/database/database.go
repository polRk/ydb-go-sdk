package database

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/deadline"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/repeater"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/discovery"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/cluster"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
	int_discovery "github.com/ydb-platform/ydb-go-sdk/v3/internal/discovery"
	discoveryConfig "github.com/ydb-platform/ydb-go-sdk/v3/internal/discovery/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/endpoint"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/xerrors"
	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type clusterConnector interface {
	Close(ctx context.Context) error
	Get(ctx context.Context) (cc conn.Conn, err error)
	Remove(ctx context.Context, endpoint endpoint.Endpoint, opts ...cluster.CrudOption)
	Pessimize(ctx context.Context, cc conn.Conn, cause error)

	// For discovery build
	cluster.Inserter
	cluster.Explorer
	sync.Locker
}

type database struct {
	config            config.Config
	discovery         discovery.Client
	discoveryRepeater repeater.Repeater
	connectionPool    conn.Pool
	clusterPointer    atomic.Value
}

func (db *database) cluster() clusterConnector {
	return db.clusterPointer.Load().(clusterConnector)
}

func (db *database) clusterSet(cluster clusterConnector) {
	db.clusterPointer.Store(cluster)
}

func (db *database) clusterDiscovery(ctx context.Context) error {
	endpoints, err := db.discovery.Discover(ctx)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	newCluster := cluster.New(deadline.ContextWithoutDeadline(ctx), db.config, db.connectionPool, endpoints)
	db.clusterPointer.Store(newCluster)
	return nil
}

func (db *database) Discovery() discovery.Client {
	return db.discovery
}

func (db *database) Close(ctx context.Context) (err error) {
	issues := make([]error, 0, 2)

	if db.discoveryRepeater != nil {
		db.discoveryRepeater.Stop()
	}

	if err = db.discovery.Close(ctx); err != nil {
		issues = append(issues, err)
	}

	if err = db.cluster().Close(ctx); err != nil {
		issues = append(issues, err)
	}

	if len(issues) > 0 {
		return xerrors.WithStackTrace(xerrors.NewWithIssues("database close failed", issues...))
	}

	return nil
}

func New(
	ctx context.Context,
	c config.Config,
	pool conn.Pool,
	opts ...discoveryConfig.Option,
) (_ Connection, err error) {
	onDone := trace.DriverOnInit(
		c.Trace(),
		&ctx,
		c.Endpoint(),
		c.Database(),
		c.Secure(),
	)
	defer func() {
		onDone(err)
	}()

	db := &database{
		config:         c,
		connectionPool: pool,
	}

	discoveryEndpoint := endpoint.New(c.Endpoint())
	discoveryConnection := pool.Get(discoveryEndpoint)

	db.discovery = int_discovery.New(
		discoveryConnection,
		opts...,
	)
	if err = db.clusterDiscovery(ctx); err != nil {
		return nil, err
	}

	if discoveryCfg := discoveryConfig.New(opts...); discoveryCfg.Interval() > 0 {
		db.discoveryRepeater = repeater.New(deadline.ContextWithoutDeadline(ctx), discoveryCfg.Interval(),
			func(ctx context.Context) (err error) {
				ctx, cancel := context.WithTimeout(ctx, discoveryCfg.Interval())
				defer cancel()

				return db.clusterDiscovery(ctx)
			},
			repeater.WithName("discovery"),
			repeater.WithTrace(db.config.Trace()),
		)
	} else {
		db.clusterSet(cluster.New(deadline.ContextWithoutDeadline(ctx), db.config, pool, []endpoint.Endpoint{discoveryEndpoint}))
	}

	var cancel context.CancelFunc
	if t := c.DialTimeout(); t > 0 {
		ctx, cancel = context.WithTimeout(ctx, c.DialTimeout())
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	return db, nil
}

func (db *database) Endpoint() string {
	return db.config.Endpoint()
}

func (db *database) Name() string {
	return db.config.Database()
}

func (db *database) Secure() bool {
	return db.config.Secure()
}

func (db *database) Invoke(
	ctx context.Context,
	method string,
	args interface{},
	reply interface{},
	opts ...grpc.CallOption,
) error {
	cc, err := db.cluster().Get(ctx)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	defer func() {
		if err != nil && xerrors.MustPessimizeEndpoint(err, db.config.ExcludeGRPCCodesForPessimization()...) {
			db.cluster().Pessimize(ctx, cc, err)
		}
	}()

	ctx, err = db.config.Meta().Meta(ctx)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	err = cc.Invoke(ctx, method, args, reply, opts...)
	if err != nil {
		return xerrors.WithStackTrace(err)
	}

	return nil
}

func (db *database) NewStream(
	ctx context.Context,
	desc *grpc.StreamDesc,
	method string,
	opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	cc, err := db.cluster().Get(ctx)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	defer func() {
		if err != nil && xerrors.MustPessimizeEndpoint(err, db.config.ExcludeGRPCCodesForPessimization()...) {
			db.cluster().Pessimize(ctx, cc, err)
		}
	}()

	ctx, err = db.config.Meta().Meta(ctx)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	var client grpc.ClientStream
	client, err = cc.NewStream(ctx, desc, method, opts...)
	if err != nil {
		return nil, xerrors.WithStackTrace(err)
	}

	return client, nil
}
