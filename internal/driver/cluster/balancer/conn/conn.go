package conn

import (
	"context"
	"fmt"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/response"
	"math"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/cluster"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/runtime"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/driver/cluster/balancer/conn/runtime/stats/state"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/operation"
	"github.com/ydb-platform/ydb-go-sdk/v3/testutil/timeutil"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Issue"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

type Conn interface {
	grpc.ClientConnInterface

	Addr() cluster.Addr
	Runtime() runtime.Runtime
	Close() error
}

func (c *conn) Address() string {
	return c.Addr().String()
}

type conn struct {
	sync.Mutex

	dial    func(context.Context, string, int) (*grpc.ClientConn, error)
	addr    cluster.Addr
	runtime runtime.Runtime
	done    chan struct{}

	config Config

	timer    timeutil.Timer
	grpcConn *grpc.ClientConn
}

func (c *conn) Addr() cluster.Addr {
	return c.addr
}

func (c *conn) Runtime() runtime.Runtime {
	return c.runtime
}

func (c *conn) Conn(ctx context.Context) (*grpc.ClientConn, error) {
	c.Lock()
	defer c.Unlock()
	if c.grpcConn == nil || isBroken(c.grpcConn) {
		raw, err := c.dial(ctx, c.addr.Host, c.addr.Port)
		if err != nil {
			return nil, err
		}
		c.grpcConn = raw
	}
	if c.runtime.GetState() != state.Banned {
		c.runtime.SetState(state.Online)
	}
	if c.config.GrpcConnectionPolicy().TTL > 0 {
		c.timer.Reset(c.config.GrpcConnectionPolicy().TTL)
	}
	return c.grpcConn, nil
}

func isBroken(raw *grpc.ClientConn) bool {
	if raw == nil {
		return true
	}
	state := raw.GetState()
	return state == connectivity.Shutdown || state == connectivity.TransientFailure
}

func (c *conn) IsReady() bool {
	c.Lock()
	defer c.Unlock()
	return c != nil && c.grpcConn != nil && c.grpcConn.GetState() == connectivity.Ready
}

func (c *conn) waitClose() {
	if c.config.GrpcConnectionPolicy().TTL <= 0 {
		return
	}
	c.timer.Reset(c.config.GrpcConnectionPolicy().TTL)
	for {
		select {
		case <-c.done:
			return
		case <-c.timer.C():
			c.Lock()
			if c.grpcConn != nil {
				_ = c.grpcConn.Close()
				c.grpcConn = nil
			}
			c.Unlock()
		}
	}
}

func (c *conn) Close() (err error) {
	c.Lock()
	defer c.Unlock()
	if c.done == nil {
		return nil
	}
	if !c.timer.Stop() {
		panic(fmt.Errorf("cant stop timer for conn to '%v'", c.addr.String()))
	}
	if c.done != nil {
		close(c.done)
		c.done = nil
	}
	if c.grpcConn != nil {
		err = c.grpcConn.Close()
		c.grpcConn = nil
	}
	return err
}

func (c *conn) pessimize(ctx context.Context, err error) {
	trace.DriverOnPessimization(c.config.Trace(ctx), ctx, c.Addr().String(), err)(c.config.Pessimize(c.addr))
}

func (c *conn) Invoke(ctx context.Context, method string, req interface{}, res interface{}, opts ...grpc.CallOption) (err error) {
	var (
		cancel context.CancelFunc
		opId   string
		issues []*Ydb_Issue.IssueMessage
	)
	if t := c.config.RequestTimeout(); t > 0 {
		ctx, cancel = context.WithTimeout(ctx, t)
	}
	defer func() {
		if cancel != nil {
			cancel()
		}
	}()
	if t := c.config.OperationTimeout(); t > 0 {
		ctx = operation.WithTimeout(ctx, t)
	}
	if t := c.config.OperationCancelAfter(); t > 0 {
		ctx = operation.WithCancelAfter(ctx, t)
	}

	params := operation.ContextParams(ctx)
	if !params.Empty() {
		operation.SetOperationParams(req, params)
	}

	start := timeutil.Now()
	c.runtime.OperationStart(start)
	t := c.config.Trace(ctx)
	operationDone := trace.DriverOnOperation(t, ctx, c.Addr().String(), trace.Method(method), params)
	defer func() {
		operationDone(opId, issues, err)
		err := errors.ErrIf(errors.IsTimeoutError(err), err)
		c.runtime.OperationDone(start, timeutil.Now(), err)
	}()

	ctx, err = c.config.Meta(ctx)
	if err != nil {
		return err
	}

	raw, err := c.Conn(ctx)
	if err != nil {
		err = errors.MapGRPCError(err)
		if errors.MustPessimizeEndpoint(err) {
			c.pessimize(ctx, err)
		}
		return
	}

	err = raw.Invoke(ctx, method, req, res, opts...)

	if err != nil {
		err = errors.MapGRPCError(err)
		if errors.MustPessimizeEndpoint(err) {
			c.pessimize(ctx, err)
		}
		return
	}
	if opResponse, ok := res.(response.OpResponse); ok {
		opId = opResponse.GetOperation().GetId()
		issues = opResponse.GetOperation().GetIssues()
		switch {
		case !opResponse.GetOperation().GetReady():
			err = errors.ErrOperationNotReady

		case opResponse.GetOperation().GetStatus() != Ydb.StatusIds_SUCCESS:
			err = errors.NewOpError(errors.WithOEOperation(opResponse.GetOperation()))
		}
	}

	return
}

func (c *conn) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (_ grpc.ClientStream, err error) {
	// Remember raw deadline to pass it for the tracing functions.
	rawCtx := ctx

	var cancel context.CancelFunc
	if t := c.config.StreamTimeout(); t > 0 {
		ctx, cancel = context.WithTimeout(ctx, t)
		defer func() {
			if err != nil {
				cancel()
			}
		}()
	}

	c.runtime.StreamStart(timeutil.Now())
	t := c.config.Trace(ctx)
	streamRecv := trace.DriverOnStream(t, ctx, c.Addr().String(), trace.Method(method))
	defer func() {
		if err != nil {
			c.runtime.StreamDone(timeutil.Now(), err)
		}
	}()

	ctx, err = c.config.Meta(ctx)
	if err != nil {
		return nil, err
	}

	raw, err := c.Conn(ctx)
	if err != nil {
		err = errors.MapGRPCError(err)
		if errors.MustPessimizeEndpoint(err) {
			c.pessimize(ctx, err)
		}
		return
	}

	s, err := raw.NewStream(ctx, desc, method, append(opts, grpc.MaxCallRecvMsgSize(50*1024*1024))...)
	if err != nil {
		return nil, errors.MapGRPCError(err)
	}

	return &grpcClientStream{
		ctx:    rawCtx,
		c:      c,
		s:      s,
		cancel: cancel,
		recv:   streamRecv,
	}, nil
}

func New(ctx context.Context, addr cluster.Addr, dial func(context.Context, string, int) (*grpc.ClientConn, error), cfg Config) Conn {
	c := &conn{
		addr:    addr,
		dial:    dial,
		config:  cfg,
		timer:   timeutil.NewTimer(time.Duration(math.MaxInt64)),
		done:    make(chan struct{}),
		runtime: runtime.New(),
	}
	go c.waitClose()
	return c
}