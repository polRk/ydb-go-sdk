package single

import (
	"context"
	"sync"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
)

func Balancer(c conn.Conn) balancer.Balancer {
	return &single{conn: c}
}

type single struct {
	conn        conn.Conn
	needRefresh chan struct{}

	sync.Mutex
	needRefreshClosed bool
}

func (b *single) Create(conns []conn.Conn) balancer.Balancer {
	connCount := len(conns)
	switch {
	case connCount == 0:
		return &single{}
	case connCount == 1:
		return &single{conn: conns[0]}
	default:
		panic("ydb: single Conn Balancer: must conains more then one value")
	}
}

func (b *single) Next() conn.Conn {
	return b.conn
}

func (b *single) Conn() conn.Conn {
	return b.conn
}

func (b *single) NeedRefresh(ctx context.Context) bool {
	if ctx.Err() != nil {
		return false
	}

	select {
	case <-ctx.Done():
		return false
	case <-b.needRefresh:
		return true
	}
}

func (b *single) checkIfNeedRefresh(){
	if b.conn
}

func IsSingle(i interface{}) bool {
	_, ok := i.(*single)
	return ok
}
