package stub

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
)

type stubBalancer struct {
	OnNext        func() conn.Conn
	OnCreate      func(conns []conn.Conn) balancer.Balancer
	OnNeedRefresh func(ctx context.Context) bool
}

func Balancer() (*[]conn.Conn, balancer.Balancer) {
	cs := &[]conn.Conn{}
	var i int
	return cs, stubBalancer{
		OnNext: func() conn.Conn {
			n := len(*cs)
			if n == 0 {
				return nil
			}
			e := (*cs)[i%n]
			i++
			return e
		},
	}
}

func (s stubBalancer) Create(conns []conn.Conn) balancer.Balancer {
	if f := s.OnCreate; f != nil {
		return f(conns)
	}

	return nil
}

func (s stubBalancer) Next() conn.Conn {
	if f := s.OnNext; f != nil {
		return f()
	}
	return nil
}

func (s stubBalancer) NeedRefresh(ctx context.Context) bool {
	if f := s.OnNeedRefresh; f != nil {
		return f(ctx)
	}

	<-ctx.Done()
	return false
}
