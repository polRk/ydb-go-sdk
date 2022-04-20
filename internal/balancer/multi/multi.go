package multi

import (
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/balancer"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
)

func Balancer(opts ...Option) balancer.Balancer {
	m := new(multi)
	for _, opt := range opts {
		opt(m)
	}
	return m
}

type multi struct {
	balancer []balancer.Balancer
	filter   []func(conn.Conn) bool
}

func (m *multi) Create(conns []conn.Conn) balancer.Balancer {
	newBalancers := make([]balancer.Balancer, len(m.balancer))
	for i, balancer := range m.balancer {
		balancerConns := make([]conn.Conn, 0, len(conns))

		filter := m.filter[i]
		for _, conn := range conns {
			if filter(conn) {
				balancerConns = append(balancerConns, conn)
			}
		}
		newBalancers[i] = balancer.Create(balancerConns)
	}

	return &multi{
		balancer: newBalancers,
		filter:   m.filter,
	}
}

func WithBalancer(b balancer.Balancer, filter func(cc conn.Conn) bool) Option {
	return func(m *multi) {
		m.balancer = append(m.balancer, b)
		m.filter = append(m.filter, filter)
	}
}

type Option func(*multi)

func (m *multi) Next() conn.Conn {
	for _, b := range m.balancer {
		if c := b.Next(); c != nil {
			return c
		}
	}
	return nil
}
