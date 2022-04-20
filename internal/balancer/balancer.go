package balancer

import (
	"context"

	"github.com/ydb-platform/ydb-go-sdk/v3/internal/conn"
)

// Element is an empty interface that holds some Balancer specific data.
type Element interface{}

// Balancer is an interface that implements particular load-balancing
// algorithm.
//
// Balancer methods called synchronized. That is, implementations must not
// provide additional goroutine safety.
type Balancer interface {
	// Next returns next connection for request.
	// Next MUST not return nil if it has at least one connection.
	Next() conn.Conn

	// Create makes empty balancer with same implementation
	Create(conns []conn.Conn) Balancer

	// NeedRefresh sync call, which return in one of cases
	// first - if balancer has many pessimized nodes and need refresh (may be never)
	// ctx cancelled, must be cancelled be caller for prevent goroutines leak
	// return true if the balancer need refresh
	NeedRefresh(ctx context.Context) bool
}

func IsOkConnection(c conn.Conn, bannedIsOk bool) bool {
	state := c.GetState()
	if state == conn.Online || state == conn.Created || state == conn.Offline {
		return true
	}
	if bannedIsOk && state == conn.Banned {
		return true
	}
	return false
}
