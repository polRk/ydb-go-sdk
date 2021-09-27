// Code generated by gtrace. DO NOT EDIT.

package trace

import (
	"context"
	"github.com/ydb-platform/ydb-go-sdk/v3/cluster"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/errors"
	"github.com/ydb-platform/ydb-go-sdk/v3/internal/operation"
)

// Compose returns a new Driver which has functional fields composed
// both from t and x.
func (t Driver) Compose(x Driver) (ret Driver) {
	switch {
	case t.OnDial == nil:
		ret.OnDial = x.OnDial
	case x.OnDial == nil:
		ret.OnDial = t.OnDial
	default:
		h1 := t.OnDial
		h2 := x.OnDial
		ret.OnDial = func(d DialStartInfo) func(DialDoneInfo) {
			r1 := h1(d)
			r2 := h2(d)
			switch {
			case r1 == nil:
				return r2
			case r2 == nil:
				return r1
			default:
				return func(d DialDoneInfo) {
					r1(d)
					r2(d)
				}
			}
		}
	}
	switch {
	case t.OnGetConn == nil:
		ret.OnGetConn = x.OnGetConn
	case x.OnGetConn == nil:
		ret.OnGetConn = t.OnGetConn
	default:
		h1 := t.OnGetConn
		h2 := x.OnGetConn
		ret.OnGetConn = func(g GetConnStartInfo) func(GetConnDoneInfo) {
			r1 := h1(g)
			r2 := h2(g)
			switch {
			case r1 == nil:
				return r2
			case r2 == nil:
				return r1
			default:
				return func(g GetConnDoneInfo) {
					r1(g)
					r2(g)
				}
			}
		}
	}
	switch {
	case t.OnPessimization == nil:
		ret.OnPessimization = x.OnPessimization
	case x.OnPessimization == nil:
		ret.OnPessimization = t.OnPessimization
	default:
		h1 := t.OnPessimization
		h2 := x.OnPessimization
		ret.OnPessimization = func(p PessimizationStartInfo) func(PessimizationDoneInfo) {
			r1 := h1(p)
			r2 := h2(p)
			switch {
			case r1 == nil:
				return r2
			case r2 == nil:
				return r1
			default:
				return func(p PessimizationDoneInfo) {
					r1(p)
					r2(p)
				}
			}
		}
	}
	switch {
	case t.OnGetCredentials == nil:
		ret.OnGetCredentials = x.OnGetCredentials
	case x.OnGetCredentials == nil:
		ret.OnGetCredentials = t.OnGetCredentials
	default:
		h1 := t.OnGetCredentials
		h2 := x.OnGetCredentials
		ret.OnGetCredentials = func(g GetCredentialsStartInfo) func(GetCredentialsDoneInfo) {
			r1 := h1(g)
			r2 := h2(g)
			switch {
			case r1 == nil:
				return r2
			case r2 == nil:
				return r1
			default:
				return func(g GetCredentialsDoneInfo) {
					r1(g)
					r2(g)
				}
			}
		}
	}
	switch {
	case t.OnDiscovery == nil:
		ret.OnDiscovery = x.OnDiscovery
	case x.OnDiscovery == nil:
		ret.OnDiscovery = t.OnDiscovery
	default:
		h1 := t.OnDiscovery
		h2 := x.OnDiscovery
		ret.OnDiscovery = func(d DiscoveryStartInfo) func(DiscoveryDoneInfo) {
			r1 := h1(d)
			r2 := h2(d)
			switch {
			case r1 == nil:
				return r2
			case r2 == nil:
				return r1
			default:
				return func(d DiscoveryDoneInfo) {
					r1(d)
					r2(d)
				}
			}
		}
	}
	switch {
	case t.OnOperation == nil:
		ret.OnOperation = x.OnOperation
	case x.OnOperation == nil:
		ret.OnOperation = t.OnOperation
	default:
		h1 := t.OnOperation
		h2 := x.OnOperation
		ret.OnOperation = func(o OperationStartInfo) func(OperationDoneInfo) {
			r1 := h1(o)
			r2 := h2(o)
			switch {
			case r1 == nil:
				return r2
			case r2 == nil:
				return r1
			default:
				return func(o OperationDoneInfo) {
					r1(o)
					r2(o)
				}
			}
		}
	}
	switch {
	case t.OnStream == nil:
		ret.OnStream = x.OnStream
	case x.OnStream == nil:
		ret.OnStream = t.OnStream
	default:
		h1 := t.OnStream
		h2 := x.OnStream
		ret.OnStream = func(s StreamStartInfo) func(StreamRecvDoneInfo) func(StreamDoneInfo) {
			r1 := h1(s)
			r2 := h2(s)
			switch {
			case r1 == nil:
				return r2
			case r2 == nil:
				return r1
			default:
				return func(s StreamRecvDoneInfo) func(StreamDoneInfo) {
					r11 := r1(s)
					r21 := r2(s)
					switch {
					case r11 == nil:
						return r21
					case r21 == nil:
						return r11
					default:
						return func(s StreamDoneInfo) {
							r11(s)
							r21(s)
						}
					}
				}
			}
		}
	}
	return ret
}
func (t Driver) onDial(d DialStartInfo) func(DialDoneInfo) {
	fn := t.OnDial
	if fn == nil {
		return func(DialDoneInfo) {
			return
		}
	}
	res := fn(d)
	if res == nil {
		return func(DialDoneInfo) {
			return
		}
	}
	return res
}
func (t Driver) onGetConn(g GetConnStartInfo) func(GetConnDoneInfo) {
	fn := t.OnGetConn
	if fn == nil {
		return func(GetConnDoneInfo) {
			return
		}
	}
	res := fn(g)
	if res == nil {
		return func(GetConnDoneInfo) {
			return
		}
	}
	return res
}
func (t Driver) onPessimization(p PessimizationStartInfo) func(PessimizationDoneInfo) {
	fn := t.OnPessimization
	if fn == nil {
		return func(PessimizationDoneInfo) {
			return
		}
	}
	res := fn(p)
	if res == nil {
		return func(PessimizationDoneInfo) {
			return
		}
	}
	return res
}
func (t Driver) onGetCredentials(g GetCredentialsStartInfo) func(GetCredentialsDoneInfo) {
	fn := t.OnGetCredentials
	if fn == nil {
		return func(GetCredentialsDoneInfo) {
			return
		}
	}
	res := fn(g)
	if res == nil {
		return func(GetCredentialsDoneInfo) {
			return
		}
	}
	return res
}
func (t Driver) onDiscovery(d DiscoveryStartInfo) func(DiscoveryDoneInfo) {
	fn := t.OnDiscovery
	if fn == nil {
		return func(DiscoveryDoneInfo) {
			return
		}
	}
	res := fn(d)
	if res == nil {
		return func(DiscoveryDoneInfo) {
			return
		}
	}
	return res
}
func (t Driver) onOperation(o OperationStartInfo) func(OperationDoneInfo) {
	fn := t.OnOperation
	if fn == nil {
		return func(OperationDoneInfo) {
			return
		}
	}
	res := fn(o)
	if res == nil {
		return func(OperationDoneInfo) {
			return
		}
	}
	return res
}
func (t Driver) onStream(s StreamStartInfo) func(StreamRecvDoneInfo) func(StreamDoneInfo) {
	fn := t.OnStream
	if fn == nil {
		return func(StreamRecvDoneInfo) func(StreamDoneInfo) {
			return func(StreamDoneInfo) {
				return
			}
		}
	}
	res := fn(s)
	if res == nil {
		return func(StreamRecvDoneInfo) func(StreamDoneInfo) {
			return func(StreamDoneInfo) {
				return
			}
		}
	}
	return func(s StreamRecvDoneInfo) func(StreamDoneInfo) {
		res := res(s)
		if res == nil {
			return func(StreamDoneInfo) {
				return
			}
		}
		return res
	}
}
func DriverOnDial(t Driver, c context.Context, address string) func(error) {
	var p DialStartInfo
	p.Context = c
	p.Address = address
	res := t.onDial(p)
	return func(e error) {
		var p DialDoneInfo
		p.Error = e
		res(p)
	}
}
func DriverOnGetConn(t Driver, c context.Context) func(address string, _ error) {
	var p GetConnStartInfo
	p.Context = c
	res := t.onGetConn(p)
	return func(address string, e error) {
		var p GetConnDoneInfo
		p.Address = address
		p.Error = e
		res(p)
	}
}
func DriverOnPessimization(t Driver, c context.Context, address string, cause error) func(error) {
	var p PessimizationStartInfo
	p.Context = c
	p.Address = address
	p.Cause = cause
	res := t.onPessimization(p)
	return func(e error) {
		var p PessimizationDoneInfo
		p.Error = e
		res(p)
	}
}
func DriverOnGetCredentials(t Driver, c context.Context) func(tokenOk bool, _ error) {
	var p GetCredentialsStartInfo
	p.Context = c
	res := t.onGetCredentials(p)
	return func(tokenOk bool, e error) {
		var p GetCredentialsDoneInfo
		p.TokenOk = tokenOk
		p.Error = e
		res(p)
	}
}
func DriverOnDiscovery(t Driver, c context.Context) func(endpoints []cluster.Endpoint, _ error) {
	var p DiscoveryStartInfo
	p.Context = c
	res := t.onDiscovery(p)
	return func(endpoints []cluster.Endpoint, e error) {
		var p DiscoveryDoneInfo
		p.Endpoints = endpoints
		p.Error = e
		res(p)
	}
}
func DriverOnOperation(t Driver, c context.Context, address string, m Method, p operation.Params) func(opID string, issues errors.IssueIterator, _ error) {
	var p1 OperationStartInfo
	p1.Context = c
	p1.Address = address
	p1.Method = m
	p1.Params = p
	res := t.onOperation(p1)
	return func(opID string, issues errors.IssueIterator, e error) {
		var p OperationDoneInfo
		p.OpID = opID
		p.Issues = issues
		p.Error = e
		res(p)
	}
}
func DriverOnStream(t Driver, c context.Context, address string, m Method) func(error) func(error) {
	var p StreamStartInfo
	p.Context = c
	p.Address = address
	p.Method = m
	res := t.onStream(p)
	return func(e error) func(error) {
		var p StreamRecvDoneInfo
		p.Error = e
		res := res(p)
		return func(e error) {
			var p StreamDoneInfo
			p.Error = e
			res(p)
		}
	}
}
