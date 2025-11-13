package grpc

import (
	"context"
	"fmt"

	"github.com/995933447/discovery"
	"github.com/995933447/runtimeutil"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"
)

const (
	StateAddressAttributeKeyNil       = ""
	StateAddressAttributeKeyHost      = "host"
	StateAddressAttributeKeyPort      = "port"
	StateAddressAttributeKeyExtra     = "extra"
	StateAddressAttributeKeyStatus    = "status"
	StateAddressAttributeKeyPriority  = "priority"
	StateAddressAttributeKeyName      = "name"
	StateAddressAttributeKeySlaveFlag = "slave_flag"
)

func NewResolver(srvName string, cc resolver.ClientConn, builder *Builder) *Resolver {
	return &Resolver{
		srvName: srvName,
		cc:      cc,
		Builder: builder,
	}
}

type Resolver struct {
	srvName string
	cc      resolver.ClientConn
	*Builder
}

func (r *Resolver) ResolveNow(options resolver.ResolveNowOptions) {
	srv, err := r.Builder.dis.Discover(context.Background(), r.srvName)
	if err != nil {
		r.LogError(runtimeutil.NewStackErr(fmt.Errorf("discover service %s error:%v", r.srvName, err)))
		return
	}
	r.UpdateSrvCfg(srv)
}

func (r *Resolver) Close() {
	r.Builder.OnResolverClosed(r)
}

func (r *Resolver) UpdateSrvCfg(srv *discovery.Service) {
	if srv.SrvName != r.srvName {
		return
	}

	state := resolver.State{}
	for _, node := range srv.Nodes {
		if !node.Available() {
			continue
		}

		attrs := attributes.New(StateAddressAttributeKeyExtra, node.Extra)
		attrs.WithValue(StateAddressAttributeKeyStatus, node.Status)
		attrs.WithValue(StateAddressAttributeKeyPriority, node.Priority)
		attrs.WithValue(StateAddressAttributeKeyName, node.Name)
		attrs.WithValue(StateAddressAttributeKeyHost, node.Host)
		attrs.WithValue(StateAddressAttributeKeyPort, node.Port)
		attrs.WithValue(StateAddressAttributeKeySlaveFlag, node.SlaveFlag)
		state.Addresses = append(state.Addresses, resolver.Address{
			Addr:       fmt.Sprintf("%s:%d", node.Host, node.Port),
			Attributes: attrs,
		})
	}

	// 这里要使用ReportError，不能更新地址为空，如果更新地址为空，下次请求，grpc不会重新调用ResolveNow更新地址
	// ReportError会把错误返回应用层调用grpc的错误，以及grpc会指数级重试调用ResolveNow(最大间隔2分钟)，直到没有ReportError
	//if len(state.Addresses) == 0 {
	//	r.cc.ReportError(errors.New("empty address of " + r.srvName))
	//	return
	//}

	if err := r.cc.UpdateState(state); err != nil {
		r.LogError(runtimeutil.NewStackErr(err))
	}
}

var _ resolver.Resolver = (*Resolver)(nil)
