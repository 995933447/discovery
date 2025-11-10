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
		r.LogError(runtimeutil.NewStackErr(err))
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

	if err := r.cc.UpdateState(state); err != nil {
		r.LogError(runtimeutil.NewStackErr(err))
	}
}

var _ resolver.Resolver = (*Resolver)(nil)
