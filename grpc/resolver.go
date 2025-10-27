package grpc

import (
	"context"
	"fmt"

	"github.com/995933447/discovery"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"
)

const StateAddressAttributeKeyExtra = "extra"

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
		r.LogError(err)
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
		state.Addresses = append(state.Addresses, resolver.Address{
			Addr:       fmt.Sprintf("%s:%d", node.Host, node.Port),
			Attributes: attributes.New(StateAddressAttributeKeyExtra, node.Extra),
		})
	}

	if err := r.cc.UpdateState(state); err != nil {
		r.LogError(err)
	}
}

var _ resolver.Resolver = (*Resolver)(nil)
