package grpc

import (
	"context"
	"errors"
	"sync"

	"github.com/995933447/discovery"
	"github.com/995933447/elemutil"
	"google.golang.org/grpc/resolver"
)

type BuilderOptions struct {
	ResolveSchema      string
	Discovery          discovery.Discovery
	OnDiscoveryUpdated OnDiscoveryUpdatedFunc
	LogErrorFunc       func(any)
}

func (opts *BuilderOptions) Check() error {
	if opts.ResolveSchema == "" {
		return errors.New("resolver schema is required")
	}

	if opts.Discovery == nil {
		return errors.New("discovery is required")
	}

	return nil
}

func NewBuilder(ctx context.Context, opts *BuilderOptions) (resolver.Builder, error) {
	if err := opts.Check(); err != nil {
		return nil, err
	}

	builder := &Builder{
		srvNameToResolversMap: map[string]*elemutil.LinkedList{},
		resolveSchema:         opts.ResolveSchema,
		onDiscoveryUpdated:    opts.OnDiscoveryUpdated,
		logErrorFunc:          opts.LogErrorFunc,
	}

	opts.Discovery.OnSrvUpdated(func(ctx context.Context, evt discovery.Evt, srv *discovery.Service) {
		builder.mu.RLock()
		defer builder.mu.RUnlock()

		resolvers, ok := builder.srvNameToResolversMap[srv.SrvName]
		if !ok {
			return
		}

		_ = resolvers.Walk(func(node *elemutil.LinkedNode) (bool, error) {
			node.Payload.(*Resolver).UpdateSrvCfg(srv)
			return true, nil
		})

		if builder.onDiscoveryUpdated != nil {
			builder.onDiscoveryUpdated(ctx, evt, srv)
		}
	})

	_, err := opts.Discovery.LoadAll(ctx)
	if err != nil {
		return nil, err
	}

	builder.dis = opts.Discovery

	return builder, nil
}

type OnDiscoveryUpdatedFunc func(ctx context.Context, evt discovery.Evt, srv *discovery.Service)

type Builder struct {
	srvNameToResolversMap map[string]*elemutil.LinkedList
	dis                   discovery.Discovery
	mu                    sync.RWMutex
	resolveSchema         string
	onDiscoveryUpdated    OnDiscoveryUpdatedFunc
	logErrorFunc          func(any)
}

func (b *Builder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	srvName := target.Endpoint()

	srv, err := b.dis.Discover(context.Background(), srvName)
	if err != nil {
		return nil, err
	}

	resolve := NewResolver(srvName, cc, b)
	resolve.UpdateSrvCfg(srv)

	b.mu.Lock()
	defer b.mu.Unlock()

	resolvers, ok := b.srvNameToResolversMap[srvName]
	if !ok {
		resolvers = &elemutil.LinkedList{}
		b.srvNameToResolversMap[srvName] = resolvers
	}
	resolvers.Append(resolve)

	return resolve, nil
}

func (b *Builder) Scheme() string {
	return b.resolveSchema
}

func (b *Builder) OnResolverClosed(resolve *Resolver) {
	b.mu.Lock()
	defer b.mu.Unlock()

	resolvers := b.srvNameToResolversMap[resolve.srvName]
	resolvers.Delete(resolve)
	if resolvers.Len() == 0 {
		delete(b.srvNameToResolversMap, resolve.srvName)
	}

	return
}

func (b *Builder) LogError(err any) {
	if b.logErrorFunc != nil {
		b.logErrorFunc(err)
	}
}

var _ resolver.Builder = (*Builder)(nil)
