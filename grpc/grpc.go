package grpc

import (
	"context"

	"google.golang.org/grpc/resolver"
)

func RegisterResolverBuilder(ctx context.Context, opts *BuilderOptions) error {
	builder, err := NewBuilder(ctx, opts)
	if err != nil {
		return err
	}

	resolver.Register(builder)

	return nil
}
