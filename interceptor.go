package metrics

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	tokenHeader     = "authorization"
	clusterIDHeader = "x-cluster-id"
)

func withMetadata(ctx context.Context, cfg Config) context.Context {
	return metadata.AppendToOutgoingContext(ctx,
		clusterIDHeader, cfg.ClusterID,
		tokenHeader, fmt.Sprintf("Token %s", cfg.APIToken),
	)
}

func unaryInterceptor(cfg Config) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = withMetadata(ctx, cfg)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func streamInterceptor(cfg Config) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx = withMetadata(ctx, cfg)
		return streamer(ctx, desc, cc, method, opts...)
	}
}
