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

func withMetadata(ctx context.Context, clusterID, token string) context.Context {
	return metadata.AppendToOutgoingContext(ctx,
		clusterIDHeader, clusterID,
		tokenHeader, fmt.Sprintf("Token %s", token),
	)
}

func unaryInterceptor(clusterID, token string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		ctx = withMetadata(ctx, clusterID, token)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func streamInterceptor(clusterID, token string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx = withMetadata(ctx, clusterID, token)
		return streamer(ctx, desc, cc, method, opts...)
	}
}
