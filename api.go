package metrics

import (
	"fmt"
	"github.com/sercand/kuberesolver/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"strings"
)
import pb "github.com/castai/metrics/api/v1beta"

func NewAPI(apiAddr, clusterID, token string, isInsecure bool) (*grpc.ClientConn, pb.IngestionAPIClient, error) {
	opts := []grpc.DialOption{
		grpc.WithUnaryInterceptor(unaryInterceptor(clusterID, token)),
		grpc.WithStreamInterceptor(streamInterceptor(clusterID, token)),
	}

	tls := credentials.NewTLS(nil)
	if isInsecure {
		tls = insecure.NewCredentials()
	}
	opts = append(opts, grpc.WithTransportCredentials(tls))

	// If the API address starts with "kubernetes://", enable kuberesolver for service discovery
	// and round-robin load balancing. This is useful when the client and server are running in the same Kubernetes cluster.
	if strings.HasPrefix(apiAddr, "kubernetes://") {
		kuberesolver.RegisterInCluster()
		opts = append(opts, grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`))
	}

	conn, err := grpc.NewClient(apiAddr, opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create client: %w", err)
	}

	client := pb.NewIngestionAPIClient(conn)

	return conn, client, nil
}
