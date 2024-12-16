package metrics

import (
	"context"
	"errors"
	"fmt"
	"github.com/castai/logging"
	pb "github.com/castai/metrics/api/v1beta"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"testing"
	"time"
)

var (
	logger  *logging.Logger
	address string
)

func TestMain(m *testing.M) {
	logHandler := logging.NewTextHandler(logging.TextHandlerConfig{
		Level: slog.LevelDebug,
	})
	logger = logging.New(logHandler)

	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		logger.Fatal(fmt.Sprintf("failed to listen: %v", err))
	}

	defer func() {
		_ = listener.Close()
	}()

	server := grpc.NewServer()
	handler := &plainServer{}
	pb.RegisterIngestionAPIServer(server, handler)
	go func() {
		if srvErr := server.Serve(listener); srvErr != nil {
			logger.Errorf("failed to serve: %v", srvErr)
		}
	}()

	defer server.Stop()

	port := listener.Addr().(*net.TCPAddr).Port
	address = listener.Addr().String()
	logger.Infof("server listening on port %d", port)

	m.Run()
}

func BenchmarkClient(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logHandler := logging.NewTextHandler(logging.TextHandlerConfig{
		Level: slog.LevelDebug,
	})
	logger := logging.New(logHandler)

	client, err := NewMetricClient(Config{
		APIAddr:   address,
		APIToken:  "token",
		ClusterID: "cluster-id",
		Insecure:  true,
	}, logger)
	require.NoError(b, err)

	go func() {
		err := client.Start(ctx)
		require.NoError(b, err)
	}()
	defer client.Close()

	type containerMetrics struct {
		ContainerID   string `avro:"container_id"`
		MemoryUsage   int    `avro:"memory_usage"`
		CPUUsage      int    `avro:"cpu_usage"`
		MemoryRequest int    `avro:"memory_request"`
		CPURequest    int    `avro:"cpu_request"`
	}

	schema := `{
	  "type": "record",
	  "name": "containerMetrics",
	  "fields": [
		{"name": "container_id", "type": "string"},
		{"name": "memory_usage", "type": "int"},
		{"name": "cpu_usage", "type": "int"},
		{"name": "memory_request", "type": "int"},
		{"name": "cpu_request", "type": "int"}
	  ]	
	}`

	containerMetric, err := NewMetric(client, WithSchema[containerMetrics](schema))
	require.NoError(b, err)

	for i := 0; i < b.N; i++ {
		err := containerMetric.Write(containerMetrics{
			ContainerID:   "container-id",
			MemoryUsage:   rand.Intn(1 * 1024 * 1024),
			CPUUsage:      rand.Intn(10000),
			MemoryRequest: rand.Intn(2 * 1024 * 1024),
			CPURequest:    rand.Intn(20000),
		})
		require.NoError(b, err)
	}

	time.Sleep(1 * time.Second)
}

type plainServer struct{}

func (s *plainServer) WriteMetrics(stream pb.IngestionAPI_WriteMetricsServer) error {
	for {
		_, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				_ = stream.SendAndClose(&pb.WriteMetricsResponse{
					Success: true,
				})
				return nil
			}
			return err
		}
	}
}
