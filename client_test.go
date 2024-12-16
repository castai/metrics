package metrics

import (
	"context"
	"errors"
	"github.com/castai/logging"
	pb "github.com/castai/metrics/api/v1beta"
	grpc_mock "github.com/castai/metrics/api/v1beta/mocks/google.golang.org/grpc"
	pb_mock "github.com/castai/metrics/api/v1beta/mocks/v1beta"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"
)

func TestMetricClient(t *testing.T) {
	handler := logging.NewTextHandler(logging.TextHandlerConfig{
		Level: slog.LevelDebug,
	})
	logger := logging.New(handler)

	t.Run("fails if no APIAddr", func(t *testing.T) {
		_, err := NewMetricClient(Config{}, logger)
		require.Error(t, err)
		require.Contains(t, err.Error(), "api address is required")
	})

	t.Run("fails if no APIToken", func(t *testing.T) {
		_, err := NewMetricClient(Config{
			APIAddr: "127.0.0.1:50051",
		}, logger)
		require.Error(t, err)
		require.Contains(t, err.Error(), "api token is required")
	})

	t.Run("fails if no ClusterID", func(t *testing.T) {
		_, err := NewMetricClient(Config{
			APIAddr:  "127.0.0.1:50051",
			APIToken: "token",
		}, logger)
		require.Error(t, err)
		require.Contains(t, err.Error(), "cluster id is required")
	})

	t.Run("succeeds with valid config", func(t *testing.T) {
		_, err := NewMetricClient(Config{
			APIAddr:   "127.0.0.1:50051",
			APIToken:  "token",
			ClusterID: "cluster_id",
		}, logger)
		require.NoError(t, err)
	})

	t.Run("doesn't collect anything if no metrics are written", func(t *testing.T) {
		mockAPIClient := pb_mock.NewMockIngestionAPIClient(t)

		client := &metricClient{
			client:        mockAPIClient,
			flushInterval: 50 * time.Millisecond,
			logger:        logger,
			stopCh:        make(chan struct{}),
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := client.Start(ctx)
			require.ErrorIs(t, err, context.Canceled)
		}()

		time.Sleep(250 * time.Millisecond)
		cancel()
		wg.Wait()
	})

	t.Run("collect with metrics", func(t *testing.T) {
		mockAPIClient := pb_mock.NewMockIngestionAPIClient(t)
		writeClient := grpc_mock.NewMockClientStreamingClient[pb.WriteMetricsRequest, pb.WriteMetricsResponse](t)

		mockAPIClient.
			EXPECT().WriteMetrics(mock.Anything, mock.Anything).
			Return(writeClient, nil)

		client := &metricClient{
			client:        mockAPIClient,
			flushInterval: 50 * time.Millisecond,
			logger:        logger,
			stopCh:        make(chan struct{}),
		}

		schemaJSON := `{
		  "type": "record",
		  "name": "testMetrics",
		  "fields": [
			{
			  "name": "value",
			  "type": "int"
			}
		  ]
		}`

		type testMetrics struct {
			Value int `avro:"value"`
		}

		m, err := NewMetric[testMetrics](client, WithSchema[testMetrics](schemaJSON))
		require.NoError(t, err)

		err = m.Write(
			testMetrics{Value: 1},
			testMetrics{Value: 2},
			testMetrics{Value: 3},
		)
		require.NoError(t, err)

		sendCalls := make([]*pb.WriteMetricsRequest, 0)
		writeClient.EXPECT().Send(mock.Anything).Run(func(req *pb.WriteMetricsRequest) {
			sendCalls = append(sendCalls, req)
		}).Return(nil)
		writeClient.EXPECT().CloseSend().Return(nil)
		writeClient.EXPECT().CloseAndRecv().Return(&pb.WriteMetricsResponse{Success: true}, nil)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := client.Start(ctx)
			require.ErrorIs(t, err, context.Canceled)
		}()

		time.Sleep(250 * time.Millisecond)
		cancel()
		wg.Wait()

		require.Len(t, sendCalls, 1)
		require.NotEmpty(t, sendCalls[0].Metrics)
		require.NotEmpty(t, sendCalls[0].Schema)
		require.Equal(t, uint64(3), sendCalls[0].Metadata.Rows)
	})

	t.Run("retry on stream open failure", func(t *testing.T) {
		mockAPIClient := pb_mock.NewMockIngestionAPIClient(t)
		writeClient := grpc_mock.NewMockClientStreamingClient[pb.WriteMetricsRequest, pb.WriteMetricsResponse](t)

		mockAPIClient.
			EXPECT().WriteMetrics(mock.Anything, mock.Anything).
			Return(nil, errors.New("stream open failed")).
			Once()

		mockAPIClient.EXPECT().WriteMetrics(mock.Anything, mock.Anything).Return(writeClient, nil)

		client := &metricClient{
			client:        mockAPIClient,
			logger:        logger,
			flushInterval: 50 * time.Millisecond,
			stopCh:        make(chan struct{}),
		}

		schemaJSON := `{
		  "type": "record",
		  "name": "testMetrics",
		  "fields": [
			{
			  "name": "value",
			  "type": "int"
			}
		  ]
		}`

		type testMetrics struct {
			Value int `avro:"value"`
		}

		m, err := NewMetric[testMetrics](client, WithSchema[testMetrics](schemaJSON))
		require.NoError(t, err)

		err = m.Write(testMetrics{Value: 42})
		require.NoError(t, err)

		writeClient.EXPECT().Send(mock.Anything).Return(nil)
		writeClient.EXPECT().CloseSend().Return(nil)
		writeClient.EXPECT().CloseAndRecv().Return(&pb.WriteMetricsResponse{Success: true}, nil)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = client.Start(ctx)
		}()

		time.Sleep(250 * time.Millisecond)
		cancel()
		wg.Wait()
	})

	t.Run("error from server doesn't interrupt metrics collection", func(t *testing.T) {
		mockAPIClient := pb_mock.NewMockIngestionAPIClient(t)
		writeClient := grpc_mock.NewMockClientStreamingClient[pb.WriteMetricsRequest, pb.WriteMetricsResponse](t)

		mockAPIClient.
			EXPECT().WriteMetrics(mock.Anything, mock.Anything).
			Return(writeClient, nil)

		client := &metricClient{
			client:        mockAPIClient,
			logger:        logger,
			flushInterval: 10 * time.Millisecond,
			stopCh:        make(chan struct{}),
		}

		schemaJSON := `{
		  "type": "record",
		  "name": "testMetrics",
		  "fields": [
			{
			  "name": "value",
			  "type": "int"
			}
		  ]
		}`

		type testMetrics struct {
			Value int `avro:"value"`
		}

		m, err := NewMetric[testMetrics](client, WithSchema[testMetrics](schemaJSON))
		require.NoError(t, err)
		err = m.Write(testMetrics{Value: 42})
		require.NoError(t, err)

		writeClient.EXPECT().Send(mock.Anything).Return(nil)
		writeClient.EXPECT().CloseSend().Return(nil)
		writeClient.EXPECT().CloseAndRecv().Return(&pb.WriteMetricsResponse{
			Success: false,
			Error:   &[]string{"some server error"}[0],
		}, nil)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := client.Start(ctx)
			require.Error(t, err, "context canceled")
		}()

		time.Sleep(250 * time.Millisecond)
		cancel()
		wg.Wait()
	})

	t.Run("metrics are chunked", func(t *testing.T) {
		mockAPIClient := pb_mock.NewMockIngestionAPIClient(t)
		writeClient := grpc_mock.NewMockClientStreamingClient[pb.WriteMetricsRequest, pb.WriteMetricsResponse](t)

		mockAPIClient.
			EXPECT().WriteMetrics(mock.Anything, mock.Anything).
			Return(writeClient, nil)

		client := &metricClient{
			client:        mockAPIClient,
			logger:        logger,
			flushInterval: 10 * time.Millisecond,
			stopCh:        make(chan struct{}),
		}

		schemaJSON := `{
		  "type": "record",
		  "name": "testMetrics",
		  "fields": [
			{
			  "name": "value",
			  "type": "string"
			}
		  ]
		}`

		type testMetrics struct {
			Value string `avro:"value"`
		}

		m, err := NewMetric[testMetrics](
			client,
			WithSchema[testMetrics](schemaJSON),
			WithMaxChunkSize[testMetrics](5),
		)
		require.NoError(t, err)

		err = m.Write(
			testMetrics{Value: "aaaa123456"},
			testMetrics{Value: "bbbb123456"},
			testMetrics{Value: "cccc123456"},
			testMetrics{Value: "dddd123456"},
			testMetrics{Value: "eeee123456"},
		)
		require.NoError(t, err)

		sendCalls := make([]*pb.WriteMetricsRequest, 0)
		writeClient.EXPECT().Send(mock.Anything).Run(func(req *pb.WriteMetricsRequest) {
			sendCalls = append(sendCalls, req)
		}).Return(nil)
		writeClient.EXPECT().CloseSend().Return(nil)
		writeClient.EXPECT().CloseAndRecv().Return(&pb.WriteMetricsResponse{Success: true}, nil)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = client.Start(ctx)
		}()

		time.Sleep(250 * time.Millisecond)
		cancel()
		wg.Wait()

		require.Greater(t, len(sendCalls), 1, "expected multiple chunks")
		var totalRows uint64
		for _, call := range sendCalls {
			totalRows += call.Metadata.Rows
		}
		require.Equal(t, uint64(5), totalRows)
	})

	t.Run("metric schema required", func(t *testing.T) {
		mockAPIClient := pb_mock.NewMockIngestionAPIClient(t)

		client := &metricClient{
			client:        mockAPIClient,
			logger:        logger,
			flushInterval: 10 * time.Millisecond,
			stopCh:        make(chan struct{}),
		}

		type testMetrics struct {
			Value int `avro:"value"`
		}

		_, err := NewMetric[testMetrics](client)
		require.Error(t, err)
		require.Contains(t, err.Error(), "schema is required")
	})
}

func TestMetricClientWithServer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logHandler := logging.NewTextHandler(logging.TextHandlerConfig{
		Level: slog.LevelDebug,
	})
	logger := logging.New(logHandler)

	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	defer func() {
		_ = listener.Close()
	}()

	server := grpc.NewServer()
	handler := &testServer{}
	pb.RegisterIngestionAPIServer(server, handler)
	go func() {
		if srvErr := server.Serve(listener); srvErr != nil {
			logger.Errorf("failed to serve: %v", srvErr)
		}
	}()

	defer server.Stop()

	port := listener.Addr().(*net.TCPAddr).Port
	logger.Infof("server listening on port %d", port)

	client, err := NewMetricClient(Config{
		APIAddr:   listener.Addr().String(),
		APIToken:  "token",
		ClusterID: "cluster-id",
		Insecure:  true,
	}, logger)
	require.NoError(t, err)

	go func() {
		err := client.Start(ctx)
		require.NoError(t, err)
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

	datapoints := []containerMetrics{
		{
			ContainerID:   "container-1",
			MemoryUsage:   100,
			CPUUsage:      50,
			MemoryRequest: 200,
			CPURequest:    100,
		},
		{
			ContainerID:   "container-2",
			MemoryUsage:   200,
			CPUUsage:      100,
			MemoryRequest: 400,
			CPURequest:    200,
		},
		{
			ContainerID:   "container-3",
			MemoryUsage:   300,
			CPUUsage:      150,
			MemoryRequest: 600,
			CPURequest:    300,
		},
	}

	testCases := []struct {
		name          string
		metrics       []containerMetrics
		serverHandler func(stream pb.IngestionAPI_WriteMetricsServer) error
		metricOptions []MetricOption[containerMetrics]
	}{
		{
			name:    "successfully send metrics",
			metrics: datapoints,
			serverHandler: func(stream pb.IngestionAPI_WriteMetricsServer) error {
				for {
					_, err := stream.Recv()
					if err != nil {
						if errors.Is(err, io.EOF) {
							_ = stream.SendAndClose(&pb.WriteMetricsResponse{Success: true})
							return nil
						}
						return err
					}
				}
			},
		},
		{
			name:    "fails to collect metrics on server error",
			metrics: datapoints,
			serverHandler: func(stream pb.IngestionAPI_WriteMetricsServer) error {
				return status.Error(codes.InvalidArgument, "invalid argument")
			},
		},
		{
			name:    "fails to collect metrics on error response",
			metrics: datapoints,
			serverHandler: func(stream pb.IngestionAPI_WriteMetricsServer) error {
				_ = stream.SendAndClose(&pb.WriteMetricsResponse{
					Success: false,
					Error:   &[]string{"some server error"}[0],
				})
				return nil
			},
		},
		{
			name:    "headers are set",
			metrics: datapoints,
			serverHandler: func(stream pb.IngestionAPI_WriteMetricsServer) error {
				requestContext := stream.Context()
				md, ok := metadata.FromIncomingContext(requestContext)
				require.True(t, ok)

				require.Equal(t, "Token token", md.Get("authorization")[0])
				require.Equal(t, "cluster-id", md.Get("x-cluster-id")[0])

				_ = stream.SendAndClose(&pb.WriteMetricsResponse{Success: true})
				return nil
			},
		},
		{
			name:    "data is sent in chunks",
			metrics: datapoints,
			serverHandler: func(stream pb.IngestionAPI_WriteMetricsServer) error {
				for {
					req, err := stream.Recv()
					if err != nil {
						if errors.Is(err, io.EOF) {
							_ = stream.SendAndClose(&pb.WriteMetricsResponse{Success: true})
							return nil
						}
						return err
					}

					require.Equal(t, uint64(1), req.Metadata.Rows)
				}
			},
			metricOptions: []MetricOption[containerMetrics]{
				WithMaxChunkSize[containerMetrics](1),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			handler.writeMetricsHandler = tc.serverHandler

			opts := []MetricOption[containerMetrics]{
				WithSchema[containerMetrics](schema),
			}

			if len(tc.metricOptions) > 0 {
				opts = append(opts, tc.metricOptions...)
			}

			containerMetric, err := NewMetric[containerMetrics](
				client,
				opts...,
			)
			require.NoError(t, err)

			err = containerMetric.Write(tc.metrics...)
			require.NoError(t, err)
			time.Sleep(1 * time.Second)
		})
	}
}

type testServer struct {
	writeMetricsHandler func(stream pb.IngestionAPI_WriteMetricsServer) error
}

func (s *testServer) WriteMetrics(stream pb.IngestionAPI_WriteMetricsServer) error {
	if s.writeMetricsHandler != nil {
		return s.writeMetricsHandler(stream)
	}

	_ = stream.SendAndClose(&pb.WriteMetricsResponse{Success: true})
	return nil
}
