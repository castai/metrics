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
	"log/slog"
	"sync"
	"testing"
	"time"
)

func TestNewMetricClient(t *testing.T) {
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

	t.Run("start and collect flow", func(t *testing.T) {
		mockAPIClient := pb_mock.NewMockIngestionAPIClient(t)
		writeClient := grpc_mock.NewMockClientStreamingClient[pb.WriteMetricsRequest, pb.WriteMetricsResponse](t)

		mockAPIClient.
			EXPECT().WriteMetrics(mock.Anything, mock.Anything).
			Return(writeClient, nil)

		client := &metricClient{
			client:        mockAPIClient,
			flushInterval: 50 * time.Millisecond,
			logger:        logger,
		}

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
		}

		type testMetrics struct {
			Value int `avro:"value"`
		}

		_, err := NewMetric[testMetrics](client)
		require.Error(t, err)
		require.Contains(t, err.Error(), "schema is required")
	})
}
