package metrics

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/castai/logging"
	pb "github.com/castai/metrics/api/v1beta"
	"github.com/cenkalti/backoff/v4"
	"google.golang.org/grpc"
)

const (
	defaultFlushInterval = 200 * time.Millisecond
	defaultMaxRetry      = 15 * time.Second
	defaultDrainTimeout  = 10 * time.Second
)

type ClientOption func(*metricClient)

func WithFlushInterval(interval time.Duration) ClientOption {
	return func(c *metricClient) {
		c.flushInterval = interval
	}
}

func WithMaxRetryTimeout(timeout time.Duration) ClientOption {
	return func(c *metricClient) {
		c.maxRetryTimeout = timeout
	}
}

func WithDrainTimeout(timeout time.Duration) ClientOption {
	return func(c *metricClient) {
		c.drainTimeout = timeout
	}
}

type metricClient struct {
	connection      *grpc.ClientConn
	client          pb.IngestionAPIClient
	metrics         []collectable
	flushInterval   time.Duration
	maxRetryTimeout time.Duration
	drainTimeout    time.Duration
	logger          *logging.Logger
	mu              sync.RWMutex
	stopCh          chan struct{}
}

// NewMetricClient creates a new MetricClient instance which is responsible for sending
// metrics to the ingestion service. It validates the provided configuration, sets up gRPC
// connection, and initializes the client.
// The returned client can then be started with the Start method to begin periodic metric flushing.
//
// Parameters:
//   - cfg: Configuration struct containing API address, token, cluster ID, and other optional settings.
//   - logger: Logger instance for diagnostic and error messages.
//
// Returns:
//   - MetricClient: an initialized metrics client ready to start flushing metrics.
//   - error: if required parameters are missing or if the gRPC client cannot be created.
func NewMetricClient(
	cfg Config,
	logger *logging.Logger,
	options ...ClientOption,
) (MetricClient, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	conn, client, err := NewAPI(cfg.APIAddr, cfg.ClusterID, cfg.APIToken, cfg.Insecure)
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}

	m := &metricClient{
		connection:      conn,
		client:          client,
		logger:          logger,
		flushInterval:   defaultFlushInterval,
		maxRetryTimeout: defaultMaxRetry,
		drainTimeout:    defaultDrainTimeout,
		stopCh:          make(chan struct{}),
	}

	for _, opt := range options {
		opt(m)
	}

	return m, nil
}

func (c *metricClient) send(request *pb.WriteMetricsRequest, client pb.IngestionAPI_WriteMetricsClient) error {
	err := client.Send(request)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}

	return nil
}

// Start begins the periodic collection and flushing of metrics. It sets up a ticker
// that triggers the collection process at the configured flushInterval. This function
// blocks until the provided context is canceled.
//
// On each tick, metrics are collected, batched, and sent via the streaming WriteMetrics RPC.
// If errors occur, they are logged. Start will continue running until the context is done.
//
// Parameters:
//   - ctx: A context that, when canceled, will stop the metric flushing loop.
//
// Returns:
//   - error: Typically ctx.Err() when the context is canceled, or other errors if they occur.
func (c *metricClient) Start(ctx context.Context) error {
	ticker := time.NewTicker(c.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			c.drain()

			if c.connection == nil {
				return nil
			}

			return c.connection.Close()
		case <-ctx.Done():
			c.drain()
			c.Close()

			return ctx.Err()
		case <-ticker.C:
			if err := c.collect(ctx); err != nil {
				c.logger.Errorf("failed to collect metrics: %v", err)
			}
		}
	}
}

func (c *metricClient) Close() {
	close(c.stopCh)
}

// collect retrieves all buffered metrics from the registered Metric instances,
// opens a streaming client, sends the metrics in one or more requests, and then
// closes the stream and awaits a response.
//
// If the stream cannot be opened, or if sending fails, these errors are returned.
// Server-side errors returned in the response are also surfaced as an error.
// The process logs errors but continues on subsequent ticks, preventing temporary
// issues from permanently halting metric ingestion.
//
// Parameters:
//   - ctx: Context for the request/stream lifecycle.
//
// Returns:
//   - error: If opening the stream, sending metrics, or closing the stream fails.
func (c *metricClient) collect(ctx context.Context) error {
	var client pb.IngestionAPI_WriteMetricsClient
	var err error

	for _, m := range c.metrics {
		if m.empty() {
			continue
		}

		client, err = c.open(ctx)
		if err != nil {
			return err
		}
		break
	}

	// If no metrics are available, return early.
	if client == nil {
		return nil
	}

	c.mu.RLock()
	for _, m := range c.metrics {
		for _, req := range m.requests() {
			if err = c.send(req, client); err != nil {
				c.mu.RUnlock()
				return err
			}
		}
	}
	c.mu.RUnlock()

	if err = client.CloseSend(); err != nil {
		return fmt.Errorf("failed to close send: %w", err)
	}

	response, err := client.CloseAndRecv()
	if err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("failed to close and recv: %w", err)
	}

	if response.Error != nil {
		return fmt.Errorf("error in response: %s", *response.Error)
	}

	return nil
}

func (c *metricClient) drain() {
	// Try to collect and send any remaining metrics before returning.
	ctxTimeout, cancel := context.WithTimeout(context.Background(), c.drainTimeout)
	if err := c.collect(ctxTimeout); err != nil {
		c.logger.Errorf("failed to drain metrics: %v", err)
	}
	cancel()
}

func (c *metricClient) add(m collectable) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.metrics = append(c.metrics, m)
}

func (c *metricClient) open(ctx context.Context) (pb.IngestionAPI_WriteMetricsClient, error) {
	var writeMetricsClient pb.IngestionAPI_WriteMetricsClient
	var err error

	operation := func() error {
		writeMetricsClient, err = c.client.WriteMetrics(ctx)
		if err != nil {
			c.logger.Warnf("failed to open stream: %v", err)
			return err
		}

		return nil
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, c.maxRetryTimeout)
	defer cancel()

	b := backoff.WithContext(backoff.NewExponentialBackOff(), timeoutCtx)

	err = backoff.Retry(operation, b)
	if err != nil {
		return nil, fmt.Errorf("failed to open stream: %w", err)
	}

	return writeMetricsClient, nil
}
