package metrics

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/sercand/kuberesolver/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/castai/logging"
	pb "github.com/castai/metrics/api/v1beta"
)

const (
	defaultFlushInterval = 200 * time.Millisecond
	defaultMaxRetry      = 15 * time.Second
)

type Config struct {
	APIAddr         string
	APIToken        string
	ClusterID       string
	Insecure        bool
	FlushInterval   time.Duration
	MaxRetryTimeout time.Duration
}

type metricClient struct {
	client          pb.IngestionAPIClient
	metrics         []collectable
	flushInterval   time.Duration
	maxRetryTimeout time.Duration
	logger          *logging.Logger
	mu              sync.RWMutex
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
) (MetricClient, error) {
	if cfg.APIAddr == "" {
		return nil, fmt.Errorf("api address is required")
	}

	if cfg.APIToken == "" {
		return nil, fmt.Errorf("api token is required")
	}

	if cfg.ClusterID == "" {
		return nil, fmt.Errorf("cluster id is required")
	}

	opts := []grpc.DialOption{
		grpc.WithUnaryInterceptor(unaryInterceptor(cfg)),
		grpc.WithStreamInterceptor(streamInterceptor(cfg)),
	}

	tls := credentials.NewTLS(nil)
	if cfg.Insecure {
		tls = insecure.NewCredentials()
	}
	opts = append(opts, grpc.WithTransportCredentials(tls))

	// If the API address starts with "kubernetes://", enable kuberesolver for service discovery
	// and round-robin load balancing. This is useful when the client and server are running in the same Kubernetes cluster.
	if strings.HasPrefix(cfg.APIAddr, "kubernetes://") {
		kuberesolver.RegisterInCluster()
		opts = append(opts, grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`))
	}

	conn, err := grpc.NewClient(cfg.APIAddr, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %w", err)
	}

	client := pb.NewIngestionAPIClient(conn)

	m := &metricClient{
		client:          client,
		logger:          logger,
		flushInterval:   defaultFlushInterval,
		maxRetryTimeout: defaultMaxRetry,
	}

	if cfg.FlushInterval > 0 {
		m.flushInterval = cfg.FlushInterval
	}

	if cfg.MaxRetryTimeout > 0 {
		m.maxRetryTimeout = cfg.MaxRetryTimeout
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
		case <-ctx.Done():
			// Try to collect and send any remaining metrics before returning.
			ctxTimeout, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if err := c.collect(ctxTimeout); err != nil {
				c.logger.Errorf("failed to drain metrics: %v", err)
			}
			cancel()

			return ctx.Err()
		case <-ticker.C:
			if err := c.collect(ctx); err != nil {
				c.logger.Errorf("failed to collect metrics: %v", err)
			}
		}
	}
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
	c.mu.RLock()
	defer c.mu.RUnlock()

	client, err := c.open(ctx)
	if err != nil {
		return err
	}

	for _, m := range c.metrics {
		for _, req := range m.requests() {
			if err = c.send(req, client); err != nil {
				return err
			}
		}
	}

	if err = client.CloseSend(); err != nil {
		return fmt.Errorf("failed to close send: %w", err)
	}

	response, err := client.CloseAndRecv()
	if err != nil {
		return fmt.Errorf("failed to close and recv: %w", err)
	}

	if response.Error != nil {
		return fmt.Errorf("error in response: %s", *response.Error)
	}

	return nil
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
