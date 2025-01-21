package metrics

import (
	"context"

	pb "github.com/castai/metrics/api/v1beta"
)

type Metric[T any] interface {
	Write(datapoints ...T) error
}

type collectable interface {
	requests() []*pb.WriteMetricsRequest
	empty() bool
}

type MetricOption[T any] func(*metric[T]) error

type MetricClient interface {
	Start(ctx context.Context) error
	Close()
	add(metric collectable)
}
