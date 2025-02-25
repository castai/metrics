package metrics

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/hamba/avro/v2"
	"sync"

	pb "github.com/castai/metrics/api/v1beta"
)

const (
	defaultMaxChunkSize = 1024 * 1024      // 1MB
	MaxAllowedChunkSize = 20 * 1024 * 1024 // 20MB
)

func WithAvroSchema[T any](schema avro.Schema) MetricOption[T] {
	return func(m *metric[T]) error {
		m.schema = schema

		return nil
	}
}

func WithSchema[T any](schema string) MetricOption[T] {
	return func(m *metric[T]) error {
		s, err := avro.Parse(schema)
		if err != nil {
			return err
		}

		m.schema = s

		return nil
	}
}

func WithMaxChunkSize[T any](size int) MetricOption[T] {
	return func(m *metric[T]) error {
		m.maxChunkSize = size

		return nil
	}
}

func WithSkipTimestamp[T any]() MetricOption[T] {
	return func(m *metric[T]) error {
		m.skipTimestamp = true

		return nil
	}
}

type metric[T any] struct {
	collection    string
	schema        avro.Schema
	schemaBytes   []byte
	maxChunkSize  int
	buf           *bytes.Buffer
	encoder       *avro.Encoder
	pendingRows   uint64
	writeRequests []*pb.WriteMetricsRequest
	mu            sync.Mutex
	skipTimestamp bool
}

func NewMetric[T any](client MetricClient, opts ...MetricOption[T]) (Metric[T], error) {
	m := &metric[T]{
		buf:           &bytes.Buffer{},
		maxChunkSize:  defaultMaxChunkSize,
		skipTimestamp: false,
	}

	for _, opt := range opts {
		if err := opt(m); err != nil {
			return nil, err
		}
	}

	if m.schema == nil {
		return nil, errors.New("schema is required")
	}

	m.schemaBytes = []byte(m.schema.String())

	schema, ok := m.schema.(*avro.RecordSchema)
	if !ok {
		return nil, errors.New("schema must be a record")
	}

	m.collection = schema.Name()

	m.encoder = avro.NewEncoderForSchema(m.schema, m.buf)

	if m.maxChunkSize > MaxAllowedChunkSize {
		m.maxChunkSize = MaxAllowedChunkSize
	}

	client.add(m)

	return m, nil
}

func (m *metric[T]) Write(datapoints ...T) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, datapoint := range datapoints {
		err := m.encoder.Encode(datapoint)
		if err != nil {
			return fmt.Errorf("failed to encode datapoint: %w", err)
		}
		m.pendingRows++

		if m.buf.Len() >= m.maxChunkSize {
			m.writeRequests = append(m.writeRequests,
				&pb.WriteMetricsRequest{
					Collection: m.collection,
					Schema:     m.schemaBytes,
					Metrics:    m.buf.Bytes(),
					Metadata: &pb.MetricsMetadata{
						Rows:          m.pendingRows,
						SkipTimestamp: m.skipTimestamp,
					},
				})

			m.buf = &bytes.Buffer{}
			m.encoder = avro.NewEncoderForSchema(m.schema, m.buf)
			m.pendingRows = 0
		}
	}

	return nil
}

func (m *metric[T]) requests() []*pb.WriteMetricsRequest {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.buf.Len() > 0 {
		m.writeRequests = append(m.writeRequests, &pb.WriteMetricsRequest{
			Collection: m.collection,
			Schema:     m.schemaBytes,
			Metrics:    m.buf.Bytes(),
			Metadata: &pb.MetricsMetadata{
				Rows:          m.pendingRows,
				SkipTimestamp: m.skipTimestamp,
			},
		})

		m.buf = &bytes.Buffer{}
		m.encoder = avro.NewEncoderForSchema(m.schema, m.buf)
		m.pendingRows = 0
	}

	requests := m.writeRequests
	m.writeRequests = nil

	return requests
}

func (m *metric[T]) empty() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.buf.Len() == 0 && len(m.writeRequests) == 0
}
