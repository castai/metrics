package metrics

import (
	"bytes"
	"errors"
	"fmt"
	"sync"

	"github.com/hamba/avro/v2"

	pb "github.com/castai/metrics/api/v1beta"
)

const (
	defaultMaxChunkSize = 1024 * 1024      // 1MB
	MAX_CHUNK_SIZE      = 20 * 1024 * 1024 // 20MB
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

type metricChunk struct {
	data []byte
	rows uint64
}

type metric[T any] struct {
	collection   string
	schema       avro.Schema
	schemaBytes  []byte
	maxChunkSize int
	buf          *bytes.Buffer
	encoder      *avro.Encoder
	pending      uint64
	chunks       []metricChunk
	mu           sync.Mutex
}

func NewMetric[T any](client MetricClient, opts ...MetricOption[T]) (Metric[T], error) {
	m := &metric[T]{
		buf:          &bytes.Buffer{},
		maxChunkSize: defaultMaxChunkSize,
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

	if m.maxChunkSize > MAX_CHUNK_SIZE {
		m.maxChunkSize = MAX_CHUNK_SIZE
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
		m.pending++

		if m.buf.Len() >= m.maxChunkSize {
			m.chunks = append(m.chunks, metricChunk{
				data: m.buf.Bytes(),
				rows: m.pending,
			})

			m.buf = &bytes.Buffer{}
			m.encoder = avro.NewEncoderForSchema(m.schema, m.buf)
			m.pending = 0
		}
	}

	return nil
}

func (m *metric[T]) requests() []*pb.WriteMetricsRequest {
	m.mu.Lock()
	defer m.mu.Unlock()

	requests := make([]*pb.WriteMetricsRequest, 0, len(m.chunks)+1)

	if m.buf.Len() > 0 {
		m.chunks = append(m.chunks, metricChunk{
			data: m.buf.Bytes(),
			rows: m.pending,
		})

		m.buf = &bytes.Buffer{}
		m.encoder = avro.NewEncoderForSchema(m.schema, m.buf)
		m.pending = 0
	}

	for _, chunk := range m.chunks {
		requests = append(requests, &pb.WriteMetricsRequest{
			Collection: m.collection,
			Schema:     m.schemaBytes,
			Metrics:    chunk.data,
			Metadata: &pb.MetricsMetadata{
				Rows: chunk.rows,
			},
		})
	}

	m.chunks = nil

	return requests
}
