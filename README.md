# Metrics Client Library

This library provides a gRPC-based metrics ingestion client. It allows you to define custom metric types, serialize them using [Avro](https://avro.apache.org/), and send them to a backend service using a streaming gRPC API. It handles automatic batching, chunking, retries, and error handling.

## Overview

The core components of this library are:

1. **MetricClient**: A client responsible for establishing a connection to the metrics ingestion service, periodically flushing queued metrics, and handling retries and backoff.
2. **Metric**: A type-specific collector that allows you to enqueue data points (metrics) which are serialized into Avro binary format.

## Key Features

- **Automatic Periodic Flush**: The client regularly attempts to send all collected metrics to the server at a configurable interval.
- **Avro Serialization**: Define your metric schema using Avro. This ensures a consistent and typed structure for your metrics.
- **Chunking**: Large metric sets are split into smaller chunks to avoid overly large requests.
- **Retry & Backoff**: On connection failures or stream errors, the client retries opening the stream with exponential backoff.
- **TLS and Insecure Modes**: Connect over TLS by default or opt out (e.g. for development environments).

# Usage Example

## Define a Metric Schema

First, define the Avro schema for your metrics. The schema must be a record. For example:

```json
{
  "type": "record",
  "name": "testMetrics",
  "fields": [
    {
      "name": "value",
      "type": "int"
    }
  ]
}
```
This schema describes a record named testMetrics with a single int field named value. The backend maps the record name to a table definition in ClickHouse.

## Create a MetricClient

he MetricClient is your main entry point. It manages connection, flushing, and streaming.

Required Parameters:

* `APIAddr`: The address of the metrics ingestion gRPC endpoint.
* `APIToken`: The authentication token to pass to the server.
* `ClusterID`: The unique identifier of the cluster associated with the metrics.
 
Optional Parameters:
* `Insecure`: Set to true to connect without TLS. Default: false.
* `FlushInterval`: How frequently metrics are flushed to the server. Default: 200ms.
* `MaxRetryInterval`: The maximum retry interval. Default: 15s.


Example:
```go
import (
    "context"
    "log"
    "time"

    "github.com/castai/logging"
    "github.com/castai/metrics"
)

func main() {
    handler := logging.NewTextHandler(logging.TextHandlerConfig{})
    logger := logging.New(handler)

    cfg := metrics.Config{
        APIAddr:       "127.0.0.1:50051",
        APIToken:      "your-api-token-here",
        ClusterID:     "your-cluster-id",
        Insecure:      false,                // default is secure (TLS)
        FlushInterval: 500 * time.Millisecond, // override the default flush interval
    }

    client, err := metrics.NewMetricClient(cfg, logger)
    if err != nil {
        log.Fatalf("failed to create metric client: %v", err)
    }

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // Start the flushing routine in the background.
    go func() {
        if err := client.Start(ctx); err != nil && err != context.Canceled {
            logger.Errorf("metric client stopped with error: %v", err)
        }
    }()
    
    // Continue your application logic...
}
```

## Define and Register a Metric

Use the typed `NewMetric[T]` function to define a metric of a particular Go type `T`, providing the Avro schema. Each `Metric` instance is associated with the client automatically.

Metric Options:
* `WithSchema[T]`: Provide an Avro schema string.
* `WithAvroSchema[T]`: Provide a pre-parsed Avro schema. (Either `WithSchema` or `WithAvroSchema` is required.)
* `WithMaxChunkSize[T]`: Control how large each chunk of metrics data can be before splitting into multiple requests. Default: 1MB.

Example:
```go
package main

import (
    "github.com/castai/metrics"
)


type TestMetrics struct {
    Value int `avro:"value"`
}

var schemaJSON = `{
  "type": "record",
  "name": "testMetrics",
  "fields": [
    { "name": "value", "type": "int" }
  ]
}`

func main() {
    // Create a client
    // ...
	
    // Create a new metric type with the specified schema.
    m, err := metrics.NewMetric[TestMetrics](client, metrics.WithSchema[TestMetrics](schemaJSON))
    if err != nil {
        log.Fatalf("failed to create metric: %v", err)
    }

    // Write some data points to the metric.
    err = m.Write(
        TestMetrics{Value: 1},
        TestMetrics{Value: 2},
        TestMetrics{Value: 3},
    )
    if err != nil {
        log.Printf("failed to write metrics: %v", err)
    }
}
```
Once written, metrics are stored in memory until the `MetricClient` flush interval elapses, at which point a streaming gRPC request is initiated to send all pending metrics.

## Flushing and Streaming


Automatic Flushing:

When `Start(ctx)` is called on the `MetricClient`, it begins a loop that triggers `collect()` at every flush interval.
During `collect()`, all metrics are read, split into one or more `WriteMetricsRequest` messages, and sent via a gRPC streaming RPC (WriteMetrics).
If the server returns an error, it's logged. The client continues to flush again at the next interval, ensuring that temporary server-side issues do not permanently stop the ingestion.


Retries:
* If establishing the streaming connection fails, the client will retry with exponential backoff for up to 15 seconds, configurable via `MaxRetryInterval`.
* If retries fail, an error is returned. If the context is still active after that, the loop will attempt again on the next tick.

## Chunking Behavior

If you write many data points at once, they might exceed the `maxChunkSize`. In that case:
* The metric data is split into multiple chunks.
* Each chunk is sent as a separate WriteMetricsRequest.
* The Rows metadata field tracks how many rows are in each chunk.


For example, if you have `WithMaxChunkSize[TestMetrics](128)` and you write metrics exceeding 128 bytes in one flush interval, this will produce at least two separate `WriteMetricsRequest` messages, each with its own chunk of data.

## Authentication and Metadata

Every gRPC call includes:
* `authorization` header with the APIToken.
* `x-cluster-id` header with the ClusterID.

The values of these headers are provided when initializing the `MetricClient`. Then they are automatically injected by unary and stream interceptor