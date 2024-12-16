# Metrics Client Library

This library provides a gRPC-based metrics ingestion client. It allows you to define custom metric types, serialize them using [Avro](https://avro.apache.org/), and send them to a backend service using a streaming gRPC API. It handles automatic batching, chunking, retries, and error handling.