# otlp-log-parser

This is a test grpc server that implements otlp receiver logic and counts unique attributes values over specified time window.

## How to run
```bash
go run cmd/main.go --listen=":4317" --attribute=foo --window=10s
```

There is also a simple tool to send some load to the server
```bash
go run cmd/testclient/main.go --concurrency=2 --attribute=foo --duration=5s
```

## Architecture
The application consists of several key components:
- `server`: Handles gRPC server setup with metrics and logging middleware
- `logsservice`: Implements the OTLP logs service protocol and searches for attributes
- `aggregator`: Provides thread-safe attribute value counting
- `exporter`: Periodically logs aggregated values

## Assumptions:
- If specified attribute name found in multiple places in one request (either in Resource, InstrumentationScope or Log record attributes), all unique attribute values will be counted
- Counter should be reset after specified time window
- String values only for tracked attributes
- Attribute values never equal "unknown"
- No auth required fpr gRPC server
