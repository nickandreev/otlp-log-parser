package main

import (
	"flag"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/nickandreev/otlp-log-parser/internal/aggregator"
	"github.com/nickandreev/otlp-log-parser/internal/exporter"
	"github.com/nickandreev/otlp-log-parser/internal/logservice"
	"github.com/nickandreev/otlp-log-parser/internal/server"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
	"go.uber.org/zap"
)

var res = resource.NewWithAttributes(
	semconv.SchemaURL,
	semconv.ServiceNameKey.String("otlp-log-parser"),
	semconv.ServiceNamespaceKey.String("dash0-exercise"),
	semconv.ServiceVersionKey.String("1.0.0"),
)

// setupOTel bootstraps the OpenTelemetry metrics pipeline using the Prometheus exporter.
func setupOTel() (*prometheus.Exporter, http.Handler, error) {
	// Create Prometheus exporter
	exporter, err := prometheus.New()
	if err != nil {
		return nil, nil, err
	}

	// Create a MeterProvider using the Prometheus exporter as a reader
	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(exporter),
	)
	otel.SetMeterProvider(meterProvider)

	return exporter, promhttp.Handler(), nil
}

func main() {
	listenAddr := flag.String("listen", ":4317", "The address to listen on for gRPC requests")
	attributeKey := flag.String("attribute", "foo", "The attribute key to aggregate on")
	windowDuration := flag.Duration("window", 10*time.Second, "The duration of the aggregation window")
	flag.Parse()

	// Initialize zap logger.
	logger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	defer logger.Sync()

	// Handle graceful shutdown.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Initialize OpenTelemetry metrics.
	_, metricsHandler, err := setupOTel()
	if err != nil {
		logger.Fatal("Failed to setup OpenTelemetry metrics", zap.Error(err))
	}

	// Create a meter and an instrument for counting aggregated prints.
	meter := otel.Meter("otlp-log-parser")

	wg := &sync.WaitGroup{}

	// Start Prometheus metrics server
	go func() {
		metricsAddr := ":2222" // Port for Prometheus to scrape metrics.
		logger.Info("Starting Prometheus metrics server", zap.String("addr", metricsAddr))
		if err := http.ListenAndServe(metricsAddr, metricsHandler); err != nil {
			logger.Fatal("Failed to start metrics server", zap.Error(err))
		}
	}()

	// Create aggregator and logs service.
	agg := aggregator.NewSimpleSyncCounterAggregator()
	logsService := logservice.NewLogsServiceServer(agg, *attributeKey)

	// Create and start server.
	srv, err := server.New(*listenAddr, logsService, logger)
	if err != nil {
		logger.Fatal("Failed to create server", zap.Error(err))
	}

	// Create and start log exporter.
	logExporter, err := exporter.NewLogExporter(agg, *windowDuration, logger, meter)
	if err != nil {
		logger.Fatal("Failed to create log exporter", zap.Error(err))
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := logExporter.Start(); err != nil {
			logger.Error("Failed to start log exporter", zap.Error(err))
		}
	}()

	// Start server in a separate goroutine.
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := srv.Start(); err != nil {
			logger.Error("Server error", zap.Error(err))
		}
	}()

	<-sigChan
	logger.Info("Shutting down server...")
	srv.Stop()
	logger.Info("Shutting down log exporter...")
	logExporter.Stop()

	logger.Info("Shutdown complete")
}
