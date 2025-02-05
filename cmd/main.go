package main

import (
	"context"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nickandreev/otlp-log-parser/internal/aggregator"
	"github.com/nickandreev/otlp-log-parser/internal/logservice"
	"github.com/nickandreev/otlp-log-parser/internal/server"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
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
	// Command line flags.
	listenAddr := flag.String("listen", ":4317", "The address to listen on for gRPC requests")
	attributeKey := flag.String("attribute", "foo", "The attribute key to aggregate on")
	windowDuration := flag.Duration("window", 10*time.Second, "The duration of the aggregation window")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize zap logger.
	logger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	defer logger.Sync()

	// Initialize OpenTelemetry metrics.
	exporter, metricsHandler, err := setupOTel()
	if err != nil {
		logger.Fatal("Failed to setup OpenTelemetry metrics", zap.Error(err))
	}
	defer func() {
		if err := exporter.Shutdown(context.Background()); err != nil {
			logger.Error("Error shutting down OpenTelemetry", zap.Error(err))
		}
	}()

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

	// Create a meter and an instrument for counting aggregated prints.
	meter := otel.Meter("otlp-log-parser")
	printCounter, err := meter.Int64Counter(
		"log_parser.aggregated_prints",
		metric.WithDescription("Number of times aggregated values printed"),
		metric.WithUnit("1"),
	)

	if err != nil {
		logger.Fatal("Failed to create print counter", zap.Error(err))
	}

	// Start periodic printing of aggregated values.
	go func() {
		ticker := time.NewTicker(*windowDuration)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				logger.Info("aggregated values",
					zap.String("attribute", *attributeKey),
					zap.Any("values", agg.Snapshot()))
				printCounter.Add(ctx, 1)
				agg.ResetAll()
			}
		}
	}()

	// Handle graceful shutdown.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start server in a separate goroutine.
	go func() {
		if err := srv.Start(); err != nil {
			logger.Error("Server error", zap.Error(err))
			cancel()
		}
	}()

	<-sigChan
	logger.Info("Shutting down gracefully...")
	srv.Stop()
}
