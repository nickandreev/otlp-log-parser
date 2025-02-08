package exporter

import (
	"context"
	"time"

	"github.com/nickandreev/otlp-log-parser/internal/aggregator"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

type Exporter interface {
	Start() error
	Stop() error
}

type logExporter struct {
	agg           aggregator.Aggregator
	logger        *zap.Logger
	ticker        *time.Ticker
	printCounter  metric.Int64Counter
	ctx           context.Context
	cancel        context.CancelFunc
}

func NewLogExporter(agg aggregator.Aggregator, interval time.Duration, logger *zap.Logger, meter metric.Meter) (Exporter, error) {
	counter, err := meter.Int64Counter(
		"log_parser.log_prints",
		metric.WithDescription("Number of times log prints"),
		metric.WithUnit("1"),
	)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &logExporter{
		agg:           agg,
		logger:        logger,
		ticker:        time.NewTicker(interval),
		printCounter:  counter,
		ctx:           ctx,
		cancel:        cancel,
	}, nil
}

func (e *logExporter) Start() error {
	for {
		select {
		case <-e.ctx.Done():
			return nil
		case <-e.ticker.C:
			snapshot := e.agg.SnapshotAndReset()
			e.logger.Info("aggregated values",
				zap.Any("values", snapshot))
			e.printCounter.Add(e.ctx, 1)
		}
	}
}

func (e *logExporter) Stop() error {
	e.ticker.Stop()
	e.cancel()
	return nil
}
