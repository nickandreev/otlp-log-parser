package exporter_test

import (
	"sync"
	"testing"
	"time"

	"github.com/nickandreev/otlp-log-parser/internal/aggregator"
	"github.com/nickandreev/otlp-log-parser/internal/exporter"
	"github.com/stretchr/testify/assert"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.uber.org/zap"
)

func TestLogExporter(t *testing.T) {
	// simple test to start and stop the exporter

	agg := aggregator.NewSimpleSyncCounterAggregator()
	meter := sdkmetric.NewMeterProvider().Meter("test")

	exporter, err := exporter.NewLogExporter(agg, time.Millisecond*100, zap.NewNop(), meter)
	assert.NoError(t, err)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		exporter.Start()
		wg.Done()
	}()

	agg.AddToKey("test", 1)
	time.Sleep(time.Millisecond * 600)

	exporter.Stop()
	wg.Wait()
	assert.Equal(t, 0, len(agg.SnapshotAndReset()))
}
