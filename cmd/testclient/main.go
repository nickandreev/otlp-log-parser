package main

import (
	"context"
	"flag"
	"log"
	"sync"
	"sync/atomic"
	"time"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	serverAddr := flag.String("server", "localhost:4317", "Server address")
	attributeKey := flag.String("attribute", "foo", "Attribute key to send")
	duration := flag.Duration("duration", 30*time.Second, "How long to run the test")
	concurrency := flag.Int("concurrency", 5, "Number of concurrent clients")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	var wg sync.WaitGroup
	var totalSent int64
	testValues := []string{"bar", "baz", "qux", "quux"}

	start := time.Now()
	log.Printf("Starting load test with %d clients for %v", *concurrency, *duration)

	// Start concurrent clients
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()

			conn, err := grpc.NewClient(*serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Printf("Client %d failed to connect: %v", clientID, err)
				return
			}
			defer conn.Close()

			client := collogspb.NewLogsServiceClient(conn)
			var sent int64

			for ctx.Err() == nil {
				req := &collogspb.ExportLogsServiceRequest{
					ResourceLogs: []*logspb.ResourceLogs{
						{
							Resource: &resourcepb.Resource{
								Attributes: []*commonpb.KeyValue{
									{
										Key: *attributeKey,
										Value: &commonpb.AnyValue{
											Value: &commonpb.AnyValue_StringValue{
												StringValue: testValues[int(sent)%len(testValues)],
											},
										},
									},
								},
							},
							ScopeLogs: []*logspb.ScopeLogs{
								{
									LogRecords: []*logspb.LogRecord{
										{
											TimeUnixNano: uint64(time.Now().UnixNano()),
											Body: &commonpb.AnyValue{
												Value: &commonpb.AnyValue_StringValue{
													StringValue: "test log message",
												},
											},
										},
									},
								},
							},
						},
					},
				}

				exportCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				if _, err := client.Export(exportCtx, req); err != nil {
					log.Printf("Client %d failed to send request: %v", clientID, err)
					cancel()
					continue
				}
				cancel()
				sent++
				atomic.AddInt64(&totalSent, 1)
			}
			log.Printf("Client %d finished, sent %d requests", clientID, sent)
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	total := atomic.LoadInt64(&totalSent)
	rps := float64(total) / elapsed.Seconds()
	log.Printf("Test completed. Total requests: %d, Duration: %v, Throughput: %.0f RPS", total, elapsed, rps)
}
