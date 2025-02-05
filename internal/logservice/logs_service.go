package logservice

import (
	"context"

	"github.com/nickandreev/otlp-log-parser/internal/aggregator"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
)

type logsServiceServer struct {
	collogspb.UnimplementedLogsServiceServer
	agg           aggregator.Aggregator
	attributeName string
}

func NewLogsServiceServer(agg aggregator.Aggregator, attributeName string) *logsServiceServer {
	return &logsServiceServer{
		agg:           agg,
		attributeName: attributeName,
	}
}

func (s *logsServiceServer) Export(ctx context.Context, req *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error) {
	// Walk through resource attributes, scope attributes, and log records.
	// Add to the aggregator for each attribute that matches the attribute name.
	for _, rl := range req.GetResourceLogs() {
		for _, attr := range rl.GetResource().GetAttributes() {
			if attr.GetKey() == s.attributeName {
				s.agg.AddToKey(attr.GetValue().GetStringValue(), 1)
			}
		}

		for _, ill := range rl.GetScopeLogs() {
			for _, attrName := range ill.GetScope().GetAttributes() {
				if attrName.GetKey() == s.attributeName {
					s.agg.AddToKey(attrName.GetValue().GetStringValue(), 1)
				}
			}
			for _, lr := range ill.GetLogRecords() {
				for _, attrName := range lr.GetAttributes() {
					if attrName.GetKey() == s.attributeName {
						s.agg.AddToKey(attrName.GetValue().GetStringValue(), 1)
					}
				}
			}
		}
	}
	return &collogspb.ExportLogsServiceResponse{}, nil
}
