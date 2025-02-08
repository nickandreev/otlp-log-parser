package logservice

import (
	"context"

	"github.com/nickandreev/otlp-log-parser/internal/aggregator"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	v1 "go.opentelemetry.io/proto/otlp/common/v1"
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
	// a map to store the found attributes values
	found := map[string]struct{}{}

	// walk through resource attributes, scope attributes, and log records
	for _, resourceLogs := range req.GetResourceLogs() {
		if res := resourceLogs.GetResource(); res != nil {
			if attr, ok := findAttribute(res.GetAttributes(), s.attributeName); ok {
				found[attr] = struct{}{}
			}
		}

		for _, scopeLogs := range resourceLogs.GetScopeLogs() {
			if scope := scopeLogs.GetScope(); scope != nil {
				if attr, ok := findAttribute(scope.GetAttributes(), s.attributeName); ok {
					found[attr] = struct{}{}
				}
			}
			for _, logRecord := range scopeLogs.GetLogRecords() {
				if attr, ok := findAttribute(logRecord.GetAttributes(), s.attributeName); ok {
					found[attr] = struct{}{}
				}
			}
		}
	}

	for k := range found {
		s.agg.AddToKey(k, 1)
	}

	if len(found) == 0 {
		s.agg.AddToKey("unknown", 1)
	}

	return &collogspb.ExportLogsServiceResponse{}, nil
}

func findAttribute(attrs []*v1.KeyValue, attributeName string) (string, bool) {
	for _, attr := range attrs {
		if attr.GetKey() == attributeName {
			if attr.GetValue() != nil {
				return attr.GetValue().GetStringValue(), true
			}
		}
	}
	return "", false
}
