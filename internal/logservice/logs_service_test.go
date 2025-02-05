package logservice

import (
	"context"
	"testing"

	"github.com/nickandreev/otlp-log-parser/internal/aggregator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

func createTestRequest(resourceAttrs, scopeAttrs, logAttrs map[string]string) *collogspb.ExportLogsServiceRequest {
	req := &collogspb.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{
			{
				Resource: &resourcepb.Resource{
					Attributes: makeKeyValues(resourceAttrs),
				},
				ScopeLogs: []*logspb.ScopeLogs{
					{
						Scope: &commonpb.InstrumentationScope{
							Attributes: makeKeyValues(scopeAttrs),
						},
						LogRecords: []*logspb.LogRecord{
							{
								Attributes: makeKeyValues(logAttrs),
							},
						},
					},
				},
			},
		},
	}
	return req
}

func makeKeyValues(attrs map[string]string) []*commonpb.KeyValue {
	if len(attrs) == 0 {
		return nil
	}
	kvs := make([]*commonpb.KeyValue, 0, len(attrs))
	for k, v := range attrs {
		kvs = append(kvs, &commonpb.KeyValue{
			Key: k,
			Value: &commonpb.AnyValue{
				Value: &commonpb.AnyValue_StringValue{
					StringValue: v,
				},
			},
		})
	}
	return kvs
}

func TestLogsServiceServer_Export(t *testing.T) {
	tests := []struct {
		name           string
		attributeKey   string
		resourceAttrs  map[string]string
		scopeAttrs     map[string]string
		logAttrs       map[string]string
		expectedCounts map[string]int
	}{
		{
			name:         "resource attributes",
			attributeKey: "foo",
			resourceAttrs: map[string]string{
				"foo": "bar",
				"baz": "ignored",
			},
			expectedCounts: map[string]int{
				"bar": 1,
			},
		},
		{
			name:         "scope attributes",
			attributeKey: "foo",
			scopeAttrs: map[string]string{
				"foo": "bar",
				"baz": "ignored",
			},
			expectedCounts: map[string]int{
				"bar": 1,
			},
		},
		{
			name:         "log record attributes",
			attributeKey: "foo",
			logAttrs: map[string]string{
				"foo": "bar",
				"baz": "ignored",
			},
			expectedCounts: map[string]int{
				"bar": 1,
			},
		},
		{
			name:         "attribute found in multiple places",
			attributeKey: "foo",
			resourceAttrs: map[string]string{
				"foo": "bar",
			},
			scopeAttrs: map[string]string{
				"foo": "baz",
			},
			logAttrs: map[string]string{
				"foo": "qux",
			},
			expectedCounts: map[string]int{
				"bar": 1,
				"baz": 1,
				"qux": 1,
			},
		},
		{
			name:         "no matching attribute",
			attributeKey: "foo",
			resourceAttrs: map[string]string{
				"other": "value",
			},
			expectedCounts: map[string]int{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			agg := aggregator.NewSimpleSyncCounterAggregator()
			srv := NewLogsServiceServer(agg, tt.attributeKey)

			req := createTestRequest(tt.resourceAttrs, tt.scopeAttrs, tt.logAttrs)
			_, err := srv.Export(context.Background(), req)
			require.NoError(t, err)

			counts := agg.SnapshotAndReset()
			assert.Equal(t, tt.expectedCounts, counts)
		})
	}
}
