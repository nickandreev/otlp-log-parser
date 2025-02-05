package server

import (
	"context"
	"net"
	"runtime/debug"

	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Server encapsulates the gRPC server and its dependencies.
type Server struct {
	grpcServer *grpc.Server
	listener   net.Listener
	logger     *zap.Logger
	metrics    *grpcprom.ServerMetrics
}

// interceptorLogger adapts zap logger to interceptor logger.
func interceptorLogger(l *zap.Logger) logging.Logger {
	return logging.LoggerFunc(func(ctx context.Context, lvl logging.Level, msg string, fields ...any) {
		// Convert fields to zap fields
		zapFields := make([]zap.Field, 0, len(fields)/2)
		for i := 0; i < len(fields); i += 2 {
			key, ok := fields[i].(string)
			if !ok {
				continue
			}
			zapFields = append(zapFields, zap.Any(key, fields[i+1]))
		}

		// Always log warn level only. Idea for improvement: make this configurable.
		if lvl >= logging.LevelWarn {
			l.Log(zap.WarnLevel, msg, zapFields...)
		}
	})
}

// New creates a new Server instance.
func New(listenAddr string, logsService collogspb.LogsServiceServer, logger *zap.Logger) (*Server, error) {
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, err
	}

	// Setup metrics
	srvMetrics := grpcprom.NewServerMetrics()
	reg := prometheus.DefaultRegisterer
	reg.MustRegister(srvMetrics)

	// Setup panic recovery handler
	panicRecoveryHandler := func(p any) error {
		logger.Error("recovered from panic",
			zap.Any("panic", p),
			zap.String("stack", string(debug.Stack())),
		)
		return status.Error(codes.Internal, "internal error")
	}

	// Create gRPC server with interceptors
	grpcServer := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(
			srvMetrics.UnaryServerInterceptor(),
			logging.UnaryServerInterceptor(interceptorLogger(logger)),
			recovery.UnaryServerInterceptor(recovery.WithRecoveryHandler(panicRecoveryHandler)),
		),
		grpc.ChainStreamInterceptor(
			srvMetrics.StreamServerInterceptor(),
			logging.StreamServerInterceptor(interceptorLogger(logger)),
			recovery.StreamServerInterceptor(recovery.WithRecoveryHandler(panicRecoveryHandler)),
		),
		grpc.Creds(insecure.NewCredentials()),
	)

	collogspb.RegisterLogsServiceServer(grpcServer, logsService)
	srvMetrics.InitializeMetrics(grpcServer)

	return &Server{
		grpcServer: grpcServer,
		listener:   lis,
		logger:     logger,
		metrics:    srvMetrics,
	}, nil
}

// Start begins serving requests and blocks until the server is stopped.
func (s *Server) Start() error {
	return s.grpcServer.Serve(s.listener)
}

// Stop gracefully stops the server.
func (s *Server) Stop() {
	s.grpcServer.GracefulStop()
}
