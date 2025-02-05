package server

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"go.uber.org/zap"
)

type mockLogsService struct {
	collogspb.UnimplementedLogsServiceServer
}

func TestServer_StartStop(t *testing.T) {
	// Get a random available port
	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	addr := listener.Addr().String()
	listener.Close()

	// Create server with mock service
	srv, err := New(addr, &mockLogsService{}, zap.NewNop())
	require.NoError(t, err)

	// Start server in goroutine
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Start()
	}()

	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	// Verify server is listening
	conn, err := net.Dial("tcp", addr)
	assert.NoError(t, err)
	if conn != nil {
		conn.Close()
	}

	// Stop server
	srv.Stop()

	// Verify server stopped without error
	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("server did not stop within timeout")
	}
}
