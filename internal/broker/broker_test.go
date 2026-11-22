package broker

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/test/bufconn"

	riftv1 "github.com/jayyy255/rift/api/gen/rift/v1"
)

func dialBufconn(t *testing.T, nodeID string) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv, _ := NewGRPCServer(nodeID, "bufconn")
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func TestGetStatusReportsSelf(t *testing.T) {
	conn := dialBufconn(t, "node-1")
	resp, err := riftv1.NewClusterServiceClient(conn).GetStatus(t.Context(), &riftv1.GetStatusRequest{})
	if err != nil {
		t.Fatalf("GetStatus: %v", err)
	}
	if resp.GetNodeId() != "node-1" {
		t.Errorf("node_id = %q, want node-1", resp.GetNodeId())
	}
	if len(resp.GetBrokers()) != 1 || resp.GetBrokers()[0].GetState() != riftv1.BrokerState_BROKER_STATE_ALIVE {
		t.Errorf("brokers = %v, want one alive broker", resp.GetBrokers())
	}
}

func TestHealthServing(t *testing.T) {
	conn := dialBufconn(t, "node-1")
	resp, err := healthpb.NewHealthClient(conn).Check(t.Context(), &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Errorf("status = %v, want SERVING", resp.GetStatus())
	}
}

func TestMetricsEndpoint(t *testing.T) {
	srv := httptest.NewServer(newHTTPHandler("node-1"))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), `rift_build_info{node_id="node-1"`) {
		t.Errorf("status %d; body missing rift_build_info:\n%.500s", resp.StatusCode, body)
	}
}
