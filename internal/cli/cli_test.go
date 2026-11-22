package cli

import (
	"bytes"
	"net"
	"strings"
	"testing"

	"github.com/jayyy255/rift/internal/broker"
)

func run(t *testing.T, args ...string) (string, error) {
	t.Helper()
	var out bytes.Buffer
	cmd := NewRootCommand()
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := cmd.ExecuteContext(t.Context())
	return out.String(), err
}

func TestClusterStatusAgainstBroker(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv, _ := broker.NewGRPCServer("node-7", lis.Addr().String())
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	out, err := run(t, "--broker", lis.Addr().String(), "cluster", "status")
	if err != nil {
		t.Fatalf("cluster status: %v\n%s", err, out)
	}
	if !strings.Contains(out, "node-7") || !strings.Contains(out, "BROKER_STATE_ALIVE") {
		t.Errorf("unexpected output:\n%s", out)
	}
}

func TestPlannedCommandsReportMilestone(t *testing.T) {
	_, err := run(t, "topic", "create", "orders", "--partitions", "6", "--replicas", "3")
	if err == nil || !strings.Contains(err.Error(), "R1") {
		t.Fatalf("err = %v, want not-implemented error naming R1", err)
	}
}
