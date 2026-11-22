package cli

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net"
	"strings"
	"testing"

	"github.com/jayyy255/rift/internal/broker"
)

func startBroker(t *testing.T) string {
	t.Helper()
	b, err := broker.New(broker.Config{
		NodeID: "node-7", DataDir: t.TempDir(), RetentionCheckInterval: -1, FlushInterval: -1,
		Logger:    slog.New(slog.DiscardHandler),
		DiskUsage: func(string) (broker.DiskStats, error) { return broker.DiskStats{Total: 10, Available: 9}, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = b.Serve(lis) }()
	t.Cleanup(func() { b.Close() })
	return lis.Addr().String()
}

func run(t *testing.T, addr, stdin string, args ...string) (string, error) {
	t.Helper()
	var out bytes.Buffer
	cmd := NewRootCommand()
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetIn(strings.NewReader(stdin))
	cmd.SetArgs(append([]string{"--broker", addr}, args...))
	err := cmd.ExecuteContext(t.Context())
	return out.String(), err
}

func mustRun(t *testing.T, addr, stdin string, args ...string) string {
	t.Helper()
	out, err := run(t, addr, stdin, args...)
	if err != nil {
		t.Fatalf("rift %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return out
}

func TestClusterStatus(t *testing.T) {
	addr := startBroker(t)
	out := mustRun(t, addr, "", "cluster", "status")
	if !strings.Contains(out, "node-7") || !strings.Contains(out, "BROKER_STATE_ALIVE") {
		t.Errorf("unexpected output:\n%s", out)
	}
}

func TestTopicCommands(t *testing.T) {
	addr := startBroker(t)
	out := mustRun(t, addr, "", "topic", "create", "orders", "--partitions", "6", "--retention", "1h")
	if !strings.Contains(out, `Created topic "orders" with 6 partition(s)`) {
		t.Errorf("create output:\n%s", out)
	}
	if out, err := run(t, addr, "", "topic", "create", "orders", "--replicas", "3"); err == nil {
		t.Errorf("replicas 3 accepted:\n%s", out)
	}
	out = mustRun(t, addr, "", "topic", "list")
	if !strings.Contains(out, "orders") || !strings.Contains(out, "1h0m0s") || !strings.Contains(out, "default") {
		t.Errorf("list output:\n%s", out)
	}
	out = mustRun(t, addr, "", "topic", "describe", "orders")
	if strings.Count(out, "node-7") != 6 {
		t.Errorf("describe output:\n%s", out)
	}
	mustRun(t, addr, "", "topic", "delete", "orders")
	if _, err := run(t, addr, "", "topic", "describe", "orders"); err == nil {
		t.Error("describe of deleted topic succeeded")
	}
}

func TestPublishAndConsumeWithGroup(t *testing.T) {
	addr := startBroker(t)
	mustRun(t, addr, "", "topic", "create", "orders", "--partitions", "1")
	out := mustRun(t, addr, "", "publish", "orders", "--key", "customer-17", "--data", `{"order_id":101}`, "--header", "src=test")
	if !strings.Contains(out, "partition 0 at offset 0") {
		t.Errorf("publish output:\n%s", out)
	}
	out = mustRun(t, addr, "line1\nline2\nline3\nline4\n", "publish", "orders", "--stdin", "--durability", "written")
	if !strings.Contains(out, "Published 4 record(s)") {
		t.Errorf("stdin publish output:\n%s", out)
	}

	out = mustRun(t, addr, "", "consume", "orders", "--group", "analytics", "--max-messages", "3")
	if strings.Count(out, "\n") != 3 || !strings.Contains(out, `[orders/0@0] key=customer-17 {"order_id":101}`) {
		t.Errorf("first consume output:\n%s", out)
	}
	// The group resumes after the three printed records, not after the
	// whole fetched batch.
	out = mustRun(t, addr, "", "consume", "orders", "--group", "analytics", "--max-messages", "2", "--output", "json")
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 2 {
		t.Fatalf("second consume output:\n%s", out)
	}
	var m struct {
		Offset int64   `json:"offset"`
		Key    *string `json:"key"`
		Value  string  `json:"value"`
	}
	if err := json.Unmarshal([]byte(lines[0]), &m); err != nil || m.Offset != 3 || m.Key != nil || m.Value != "line3" {
		t.Fatalf("resumed record = %+v (%v)", m, err)
	}

	// Without a group, consumption starts at the beginning and stops when idle.
	out = mustRun(t, addr, "", "consume", "orders", "--idle-timeout", "300ms")
	if strings.Count(out, "\n") != 5 {
		t.Errorf("idle consume printed:\n%s", out)
	}
}

func TestCommandValidation(t *testing.T) {
	addr := startBroker(t)
	mustRun(t, addr, "", "topic", "create", "t")
	for _, args := range [][]string{
		{"publish", "t"}, // neither --data nor --stdin
		{"publish", "t", "--data", "x", "--durability", "x"}, // bad durability
		{"publish", "t", "--data", "x", "--header", "novalue"},
		{"consume", "t", "--mode", "queue"},
		{"consume", "t", "--from", "committed"}, // needs --group
		{"consume", "t", "--output", "xml"},
		{"partition", "rebalance", "--dry-run"},
		{"benchmark", "run"},
	} {
		if out, err := run(t, addr, "", args...); err == nil {
			t.Errorf("rift %s succeeded:\n%s", strings.Join(args, " "), out)
		}
	}
	_, err := run(t, addr, "", "consume", "t", "--mode", "queue")
	if err == nil || !strings.Contains(err.Error(), "R5") {
		t.Errorf("queue mode error = %v, want milestone R5", err)
	}
}
