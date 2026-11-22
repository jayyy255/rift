// Package integration runs the real rift-broker and rift binaries.
package integration

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jayyy255/rift/pkg/client"
	"github.com/jayyy255/rift/tests/internal/report"
)

var binDir string

// TestMain builds the broker and CLI once for all tests.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "rift-integration-bin")
	if err != nil {
		panic(err)
	}
	binDir = dir
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Join(filepath.Dir(file), "..", "..")
	for _, name := range []string{"rift-broker", "rift"} {
		cmd := exec.Command("go", "build", "-o", filepath.Join(dir, name), "./cmd/"+name)
		cmd.Dir = root
		if out, err := cmd.CombinedOutput(); err != nil {
			fmt.Fprintf(os.Stderr, "build %s: %v\n%s", name, err, out)
			os.Exit(1)
		}
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

type brokerProc struct {
	t        *testing.T
	cmd      *exec.Cmd
	addr     string
	httpAddr string
	logPath  string
	exited   chan struct{}
}

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

// startBroker launches rift-broker on dataDir and waits until /readyz is ok.
func startBroker(t *testing.T, dataDir string, extra ...string) *brokerProc {
	t.Helper()
	p := &brokerProc{t: t, addr: freePort(t), httpAddr: freePort(t), exited: make(chan struct{})}
	p.logPath = filepath.Join(t.TempDir(), "broker.log")
	logFile, err := os.Create(p.logPath)
	if err != nil {
		t.Fatal(err)
	}
	args := append([]string{"--data-dir", dataDir, "--listen", p.addr, "--metrics-listen", p.httpAddr, "--log-level", "debug"}, extra...)
	p.cmd = exec.Command(filepath.Join(binDir, "rift-broker"), args...)
	p.cmd.Stdout, p.cmd.Stderr = logFile, logFile
	if err := p.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() {
		_ = p.cmd.Wait() // a killed broker exits non-zero by design
		logFile.Close()
		close(p.exited)
	}()
	t.Cleanup(func() {
		select {
		case <-p.exited:
		default:
			_ = p.cmd.Process.Kill()
			<-p.exited
		}
		if t.Failed() {
			p.dumpLog()
		}
	})

	deadline := time.Now().Add(15 * time.Second)
	for {
		resp, err := http.Get("http://" + p.httpAddr + "/readyz")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return p
			}
		}
		select {
		case <-p.exited:
			p.dumpLog()
			t.Fatalf("broker exited during startup: %v", p.cmd.ProcessState)
		default:
		}
		if time.Now().After(deadline) {
			p.dumpLog()
			t.Fatal("broker did not become ready")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (p *brokerProc) dumpLog() {
	data, _ := os.ReadFile(p.logPath)
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) > 40 {
		lines = lines[len(lines)-40:]
	}
	p.t.Logf("broker log tail:\n%s", strings.Join(lines, "\n"))
}

func (p *brokerProc) kill() {
	p.t.Helper()
	if err := p.cmd.Process.Signal(syscall.SIGKILL); err != nil {
		p.t.Fatal(err)
	}
	<-p.exited
}

func (p *brokerProc) terminate() {
	p.t.Helper()
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		p.t.Fatal(err)
	}
	select {
	case <-p.exited:
	case <-time.After(15 * time.Second):
		p.t.Fatal("broker did not exit after SIGTERM")
	}
	if !p.cmd.ProcessState.Success() {
		p.t.Fatalf("broker exited with %v after SIGTERM", p.cmd.ProcessState)
	}
}

func (p *brokerProc) client(t *testing.T) *client.Client {
	t.Helper()
	c, err := client.Dial(p.addr, client.WithTimeout(5*time.Second), client.WithRetry(3, 20*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

type ackedRecord struct {
	partition int32
	offset    int64
	key       string
	value     string
}

type cycleResult struct {
	Cycle            int   `json:"cycle"`
	KilledAfterMs    int64 `json:"killed_after_ms"`
	AckedRecords     int   `json:"acked_records_total"`
	LogRecords       int64 `json:"log_records_total"`
	CommittedOffsets int   `json:"committed_partitions"`
	RecoveryMs       int64 `json:"restart_to_ready_ms"`
}

// TestR1_BrokerCrashRecovery kills the broker process with SIGKILL while
// producers publish FSYNCED records and a consumer group commits offsets,
// then restarts it and checks that:
//   - every acknowledged record is readable at its acknowledged offset with
//     its exact key and value;
//   - each partition is a contiguous sequence of valid records;
//   - records sharing a key stay in publish order;
//   - committed offsets never exceed the partition's end offset;
//   - the broker accepts publishes again after recovery.
func TestR1_BrokerCrashRecovery(t *testing.T) {
	const topic, partitions, producers = "orders", 3, 4
	cycles := 4
	if testing.Short() {
		cycles = 2
	}
	dataDir := t.TempDir()
	seed := uint64(time.Now().UnixNano())
	rng := rand.New(rand.NewPCG(seed, 2))
	t.Logf("seed %d", seed)

	var (
		mu      sync.Mutex
		acked   []ackedRecord
		results []cycleResult
	)
	b := startBroker(t, dataDir, "--segment-bytes", "65536")
	if _, err := b.client(t).CreateTopic(t.Context(), topic, client.TopicConfig{Partitions: partitions}); err != nil {
		t.Fatal(err)
	}

	for cycle := 1; cycle <= cycles; cycle++ {
		c := b.client(t)
		ctx, stopLoad := context.WithCancel(t.Context())
		var wg sync.WaitGroup
		for p := range producers {
			wg.Go(func() {
				for seq := 0; ctx.Err() == nil; seq++ {
					key := fmt.Sprintf("c%d-p%d-k%d", cycle, p, seq%5)
					value := fmt.Sprintf("%s-seq%06d", key, seq)
					mds, err := c.Publish(ctx, topic, client.DurabilityFsynced, client.Record{Key: []byte(key), Value: []byte(value)})
					if err != nil {
						return // broker killed: stop this producer; unacked records may or may not exist
					}
					mu.Lock()
					acked = append(acked, ackedRecord{mds[0].Partition, mds[0].Offset, key, value})
					mu.Unlock()
				}
			})
		}
		wg.Go(func() { // a consumer group that commits as it goes
			cons, err := c.NewConsumer(ctx, client.ConsumerConfig{Topic: topic, Group: "auditors", MaxWait: 50 * time.Millisecond})
			if err != nil {
				return
			}
			for ctx.Err() == nil {
				if _, err := cons.Poll(ctx); err != nil {
					return
				}
				if err := cons.Commit(ctx); err != nil {
					return
				}
			}
		})

		run := time.Duration(300+rng.IntN(700)) * time.Millisecond
		time.Sleep(run)
		b.kill()
		stopLoad()
		wg.Wait()

		restart := time.Now()
		b = startBroker(t, dataDir, "--segment-bytes", "65536")
		res := cycleResult{Cycle: cycle, KilledAfterMs: run.Milliseconds(), RecoveryMs: time.Since(restart).Milliseconds()}
		mu.Lock()
		res.AckedRecords = len(acked)
		snapshot := append([]ackedRecord(nil), acked...)
		mu.Unlock()
		res.LogRecords, res.CommittedOffsets = verifyBrokerState(t, b.client(t), topic, snapshot)
		results = append(results, res)
		t.Logf("cycle %d: killed after %v; %d acked records verified; %d records in log; restart-to-ready %dms",
			cycle, run, res.AckedRecords, res.LogRecords, res.RecoveryMs)
		if res.AckedRecords == 0 {
			t.Fatalf("cycle %d: no records acknowledged; test exercised nothing", cycle)
		}
	}

	// The recovered broker accepts writes at the end of each partition.
	c := b.client(t)
	_, parts, _ := c.DescribeTopic(t.Context(), topic)
	for _, p := range parts {
		pi := p.Partition
		mds, err := c.Publish(t.Context(), topic, client.DurabilityFsynced, client.Record{Value: []byte("after"), Partition: &pi})
		if err != nil || mds[0].Offset != p.HighWatermark {
			t.Fatalf("publish after recovery: %+v %v (end was %d)", mds, err, p.HighWatermark)
		}
	}
	report.Write(t, "r1-broker-crash", map[string]any{
		"test": "TestR1_BrokerCrashRecovery", "passed": !t.Failed(), "seed": seed, "cycles": results,
	})
}

func verifyBrokerState(t *testing.T, c *client.Client, topic string, acked []ackedRecord) (int64, int) {
	t.Helper()
	ctx := t.Context()
	_, parts, err := c.DescribeTopic(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	logs := map[int32][]client.Message{}
	var total int64
	for _, p := range parts {
		cons, err := c.NewConsumer(ctx, client.ConsumerConfig{Topic: topic, Partitions: []int32{p.Partition}, Start: client.StartEarliest, MaxWait: time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		var msgs []client.Message
		for int64(len(msgs)) < p.HighWatermark {
			batch, err := cons.Poll(ctx)
			if err != nil {
				t.Fatalf("partition %d: read: %v", p.Partition, err)
			}
			if len(batch) == 0 {
				t.Fatalf("partition %d: read stalled at %d of %d", p.Partition, len(msgs), p.HighWatermark)
			}
			msgs = append(msgs, batch...)
		}
		for i, m := range msgs {
			if m.Offset != int64(i) {
				t.Fatalf("partition %d: offset gap at %d (found %d)", p.Partition, i, m.Offset)
			}
		}
		logs[p.Partition] = msgs
		total += int64(len(msgs))

		// Records with the same key appear in publish (sequence) order.
		lastSeq := map[string]string{}
		for _, m := range msgs {
			k, v := string(m.Key), string(m.Value)
			if prev, ok := lastSeq[k]; ok && v <= prev {
				t.Fatalf("partition %d: key %s out of order: %q after %q", p.Partition, k, v, prev)
			}
			lastSeq[k] = v
		}
	}
	for _, a := range acked {
		msgs := logs[a.partition]
		if a.offset >= int64(len(msgs)) {
			t.Fatalf("acknowledged record %s at %d/%d lost (partition end %d)", a.value, a.partition, a.offset, len(msgs))
		}
		if m := msgs[a.offset]; string(m.Key) != a.key || string(m.Value) != a.value {
			t.Fatalf("offset %d/%d holds %q, acknowledged %q", a.partition, a.offset, m.Value, a.value)
		}
	}
	committed, err := c.CommittedOffsets(ctx, "auditors", topic)
	if err != nil {
		t.Fatal(err)
	}
	for p, off := range committed {
		if off > int64(len(logs[p])) {
			t.Fatalf("committed offset %d for partition %d exceeds end %d", off, p, len(logs[p]))
		}
	}
	return total, len(committed)
}

// TestR1_GracefulShutdownKeepsWrittenRecords checks that SIGTERM flushes
// DURABILITY_WRITTEN records, which are not fsynced at acknowledgment time.
func TestR1_GracefulShutdownKeepsWrittenRecords(t *testing.T) {
	dataDir := t.TempDir()
	b := startBroker(t, dataDir, "--flush-interval", "1h") // no background flush
	c := b.client(t)
	if _, err := c.CreateTopic(t.Context(), "events", client.TopicConfig{Partitions: 2}); err != nil {
		t.Fatal(err)
	}
	p := c.NewProducer("events", client.ProducerConfig{Durability: client.DurabilityWritten})
	for i := range 500 {
		if err := p.SendAsync(t.Context(), client.Record{Value: []byte(fmt.Sprint(i))}, func(_ client.RecordMetadata, err error) {
			if err != nil {
				t.Error(err)
			}
		}); err != nil {
			t.Fatal(err)
		}
	}
	p.Close()
	_, parts, _ := c.DescribeTopic(t.Context(), "events")
	if parts[0].SyncedOffset+parts[1].SyncedOffset == 500 {
		t.Log("note: all records were already synced (segment roll or fast flush)")
	}
	b.terminate()

	b = startBroker(t, dataDir)
	_, parts, err := b.client(t).DescribeTopic(t.Context(), "events")
	if err != nil {
		t.Fatal(err)
	}
	if got := parts[0].HighWatermark + parts[1].HighWatermark; got != 500 {
		t.Fatalf("after graceful restart %d records remain, want 500", got)
	}
	logData, _ := os.ReadFile(b.logPath)
	if bytes.Contains(logData, []byte("truncated tail")) {
		t.Fatal("graceful shutdown left a torn tail")
	}
}

// TestR1_CLIAgainstBrokerProcess drives the target CLI experience from the
// specification against a real broker process.
func TestR1_CLIAgainstBrokerProcess(t *testing.T) {
	b := startBroker(t, t.TempDir())
	rift := func(stdin string, args ...string) string {
		t.Helper()
		cmd := exec.Command(filepath.Join(binDir, "rift"), append([]string{"--broker", b.addr}, args...)...)
		cmd.Stdin = strings.NewReader(stdin)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("rift %s: %v\n%s", strings.Join(args, " "), err, out)
		}
		return string(out)
	}
	if out := rift("", "cluster", "status"); !strings.Contains(out, "BROKER_STATE_ALIVE") {
		t.Errorf("cluster status:\n%s", out)
	}
	rift("", "topic", "create", "orders", "--partitions", "6", "--replicas", "1")
	if out := rift("", "publish", "orders", "--key", "customer-17", "--data", `{"order_id":101}`); !strings.Contains(out, "Published to orders") {
		t.Errorf("publish:\n%s", out)
	}
	rift("a\nb\nc\n", "publish", "orders", "--key", "customer-17", "--stdin")
	out := rift("", "consume", "orders", "--group", "analytics", "--mode", "stream", "--max-messages", "4")
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 4 || !strings.Contains(lines[0], `{"order_id":101}`) || !strings.HasSuffix(lines[3], " c") {
		t.Errorf("consume (same key keeps order):\n%s", out)
	}
	if out := rift("", "topic", "describe", "orders"); strings.Count(out, "rift-1") != 6 {
		t.Errorf("describe:\n%s", out)
	}
	resp, err := http.Get("http://" + b.httpAddr + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(resp.Body); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), `rift_consumer_group_lag{group="analytics"`) {
		t.Error("consumer lag metric missing after CLI consume")
	}
}
