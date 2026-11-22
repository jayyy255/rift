package broker

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	riftv1 "github.com/jayyy255/rift/api/gen/rift/v1"
	"github.com/jayyy255/rift/internal/logstore"
)

type harness struct {
	t        *testing.T
	b        *Broker
	conn     *grpc.ClientConn
	topics   riftv1.TopicServiceClient
	producer riftv1.ProducerServiceClient
	consumer riftv1.ConsumerServiceClient
	dir      string
}

func testConfig(dir string) Config {
	return Config{
		NodeID:                 "node-1",
		DataDir:                dir,
		RetentionCheckInterval: -1,
		FlushInterval:          -1,
		Logger:                 slog.New(slog.DiscardHandler),
		DiskUsage:              func(string) (DiskStats, error) { return DiskStats{Total: 100, Available: 50}, nil },
	}
}

func start(t *testing.T, dir string, mutate ...func(*Config)) *harness {
	t.Helper()
	cfg := testConfig(dir)
	for _, m := range mutate {
		m(&cfg)
	}
	b, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	lis := bufconn.Listen(1 << 20)
	go func() { _ = b.Serve(lis) }()
	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{t: t, b: b, conn: conn, dir: dir,
		topics:   riftv1.NewTopicServiceClient(conn),
		producer: riftv1.NewProducerServiceClient(conn),
		consumer: riftv1.NewConsumerServiceClient(conn),
	}
	t.Cleanup(h.stop)
	return h
}

func (h *harness) stop() {
	h.conn.Close()
	if err := h.b.Close(); err != nil {
		h.t.Errorf("Close: %v", err)
	}
}

func (h *harness) createTopic(name string, partitions int32) {
	h.t.Helper()
	if _, err := h.topics.CreateTopic(h.t.Context(), &riftv1.CreateTopicRequest{Name: name, Partitions: partitions}); err != nil {
		h.t.Fatalf("CreateTopic: %v", err)
	}
}

func (h *harness) publish(topic string, d riftv1.Durability, recs ...*riftv1.PublishRecord) *riftv1.PublishResponse {
	h.t.Helper()
	resp, err := h.producer.Publish(h.t.Context(), &riftv1.PublishRequest{Topic: topic, Records: recs, Durability: d})
	if err != nil {
		h.t.Fatalf("Publish: %v", err)
	}
	return resp
}

func (h *harness) fetch(topic string, partition int32, offset int64) *riftv1.PartitionRecords {
	h.t.Helper()
	resp, err := h.consumer.Fetch(h.t.Context(), &riftv1.FetchRequest{
		Topic: topic, Partitions: []*riftv1.PartitionOffset{{Partition: partition, Offset: offset}},
	})
	if err != nil {
		h.t.Fatalf("Fetch: %v", err)
	}
	return resp.GetPartitions()[0]
}

func val(s string) *riftv1.PublishRecord { return &riftv1.PublishRecord{Value: []byte(s)} }

func keyed(k, v string) *riftv1.PublishRecord {
	return &riftv1.PublishRecord{Key: []byte(k), Value: []byte(v)}
}

func wantCode(t *testing.T, err error, code codes.Code) {
	t.Helper()
	if status.Code(err) != code {
		t.Fatalf("err = %v, want code %v", err, code)
	}
}

func scrape(t *testing.T, b *Broker) string {
	t.Helper()
	srv := httptest.NewServer(b.HTTPHandler())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return string(body)
}

func TestClusterStatusAndHealth(t *testing.T) {
	h := start(t, t.TempDir())
	resp, err := riftv1.NewClusterServiceClient(h.conn).GetStatus(t.Context(), &riftv1.GetStatusRequest{})
	if err != nil || resp.GetNodeId() != "node-1" || len(resp.GetBrokers()) != 1 {
		t.Fatalf("GetStatus = %v, %v", resp, err)
	}
	hc, err := healthpb.NewHealthClient(h.conn).Check(t.Context(), &healthpb.HealthCheckRequest{})
	if err != nil || hc.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("health = %v, %v", hc, err)
	}
}

func TestReadinessProbe(t *testing.T) {
	h := start(t, t.TempDir())
	srv := httptest.NewServer(h.b.HTTPHandler())
	defer srv.Close()
	get := func(path string) int {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	deadline := time.Now().Add(2 * time.Second)
	for get("/readyz") != http.StatusOK {
		if time.Now().After(deadline) {
			t.Fatal("never became ready")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if get("/healthz") != http.StatusOK {
		t.Fatal("healthz not ok")
	}
	h.b.Close()
	if code := get("/readyz"); code != http.StatusServiceUnavailable {
		t.Fatalf("readyz after close = %d, want 503", code)
	}
}

func TestTopicLifecycle(t *testing.T) {
	h := start(t, t.TempDir())
	ctx := t.Context()
	h.createTopic("orders", 3)
	_, err := h.topics.CreateTopic(ctx, &riftv1.CreateTopicRequest{Name: "orders"})
	wantCode(t, err, codes.AlreadyExists)
	_, err = h.topics.CreateTopic(ctx, &riftv1.CreateTopicRequest{Name: "bad/name"})
	wantCode(t, err, codes.InvalidArgument)
	_, err = h.topics.CreateTopic(ctx, &riftv1.CreateTopicRequest{Name: "replicated", ReplicationFactor: 3})
	wantCode(t, err, codes.Unimplemented)

	list, _ := h.topics.ListTopics(ctx, &riftv1.ListTopicsRequest{})
	if len(list.GetTopics()) != 1 || list.GetTopics()[0].GetPartitions() != 3 {
		t.Fatalf("ListTopics = %v", list)
	}
	desc, err := h.topics.DescribeTopic(ctx, &riftv1.DescribeTopicRequest{Name: "orders"})
	if err != nil || len(desc.GetPartitions()) != 3 || desc.GetPartitions()[2].GetLeader() != "node-1" {
		t.Fatalf("DescribeTopic = %v, %v", desc, err)
	}
	if _, err := h.topics.DeleteTopic(ctx, &riftv1.DeleteTopicRequest{Name: "orders"}); err != nil {
		t.Fatal(err)
	}
	_, err = h.topics.DescribeTopic(ctx, &riftv1.DescribeTopicRequest{Name: "orders"})
	wantCode(t, err, codes.NotFound)
	_, err = h.topics.DeleteTopic(ctx, &riftv1.DeleteTopicRequest{Name: "orders"})
	wantCode(t, err, codes.NotFound)
}

func TestPublishRoutingAndPerKeyOrder(t *testing.T) {
	h := start(t, t.TempDir())
	h.createTopic("orders", 4)

	// Same key across several batches: one partition, consecutive offsets.
	var part int32 = -1
	var last int64 = -1
	for batch := range 5 {
		var recs []*riftv1.PublishRecord
		for i := range 4 {
			recs = append(recs, keyed("customer-17", fmt.Sprintf("%d-%d", batch, i)))
		}
		for _, a := range h.publish("orders", 0, recs...).GetAcks() {
			if part == -1 {
				part = a.GetPartition()
			}
			if a.GetPartition() != part || a.GetOffset() != last+1 {
				t.Fatalf("ack %v breaks per-key order (partition %d, last offset %d)", a, part, last)
			}
			last = a.GetOffset()
		}
	}
	got := h.fetch("orders", part, 0)
	if len(got.GetRecords()) != 20 || string(got.GetRecords()[19].GetValue()) != "4-3" {
		t.Fatalf("fetched %d records", len(got.GetRecords()))
	}
	if string(got.GetRecords()[0].GetKey()) != "customer-17" {
		t.Fatalf("key not stored: %q", got.GetRecords()[0].GetKey())
	}

	// Keyless records spread round-robin over all partitions.
	seen := map[int32]bool{}
	var recs []*riftv1.PublishRecord
	for range 8 {
		recs = append(recs, val("x"))
	}
	for _, a := range h.publish("orders", 0, recs...).GetAcks() {
		seen[a.GetPartition()] = true
	}
	if len(seen) != 4 {
		t.Fatalf("keyless records used partitions %v", seen)
	}

	// Explicit partitions override routing.
	p := int32(2)
	ack := h.publish("orders", 0, &riftv1.PublishRecord{Key: []byte("customer-17"), Value: []byte("v"), Partition: &p}).GetAcks()[0]
	if ack.GetPartition() != 2 {
		t.Fatalf("explicit partition ignored: %v", ack)
	}
	bad := int32(9)
	_, err := h.producer.Publish(t.Context(), &riftv1.PublishRequest{Topic: "orders", Records: []*riftv1.PublishRecord{{Value: []byte("v"), Partition: &bad}}})
	wantCode(t, err, codes.InvalidArgument)
}

func TestPublishPreservesKeyPresenceAndHeaders(t *testing.T) {
	h := start(t, t.TempDir())
	h.createTopic("t", 1)
	h.publish("t", 0,
		&riftv1.PublishRecord{Value: []byte("keyless")},
		&riftv1.PublishRecord{Key: []byte{}, Value: []byte("empty key")},
		&riftv1.PublishRecord{Key: []byte("k"), Value: []byte("v"), Headers: []*riftv1.Header{{Key: "trace", Value: []byte("abc")}, {Key: "trace", Value: []byte("def")}}},
	)
	recs := h.fetch("t", 0, 0).GetRecords()
	if recs[0].Key != nil {
		t.Errorf("keyless record has key %q", recs[0].Key)
	}
	if recs[1].Key == nil || len(recs[1].Key) != 0 {
		t.Errorf("empty key lost presence: %v", recs[1].Key)
	}
	if hs := recs[2].GetHeaders(); len(hs) != 2 || string(hs[1].GetValue()) != "def" {
		t.Errorf("headers = %v", hs)
	}
	if recs[0].GetTimestampMs() == 0 {
		t.Error("timestamp not set")
	}
}

func TestPublishValidation(t *testing.T) {
	h := start(t, t.TempDir(), func(c *Config) { c.MaxRecordBytes = 1024; c.MaxBatchBytes = 4096 })
	h.createTopic("t", 1)
	ctx := t.Context()
	cases := []struct {
		name string
		req  *riftv1.PublishRequest
		code codes.Code
	}{
		{"unknown topic", &riftv1.PublishRequest{Topic: "nope", Records: []*riftv1.PublishRecord{val("x")}}, codes.NotFound},
		{"empty batch", &riftv1.PublishRequest{Topic: "t"}, codes.InvalidArgument},
		{"record too large", &riftv1.PublishRequest{Topic: "t", Records: []*riftv1.PublishRecord{{Value: make([]byte, 2000)}}}, codes.InvalidArgument},
		{"batch too large", &riftv1.PublishRequest{Topic: "t", Records: []*riftv1.PublishRecord{
			{Value: make([]byte, 900)}, {Value: make([]byte, 900)}, {Value: make([]byte, 900)}, {Value: make([]byte, 900)}, {Value: make([]byte, 900)}}}, codes.InvalidArgument},
		{"bad durability", &riftv1.PublishRequest{Topic: "t", Durability: 99, Records: []*riftv1.PublishRecord{val("x")}}, codes.InvalidArgument},
	}
	for _, tc := range cases {
		_, err := h.producer.Publish(ctx, tc.req)
		if status.Code(err) != tc.code {
			t.Errorf("%s: err = %v, want %v", tc.name, err, tc.code)
		}
	}
	// Nothing was appended by the rejected requests.
	if recs := h.fetch("t", 0, 0).GetRecords(); len(recs) != 0 {
		t.Fatalf("rejected publishes appended %d records", len(recs))
	}
	if m := scrape(t, h.b); !strings.Contains(m, `rift_produce_rejected_total{reason="too_large"} 2`) {
		t.Errorf("rejection metric missing:\n%s", grepLines(m, "rejected"))
	}
}

func TestPublishDurabilityLevels(t *testing.T) {
	h := start(t, t.TempDir())
	h.createTopic("t", 1)
	if d := h.publish("t", riftv1.Durability_DURABILITY_UNSPECIFIED, val("a")).GetDurability(); d != riftv1.Durability_DURABILITY_FSYNCED {
		t.Fatalf("default durability = %v, want FSYNCED", d)
	}
	desc, _ := h.topics.DescribeTopic(t.Context(), &riftv1.DescribeTopicRequest{Name: "t"})
	if p := desc.GetPartitions()[0]; p.GetSyncedOffset() < 1 {
		t.Fatalf("FSYNCED ack returned before sync: %v", p)
	}
	if d := h.publish("t", riftv1.Durability_DURABILITY_WRITTEN, val("b")).GetDurability(); d != riftv1.Durability_DURABILITY_WRITTEN {
		t.Fatalf("durability = %v, want WRITTEN", d)
	}
}

func TestFetchLongPollWakesOnPublish(t *testing.T) {
	h := start(t, t.TempDir())
	h.createTopic("t", 2)
	type result struct {
		resp    *riftv1.FetchResponse
		err     error
		elapsed time.Duration
	}
	done := make(chan result, 1)
	go func() {
		st := time.Now()
		resp, err := h.consumer.Fetch(t.Context(), &riftv1.FetchRequest{
			Topic: "t", MaxWaitMs: 10000,
			Partitions: []*riftv1.PartitionOffset{{Partition: 0}, {Partition: 1}},
		})
		done <- result{resp, err, time.Since(st)}
	}()
	time.Sleep(100 * time.Millisecond)
	p := int32(1)
	h.publish("t", 0, &riftv1.PublishRecord{Value: []byte("wake"), Partition: &p})
	r := <-done
	if r.err != nil {
		t.Fatal(r.err)
	}
	if r.elapsed > 5*time.Second {
		t.Fatalf("fetch waited %v instead of waking on publish", r.elapsed)
	}
	if recs := r.resp.GetPartitions()[1].GetRecords(); len(recs) != 1 || string(recs[0].GetValue()) != "wake" {
		t.Fatalf("fetched %v", r.resp)
	}
}

func TestFetchWaitExpires(t *testing.T) {
	h := start(t, t.TempDir())
	h.createTopic("t", 1)
	st := time.Now()
	resp, err := h.consumer.Fetch(t.Context(), &riftv1.FetchRequest{Topic: "t", MaxWaitMs: 200, Partitions: []*riftv1.PartitionOffset{{}}})
	if err != nil {
		t.Fatal(err)
	}
	if el := time.Since(st); el < 180*time.Millisecond || el > 3*time.Second {
		t.Fatalf("fetch returned after %v, want about 200ms", el)
	}
	if len(resp.GetPartitions()[0].GetRecords()) != 0 {
		t.Fatal("unexpected records")
	}
}

func TestFetchErrors(t *testing.T) {
	h := start(t, t.TempDir())
	h.createTopic("t", 1)
	h.publish("t", 0, val("a"), val("b"))
	pr := h.fetch("t", 0, 5)
	if pr.GetError() != riftv1.PartitionError_PARTITION_ERROR_OFFSET_OUT_OF_RANGE || pr.GetHighWatermark() != 2 {
		t.Fatalf("fetch beyond end = %v", pr)
	}
	pr = h.fetch("t", 3, 0)
	if pr.GetError() != riftv1.PartitionError_PARTITION_ERROR_UNKNOWN_PARTITION {
		t.Fatalf("fetch unknown partition = %v", pr)
	}
	_, err := h.consumer.Fetch(t.Context(), &riftv1.FetchRequest{Topic: "nope", Partitions: []*riftv1.PartitionOffset{{}}})
	wantCode(t, err, codes.NotFound)
	_, err = h.consumer.Fetch(t.Context(), &riftv1.FetchRequest{Topic: "t"})
	wantCode(t, err, codes.InvalidArgument)
	if pr := h.fetch("t", 0, 2); pr.GetError() != 0 || len(pr.GetRecords()) != 0 {
		t.Fatalf("fetch at end = %v", pr)
	}
}

func TestFetchLimits(t *testing.T) {
	h := start(t, t.TempDir())
	h.createTopic("t", 1)
	var recs []*riftv1.PublishRecord
	for range 100 {
		recs = append(recs, &riftv1.PublishRecord{Value: make([]byte, 100)})
	}
	h.publish("t", 0, recs...)
	resp, _ := h.consumer.Fetch(t.Context(), &riftv1.FetchRequest{Topic: "t", MaxBytes: 1000, Partitions: []*riftv1.PartitionOffset{{}}})
	if n := len(resp.GetPartitions()[0].GetRecords()); n < 1 || n > 10 {
		t.Fatalf("max_bytes 1000 returned %d records of ~140 bytes", n)
	}
	resp, _ = h.consumer.Fetch(t.Context(), &riftv1.FetchRequest{Topic: "t", MaxRecordsPerPartition: 7, Partitions: []*riftv1.PartitionOffset{{}}})
	if n := len(resp.GetPartitions()[0].GetRecords()); n != 7 {
		t.Fatalf("max_records 7 returned %d", n)
	}
}

func TestCommitOffsets(t *testing.T) {
	dir := t.TempDir()
	h := start(t, dir)
	h.createTopic("t", 2)
	h.publish("t", 0, val("a"), val("b"), val("c"), val("d"))
	ctx := t.Context()
	commit := func(group string, p int32, off int64) error {
		_, err := h.consumer.CommitOffsets(ctx, &riftv1.CommitOffsetsRequest{Group: group, Topic: "t",
			Offsets: []*riftv1.PartitionOffset{{Partition: p, Offset: off}}})
		return err
	}
	if err := commit("analytics", 0, 2); err != nil {
		t.Fatal(err)
	}
	wantCode(t, commit("analytics", 0, 99), codes.OutOfRange)
	wantCode(t, commit("../bad", 0, 1), codes.InvalidArgument)
	wantCode(t, commit("analytics", 7, 1), codes.NotFound)

	got, _ := h.consumer.GetCommittedOffsets(ctx, &riftv1.GetCommittedOffsetsRequest{Group: "analytics", Topic: "t"})
	if len(got.GetOffsets()) != 1 || got.GetOffsets()[0].GetOffset() != 2 {
		t.Fatalf("committed = %v", got)
	}
	if m := scrape(t, h.b); !strings.Contains(m, `rift_consumer_group_lag{group="analytics",partition="0",topic="t"} 0`) {
		t.Errorf("lag metric missing:\n%s", grepLines(m, "lag"))
	}

	h.stop()
	h = start(t, dir)
	got, _ = h.consumer.GetCommittedOffsets(t.Context(), &riftv1.GetCommittedOffsetsRequest{Group: "analytics", Topic: "t"})
	if len(got.GetOffsets()) != 1 || got.GetOffsets()[0].GetOffset() != 2 {
		t.Fatalf("committed after restart = %v", got)
	}
}

func TestDiskCriticalWatermarkRejectsPublishes(t *testing.T) {
	var mu sync.Mutex
	used := 0.97
	h := start(t, t.TempDir(), func(c *Config) {
		c.DiskUsage = func(string) (DiskStats, error) {
			mu.Lock()
			defer mu.Unlock()
			return DiskStats{Total: 1000, Available: uint64((1 - used) * 1000)}, nil
		}
	})
	h.createTopic("t", 1)
	_, err := h.producer.Publish(t.Context(), &riftv1.PublishRequest{Topic: "t", Records: []*riftv1.PublishRecord{val("x")}})
	wantCode(t, err, codes.ResourceExhausted)
	if m := scrape(t, h.b); !strings.Contains(m, "rift_disk_critical 1") {
		t.Errorf("disk critical gauge not set:\n%s", grepLines(m, "disk"))
	}
	// Reads keep working while publishes are rejected.
	h.fetch("t", 0, 0)

	mu.Lock()
	used = 0.5
	mu.Unlock()
	h.b.disk.check()
	h.publish("t", 0, val("x"))
}

// After a crash that loses the log tail, committed offsets beyond the new end
// are clamped so the group does not skip records later written there.
func TestRestartRecoversTruncatedTailAndClampsOffsets(t *testing.T) {
	dir := t.TempDir()
	h := start(t, dir)
	h.createTopic("t", 1)
	for i := range 10 {
		h.publish("t", 0, val(fmt.Sprint(i)))
	}
	if _, err := h.consumer.CommitOffsets(t.Context(), &riftv1.CommitOffsetsRequest{Group: "g", Topic: "t",
		Offsets: []*riftv1.PartitionOffset{{Partition: 0, Offset: 10}}}); err != nil {
		t.Fatal(err)
	}
	h.stop()

	partDir := filepath.Join(dir, "topics", "t", "0")
	files, _ := logstore.SegmentFiles(partDir)
	in, _ := logstore.InspectSegment(files[len(files)-1])
	cut := in.Frames[7].Position + 3 // tear record 7
	if err := os.Truncate(files[len(files)-1], cut); err != nil {
		t.Fatal(err)
	}

	h = start(t, dir)
	desc, _ := h.topics.DescribeTopic(t.Context(), &riftv1.DescribeTopicRequest{Name: "t"})
	if end := desc.GetPartitions()[0].GetHighWatermark(); end != 7 {
		t.Fatalf("recovered end = %d, want 7", end)
	}
	got, _ := h.consumer.GetCommittedOffsets(t.Context(), &riftv1.GetCommittedOffsetsRequest{Group: "g", Topic: "t"})
	if off := got.GetOffsets()[0].GetOffset(); off != 7 {
		t.Fatalf("committed offset = %d, want clamped to 7", off)
	}
	m := scrape(t, h.b)
	for _, want := range []string{"rift_storage_recovery_truncations_total 1", "rift_consumer_offsets_clamped_total 1"} {
		if !strings.Contains(m, want) {
			t.Errorf("missing %q in metrics:\n%s", want, grepLines(m, "recovery|clamped"))
		}
	}
	if ack := h.publish("t", 0, val("new")).GetAcks()[0]; ack.GetOffset() != 7 {
		t.Fatalf("next offset = %d, want 7", ack.GetOffset())
	}
}

func TestDeleteTopicWakesFetchAndRemovesOffsets(t *testing.T) {
	h := start(t, t.TempDir())
	h.createTopic("t", 1)
	h.publish("t", 0, val("a"))
	if _, err := h.consumer.CommitOffsets(t.Context(), &riftv1.CommitOffsetsRequest{Group: "g", Topic: "t", Offsets: []*riftv1.PartitionOffset{{Offset: 1}}}); err != nil {
		t.Fatal(err)
	}

	errc := make(chan error, 1)
	go func() {
		_, err := h.consumer.Fetch(t.Context(), &riftv1.FetchRequest{Topic: "t", MaxWaitMs: 10000, Partitions: []*riftv1.PartitionOffset{{Offset: 1}}})
		errc <- err
	}()
	time.Sleep(100 * time.Millisecond)
	if _, err := h.topics.DeleteTopic(t.Context(), &riftv1.DeleteTopicRequest{Name: "t"}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errc:
		wantCode(t, err, codes.NotFound)
	case <-time.After(5 * time.Second):
		t.Fatal("fetch was not woken by topic deletion")
	}
	h.createTopic("t", 1)
	got, _ := h.consumer.GetCommittedOffsets(t.Context(), &riftv1.GetCommittedOffsetsRequest{Group: "g", Topic: "t"})
	if len(got.GetOffsets()) != 0 {
		t.Fatalf("offsets of deleted topic survived: %v", got)
	}
}

func TestGzipCompressedRequests(t *testing.T) {
	h := start(t, t.TempDir())
	h.createTopic("t", 1)
	value := strings.Repeat("compressible ", 1000)
	_, err := h.producer.Publish(t.Context(), &riftv1.PublishRequest{Topic: "t", Records: []*riftv1.PublishRecord{val(value)}}, grpc.UseCompressor(gzip.Name))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := h.consumer.Fetch(t.Context(), &riftv1.FetchRequest{Topic: "t", Partitions: []*riftv1.PartitionOffset{{}}}, grpc.UseCompressor(gzip.Name))
	if err != nil || string(resp.GetPartitions()[0].GetRecords()[0].GetValue()) != value {
		t.Fatalf("compressed round trip failed: %v", err)
	}
}

func TestMetricsExposePartitionState(t *testing.T) {
	h := start(t, t.TempDir())
	h.createTopic("t", 1)
	h.publish("t", 0, val("a"), val("b"), val("c"))
	m := scrape(t, h.b)
	for _, want := range []string{
		`rift_partition_high_watermark{partition="0",topic="t"} 3`,
		`rift_partition_synced_offset{partition="0",topic="t"} 3`,
		`rift_produce_records_total{topic="t"} 3`,
		`rift_produce_requests_total{durability="fsynced",result="ok"} 1`,
		`rift_build_info{node_id="node-1"`,
		`rift_disk_used_ratio 0.5`,
	} {
		if !strings.Contains(m, want) {
			t.Errorf("metrics missing %q", want)
		}
	}
}

func TestConfigValidation(t *testing.T) {
	for _, mutate := range []func(*Config){
		func(c *Config) { c.NodeID = "" },
		func(c *Config) { c.DataDir = "" },
		func(c *Config) { c.DiskHighWatermark, c.DiskCriticalWatermark = 0.9, 0.8 },
		func(c *Config) { c.MaxRecordBytes, c.MaxBatchBytes = 100, 10 },
	} {
		cfg := testConfig(t.TempDir())
		mutate(&cfg)
		if _, err := New(cfg); err == nil {
			t.Errorf("New accepted invalid config %+v", cfg)
		}
	}
}

func grepLines(s, pattern string) string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		for _, p := range strings.Split(pattern, "|") {
			if strings.Contains(l, p) && !strings.HasPrefix(l, "#") {
				out = append(out, l)
				break
			}
		}
	}
	return strings.Join(out, "\n")
}
