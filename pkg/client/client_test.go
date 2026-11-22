package client_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	riftv1 "github.com/jayyy255/rift/api/gen/rift/v1"
	"github.com/jayyy255/rift/internal/broker"
	"github.com/jayyy255/rift/pkg/client"
)

// startBroker runs an in-process broker on a loopback port.
func startBroker(t *testing.T, mutate ...func(*broker.Config)) string {
	t.Helper()
	cfg := broker.Config{
		NodeID: "node-1", DataDir: t.TempDir(), RetentionCheckInterval: -1, FlushInterval: -1,
		Logger:    slog.New(slog.DiscardHandler),
		DiskUsage: func(string) (broker.DiskStats, error) { return broker.DiskStats{Total: 100, Available: 90}, nil },
	}
	for _, m := range mutate {
		m(&cfg)
	}
	b, err := broker.New(cfg)
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

func dial(t *testing.T, addr string, opts ...client.Option) *client.Client {
	t.Helper()
	c, err := client.Dial(addr, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func mustCreate(t *testing.T, c *client.Client, name string, partitions int32) {
	t.Helper()
	if _, err := c.CreateTopic(t.Context(), name, client.TopicConfig{Partitions: partitions}); err != nil {
		t.Fatal(err)
	}
}

func publishN(t *testing.T, c *client.Client, topic string, n int) {
	t.Helper()
	recs := make([]client.Record, n)
	for i := range recs {
		recs[i] = client.Record{Value: []byte(fmt.Sprintf("m%d", i))}
	}
	if _, err := c.Publish(t.Context(), topic, client.DurabilityFsynced, recs...); err != nil {
		t.Fatal(err)
	}
}

func pollAll(t *testing.T, cons *client.Consumer, want int) []client.Message {
	t.Helper()
	var out []client.Message
	deadline := time.Now().Add(10 * time.Second)
	for len(out) < want {
		if time.Now().After(deadline) {
			t.Fatalf("polled %d of %d messages", len(out), want)
		}
		msgs, err := cons.Poll(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, msgs...)
	}
	return out
}

func TestAdminRoundTrip(t *testing.T) {
	c := dial(t, startBroker(t))
	ctx := t.Context()
	info, err := c.CreateTopic(ctx, "orders", client.TopicConfig{Partitions: 6, Retention: time.Hour, RetentionBytes: -1})
	if err != nil {
		t.Fatal(err)
	}
	if info.Partitions != 6 || info.RetentionMs != 3600000 || info.RetentionBytes != -1 {
		t.Fatalf("CreateTopic = %+v", info)
	}
	topics, _ := c.ListTopics(ctx)
	if len(topics) != 1 || topics[0].Name != "orders" {
		t.Fatalf("ListTopics = %+v", topics)
	}
	_, parts, err := c.DescribeTopic(ctx, "orders")
	if err != nil || len(parts) != 6 {
		t.Fatalf("DescribeTopic = %v, %v", parts, err)
	}
	node, brokers, err := c.ClusterStatus(ctx)
	if err != nil || node != "node-1" || len(brokers) != 1 {
		t.Fatalf("ClusterStatus = %s %v %v", node, brokers, err)
	}
	if err := c.DeleteTopic(ctx, "orders"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.DescribeTopic(ctx, "orders"); status.Code(err) != codes.NotFound {
		t.Fatalf("describe deleted topic: %v", err)
	}
}

func TestPublishReturnsAcknowledgments(t *testing.T) {
	c := dial(t, startBroker(t))
	mustCreate(t, c, "t", 1)
	mds, err := c.Publish(t.Context(), "t", client.DurabilityWritten,
		client.Record{Key: []byte("k"), Value: []byte("a"), Headers: []client.Header{{Key: "h", Value: []byte("1")}}},
		client.Record{Value: []byte("b")})
	if err != nil {
		t.Fatal(err)
	}
	if len(mds) != 2 || mds[1].Offset != 1 || mds[0].Durability != client.DurabilityWritten || mds[0].Timestamp.IsZero() {
		t.Fatalf("acks = %+v", mds)
	}
}

func TestProducerBatchesAndKeepsSendOrder(t *testing.T) {
	var rpcs atomic.Int64
	count := grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, inv grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if method == riftv1.ProducerService_Publish_FullMethodName {
			rpcs.Add(1)
		}
		return inv(ctx, method, req, reply, cc, opts...)
	})
	c := dial(t, startBroker(t), client.WithDialOptions(count))
	mustCreate(t, c, "t", 3)

	p := c.NewProducer("t", client.ProducerConfig{Linger: 20 * time.Millisecond, BatchMaxRecords: 200})
	const n = 1000
	offsets := make([]int64, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		err := p.SendAsync(t.Context(), client.Record{Key: []byte("same-key"), Value: []byte(fmt.Sprint(i))},
			func(md client.RecordMetadata, err error) {
				defer wg.Done()
				if err != nil {
					t.Error(err)
				}
				offsets[i] = md.Offset
			})
		if err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
	p.Close()
	for i := 1; i < n; i++ {
		if offsets[i] != offsets[i-1]+1 {
			t.Fatalf("send order broken at %d: offsets %d then %d", i, offsets[i-1], offsets[i])
		}
	}
	if got := rpcs.Load(); got > n/50 {
		t.Fatalf("%d records took %d publish RPCs; batching is not working", n, got)
	}
	t.Logf("%d records sent in %d batches", n, rpcs.Load())
}

func TestProducerSendAndClose(t *testing.T) {
	c := dial(t, startBroker(t))
	mustCreate(t, c, "t", 1)
	p := c.NewProducer("t", client.ProducerConfig{})
	md, err := p.Send(t.Context(), client.Record{Value: []byte("x")})
	if err != nil || md.Offset != 0 || md.Topic != "t" {
		t.Fatalf("Send = %+v, %v", md, err)
	}
	// Close flushes queued records before returning.
	var delivered atomic.Int32
	for range 50 {
		err := p.SendAsync(t.Context(), client.Record{Value: []byte("y")}, func(_ client.RecordMetadata, err error) {
			if err == nil {
				delivered.Add(1)
			}
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	p.Close()
	if delivered.Load() != 50 {
		t.Fatalf("delivered %d of 50 before Close returned", delivered.Load())
	}
	if _, err := p.Send(t.Context(), client.Record{}); !errors.Is(err, client.ErrProducerClosed) {
		t.Fatalf("Send after Close: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestProducerReportsBrokerErrors(t *testing.T) {
	c := dial(t, startBroker(t))
	p := c.NewProducer("missing", client.ProducerConfig{})
	defer p.Close()
	if _, err := p.Send(t.Context(), client.Record{Value: []byte("x")}); status.Code(err) != codes.NotFound {
		t.Fatalf("Send to missing topic: %v", err)
	}
}

// fakeProducer is a ProducerService whose behavior tests control.
type fakeProducer struct {
	riftv1.UnimplementedProducerServiceServer
	calls   atomic.Int32
	failFor int32
	code    codes.Code
	block   chan struct{}
}

func (f *fakeProducer) Publish(ctx context.Context, req *riftv1.PublishRequest) (*riftv1.PublishResponse, error) {
	n := f.calls.Add(1)
	if f.block != nil {
		select {
		case <-f.block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if n <= f.failFor {
		return nil, status.Error(f.code, "injected failure")
	}
	resp := &riftv1.PublishResponse{Durability: riftv1.Durability_DURABILITY_FSYNCED}
	for i := range req.GetRecords() {
		resp.Acks = append(resp.Acks, &riftv1.RecordAck{Offset: int64(i)})
	}
	return resp, nil
}

func startFake(t *testing.T, f *fakeProducer) string {
	t.Helper()
	srv := grpc.NewServer()
	riftv1.RegisterProducerServiceServer(srv, f)
	lis, _ := net.Listen("tcp", "127.0.0.1:0")
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

func TestRetriesAreBounded(t *testing.T) {
	rec := client.Record{Value: []byte("x")}
	t.Run("transient errors are retried", func(t *testing.T) {
		f := &fakeProducer{failFor: 2, code: codes.Unavailable}
		c := dial(t, startFake(t, f), client.WithRetry(3, time.Millisecond))
		if _, err := c.Publish(t.Context(), "t", 0, rec); err != nil {
			t.Fatal(err)
		}
		if f.calls.Load() != 3 {
			t.Fatalf("calls = %d, want 3", f.calls.Load())
		}
	})
	t.Run("attempts are capped", func(t *testing.T) {
		f := &fakeProducer{failFor: 100, code: codes.ResourceExhausted}
		c := dial(t, startFake(t, f), client.WithRetry(4, time.Millisecond))
		_, err := c.Publish(t.Context(), "t", 0, rec)
		if status.Code(err) != codes.ResourceExhausted || f.calls.Load() != 4 {
			t.Fatalf("err = %v after %d calls, want ResourceExhausted after 4", err, f.calls.Load())
		}
	})
	t.Run("permanent errors are not retried", func(t *testing.T) {
		f := &fakeProducer{failFor: 100, code: codes.InvalidArgument}
		c := dial(t, startFake(t, f), client.WithRetry(5, time.Millisecond))
		_, err := c.Publish(t.Context(), "t", 0, rec)
		if status.Code(err) != codes.InvalidArgument || f.calls.Load() != 1 {
			t.Fatalf("err = %v after %d calls, want 1 attempt", err, f.calls.Load())
		}
	})
	t.Run("caller deadline stops retries", func(t *testing.T) {
		f := &fakeProducer{failFor: 1000, code: codes.Unavailable}
		c := dial(t, startFake(t, f), client.WithRetry(1000, 20*time.Millisecond))
		ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
		defer cancel()
		st := time.Now()
		_, err := c.Publish(ctx, "t", 0, rec)
		if !errors.Is(err, context.DeadlineExceeded) || time.Since(st) > 2*time.Second {
			t.Fatalf("err = %v after %v", err, time.Since(st))
		}
	})
}

func TestProducerBufferIsBounded(t *testing.T) {
	f := &fakeProducer{block: make(chan struct{})}
	c := dial(t, startFake(t, f))
	p := c.NewProducer("t", client.ProducerConfig{MaxBuffered: 5, BatchMaxRecords: 1, Linger: time.Millisecond})
	defer func() { close(f.block); p.Close() }()

	// One record is in flight (blocked); five fill the buffer.
	for i := range 6 {
		if err := p.SendAsync(t.Context(), client.Record{}, func(client.RecordMetadata, error) {}); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
		if i == 0 {
			for f.calls.Load() == 0 {
				time.Sleep(time.Millisecond)
			}
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	if err := p.SendAsync(ctx, client.Record{}, func(client.RecordMetadata, error) {}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("send into full buffer: %v, want DeadlineExceeded (backpressure)", err)
	}
}

func TestConsumerStartPositions(t *testing.T) {
	c := dial(t, startBroker(t))
	mustCreate(t, c, "t", 2)
	publishN(t, c, "t", 10)
	ctx := t.Context()

	earliest, _ := c.NewConsumer(ctx, client.ConsumerConfig{Topic: "t"})
	if msgs := pollAll(t, earliest, 10); len(msgs) != 10 {
		t.Fatalf("earliest consumer got %d", len(msgs))
	}

	latest, _ := c.NewConsumer(ctx, client.ConsumerConfig{Topic: "t", Start: client.StartLatest, MaxWait: 50 * time.Millisecond})
	if msgs, _ := latest.Poll(ctx); len(msgs) != 0 {
		t.Fatalf("latest consumer got %d old messages", len(msgs))
	}
	publishN(t, c, "t", 2)
	if msgs := pollAll(t, latest, 2); len(msgs) != 2 {
		t.Fatalf("latest consumer got %d new messages", len(msgs))
	}

	one, _ := c.NewConsumer(ctx, client.ConsumerConfig{Topic: "t", Partitions: []int32{1}})
	for _, m := range pollAll(t, one, 6) {
		if m.Partition != 1 {
			t.Fatalf("partition filter ignored: %+v", m)
		}
	}
	if _, err := c.NewConsumer(ctx, client.ConsumerConfig{Topic: "t", Partitions: []int32{5}}); err == nil {
		t.Fatal("unknown partition accepted")
	}
	if _, err := c.NewConsumer(ctx, client.ConsumerConfig{Topic: "t", AutoCommit: true}); err == nil {
		t.Fatal("AutoCommit without group accepted")
	}
}

func TestConsumerCommitAndResume(t *testing.T) {
	c := dial(t, startBroker(t))
	mustCreate(t, c, "t", 1)
	publishN(t, c, "t", 10)
	ctx := t.Context()

	cons, _ := c.NewConsumer(ctx, client.ConsumerConfig{Topic: "t", Group: "g", MaxRecordsPerPartition: 4})
	first, _ := cons.Poll(ctx)
	if len(first) != 4 {
		t.Fatalf("first poll = %d records", len(first))
	}
	if err := cons.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	committed, _ := c.CommittedOffsets(ctx, "g", "t")
	if committed[0] != 4 {
		t.Fatalf("committed = %v, want 4", committed)
	}

	resumed, _ := c.NewConsumer(ctx, client.ConsumerConfig{Topic: "t", Group: "g"})
	msgs := pollAll(t, resumed, 6)
	if msgs[0].Offset != 4 || len(msgs) != 6 {
		t.Fatalf("resumed at %d with %d messages, want 4 and 6", msgs[0].Offset, len(msgs))
	}
	if lag := resumed.Lag(); lag[0] != 0 {
		t.Fatalf("lag = %v", lag)
	}
}

func TestConsumerAutoCommitOnClose(t *testing.T) {
	c := dial(t, startBroker(t))
	mustCreate(t, c, "t", 1)
	publishN(t, c, "t", 5)
	cons, _ := c.NewConsumer(t.Context(), client.ConsumerConfig{Topic: "t", Group: "g", AutoCommit: true, AutoCommitInterval: time.Hour})
	pollAll(t, cons, 5)
	if err := cons.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got, _ := c.CommittedOffsets(t.Context(), "g", "t"); got[0] != 5 {
		t.Fatalf("committed after Close = %v", got)
	}
}

func TestConsumerSeekReplays(t *testing.T) {
	c := dial(t, startBroker(t))
	mustCreate(t, c, "t", 1)
	publishN(t, c, "t", 5)
	cons, _ := c.NewConsumer(t.Context(), client.ConsumerConfig{Topic: "t"})
	pollAll(t, cons, 5)
	if err := cons.Seek(0, 2); err != nil {
		t.Fatal(err)
	}
	msgs := pollAll(t, cons, 3)
	if msgs[0].Offset != 2 || string(msgs[0].Value) != "m2" {
		t.Fatalf("replay started at %+v", msgs[0])
	}
	if err := cons.Seek(9, 0); err == nil {
		t.Fatal("seek on unassigned partition accepted")
	}
}

func TestConsumerResetsWhenRetentionDeletedPosition(t *testing.T) {
	c := dial(t, startBroker(t, func(cfg *broker.Config) {
		cfg.Log.SegmentBytes = 1024
		cfg.RetentionMaxBytes = 2048
		cfg.RetentionCheckInterval = 10 * time.Millisecond
	}))
	mustCreate(t, c, "t", 1)
	ctx := t.Context()
	stale, _ := c.NewConsumer(ctx, client.ConsumerConfig{Topic: "t", OnReset: client.ResetNone})
	strict, _ := c.NewConsumer(ctx, client.ConsumerConfig{Topic: "t"}) // ResetEarliest
	for range 20 {
		publishN(t, c, "t", 10)
	}
	// Wait until retention has trimmed the partition to its limit; with no
	// further publishes the log start then stays put.
	deadline := time.Now().Add(5 * time.Second)
	var logStart int64
	for {
		_, parts, _ := c.DescribeTopic(ctx, "t")
		if parts[0].LogStartOffset > 0 && parts[0].SizeBytes <= 2048+1024 {
			logStart = parts[0].LogStartOffset
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("retention did not settle: %+v", parts[0])
		}
		time.Sleep(10 * time.Millisecond)
	}
	var oor *client.OffsetOutOfRangeError
	if _, err := stale.Poll(ctx); !errors.As(err, &oor) {
		t.Fatalf("ResetNone poll err = %v, want OffsetOutOfRangeError", err)
	}
	if _, err := strict.Poll(ctx); err != nil {
		t.Fatal(err)
	}
	msgs, _ := strict.Poll(ctx)
	if len(msgs) == 0 || msgs[0].Offset != logStart {
		first := int64(-1)
		if len(msgs) > 0 {
			first = msgs[0].Offset
		}
		t.Fatalf("after reset got %d messages starting at offset %d, want log start %d", len(msgs), first, logStart)
	}
}

func TestCompressionEndToEnd(t *testing.T) {
	c := dial(t, startBroker(t), client.WithCompression())
	mustCreate(t, c, "t", 1)
	big := make([]byte, 200_000)
	for i := range big {
		big[i] = byte('a' + i%7)
	}
	if _, err := c.Publish(t.Context(), "t", 0, client.Record{Value: big}); err != nil {
		t.Fatal(err)
	}
	cons, _ := c.NewConsumer(t.Context(), client.ConsumerConfig{Topic: "t"})
	if msgs := pollAll(t, cons, 1); string(msgs[0].Value) != string(big) {
		t.Fatal("value corrupted through compression")
	}
}
