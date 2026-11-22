// Package client is the Go SDK for Rift brokers.
//
// A Client wraps one gRPC connection and offers topic administration, a
// batching Producer, and a stream Consumer. Delivery is at-least-once:
// retried publishes and uncommitted consumption can produce duplicates,
// so applications must tolerate them.
package client

import (
	"context"
	"errors"
	"math/rand/v2"
	"sort"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/status"

	riftv1 "github.com/jayyy255/rift/api/gen/rift/v1"
)

// Client is a connection to a Rift broker. It is safe for concurrent use.
type Client struct {
	conn     *grpc.ClientConn
	topics   riftv1.TopicServiceClient
	producer riftv1.ProducerServiceClient
	consumer riftv1.ConsumerServiceClient
	cluster  riftv1.ClusterServiceClient
	opts     options
}

type options struct {
	timeout     time.Duration
	maxAttempts int
	backoff     time.Duration
	maxBackoff  time.Duration
	compress    bool
	dialOptions []grpc.DialOption
}

// Option configures a Client.
type Option func(*options)

// WithTimeout sets the per-attempt timeout for unary requests (default 10s).
// Fetch long-polls extend it by their wait time.
func WithTimeout(d time.Duration) Option { return func(o *options) { o.timeout = d } }

// WithRetry bounds retries of transient failures: at most maxAttempts
// attempts in total, with exponential backoff starting at backoff (with
// jitter, capped at 2s). Defaults: 5 attempts, 50ms. maxAttempts 1 disables
// retries.
func WithRetry(maxAttempts int, backoff time.Duration) Option {
	return func(o *options) { o.maxAttempts, o.backoff = max(maxAttempts, 1), backoff }
}

// WithCompression gzip-compresses requests and responses.
func WithCompression() Option { return func(o *options) { o.compress = true } }

// WithDialOptions adds gRPC dial options, e.g. interceptors or credentials.
func WithDialOptions(opts ...grpc.DialOption) Option {
	return func(o *options) { o.dialOptions = append(o.dialOptions, opts...) }
}

// Dial creates a client for the broker at addr (host:port). The connection
// is established lazily on the first request.
func Dial(addr string, opts ...Option) (*Client, error) {
	o := options{timeout: 10 * time.Second, maxAttempts: 5, backoff: 50 * time.Millisecond, maxBackoff: 2 * time.Second}
	for _, opt := range opts {
		opt(&o)
	}
	callOpts := []grpc.CallOption{grpc.MaxCallRecvMsgSize(64 << 20)}
	if o.compress {
		callOpts = append(callOpts, grpc.UseCompressor(gzip.Name))
	}
	dial := append([]grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(callOpts...),
	}, o.dialOptions...)
	conn, err := grpc.NewClient(addr, dial...)
	if err != nil {
		return nil, err
	}
	return &Client{
		conn:     conn,
		topics:   riftv1.NewTopicServiceClient(conn),
		producer: riftv1.NewProducerServiceClient(conn),
		consumer: riftv1.NewConsumerServiceClient(conn),
		cluster:  riftv1.NewClusterServiceClient(conn),
		opts:     o,
	}, nil
}

// Close closes the connection.
func (c *Client) Close() error { return c.conn.Close() }

// retryable reports whether a failed request may succeed if repeated.
func retryable(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.ResourceExhausted, codes.Aborted:
		return true
	case codes.DeadlineExceeded:
		// Per-attempt timeout, not the caller's deadline (checked separately).
		return true
	}
	return false
}

// call runs fn with a per-attempt timeout, retrying transient failures with
// bounded exponential backoff until ctx is done.
func (c *Client) call(ctx context.Context, extra time.Duration, fn func(context.Context) error) error {
	backoff := c.opts.backoff
	var err error
	for attempt := 1; ; attempt++ {
		actx, cancel := context.WithTimeout(ctx, c.opts.timeout+extra)
		err = fn(actx)
		cancel()
		if err == nil || !retryable(err) || attempt >= c.opts.maxAttempts || ctx.Err() != nil {
			break
		}
		sleep := backoff/2 + rand.N(backoff/2+1)
		select {
		case <-time.After(sleep):
		case <-ctx.Done():
			return ctx.Err()
		}
		backoff = min(backoff*2, c.opts.maxBackoff)
	}
	if ctx.Err() != nil && (status.Code(err) == codes.DeadlineExceeded || status.Code(err) == codes.Canceled) {
		return errors.Join(ctx.Err(), err)
	}
	return err
}

// Broker describes a cluster member.
type Broker struct {
	NodeID  string
	Address string
	State   string
}

// ClusterStatus reports the brokers known to the connected broker.
func (c *Client) ClusterStatus(ctx context.Context) (answeredBy string, brokers []Broker, err error) {
	var resp *riftv1.GetStatusResponse
	err = c.call(ctx, 0, func(ctx context.Context) error {
		var e error
		resp, e = c.cluster.GetStatus(ctx, &riftv1.GetStatusRequest{})
		return e
	})
	if err != nil {
		return "", nil, err
	}
	for _, b := range resp.GetBrokers() {
		brokers = append(brokers, Broker{NodeID: b.GetNodeId(), Address: b.GetAddress(), State: b.GetState().String()})
	}
	return resp.GetNodeId(), brokers, nil
}

// TopicConfig configures a new topic.
type TopicConfig struct {
	Partitions        int32
	ReplicationFactor int32
	// Retention per partition: 0 = broker default, negative = unlimited.
	Retention      time.Duration
	RetentionBytes int64
}

// TopicInfo describes a topic.
type TopicInfo struct {
	Name              string
	Partitions        int32
	ReplicationFactor int32
	RetentionMs       int64
	RetentionBytes    int64
	CreatedAt         time.Time
}

// PartitionInfo describes one partition's log.
type PartitionInfo struct {
	Partition      int32
	LogStartOffset int64
	HighWatermark  int64
	SyncedOffset   int64
	SizeBytes      int64
	Segments       int32
	Leader         string
}

func fromPBTopic(t *riftv1.TopicInfo) TopicInfo {
	return TopicInfo{
		Name: t.GetName(), Partitions: t.GetPartitions(), ReplicationFactor: t.GetReplicationFactor(),
		RetentionMs: t.GetRetentionMs(), RetentionBytes: t.GetRetentionBytes(), CreatedAt: time.UnixMilli(t.GetCreatedAtMs()),
	}
}

// CreateTopic creates a topic.
func (c *Client) CreateTopic(ctx context.Context, name string, cfg TopicConfig) (TopicInfo, error) {
	req := &riftv1.CreateTopicRequest{
		Name: name, Partitions: cfg.Partitions, ReplicationFactor: cfg.ReplicationFactor,
		RetentionMs: cfg.Retention.Milliseconds(), RetentionBytes: cfg.RetentionBytes,
	}
	if cfg.Retention < 0 {
		req.RetentionMs = -1
	}
	if cfg.RetentionBytes < 0 {
		req.RetentionBytes = -1
	}
	var resp *riftv1.CreateTopicResponse
	err := c.call(ctx, 0, func(ctx context.Context) error {
		var e error
		resp, e = c.topics.CreateTopic(ctx, req)
		return e
	})
	if err != nil {
		return TopicInfo{}, err
	}
	return fromPBTopic(resp.GetTopic()), nil
}

// DeleteTopic deletes a topic and its data.
func (c *Client) DeleteTopic(ctx context.Context, name string) error {
	return c.call(ctx, 0, func(ctx context.Context) error {
		_, err := c.topics.DeleteTopic(ctx, &riftv1.DeleteTopicRequest{Name: name})
		return err
	})
}

// ListTopics lists all topics.
func (c *Client) ListTopics(ctx context.Context) ([]TopicInfo, error) {
	var resp *riftv1.ListTopicsResponse
	err := c.call(ctx, 0, func(ctx context.Context) error {
		var e error
		resp, e = c.topics.ListTopics(ctx, &riftv1.ListTopicsRequest{})
		return e
	})
	if err != nil {
		return nil, err
	}
	out := make([]TopicInfo, 0, len(resp.GetTopics()))
	for _, t := range resp.GetTopics() {
		out = append(out, fromPBTopic(t))
	}
	return out, nil
}

// DescribeTopic returns a topic's configuration and partition state.
func (c *Client) DescribeTopic(ctx context.Context, name string) (TopicInfo, []PartitionInfo, error) {
	var resp *riftv1.DescribeTopicResponse
	err := c.call(ctx, 0, func(ctx context.Context) error {
		var e error
		resp, e = c.topics.DescribeTopic(ctx, &riftv1.DescribeTopicRequest{Name: name})
		return e
	})
	if err != nil {
		return TopicInfo{}, nil, err
	}
	parts := make([]PartitionInfo, 0, len(resp.GetPartitions()))
	for _, p := range resp.GetPartitions() {
		parts = append(parts, PartitionInfo{
			Partition: p.GetPartition(), LogStartOffset: p.GetLogStartOffset(), HighWatermark: p.GetHighWatermark(),
			SyncedOffset: p.GetSyncedOffset(), SizeBytes: p.GetSizeBytes(), Segments: p.GetSegments(), Leader: p.GetLeader(),
		})
	}
	return fromPBTopic(resp.GetTopic()), parts, nil
}

// CommitOffsets durably stores, for a consumer group, the next offset to
// consume from each partition.
func (c *Client) CommitOffsets(ctx context.Context, group, topic string, offsets map[int32]int64) error {
	req := &riftv1.CommitOffsetsRequest{Group: group, Topic: topic}
	for p, off := range offsets {
		req.Offsets = append(req.Offsets, &riftv1.PartitionOffset{Partition: p, Offset: off})
	}
	sort.Slice(req.Offsets, func(i, j int) bool { return req.Offsets[i].Partition < req.Offsets[j].Partition })
	return c.call(ctx, 0, func(ctx context.Context) error {
		_, err := c.consumer.CommitOffsets(ctx, req)
		return err
	})
}

// CommittedOffsets returns a group's committed offsets for a topic.
func (c *Client) CommittedOffsets(ctx context.Context, group, topic string) (map[int32]int64, error) {
	var resp *riftv1.GetCommittedOffsetsResponse
	err := c.call(ctx, 0, func(ctx context.Context) error {
		var e error
		resp, e = c.consumer.GetCommittedOffsets(ctx, &riftv1.GetCommittedOffsetsRequest{Group: group, Topic: topic})
		return e
	})
	if err != nil {
		return nil, err
	}
	out := make(map[int32]int64, len(resp.GetOffsets()))
	for _, po := range resp.GetOffsets() {
		out[po.GetPartition()] = po.GetOffset()
	}
	return out, nil
}
