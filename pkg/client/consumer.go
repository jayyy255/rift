package client

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	riftv1 "github.com/jayyy255/rift/api/gen/rift/v1"
)

// StartPosition chooses where a consumer starts on partitions without a
// position.
type StartPosition int

const (
	// StartCommitted resumes from the group's committed offset, falling back
	// to the earliest retained offset. Default when a group is set.
	StartCommitted StartPosition = iota
	// StartEarliest starts at the oldest retained record.
	StartEarliest
	// StartLatest starts after the newest record.
	StartLatest
)

// ResetPolicy decides what happens when a position falls outside the log,
// e.g. after retention deleted the records.
type ResetPolicy int

const (
	// ResetEarliest moves to the oldest retained record (default).
	ResetEarliest ResetPolicy = iota
	// ResetLatest moves past the newest record.
	ResetLatest
	// ResetNone returns an *OffsetOutOfRangeError from Poll.
	ResetNone
)

// ConsumerConfig configures a stream consumer. In R1 there is no group
// coordination: a group is a named offset checkpoint, and each consumer
// reads the partitions it is given (all, by default).
type ConsumerConfig struct {
	Topic string
	// Group names the offset checkpoint; required for Commit and AutoCommit.
	Group string
	// Partitions to read; nil means every partition of the topic.
	Partitions []int32
	// Start defaults to StartCommitted with a group, else StartEarliest.
	Start   StartPosition
	OnReset ResetPolicy
	// AutoCommit commits positions every AutoCommitInterval (default 5s)
	// during Poll and on Close. Positions cover records already returned by
	// Poll, so records are processed at least once if the application
	// finishes handling each batch before the next Poll.
	AutoCommit         bool
	AutoCommitInterval time.Duration
	// MaxWait bounds how long a Poll waits for records (default 500ms).
	MaxWait time.Duration
	// MaxBytes is a soft cap on bytes per Poll (default 1 MiB).
	MaxBytes int
	// MaxRecordsPerPartition per Poll (default 500).
	MaxRecordsPerPartition int
}

// Message is a consumed record.
type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Timestamp time.Time
	// Key is nil for keyless records.
	Key     []byte
	Value   []byte
	Headers []Header
}

// OffsetOutOfRangeError is returned by Poll with ResetNone when a position is
// outside the retained log.
type OffsetOutOfRangeError struct {
	Partition int32
	Offset    int64
	Message   string
}

func (e *OffsetOutOfRangeError) Error() string {
	return fmt.Sprintf("rift: partition %d offset %d out of range: %s", e.Partition, e.Offset, e.Message)
}

// ErrCorruptData is wrapped when the broker reports a record failing
// checksum validation.
var ErrCorruptData = errors.New("rift: broker reported corrupt data")

// Consumer reads records from a topic. Poll and Commit must not be called
// concurrently with each other.
type Consumer struct {
	c   *Client
	cfg ConsumerConfig

	mu         sync.Mutex
	positions  map[int32]int64
	order      []int32 // fetch order, rotated for fairness
	committed  map[int32]int64
	lastCommit time.Time
	lag        map[int32]int64
}

// NewConsumer creates a consumer and resolves its starting positions.
func (c *Client) NewConsumer(ctx context.Context, cfg ConsumerConfig) (*Consumer, error) {
	if cfg.AutoCommit && cfg.Group == "" {
		return nil, errors.New("rift: AutoCommit requires a Group")
	}
	if cfg.Group == "" && cfg.Start == StartCommitted {
		cfg.Start = StartEarliest
	}
	if cfg.AutoCommitInterval <= 0 {
		cfg.AutoCommitInterval = 5 * time.Second
	}
	if cfg.MaxWait <= 0 {
		cfg.MaxWait = 500 * time.Millisecond
	}
	if cfg.MaxBytes <= 0 {
		cfg.MaxBytes = 1 << 20
	}
	if cfg.MaxRecordsPerPartition <= 0 {
		cfg.MaxRecordsPerPartition = 500
	}
	info, parts, err := c.DescribeTopic(ctx, cfg.Topic)
	if err != nil {
		return nil, err
	}
	if cfg.Partitions == nil {
		for p := range info.Partitions {
			cfg.Partitions = append(cfg.Partitions, p)
		}
	}
	byID := map[int32]PartitionInfo{}
	for _, p := range parts {
		byID[p.Partition] = p
	}
	var committed map[int32]int64
	if cfg.Start == StartCommitted {
		if committed, err = c.CommittedOffsets(ctx, cfg.Group, cfg.Topic); err != nil {
			return nil, err
		}
	}
	cons := &Consumer{c: c, cfg: cfg, positions: map[int32]int64{}, committed: map[int32]int64{}, lag: map[int32]int64{}, lastCommit: time.Now()}
	for _, p := range cfg.Partitions {
		pi, ok := byID[p]
		if !ok {
			return nil, fmt.Errorf("rift: topic %q has no partition %d", cfg.Topic, p)
		}
		switch cfg.Start {
		case StartLatest:
			cons.positions[p] = pi.HighWatermark
		case StartCommitted:
			if off, ok := committed[p]; ok {
				cons.positions[p] = off
				cons.committed[p] = off
			} else {
				cons.positions[p] = pi.LogStartOffset
			}
		default:
			cons.positions[p] = pi.LogStartOffset
		}
		cons.order = append(cons.order, p)
	}
	return cons, nil
}

// Poll fetches the next records, waiting up to MaxWait when none are
// available. It returns an empty slice when the wait expires.
func (cs *Consumer) Poll(ctx context.Context) ([]Message, error) {
	if cs.cfg.AutoCommit && time.Since(cs.lastCommit) >= cs.cfg.AutoCommitInterval {
		if err := cs.Commit(ctx); err != nil {
			return nil, fmt.Errorf("rift: auto-commit: %w", err)
		}
	}

	cs.mu.Lock()
	req := &riftv1.FetchRequest{
		Topic:                  cs.cfg.Topic,
		MaxWaitMs:              int32(cs.cfg.MaxWait.Milliseconds()),
		MaxBytes:               int32(cs.cfg.MaxBytes),
		MaxRecordsPerPartition: int32(cs.cfg.MaxRecordsPerPartition),
	}
	for _, p := range cs.order {
		req.Partitions = append(req.Partitions, &riftv1.PartitionOffset{Partition: p, Offset: cs.positions[p]})
	}
	if len(cs.order) > 1 { // rotate so no partition is starved by the byte cap
		cs.order = append(cs.order[1:], cs.order[0])
	}
	cs.mu.Unlock()

	var resp *riftv1.FetchResponse
	err := cs.c.call(ctx, cs.cfg.MaxWait, func(ctx context.Context) error {
		var e error
		resp, e = cs.c.consumer.Fetch(ctx, req)
		return e
	})
	if err != nil {
		return nil, err
	}

	cs.mu.Lock()
	defer cs.mu.Unlock()
	var out []Message
	for _, pr := range resp.GetPartitions() {
		p := pr.GetPartition()
		switch pr.GetError() {
		case riftv1.PartitionError_PARTITION_ERROR_UNSPECIFIED:
		case riftv1.PartitionError_PARTITION_ERROR_OFFSET_OUT_OF_RANGE:
			switch cs.cfg.OnReset {
			case ResetNone:
				return out, &OffsetOutOfRangeError{Partition: p, Offset: cs.positions[p], Message: pr.GetErrorMessage()}
			case ResetLatest:
				cs.positions[p] = pr.GetHighWatermark()
			default:
				cs.positions[p] = pr.GetLogStartOffset()
			}
			continue
		case riftv1.PartitionError_PARTITION_ERROR_CORRUPT_DATA:
			return out, fmt.Errorf("%w: partition %d: %s", ErrCorruptData, p, pr.GetErrorMessage())
		default:
			return out, fmt.Errorf("rift: partition %d: %s", p, pr.GetErrorMessage())
		}
		for _, r := range pr.GetRecords() {
			m := Message{
				Topic: cs.cfg.Topic, Partition: p, Offset: r.GetOffset(),
				Timestamp: time.UnixMilli(r.GetTimestampMs()), Key: r.Key, Value: r.GetValue(),
			}
			for _, h := range r.GetHeaders() {
				m.Headers = append(m.Headers, Header{Key: h.GetKey(), Value: h.GetValue()})
			}
			out = append(out, m)
			cs.positions[p] = r.GetOffset() + 1
		}
		cs.lag[p] = max(pr.GetHighWatermark()-cs.positions[p], 0)
	}
	return out, nil
}

// Commit durably stores the current positions (the offset after the last
// record returned by Poll, or set by Seek) for the consumer's group.
func (cs *Consumer) Commit(ctx context.Context) error {
	if cs.cfg.Group == "" {
		return errors.New("rift: Commit requires a Group")
	}
	cs.mu.Lock()
	changed := map[int32]int64{}
	for p, off := range cs.positions {
		if prev, ok := cs.committed[p]; !ok || prev != off {
			changed[p] = off
		}
	}
	cs.mu.Unlock()
	cs.lastCommit = time.Now()
	if len(changed) == 0 {
		return nil
	}
	if err := cs.c.CommitOffsets(ctx, cs.cfg.Group, cs.cfg.Topic, changed); err != nil {
		return err
	}
	cs.mu.Lock()
	for p, off := range changed {
		cs.committed[p] = off
	}
	cs.mu.Unlock()
	return nil
}

// Seek sets the next offset to read from a partition, e.g. to replay.
func (cs *Consumer) Seek(partition int32, offset int64) error {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if _, ok := cs.positions[partition]; !ok {
		return fmt.Errorf("rift: consumer is not reading partition %d", partition)
	}
	cs.positions[partition] = offset
	return nil
}

// Positions returns the next offset to read per partition.
func (cs *Consumer) Positions() map[int32]int64 {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	out := make(map[int32]int64, len(cs.positions))
	for p, off := range cs.positions {
		out[p] = off
	}
	return out
}

// Lag returns, per partition, how many records remained after the last
// Poll (high watermark minus position).
func (cs *Consumer) Lag() map[int32]int64 {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	out := make(map[int32]int64, len(cs.lag))
	for p, l := range cs.lag {
		out[p] = l
	}
	return out
}

// Close commits positions when AutoCommit is enabled.
func (cs *Consumer) Close(ctx context.Context) error {
	if cs.cfg.AutoCommit {
		return cs.Commit(ctx)
	}
	return nil
}
