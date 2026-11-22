package client

import (
	"context"
	"errors"
	"sync"
	"time"

	riftv1 "github.com/jayyy255/rift/api/gen/rift/v1"
)

// Durability selects when a publish is acknowledged. See the protocol docs:
// local write and fsync acknowledgments are distinct, documented levels.
type Durability int

const (
	// DurabilityDefault uses the broker default (FSYNCED in R1).
	DurabilityDefault Durability = iota
	// DurabilityWritten acknowledges once the record is in the broker's OS
	// page cache. It survives a broker crash, not a power failure.
	DurabilityWritten
	// DurabilityFsynced acknowledges once the record is fsynced on the broker.
	DurabilityFsynced
)

func (d Durability) pb() riftv1.Durability {
	switch d {
	case DurabilityWritten:
		return riftv1.Durability_DURABILITY_WRITTEN
	case DurabilityFsynced:
		return riftv1.Durability_DURABILITY_FSYNCED
	default:
		return riftv1.Durability_DURABILITY_UNSPECIFIED
	}
}

func durabilityFromPB(d riftv1.Durability) Durability {
	switch d {
	case riftv1.Durability_DURABILITY_WRITTEN:
		return DurabilityWritten
	case riftv1.Durability_DURABILITY_FSYNCED:
		return DurabilityFsynced
	default:
		return DurabilityDefault
	}
}

// Header is a record header. Keys may repeat.
type Header struct {
	Key   string
	Value []byte
}

// Record is a record to publish.
type Record struct {
	// Key routes the record: records with equal keys go to the same
	// partition and keep their order. nil means keyless (round-robin).
	Key     []byte
	Value   []byte
	Headers []Header
	// Partition, if non-nil, selects the partition explicitly.
	Partition *int32
}

// RecordMetadata is the broker's acknowledgment of one record.
type RecordMetadata struct {
	Topic       string
	Partition   int32
	Offset      int64
	LeaderEpoch int64
	Timestamp   time.Time
	// Durability is the level that held when the broker acknowledged.
	Durability Durability
}

func (r Record) size() int {
	n := 64 + len(r.Key) + len(r.Value)
	for _, h := range r.Headers {
		n += len(h.Key) + len(h.Value) + 8
	}
	return n
}

// Publish sends records in one request and returns their acknowledgments in
// order. Transient failures are retried (see WithRetry); a retry after a lost
// response can append duplicates.
func (c *Client) Publish(ctx context.Context, topic string, durability Durability, records ...Record) ([]RecordMetadata, error) {
	req := &riftv1.PublishRequest{Topic: topic, Durability: durability.pb(), Records: make([]*riftv1.PublishRecord, len(records))}
	for i, r := range records {
		pr := &riftv1.PublishRecord{Key: r.Key, Value: r.Value, Partition: r.Partition}
		if pr.Value == nil {
			pr.Value = []byte{}
		}
		for _, h := range r.Headers {
			pr.Headers = append(pr.Headers, &riftv1.Header{Key: h.Key, Value: h.Value})
		}
		req.Records[i] = pr
	}
	var resp *riftv1.PublishResponse
	err := c.call(ctx, 0, func(ctx context.Context) error {
		var e error
		resp, e = c.producer.Publish(ctx, req)
		return e
	})
	if err != nil {
		return nil, err
	}
	out := make([]RecordMetadata, len(resp.GetAcks()))
	for i, a := range resp.GetAcks() {
		out[i] = RecordMetadata{
			Topic: topic, Partition: a.GetPartition(), Offset: a.GetOffset(), LeaderEpoch: a.GetLeaderEpoch(),
			Timestamp: time.UnixMilli(a.GetTimestampMs()), Durability: durabilityFromPB(resp.GetDurability()),
		}
	}
	return out, nil
}

// ProducerConfig configures a Producer. Zero values select defaults.
type ProducerConfig struct {
	Durability Durability
	// A batch is sent when it reaches BatchMaxRecords (default 500) or
	// BatchMaxBytes (default 1 MiB), or Linger (default 5ms) after its first
	// record arrived.
	BatchMaxRecords int
	BatchMaxBytes   int
	Linger          time.Duration
	// MaxBuffered bounds records waiting to be sent (default 10000). When it
	// is full, Send blocks: backpressure instead of unbounded memory growth.
	MaxBuffered int
	// RequestTimeout bounds one batch including retries (default 30s).
	RequestTimeout time.Duration
}

// ErrProducerClosed is returned by Send after Close.
var ErrProducerClosed = errors.New("rift: producer closed")

// Producer batches records for one topic. One batch is in flight at a time,
// so records keep their send order (including across retries). It is safe
// for concurrent use.
type Producer struct {
	c     *Client
	topic string
	cfg   ProducerConfig

	mu     sync.RWMutex
	closed bool
	queue  chan *pending
	done   chan struct{}
}

type pending struct {
	rec    Record
	result chan sendResult
	cb     func(RecordMetadata, error)
}

type sendResult struct {
	md  RecordMetadata
	err error
}

// NewProducer starts a batching producer for topic. Close it to flush.
func (c *Client) NewProducer(topic string, cfg ProducerConfig) *Producer {
	if cfg.BatchMaxRecords <= 0 {
		cfg.BatchMaxRecords = 500
	}
	if cfg.BatchMaxBytes <= 0 {
		cfg.BatchMaxBytes = 1 << 20
	}
	if cfg.Linger <= 0 {
		cfg.Linger = 5 * time.Millisecond
	}
	if cfg.MaxBuffered <= 0 {
		cfg.MaxBuffered = 10000
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = 30 * time.Second
	}
	p := &Producer{c: c, topic: topic, cfg: cfg, queue: make(chan *pending, cfg.MaxBuffered), done: make(chan struct{})}
	go p.run()
	return p
}

// Send publishes a record and waits for its acknowledgment. If ctx ends
// while waiting, Send returns ctx.Err() but the record may still be
// published.
func (p *Producer) Send(ctx context.Context, r Record) (RecordMetadata, error) {
	pd := &pending{rec: r, result: make(chan sendResult, 1)}
	if err := p.enqueue(ctx, pd); err != nil {
		return RecordMetadata{}, err
	}
	select {
	case res := <-pd.result:
		return res.md, res.err
	case <-ctx.Done():
		return RecordMetadata{}, ctx.Err()
	}
}

// SendAsync queues a record and calls cb (from the producer's goroutine)
// with the outcome. It blocks only while the buffer is full.
func (p *Producer) SendAsync(ctx context.Context, r Record, cb func(RecordMetadata, error)) error {
	return p.enqueue(ctx, &pending{rec: r, cb: cb})
}

func (p *Producer) enqueue(ctx context.Context, pd *pending) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return ErrProducerClosed
	}
	select {
	case p.queue <- pd:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close stops accepting records, sends everything buffered, and waits for
// the outcome of each.
func (p *Producer) Close() error {
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		close(p.queue)
	}
	p.mu.Unlock()
	<-p.done
	return nil
}

func (p *Producer) run() {
	defer close(p.done)
	for first := range p.queue {
		batch := []*pending{first}
		size := first.rec.size()
		linger := time.NewTimer(p.cfg.Linger)
	collect:
		for len(batch) < p.cfg.BatchMaxRecords && size < p.cfg.BatchMaxBytes {
			select {
			case pd, ok := <-p.queue:
				if !ok {
					break collect
				}
				batch = append(batch, pd)
				size += pd.rec.size()
			case <-linger.C:
				break collect
			}
		}
		linger.Stop()
		p.flush(batch)
	}
}

func (p *Producer) flush(batch []*pending) {
	recs := make([]Record, len(batch))
	for i, pd := range batch {
		recs[i] = pd.rec
	}
	ctx, cancel := context.WithTimeout(context.Background(), p.cfg.RequestTimeout)
	mds, err := p.c.Publish(ctx, p.topic, p.cfg.Durability, recs...)
	cancel()
	for i, pd := range batch {
		var res sendResult
		if err != nil {
			res.err = err
		} else {
			res.md = mds[i]
		}
		if pd.cb != nil {
			pd.cb(res.md, res.err)
		} else {
			pd.result <- res
		}
	}
}
