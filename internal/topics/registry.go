// Package topics owns the broker's topics: their persisted metadata, one
// logstore.Log per partition, key routing, background flushing, and retention.
package topics

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jayyy255/rift/internal/fsutil"
	"github.com/jayyy255/rift/internal/logstore"
	"github.com/jayyy255/rift/internal/names"
	"github.com/jayyy255/rift/internal/routing"
)

// Errors returned by the registry; the broker maps them to gRPC codes.
var (
	ErrTopicNotFound     = errors.New("topic not found")
	ErrTopicExists       = errors.New("topic already exists")
	ErrPartitionNotFound = errors.New("partition not found")
	// ErrReplicationUnavailable is returned for replication factors above 1
	// until replication exists (milestone R3).
	ErrReplicationUnavailable = errors.New("replication factor above 1 is not supported until milestone R3")
	ErrClosed                 = errors.New("topic registry closed")
)

// InvalidArgumentError reports a rejected request parameter.
type InvalidArgumentError struct{ Msg string }

func (e *InvalidArgumentError) Error() string { return e.Msg }

// MaxPartitions is the largest partition count a topic may have.
const MaxPartitions = 1024

const (
	metadataFile      = "topic.json"
	metadataFormat    = 1
	creatingDirPrefix = ".creating-"
	deletedDirPrefix  = ".deleted-"
)

// Metadata is a topic's persisted configuration.
type Metadata struct {
	FormatVersion     int    `json:"format_version"`
	Name              string `json:"name"`
	Partitions        int32  `json:"partitions"`
	ReplicationFactor int32  `json:"replication_factor"`
	// RetentionMs and RetentionBytes: 0 = broker default, -1 = unlimited.
	RetentionMs    int64 `json:"retention_ms"`
	RetentionBytes int64 `json:"retention_bytes"`
	CreatedAtMs    int64 `json:"created_at_ms"`
}

// Config configures a Registry. Zero durations select defaults; negative
// intervals disable the corresponding background loop.
type Config struct {
	DataDir string
	Log     logstore.Options
	// Broker-wide retention defaults; 0 means unlimited.
	RetentionMaxAge   time.Duration
	RetentionMaxBytes int64
	// RetentionCheckInterval defaults to 1 minute.
	RetentionCheckInterval time.Duration
	// FlushInterval bounds how long WRITTEN-durability data stays unsynced.
	// Defaults to 1 second.
	FlushInterval time.Duration
	Logger        *slog.Logger
	// OnRetention, if set, is called after retention deletes segments.
	OnRetention func(topic string, partition int32, res logstore.RetentionResult)
}

// PartitionRecovery is the recovery report of one partition opened at startup.
type PartitionRecovery struct {
	Topic     string
	Partition int32
	Report    logstore.RecoveryReport
}

// Registry is the set of topics on this broker. It is safe for concurrent use.
type Registry struct {
	cfg       Config
	topicsDir string
	log       *slog.Logger

	mu     sync.RWMutex
	topics map[string]*Topic
	closed bool

	recoveries []PartitionRecovery
	stop       chan struct{}
	wg         sync.WaitGroup
}

// Open loads every topic under cfg.DataDir/topics, recovering each partition
// log, and starts the flush and retention loops.
func Open(cfg Config) (*Registry, error) {
	if cfg.DataDir == "" {
		return nil, errors.New("topics: data directory is required")
	}
	if cfg.RetentionCheckInterval == 0 {
		cfg.RetentionCheckInterval = time.Minute
	}
	if cfg.FlushInterval == 0 {
		cfg.FlushInterval = time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	r := &Registry{
		cfg:       cfg,
		topicsDir: filepath.Join(cfg.DataDir, "topics"),
		log:       cfg.Logger,
		topics:    map[string]*Topic{},
		stop:      make(chan struct{}),
	}
	if err := os.MkdirAll(r.topicsDir, 0o755); err != nil {
		return nil, err
	}
	if err := r.load(); err != nil {
		_ = r.closeTopics() // best effort; the load error is what matters
		return nil, err
	}
	if cfg.FlushInterval > 0 {
		r.wg.Go(func() { r.loop(cfg.FlushInterval, r.flushAll) })
	}
	if cfg.RetentionCheckInterval > 0 {
		r.wg.Go(func() { r.loop(cfg.RetentionCheckInterval, r.ApplyRetention) })
	}
	return r, nil
}

func (r *Registry) load() error {
	entries, err := os.ReadDir(r.topicsDir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		name := e.Name()
		path := filepath.Join(r.topicsDir, name)
		if !e.IsDir() {
			continue
		}
		if strings.HasPrefix(name, creatingDirPrefix) || strings.HasPrefix(name, deletedDirPrefix) {
			// An interrupted create or delete: the topic never existed, or
			// its deletion was already committed by the rename.
			r.log.Info("removing leftover topic directory", "path", path)
			if err := os.RemoveAll(path); err != nil {
				return err
			}
			continue
		}
		data, err := os.ReadFile(filepath.Join(path, metadataFile))
		if err != nil {
			r.log.Warn("skipping directory without topic metadata", "path", path, "error", err)
			continue
		}
		var meta Metadata
		if err := json.Unmarshal(data, &meta); err != nil {
			return fmt.Errorf("topics: %s: %w", filepath.Join(path, metadataFile), err)
		}
		if meta.FormatVersion != metadataFormat || meta.Name != name || meta.Partitions < 1 {
			return fmt.Errorf("topics: %s: unsupported or inconsistent metadata", filepath.Join(path, metadataFile))
		}
		t, err := r.openTopic(path, meta)
		if err != nil {
			return err
		}
		r.topics[name] = t
		for p, l := range t.logs {
			rep := l.Recovery()
			r.recoveries = append(r.recoveries, PartitionRecovery{Topic: name, Partition: int32(p), Report: rep})
			attrs := []any{"topic", name, "partition", p, "start_offset", rep.StartOffset,
				"end_offset", rep.EndOffset, "segments", rep.Segments, "duration", rep.Duration}
			if rep.RebuiltIndexes > 0 {
				attrs = append(attrs, "rebuilt_indexes", rep.RebuiltIndexes)
			}
			if tr := rep.Truncation; tr != nil {
				r.log.Warn("partition recovered with truncated tail", append(attrs,
					"segment", tr.Segment, "truncated_at", tr.Position, "truncated_bytes", tr.Bytes, "reason", tr.Reason)...)
			} else {
				r.log.Debug("partition recovered", attrs...)
			}
		}
	}
	return nil
}

func (r *Registry) openTopic(dir string, meta Metadata) (*Topic, error) {
	t := &Topic{meta: meta, dir: dir}
	ch := make(chan struct{})
	t.notify.Store(&ch)
	for p := range meta.Partitions {
		l, err := logstore.Open(filepath.Join(dir, strconv.Itoa(int(p))), r.cfg.Log)
		if err != nil {
			_ = t.closeLogs() // best effort; the open error is what matters
			return nil, fmt.Errorf("topics: open %s partition %d: %w", meta.Name, p, err)
		}
		t.logs = append(t.logs, l)
	}
	return t, nil
}

// Recoveries returns the per-partition recovery reports from startup.
func (r *Registry) Recoveries() []PartitionRecovery { return r.recoveries }

// CreateOptions are the settable properties of a new topic.
type CreateOptions struct {
	Partitions        int32
	ReplicationFactor int32
	RetentionMs       int64
	RetentionBytes    int64
}

// Create atomically creates a topic: it is built in a temporary directory
// and renamed into place, so a crash never leaves a partial topic.
func (r *Registry) Create(name string, o CreateOptions) (*Topic, error) {
	if err := names.Validate("topic", name); err != nil {
		return nil, &InvalidArgumentError{err.Error()}
	}
	if o.Partitions == 0 {
		o.Partitions = 1
	}
	if o.Partitions < 1 || o.Partitions > MaxPartitions {
		return nil, &InvalidArgumentError{fmt.Sprintf("partitions must be between 1 and %d", MaxPartitions)}
	}
	if o.ReplicationFactor == 0 {
		o.ReplicationFactor = 1
	}
	if o.ReplicationFactor < 0 {
		return nil, &InvalidArgumentError{"replication factor must be positive"}
	}
	if o.ReplicationFactor > 1 {
		return nil, ErrReplicationUnavailable
	}
	if o.RetentionMs < -1 || o.RetentionBytes < -1 {
		return nil, &InvalidArgumentError{"retention values must be -1 (unlimited), 0 (broker default), or positive"}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, ErrClosed
	}
	if _, ok := r.topics[name]; ok {
		return nil, ErrTopicExists
	}

	meta := Metadata{
		FormatVersion:     metadataFormat,
		Name:              name,
		Partitions:        o.Partitions,
		ReplicationFactor: o.ReplicationFactor,
		RetentionMs:       o.RetentionMs,
		RetentionBytes:    o.RetentionBytes,
		CreatedAtMs:       time.Now().UnixMilli(),
	}
	tmp, err := os.MkdirTemp(r.topicsDir, creatingDirPrefix+name+"-")
	if err != nil {
		return nil, err
	}
	final := filepath.Join(r.topicsDir, name)
	if err := r.buildTopicDir(tmp, meta); err != nil {
		os.RemoveAll(tmp)
		return nil, err
	}
	if err := os.Rename(tmp, final); err != nil {
		os.RemoveAll(tmp)
		return nil, err
	}
	if err := fsutil.SyncDir(r.topicsDir); err != nil {
		return nil, err
	}
	t, err := r.openTopic(final, meta)
	if err != nil {
		return nil, err
	}
	r.topics[name] = t
	r.log.Info("topic created", "topic", name, "partitions", meta.Partitions)
	return t, nil
}

func (r *Registry) buildTopicDir(dir string, meta Metadata) error {
	for p := range meta.Partitions {
		l, err := logstore.Open(filepath.Join(dir, strconv.Itoa(int(p))), r.cfg.Log)
		if err != nil {
			return err
		}
		if err := l.Close(); err != nil {
			return err
		}
	}
	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return err
	}
	return fsutil.WriteFileAtomic(filepath.Join(dir, metadataFile), data)
}

// Delete removes a topic and all of its data. The rename to a hidden
// directory is the commit point; the files are removed afterwards.
func (r *Registry) Delete(name string) error {
	r.mu.Lock()
	t, ok := r.topics[name]
	if ok {
		delete(r.topics, name)
	}
	closed := r.closed
	r.mu.Unlock()
	if closed {
		return ErrClosed
	}
	if !ok {
		return ErrTopicNotFound
	}

	t.deleted.Store(true)
	cerr := t.closeLogs()
	t.wake()
	trash := filepath.Join(r.topicsDir, fmt.Sprintf("%s%s-%d", deletedDirPrefix, name, time.Now().UnixNano()))
	if err := os.Rename(t.dir, trash); err != nil {
		return errors.Join(cerr, err)
	}
	if err := fsutil.SyncDir(r.topicsDir); err != nil {
		return errors.Join(cerr, err)
	}
	r.log.Info("topic deleted", "topic", name)
	return errors.Join(cerr, os.RemoveAll(trash))
}

// Get returns the named topic.
func (r *Registry) Get(name string) (*Topic, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return nil, ErrClosed
	}
	t, ok := r.topics[name]
	if !ok {
		return nil, ErrTopicNotFound
	}
	return t, nil
}

// List returns all topics ordered by name.
func (r *Registry) List() []*Topic {
	r.mu.RLock()
	out := make([]*Topic, 0, len(r.topics))
	for _, t := range r.topics {
		out = append(out, t)
	}
	r.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].meta.Name < out[j].meta.Name })
	return out
}

func (r *Registry) loop(every time.Duration, fn func()) {
	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		select {
		case <-r.stop:
			return
		case <-tick.C:
			fn()
		}
	}
}

// flushAll fsyncs partitions that have unsynced records.
func (r *Registry) flushAll() {
	for _, t := range r.List() {
		for p, l := range t.logs {
			if end := l.EndOffset(); l.SyncedOffset() < end {
				if err := l.Sync(end); err != nil && !t.deleted.Load() {
					r.log.Error("background flush failed", "topic", t.meta.Name, "partition", p, "error", err)
				}
			}
		}
	}
}

// ApplyRetention runs retention on every partition using each topic's
// effective limits.
func (r *Registry) ApplyRetention() {
	for _, t := range r.List() {
		maxAge, maxBytes := r.effectiveRetention(t.meta)
		if maxAge <= 0 && maxBytes <= 0 {
			continue
		}
		for p, l := range t.logs {
			res, err := l.ApplyRetention(maxAge, maxBytes)
			if err != nil {
				if !t.deleted.Load() {
					r.log.Error("retention failed", "topic", t.meta.Name, "partition", p, "error", err)
				}
				continue
			}
			if res.Segments > 0 {
				r.log.Info("retention deleted segments", "topic", t.meta.Name, "partition", p,
					"segments", res.Segments, "bytes", res.Bytes, "start_offset", l.StartOffset())
				if r.cfg.OnRetention != nil {
					r.cfg.OnRetention(t.meta.Name, int32(p), res)
				}
			}
		}
	}
}

func (r *Registry) effectiveRetention(m Metadata) (time.Duration, int64) {
	age := r.cfg.RetentionMaxAge
	switch {
	case m.RetentionMs > 0:
		age = time.Duration(m.RetentionMs) * time.Millisecond
	case m.RetentionMs == -1:
		age = 0
	}
	size := r.cfg.RetentionMaxBytes
	switch {
	case m.RetentionBytes > 0:
		size = m.RetentionBytes
	case m.RetentionBytes == -1:
		size = 0
	}
	return age, size
}

// Close stops background work and closes every partition log (fsyncing it).
func (r *Registry) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	r.mu.Unlock()
	close(r.stop)
	r.wg.Wait()
	return r.closeTopics()
}

func (r *Registry) closeTopics() error {
	var errs []error
	for _, t := range r.topics {
		errs = append(errs, t.closeLogs())
		t.wake()
	}
	return errors.Join(errs...)
}

// Topic is one topic and its partition logs.
type Topic struct {
	meta    Metadata
	dir     string
	logs    []*logstore.Log
	rr      atomic.Uint32
	notify  atomic.Pointer[chan struct{}]
	deleted atomic.Bool
}

// Metadata returns the topic's configuration.
func (t *Topic) Metadata() Metadata { return t.meta }

// Partitions returns the partition count.
func (t *Topic) Partitions() int32 { return t.meta.Partitions }

// Log returns the log of a partition.
func (t *Topic) Log(partition int32) (*logstore.Log, error) {
	if partition < 0 || partition >= t.meta.Partitions {
		return nil, fmt.Errorf("%w: %d (topic %q has %d)", ErrPartitionNotFound, partition, t.meta.Name, t.meta.Partitions)
	}
	return t.logs[partition], nil
}

// ChoosePartition routes a record: keyed records by key hash, keyless records
// round-robin. A nil key means keyless.
func (t *Topic) ChoosePartition(key []byte) int32 {
	if key != nil {
		return routing.PartitionForKey(key, t.meta.Partitions)
	}
	return int32((t.rr.Add(1) - 1) % uint32(t.meta.Partitions))
}

// Append appends records to a partition and wakes waiting fetches.
func (t *Topic) Append(partition int32, records []logstore.Record) (first, timestampMs int64, err error) {
	l, err := t.Log(partition)
	if err != nil {
		return 0, 0, err
	}
	first, timestampMs, err = l.Append(records)
	if err != nil {
		return 0, 0, t.mapErr(err)
	}
	t.wake()
	return first, timestampMs, nil
}

// Sync makes records below upTo in a partition durable.
func (t *Topic) Sync(partition int32, upTo int64) error {
	l, err := t.Log(partition)
	if err != nil {
		return err
	}
	return t.mapErr(l.Sync(upTo))
}

// Read reads records from a partition; see logstore.Log.Read.
func (t *Topic) Read(partition int32, offset int64, maxBytes, maxRecords int) ([]logstore.Record, error) {
	l, err := t.Log(partition)
	if err != nil {
		return nil, err
	}
	recs, err := l.Read(offset, maxBytes, maxRecords)
	return recs, t.mapErr(err)
}

// Changed returns a channel that is closed at the next append to any
// partition, or when the topic is deleted or closed.
func (t *Topic) Changed() <-chan struct{} { return *t.notify.Load() }

// Deleted reports whether the topic was deleted.
func (t *Topic) Deleted() bool { return t.deleted.Load() }

func (t *Topic) wake() {
	ch := make(chan struct{})
	old := t.notify.Swap(&ch)
	close(*old)
}

func (t *Topic) mapErr(err error) error {
	if err != nil && t.deleted.Load() && errors.Is(err, logstore.ErrClosed) {
		return ErrTopicNotFound
	}
	return err
}

func (t *Topic) closeLogs() error {
	var errs []error
	for _, l := range t.logs {
		errs = append(errs, l.Close())
	}
	return errors.Join(errs...)
}
