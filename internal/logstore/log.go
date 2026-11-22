package logstore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrOffsetOutOfRange is returned when reading below the log start offset
	// or above the log end offset.
	ErrOffsetOutOfRange = errors.New("logstore: offset out of range")
	// ErrClosed is returned by operations on a closed log.
	ErrClosed = errors.New("logstore: log closed")
	// ErrCorrupt is wrapped by every CorruptionError.
	ErrCorrupt = errors.New("logstore: corrupt data")
)

// CorruptionError describes stored bytes that failed validation.
type CorruptionError struct {
	Segment  string
	Position int64
	Reason   string
}

func (e *CorruptionError) Error() string {
	return fmt.Sprintf("logstore: corrupt data in %s at byte %d: %s", e.Segment, e.Position, e.Reason)
}

func (e *CorruptionError) Unwrap() error { return ErrCorrupt }

// Truncation records an invalid tail removed from the active segment during
// recovery: either a torn write or corruption after the last valid record.
type Truncation struct {
	Segment  string
	Position int64 // new segment size: bytes before this position were kept
	Bytes    int64 // bytes removed
	Reason   string
}

// RecoveryReport summarizes what Open found and repaired.
type RecoveryReport struct {
	Segments       int
	RebuiltIndexes int
	Truncation     *Truncation // nil when the active segment was clean
	StartOffset    int64
	EndOffset      int64
	Duration       time.Duration
}

// Options configure a Log. Zero values select the defaults.
type Options struct {
	// SegmentBytes is the size at which the active segment is rolled.
	// Default 64 MiB; at most 1 GiB.
	SegmentBytes int64
	// SegmentMaxAge rolls a non-empty active segment once its first record is
	// this old, so time-based retention can delete it. 0 disables age rolls.
	SegmentMaxAge time.Duration
	// IndexIntervalBytes is the approximate spacing of sparse index entries.
	// Default 4 KiB.
	IndexIntervalBytes int64
	// Now returns the current time; it stamps records and drives age checks.
	Now func() time.Time
}

// Storage defaults and limits.
const (
	DefaultSegmentBytes       = 64 << 20
	MaxSegmentBytes           = 1 << 30
	DefaultIndexIntervalBytes = 4 << 10
)

func (o Options) withDefaults() (Options, error) {
	if o.SegmentBytes == 0 {
		o.SegmentBytes = DefaultSegmentBytes
	}
	if o.SegmentBytes < 1024 || o.SegmentBytes > MaxSegmentBytes {
		return o, fmt.Errorf("logstore: segment size %d outside [1KiB, 1GiB]", o.SegmentBytes)
	}
	if o.IndexIntervalBytes == 0 {
		o.IndexIntervalBytes = DefaultIndexIntervalBytes
	}
	if o.IndexIntervalBytes < 0 {
		return o, errors.New("logstore: negative index interval")
	}
	if o.SegmentMaxAge < 0 {
		return o, errors.New("logstore: negative segment max age")
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	return o, nil
}

// Log is a durable, append-only, segmented record log for one partition.
// It is safe for concurrent use. Writes are serialized; reads run concurrently
// with each other.
type Log struct {
	dir  string
	opts Options

	mu       sync.RWMutex
	segments []*segment // ordered by base offset; the last is active
	closed   bool
	failed   error // sticky: set when the on-disk state can no longer be trusted
	lastTs   int64

	end    atomic.Int64 // log end offset: the offset the next record receives
	synced atomic.Int64 // all offsets below this are fsynced

	syncMu   sync.Mutex
	recovery RecoveryReport
}

// Open opens or creates the log in dir, recovering from any crash: the active
// segment is validated record by record and an invalid tail is truncated.
// Corruption in a sealed segment is returned as an error wrapping ErrCorrupt.
func Open(dir string, opts Options) (*Log, error) {
	start := time.Now()
	opts, err := opts.withDefaults()
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	bases, err := discoverSegments(dir)
	if err != nil {
		return nil, err
	}

	l := &Log{dir: dir, opts: opts}
	if len(bases) == 0 {
		s, err := createSegment(dir, 0)
		if err != nil {
			return nil, err
		}
		l.segments = []*segment{s}
	} else {
		for i, base := range bases {
			if i < len(bases)-1 {
				s, rebuilt, err := openSealed(dir, base, bases[i+1], opts.IndexIntervalBytes)
				if err != nil {
					_ = l.closeSegments() // best effort; the open error is what matters
					return nil, err
				}
				if rebuilt {
					l.recovery.RebuiltIndexes++
				}
				l.segments = append(l.segments, s)
				continue
			}
			s, trunc, err := recoverActive(dir, base, opts.IndexIntervalBytes)
			if err != nil {
				_ = l.closeSegments() // best effort; the open error is what matters
				return nil, err
			}
			l.recovery.Truncation = trunc
			l.segments = append(l.segments, s)
		}
	}

	active := l.active()
	l.end.Store(active.next)
	l.synced.Store(active.next) // recoverActive fsynced the active segment
	for i := len(l.segments) - 1; i >= 0; i-- {
		if l.segments[i].next > l.segments[i].base {
			l.lastTs = l.segments[i].lastTs
			break
		}
	}
	l.recovery.Segments = len(l.segments)
	l.recovery.StartOffset = l.segments[0].base
	l.recovery.EndOffset = active.next
	l.recovery.Duration = time.Since(start)
	return l, nil
}

// discoverSegments returns the sorted base offsets of the segment log files in
// dir, removing orphaned index files and temporary files left by a crash.
func discoverSegments(dir string) ([]int64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	logs := map[int64]bool{}
	var indexes []int64
	for _, e := range entries {
		name := e.Name()
		switch {
		case strings.HasSuffix(name, ".tmp"):
			if err := os.Remove(filepath.Join(dir, name)); err != nil {
				return nil, err
			}
		case strings.HasSuffix(name, logSuffix):
			if base, ok := parseBase(name, logSuffix); ok {
				logs[base] = true
			}
		case strings.HasSuffix(name, indexSuffix):
			if base, ok := parseBase(name, indexSuffix); ok {
				indexes = append(indexes, base)
			}
		}
	}
	for _, base := range indexes {
		if !logs[base] {
			if err := os.Remove(segmentPath(dir, base, indexSuffix)); err != nil {
				return nil, err
			}
		}
	}
	bases := make([]int64, 0, len(logs))
	for b := range logs {
		bases = append(bases, b)
	}
	sort.Slice(bases, func(i, j int) bool { return bases[i] < bases[j] })
	return bases, nil
}

func parseBase(name, suffix string) (int64, bool) {
	stem := strings.TrimSuffix(name, suffix)
	if len(stem) != 20 {
		return 0, false
	}
	b, err := strconv.ParseInt(stem, 10, 64)
	return b, err == nil && b >= 0
}

func (l *Log) active() *segment { return l.segments[len(l.segments)-1] }

// Append writes records as one batch and returns the offset assigned to the
// first record and the append timestamp shared by all of them. Offsets are
// consecutive. The batch is in the OS page cache when Append returns; call
// Sync to make it durable.
func (l *Log) Append(records []Record) (firstOffset, timestampMs int64, err error) {
	if len(records) == 0 {
		return 0, 0, errors.New("logstore: empty batch")
	}
	total := 0
	for i := range records {
		if err := validateRecord(&records[i]); err != nil {
			return 0, 0, err
		}
		total += records[i].EncodedSize()
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return 0, 0, ErrClosed
	}
	if l.failed != nil {
		return 0, 0, l.failed
	}

	now := l.opts.Now().UnixMilli()
	ts := max(now, l.lastTs)
	seg := l.active()
	if seg.size > 0 && (seg.size+int64(total) > l.opts.SegmentBytes ||
		(l.opts.SegmentMaxAge > 0 && now-seg.firstTs >= l.opts.SegmentMaxAge.Milliseconds())) {
		if err := l.roll(); err != nil {
			return 0, 0, err
		}
		seg = l.active()
	}
	if seg.size+int64(total) > maxSegmentPos {
		return 0, 0, fmt.Errorf("logstore: batch of %d bytes does not fit in a segment", total)
	}

	first := seg.next
	buf := make([]byte, 0, total)
	var newEntries []indexEntry
	lastIndexed := seg.lastIndexedPos
	for i := range records {
		pos := seg.size + int64(len(buf))
		if pos > 0 && pos-lastIndexed >= l.opts.IndexIntervalBytes {
			newEntries = append(newEntries, indexEntry{rel: uint32(first + int64(i) - seg.base), pos: uint32(pos)})
			lastIndexed = pos
		}
		r := records[i]
		r.Offset, r.TimestampMs = first+int64(i), ts
		buf = appendFrame(buf, &r)
	}

	if _, err := seg.log.WriteAt(buf, seg.size); err != nil {
		// Remove any partial write so the log stays a valid prefix. If that
		// fails too, refuse further writes until recovery runs on restart.
		if terr := seg.log.Truncate(seg.size); terr != nil {
			l.failed = fmt.Errorf("logstore: write failed and could not be undone: %w", errors.Join(err, terr))
		}
		return 0, 0, err
	}
	if len(newEntries) > 0 {
		ib := make([]byte, 0, len(newEntries)*indexEntrySize)
		for _, e := range newEntries {
			ib = binary.BigEndian.AppendUint32(ib, e.rel)
			ib = binary.BigEndian.AppendUint32(ib, e.pos)
		}
		if _, err := seg.idx.WriteAt(ib, int64(len(seg.index))*indexEntrySize); err != nil {
			l.failed = fmt.Errorf("logstore: index write failed: %w", err)
			return 0, 0, l.failed
		}
		seg.index = append(seg.index, newEntries...)
		seg.lastIndexedPos = lastIndexed
	}

	if seg.next == seg.base {
		seg.firstTs = ts
	}
	seg.size += int64(len(buf))
	seg.next = first + int64(len(records))
	seg.lastTs = ts
	l.lastTs = ts
	l.end.Store(seg.next)
	return first, ts, nil
}

// roll seals the active segment (fsyncing it) and starts a new one.
// Caller holds l.mu.
func (l *Log) roll() error {
	seg := l.active()
	if err := seg.log.Sync(); err != nil {
		l.failed = fmt.Errorf("logstore: fsync on roll: %w", err)
		return l.failed
	}
	if err := seg.idx.Sync(); err != nil {
		l.failed = fmt.Errorf("logstore: index fsync on roll: %w", err)
		return l.failed
	}
	seg.sealed = true
	l.storeSynced(seg.next)

	ns, err := createSegment(l.dir, seg.next)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, ns)
	return nil
}

func (l *Log) storeSynced(v int64) {
	for {
		cur := l.synced.Load()
		if v <= cur || l.synced.CompareAndSwap(cur, v) {
			return
		}
	}
}

// Sync makes every record below upTo durable. Concurrent callers share fsync
// calls (group commit): one fsync covers all records appended before it.
// An fsync failure is sticky; the log then rejects writes, because the kernel
// may have dropped the unwritten pages.
func (l *Log) Sync(upTo int64) error {
	if l.synced.Load() >= upTo {
		return nil
	}
	l.syncMu.Lock()
	defer l.syncMu.Unlock()
	if l.synced.Load() >= upTo {
		return nil
	}

	l.mu.RLock()
	if l.closed {
		l.mu.RUnlock()
		return ErrClosed
	}
	if l.failed != nil {
		l.mu.RUnlock()
		return l.failed
	}
	seg := l.active()
	end := seg.next
	l.mu.RUnlock()

	if err := seg.log.Sync(); err != nil {
		l.mu.Lock()
		defer l.mu.Unlock()
		if seg.sealed { // rolled concurrently: roll fsynced it and the file may be closed
			l.storeSynced(end)
			return nil
		}
		if l.closed {
			return ErrClosed
		}
		l.failed = fmt.Errorf("logstore: fsync: %w", err)
		return l.failed
	}
	l.storeSynced(end)
	return nil
}

// Read returns up to maxRecords records starting at offset, stopping before
// the total encoded size would exceed maxBytes (but always returning at least
// one record when offset is below the end offset). Reading at the end offset
// returns no records.
func (l *Log) Read(offset int64, maxBytes, maxRecords int) ([]Record, error) {
	if maxRecords <= 0 {
		maxRecords = 1
	}
	l.mu.RLock()
	defer l.mu.RUnlock()
	if l.closed {
		return nil, ErrClosed
	}
	start, end := l.segments[0].base, l.active().next
	if offset < start || offset > end {
		return nil, fmt.Errorf("%w: offset %d not in [%d, %d]", ErrOffsetOutOfRange, offset, start, end)
	}

	i := sort.Search(len(l.segments), func(i int) bool { return l.segments[i].base > offset }) - 1
	var out []Record
	bytes := 0
	for ; i < len(l.segments) && offset < end; i++ {
		seg := l.segments[i]
		if offset >= seg.next {
			continue
		}
		budget := maxBytes - bytes
		if len(out) > 0 && budget <= 0 {
			break
		}
		if len(out) == 0 {
			budget = max(budget, 1)
		}
		recs, n, err := seg.read(offset, budget, maxRecords-len(out))
		if err != nil {
			return nil, err
		}
		out = append(out, recs...)
		bytes += n
		offset = seg.next
		if len(out) >= maxRecords || len(recs) == 0 || recs[len(recs)-1].Offset+1 < seg.next {
			break // limits reached inside this segment
		}
	}
	return out, nil
}

// StartOffset is the oldest retained offset.
func (l *Log) StartOffset() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.segments[0].base
}

// EndOffset is the offset the next appended record will receive.
func (l *Log) EndOffset() int64 { return l.end.Load() }

// SyncedOffset reports that every record below it is fsynced.
func (l *Log) SyncedOffset() int64 { return l.synced.Load() }

// Stats is a point-in-time summary of the log.
type Stats struct {
	StartOffset  int64
	EndOffset    int64
	SyncedOffset int64
	SizeBytes    int64
	Segments     int
}

// Stats returns a consistent snapshot of the log's offsets and size.
func (l *Log) Stats() Stats {
	l.mu.RLock()
	defer l.mu.RUnlock()
	st := Stats{
		StartOffset:  l.segments[0].base,
		EndOffset:    l.active().next,
		SyncedOffset: l.synced.Load(),
		Segments:     len(l.segments),
	}
	for _, s := range l.segments {
		st.SizeBytes += s.size
	}
	return st
}

// Recovery returns what Open found and repaired.
func (l *Log) Recovery() RecoveryReport { return l.recovery }

// RetentionResult reports segments removed by ApplyRetention.
type RetentionResult struct {
	Segments int
	Bytes    int64
}

// ApplyRetention deletes the oldest sealed segments whose newest record is
// older than maxAge, or while the log exceeds maxBytes without them. A zero
// or negative limit is disabled. The active segment is never deleted, so the
// log keeps its end offset.
func (l *Log) ApplyRetention(maxAge time.Duration, maxBytes int64) (RetentionResult, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	var res RetentionResult
	if l.closed {
		return res, ErrClosed
	}
	now := l.opts.Now().UnixMilli()
	var total int64
	for _, s := range l.segments {
		total += s.size
	}
	for len(l.segments) > 1 {
		s := l.segments[0]
		byAge := maxAge > 0 && now-s.lastTs > maxAge.Milliseconds()
		bySize := maxBytes > 0 && total-s.size >= maxBytes
		if !byAge && !bySize {
			break
		}
		if err := s.remove(l.dir); err != nil {
			return res, err
		}
		l.segments = l.segments[1:]
		total -= s.size
		res.Segments++
		res.Bytes += s.size
	}
	if res.Segments > 0 {
		if err := syncDir(l.dir); err != nil {
			return res, err
		}
	}
	return res, nil
}

// Close fsyncs the active segment and closes all files.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	var err error
	if l.failed == nil {
		seg := l.active()
		if err = seg.log.Sync(); err == nil {
			l.storeSynced(seg.next)
		}
	}
	return errors.Join(err, l.closeSegments())
}

func (l *Log) closeSegments() error {
	var errs []error
	for _, s := range l.segments {
		errs = append(errs, s.close())
	}
	return errors.Join(errs...)
}
