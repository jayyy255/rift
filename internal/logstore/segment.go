package logstore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
)

const (
	logSuffix      = ".log"
	indexSuffix    = ".index"
	indexEntrySize = 8
	scanChunk      = 1 << 20
	// maxSegmentPos keeps every record position representable in an index entry.
	maxSegmentPos = math.MaxUint32
)

// indexEntry maps a record's offset (relative to the segment base) to its
// byte position in the segment's log file.
type indexEntry struct {
	rel uint32
	pos uint32
}

// segment is one log file plus its sparse index. Positions and sizes count
// only the validated prefix of the file.
type segment struct {
	base int64
	log  *os.File
	idx  *os.File

	index          []indexEntry
	lastIndexedPos int64 // position of the newest index entry; 0 = implicit start entry

	size    int64 // validated bytes in log
	next    int64 // offset after the last record
	firstTs int64 // timestamp of the first record (0 when empty)
	lastTs  int64 // timestamp of the last record (0 when empty)
	sealed  bool
}

func segmentPath(dir string, base int64, suffix string) string {
	return filepath.Join(dir, fmt.Sprintf("%020d%s", base, suffix))
}

// shouldIndex applies the sparse-index rule: index a record when at least
// interval bytes were written since the previous indexed position. The same
// rule runs on append and on rebuild, so both produce identical indexes.
func (s *segment) shouldIndex(pos, interval int64) bool {
	return pos > 0 && pos-s.lastIndexedPos >= interval
}

func createSegment(dir string, base int64) (*segment, error) {
	logF, err := os.OpenFile(segmentPath(dir, base, logSuffix), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return nil, err
	}
	idxF, err := os.OpenFile(segmentPath(dir, base, indexSuffix), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		logF.Close()
		return nil, err
	}
	if err := syncDir(dir); err != nil {
		logF.Close()
		idxF.Close()
		return nil, err
	}
	return &segment{base: base, log: logF, idx: idxF, next: base}, nil
}

func openSegmentFiles(dir string, base int64) (*segment, error) {
	logF, err := os.OpenFile(segmentPath(dir, base, logSuffix), os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	idxF, err := os.OpenFile(segmentPath(dir, base, indexSuffix), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		logF.Close()
		return nil, err
	}
	return &segment{base: base, log: logF, idx: idxF, next: base}, nil
}

// recoverActive opens the newest segment, validates every frame, truncates a
// torn or corrupt tail, and rebuilds the index from the surviving records.
func recoverActive(dir string, base, interval int64) (*segment, *Truncation, error) {
	s, err := openSegmentFiles(dir, base)
	if err != nil {
		return nil, nil, err
	}
	st, err := s.log.Stat()
	if err != nil {
		s.close()
		return nil, nil, err
	}
	fileSize := st.Size()

	s.index = s.index[:0]
	end, scanErr := scanFrames(s.log, 0, fileSize, func(r *Record, pos int64, n int) error {
		if r.Offset != s.next {
			return &frameError{reason: fmt.Sprintf("offset %d out of sequence, expected %d", r.Offset, s.next)}
		}
		s.observe(r, pos, n, interval)
		return nil
	})
	s.size = end

	var trunc *Truncation
	if scanErr != nil {
		trunc = &Truncation{
			Segment:  segmentPath(dir, base, logSuffix),
			Position: end,
			Bytes:    fileSize - end,
			Reason:   scanErr.Error(),
		}
		if err := s.log.Truncate(end); err != nil {
			s.close()
			return nil, nil, fmt.Errorf("logstore: truncate %s to %d: %w", trunc.Segment, end, err)
		}
	}
	if err := s.writeIndexFile(); err != nil {
		s.close()
		return nil, nil, err
	}
	if err := s.log.Sync(); err != nil {
		s.close()
		return nil, nil, err
	}
	return s, trunc, nil
}

// openSealed opens a segment that was fsynced when it was rolled. It trusts a
// valid index file and verifies the records after the last index entry, which
// yields the segment's end offset and last timestamp. Any invalid frame in a
// sealed segment is reported as corruption rather than silently truncated.
func openSealed(dir string, base, nextBase, interval int64) (_ *segment, rebuilt bool, err error) {
	s, err := openSegmentFiles(dir, base)
	if err != nil {
		return nil, false, err
	}
	defer func() {
		if err != nil {
			s.close()
		}
	}()
	st, err := s.log.Stat()
	if err != nil {
		return nil, false, err
	}
	s.size = st.Size()
	path := segmentPath(dir, base, logSuffix)

	// Resume indexing from the last trusted entry (or the start, when the index
	// file is invalid). Scanning the tail with the same rule used on append
	// both verifies those records and restores any index entries that were
	// missing, e.g. a lost or stale index file.
	valid := s.loadIndexFile(nextBase)
	if !valid {
		s.index, s.lastIndexedPos = nil, 0
	}
	trusted := len(s.index)
	startPos := s.lastIndexedPos
	s.next = base
	if trusted > 0 {
		s.next = base + int64(s.index[trusted-1].rel)
	}
	end, scanErr := scanFrames(s.log, startPos, s.size, func(r *Record, pos int64, n int) error {
		if r.Offset != s.next {
			return &frameError{reason: fmt.Sprintf("offset %d out of sequence, expected %d", r.Offset, s.next)}
		}
		s.observe(r, pos, n, interval)
		return nil
	})
	if scanErr != nil {
		return nil, false, &CorruptionError{Segment: path, Position: end, Reason: scanErr.Error()}
	}
	if !valid || len(s.index) != trusted {
		rebuilt = true
		if err := s.writeIndexFile(); err != nil {
			return nil, false, err
		}
	}
	if s.next != nextBase {
		return nil, false, &CorruptionError{
			Segment:  path,
			Position: s.size,
			Reason:   fmt.Sprintf("segment ends at offset %d but next segment starts at %d", s.next, nextBase),
		}
	}
	s.sealed = true
	return s, rebuilt, nil
}

// observe records r at pos as the segment's newest record, maintaining the
// in-memory index. Used while scanning a segment from its start.
func (s *segment) observe(r *Record, pos int64, n int, interval int64) {
	if s.shouldIndex(pos, interval) {
		s.index = append(s.index, indexEntry{rel: uint32(r.Offset - s.base), pos: uint32(pos)})
		s.lastIndexedPos = pos
	}
	if s.next == s.base {
		s.firstTs = r.TimestampMs
	}
	s.lastTs = r.TimestampMs
	s.next = r.Offset + 1
	s.size = pos + int64(n)
}

// loadIndexFile reads the index file and reports whether it is structurally
// valid for this segment. Index files are derived data: an invalid one is
// rebuilt, never trusted.
func (s *segment) loadIndexFile(nextBase int64) bool {
	data, err := io.ReadAll(io.NewSectionReader(s.idx, 0, math.MaxInt64))
	if err != nil || len(data)%indexEntrySize != 0 {
		return false
	}
	entries := make([]indexEntry, len(data)/indexEntrySize)
	var prev indexEntry
	for i := range entries {
		e := indexEntry{
			rel: binary.BigEndian.Uint32(data[i*indexEntrySize:]),
			pos: binary.BigEndian.Uint32(data[i*indexEntrySize+4:]),
		}
		if e.pos == 0 || int64(e.pos) >= s.size || s.base+int64(e.rel) >= nextBase {
			return false
		}
		if i > 0 && (e.rel <= prev.rel || e.pos <= prev.pos) {
			return false
		}
		entries[i], prev = e, e
	}
	s.index = entries
	if len(entries) > 0 {
		s.lastIndexedPos = int64(entries[len(entries)-1].pos)
	}
	return true
}

func (s *segment) writeIndexFile() error {
	buf := make([]byte, 0, len(s.index)*indexEntrySize)
	for _, e := range s.index {
		buf = binary.BigEndian.AppendUint32(buf, e.rel)
		buf = binary.BigEndian.AppendUint32(buf, e.pos)
	}
	if err := s.idx.Truncate(0); err != nil {
		return err
	}
	_, err := s.idx.WriteAt(buf, 0)
	return err
}

// lookup returns the byte position from which to scan for offset.
func (s *segment) lookup(offset int64) int64 {
	rel := uint32(offset - s.base)
	i := sort.Search(len(s.index), func(i int) bool { return s.index[i].rel > rel })
	if i == 0 {
		return 0
	}
	return int64(s.index[i-1].pos)
}

var errStopScan = errors.New("stop scan")

// read returns records starting at offset, stopping once maxRecords are
// collected or the encoded size would exceed maxBytes (always at least one).
func (s *segment) read(offset int64, maxBytes, maxRecords int) ([]Record, int, error) {
	var (
		out   []Record
		bytes int
	)
	start := s.lookup(offset)
	end, err := scanFrames(s.log, start, s.size, func(r *Record, _ int64, n int) error {
		if r.Offset < offset {
			return nil
		}
		if len(out) == 0 && r.Offset != offset {
			return &frameError{reason: fmt.Sprintf("expected offset %d, found %d", offset, r.Offset)}
		}
		if len(out) > 0 && (bytes+n > maxBytes || len(out) >= maxRecords) {
			return errStopScan
		}
		out = append(out, *r)
		bytes += n
		return nil
	})
	if err != nil && !errors.Is(err, errStopScan) {
		return nil, 0, &CorruptionError{Segment: s.log.Name(), Position: end, Reason: err.Error()}
	}
	return out, bytes, nil
}

func (s *segment) close() error {
	return errors.Join(s.log.Close(), s.idx.Close())
}

func (s *segment) remove(dir string) error {
	cerr := s.close()
	lerr := os.Remove(segmentPath(dir, s.base, logSuffix))
	ierr := os.Remove(segmentPath(dir, s.base, indexSuffix))
	if errors.Is(ierr, os.ErrNotExist) {
		ierr = nil
	}
	return errors.Join(cerr, lerr, ierr)
}

// scanFrames decodes consecutive frames in f between start and size, calling
// fn for each. It returns the position after the last valid frame and the
// first decoding error (or fn's error), if any. Records passed to fn alias a
// buffer that is never reused, so fn may retain them.
func scanFrames(f io.ReaderAt, start, size int64, fn func(r *Record, pos int64, n int) error) (int64, error) {
	var buf []byte
	bufStart := start
	pos := start
	refill := func(want int) error {
		n := int(min(int64(want), size-pos))
		nb := make([]byte, n)
		got, err := f.ReadAt(nb, pos)
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}
		if got < n {
			// The file is shorter than expected; treat the rest as missing.
			size = pos + int64(got)
		}
		buf, bufStart = nb[:got], pos
		return nil
	}
	for pos < size {
		avail := buf[pos-bufStart:]
		need := frameHeaderSize
		if len(avail) >= frameHeaderSize {
			if bl := int(binary.BigEndian.Uint32(avail)); bl >= minBodySize && bl <= maxBodySize {
				need += bl
			}
		}
		if len(avail) < need && bufStart+int64(len(buf)) < size {
			if err := refill(max(scanChunk, need)); err != nil {
				return pos, err
			}
			continue
		}
		rec, n, err := decodeFrame(avail)
		if err != nil {
			return pos, err
		}
		if err := fn(&rec, pos, n); err != nil {
			return pos, err
		}
		pos += int64(n)
	}
	return pos, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
