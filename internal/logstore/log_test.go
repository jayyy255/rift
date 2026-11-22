package logstore

import (
	"bytes"
	"errors"
	"os"
	"sync"
	"testing"
	"time"
)

func TestEmptyLog(t *testing.T) {
	l := mustOpen(t, t.TempDir(), Options{})
	if l.StartOffset() != 0 || l.EndOffset() != 0 {
		t.Fatalf("start/end = %d/%d, want 0/0", l.StartOffset(), l.EndOffset())
	}
	recs, err := l.Read(0, 1024, 10)
	if err != nil || len(recs) != 0 {
		t.Fatalf("Read at end = %v, %v; want no records", recs, err)
	}
	if _, _, err := l.Append(nil); err == nil {
		t.Fatal("empty batch accepted")
	}
}

func TestAppendAssignsSequentialOffsetsAndReads(t *testing.T) {
	l := mustOpen(t, t.TempDir(), Options{})
	end := appendN(t, l, 500, 7)
	if end != 500 {
		t.Fatalf("end = %d, want 500", end)
	}
	verifyAll(t, l, 0, end)
}

func TestReadEveryOffsetThroughSparseIndex(t *testing.T) {
	l := mustOpen(t, t.TempDir(), Options{IndexIntervalBytes: 64, SegmentBytes: 8 << 10})
	end := appendN(t, l, 400, 3)
	if st := l.Stats(); st.Segments < 3 {
		t.Fatalf("expected several segments, got %d", st.Segments)
	}
	for off := range end {
		recs, err := l.Read(off, 1, 1)
		if err != nil || len(recs) != 1 {
			t.Fatalf("Read(%d) = %d records, %v", off, len(recs), err)
		}
		checkRecord(t, recs[0], off)
	}
}

func TestReadLimits(t *testing.T) {
	l := mustOpen(t, t.TempDir(), Options{})
	appendN(t, l, 50, 50)
	recs, _ := l.Read(10, 1, 100)
	if len(recs) != 1 {
		t.Fatalf("maxBytes=1 returned %d records, want exactly 1", len(recs))
	}
	recs, _ = l.Read(10, 1<<20, 5)
	if len(recs) != 5 || recs[4].Offset != 14 {
		t.Fatalf("maxRecords=5 returned %d records", len(recs))
	}
	size := recs[0].EncodedSize() + recs[1].EncodedSize()
	recs, _ = l.Read(10, size, 100)
	if len(recs) != 2 {
		t.Fatalf("maxBytes for 2 records returned %d", len(recs))
	}
}

func TestReadSpansSegments(t *testing.T) {
	l := mustOpen(t, t.TempDir(), Options{SegmentBytes: 2048})
	appendN(t, l, 100, 1)
	recs, err := l.Read(0, 1<<20, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 100 {
		t.Fatalf("got %d records across segments, want 100", len(recs))
	}
}

func TestReadOutOfRange(t *testing.T) {
	l := mustOpen(t, t.TempDir(), Options{})
	appendN(t, l, 10, 10)
	for _, off := range []int64{-1, 11, 1 << 40} {
		if _, err := l.Read(off, 100, 1); !errors.Is(err, ErrOffsetOutOfRange) {
			t.Errorf("Read(%d) err = %v, want ErrOffsetOutOfRange", off, err)
		}
	}
}

func TestSegmentRollBySize(t *testing.T) {
	dir := t.TempDir()
	l := mustOpen(t, dir, Options{SegmentBytes: 4096})
	end := appendN(t, l, 300, 4)
	st := l.Stats()
	if st.Segments < 5 {
		t.Fatalf("segments = %d, want >= 5", st.Segments)
	}
	files, _ := SegmentFiles(dir)
	for _, f := range files[:len(files)-1] {
		fi, _ := os.Stat(f)
		// A segment may exceed the limit only by the batch that started it.
		if fi.Size() > 4096+4*200 {
			t.Errorf("%s is %d bytes", f, fi.Size())
		}
	}
	verifyAll(t, l, 0, end)
}

func TestSegmentRollByAge(t *testing.T) {
	clk := newFakeClock()
	l := mustOpen(t, t.TempDir(), Options{SegmentMaxAge: time.Hour, Now: clk.Now})
	appendN(t, l, 5, 1)
	clk.Advance(59 * time.Minute)
	appendN(t, l, 5, 1)
	if s := l.Stats().Segments; s != 1 {
		t.Fatalf("segments = %d before max age, want 1", s)
	}
	clk.Advance(2 * time.Minute)
	appendN(t, l, 1, 1)
	if s := l.Stats().Segments; s != 2 {
		t.Fatalf("segments = %d after max age, want 2", s)
	}
}

func TestReopenPreservesDataAndContinuesOffsets(t *testing.T) {
	dir := t.TempDir()
	opts := Options{SegmentBytes: 4096, IndexIntervalBytes: 128}
	l, _ := Open(dir, opts)
	appendN(t, l, 200, 5)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	l = mustOpen(t, dir, opts)
	rep := l.Recovery()
	if rep.Truncation != nil || rep.RebuiltIndexes != 0 {
		t.Fatalf("clean reopen reported repairs: %+v", rep)
	}
	if l.EndOffset() != 200 || rep.EndOffset != 200 {
		t.Fatalf("end after reopen = %d", l.EndOffset())
	}
	end := appendN(t, l, 50, 5)
	verifyAll(t, l, 0, end)
}

// Index files written during appends must match what a rebuild produces.
func TestRebuiltIndexMatchesWrittenIndex(t *testing.T) {
	dir := t.TempDir()
	opts := Options{SegmentBytes: 4096, IndexIntervalBytes: 100}
	l, _ := Open(dir, opts)
	appendN(t, l, 300, 3)
	l.Close()

	files, _ := SegmentFiles(dir)
	written := map[string][]byte{}
	for _, f := range files {
		idx := f[:len(f)-len(logSuffix)] + indexSuffix
		written[idx], _ = os.ReadFile(idx)
		os.Remove(idx)
	}
	l = mustOpen(t, dir, opts)
	if got := l.Recovery().RebuiltIndexes; got != len(files)-1 {
		t.Fatalf("rebuilt %d sealed indexes, want %d", got, len(files)-1)
	}
	l.Close()
	for idx, want := range written {
		got, _ := os.ReadFile(idx)
		if !bytes.Equal(got, want) {
			t.Errorf("%s: rebuilt index differs (%d vs %d bytes)", idx, len(got), len(want))
		}
	}
}

func TestRetentionBySize(t *testing.T) {
	l := mustOpen(t, t.TempDir(), Options{SegmentBytes: 2048})
	end := appendN(t, l, 400, 2)
	before := l.Stats()
	res, err := l.ApplyRetention(0, 8192)
	if err != nil {
		t.Fatal(err)
	}
	after := l.Stats()
	if res.Segments == 0 || after.SizeBytes > 8192+2048+400 || after.SizeBytes < 8192 {
		t.Fatalf("retention removed %d segments: size %d -> %d", res.Segments, before.SizeBytes, after.SizeBytes)
	}
	if after.StartOffset == 0 || after.EndOffset != end {
		t.Fatalf("start/end = %d/%d", after.StartOffset, after.EndOffset)
	}
	if _, err := l.Read(0, 100, 1); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatalf("read of deleted offset: %v", err)
	}
	verifyAll(t, l, after.StartOffset, end)
}

func TestRetentionByAge(t *testing.T) {
	clk := newFakeClock()
	l := mustOpen(t, t.TempDir(), Options{SegmentBytes: 2048, Now: clk.Now})
	appendN(t, l, 100, 2) // old data
	clk.Advance(2 * time.Hour)
	appendN(t, l, 100, 2) // new data, rolls into fresh segments
	res, err := l.ApplyRetention(time.Hour, 0)
	if err != nil || res.Segments == 0 {
		t.Fatalf("retention = %+v, %v", res, err)
	}
	start := l.StartOffset()
	if start > 100 {
		t.Fatalf("start = %d: deleted records newer than max age", start)
	}
	recs, _ := l.Read(start, 1<<20, 1)
	if age := clk.Now().UnixMilli() - recs[0].TimestampMs; age > time.Hour.Milliseconds() && start < 100 {
		// Records in the first retained segment may be old only if the
		// segment also contains newer records.
		seg := l.segments[0]
		if clk.Now().UnixMilli()-seg.lastTs > time.Hour.Milliseconds() {
			t.Fatalf("retained a segment whose newest record is %d ms old", age)
		}
	}
}

func TestRetentionNeverDeletesActiveSegment(t *testing.T) {
	clk := newFakeClock()
	l := mustOpen(t, t.TempDir(), Options{Now: clk.Now})
	appendN(t, l, 10, 10)
	clk.Advance(1000 * time.Hour)
	res, err := l.ApplyRetention(time.Millisecond, 1)
	if err != nil || res.Segments != 0 {
		t.Fatalf("deleted active segment: %+v %v", res, err)
	}
	verifyAll(t, l, 0, 10)
}

func TestTimestampsNeverGoBackwards(t *testing.T) {
	clk := newFakeClock()
	l := mustOpen(t, t.TempDir(), Options{Now: clk.Now})
	_, ts1, _ := l.Append([]Record{{Value: []byte("a")}})
	clk.Advance(-time.Hour)
	_, ts2, _ := l.Append([]Record{{Value: []byte("b")}})
	if ts2 < ts1 {
		t.Fatalf("timestamp went backwards: %d then %d", ts1, ts2)
	}
}

func TestSyncAdvancesSyncedOffset(t *testing.T) {
	l := mustOpen(t, t.TempDir(), Options{})
	appendN(t, l, 10, 10)
	if err := l.Sync(10); err != nil {
		t.Fatal(err)
	}
	if l.SyncedOffset() < 10 {
		t.Fatalf("synced = %d, want >= 10", l.SyncedOffset())
	}
	if err := l.Sync(5); err != nil { // already durable: no-op
		t.Fatal(err)
	}
}

func TestConcurrentAppendSyncAndRead(t *testing.T) {
	l := mustOpen(t, t.TempDir(), Options{SegmentBytes: 16 << 10, IndexIntervalBytes: 256})
	const writers, perWriter = 8, 200
	var wg sync.WaitGroup
	for range writers {
		wg.Go(func() {
			for range perWriter {
				first, _, err := l.Append([]Record{{Value: []byte("payload")}})
				if err != nil {
					t.Error(err)
					return
				}
				if err := l.Sync(first + 1); err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	stop := make(chan struct{})
	var readers sync.WaitGroup
	for range 4 {
		readers.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				end := l.EndOffset()
				if end == 0 {
					continue
				}
				recs, err := l.Read(end-1, 1<<20, 10)
				if err != nil {
					t.Error(err)
					return
				}
				if len(recs) == 0 || recs[0].Offset != end-1 {
					t.Errorf("read at %d returned %v", end-1, recs)
					return
				}
			}
		})
	}
	wg.Wait()
	close(stop)
	readers.Wait()

	if l.EndOffset() != writers*perWriter || l.SyncedOffset() != writers*perWriter {
		t.Fatalf("end/synced = %d/%d, want %d", l.EndOffset(), l.SyncedOffset(), writers*perWriter)
	}
	recs, err := l.Read(0, 1<<30, writers*perWriter)
	if err != nil || len(recs) != writers*perWriter {
		t.Fatalf("read back %d records, %v", len(recs), err)
	}
	for i, r := range recs {
		if r.Offset != int64(i) {
			t.Fatalf("record %d has offset %d", i, r.Offset)
		}
	}
}

func TestClosedLogRejectsOperations(t *testing.T) {
	l, _ := Open(t.TempDir(), Options{})
	appendN(t, l, 3, 3)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := l.Append([]Record{{}}); !errors.Is(err, ErrClosed) {
		t.Errorf("Append after close: %v", err)
	}
	if _, err := l.Read(0, 10, 1); !errors.Is(err, ErrClosed) {
		t.Errorf("Read after close: %v", err)
	}
	if err := l.Sync(100); !errors.Is(err, ErrClosed) {
		t.Errorf("Sync after close: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

func TestInvalidOptions(t *testing.T) {
	for _, o := range []Options{{SegmentBytes: 10}, {SegmentBytes: 2 << 30}, {IndexIntervalBytes: -1}, {SegmentMaxAge: -1}} {
		if _, err := Open(t.TempDir(), o); err == nil {
			t.Errorf("Open accepted %+v", o)
		}
	}
}
