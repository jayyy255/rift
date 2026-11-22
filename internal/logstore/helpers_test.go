package logstore

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// fakeClock is a settable clock for age-based roll and retention tests.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{t: time.UnixMilli(1_700_000_000_000)} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func mustOpen(t *testing.T, dir string, opts Options) *Log {
	t.Helper()
	l, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { l.Close() })
	return l
}

// value derives a record's payload from its offset so any record can be
// verified independently.
func value(offset int64) []byte {
	return []byte(fmt.Sprintf("value-%06d-%s", offset, bytes.Repeat([]byte{byte('a' + offset%26)}, int(offset%37))))
}

func key(offset int64) []byte {
	if offset%3 == 0 {
		return nil
	}
	return []byte(fmt.Sprintf("key-%d", offset%5))
}

// appendN appends n records in batches of batch and returns the end offset.
func appendN(t *testing.T, l *Log, n, batch int) int64 {
	t.Helper()
	for done := 0; done < n; {
		size := min(batch, n-done)
		start := l.EndOffset()
		recs := make([]Record, size)
		for i := range recs {
			off := start + int64(i)
			recs[i] = Record{Key: key(off), Value: value(off), Headers: []Header{{"n", []byte(fmt.Sprint(off))}}}
		}
		first, _, err := l.Append(recs)
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		if first != start {
			t.Fatalf("first offset = %d, want %d", first, start)
		}
		done += size
	}
	return l.EndOffset()
}

// verifyAll reads [from, to) one call at a time and checks every record.
func verifyAll(t *testing.T, l *Log, from, to int64) {
	t.Helper()
	next := from
	for next < to {
		recs, err := l.Read(next, 64<<10, 1000)
		if err != nil {
			t.Fatalf("Read(%d): %v", next, err)
		}
		if len(recs) == 0 {
			t.Fatalf("Read(%d) returned no records, end %d", next, to)
		}
		for _, r := range recs {
			checkRecord(t, r, next)
			next++
		}
	}
	if next != to {
		t.Fatalf("read up to %d, want %d", next, to)
	}
}

func checkRecord(t *testing.T, r Record, want int64) {
	t.Helper()
	if r.Offset != want {
		t.Fatalf("offset = %d, want %d", r.Offset, want)
	}
	if !bytes.Equal(r.Value, value(want)) {
		t.Fatalf("offset %d: value %q, want %q", want, r.Value, value(want))
	}
	if wk := key(want); (wk == nil) != (r.Key == nil) || !bytes.Equal(wk, r.Key) {
		t.Fatalf("offset %d: key %q, want %q", want, r.Key, wk)
	}
}

func activeSegmentPath(t *testing.T, dir string) string {
	t.Helper()
	files, err := SegmentFiles(dir)
	if err != nil || len(files) == 0 {
		t.Fatalf("SegmentFiles: %v %v", files, err)
	}
	return files[len(files)-1]
}

func copyDir(t *testing.T, src string) string {
	t.Helper()
	dst := t.TempDir()
	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		data, err := os.ReadFile(filepath.Join(src, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dst, e.Name()), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dst
}
