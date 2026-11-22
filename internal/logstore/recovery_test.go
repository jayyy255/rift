package logstore

import (
	"errors"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Simulates a crash at every possible byte of the final writes: the active
// segment is cut at each position. Recovery must keep exactly the complete
// records before the cut, report a truncation for any partial tail, and accept
// new appends at the recovered end offset.
func TestRecoveryAfterTruncationAtEveryByte(t *testing.T) {
	src := t.TempDir()
	opts := Options{SegmentBytes: 4096, IndexIntervalBytes: 64}
	l, _ := Open(src, opts)
	appendN(t, l, 120, 4) // several sealed segments plus an active one
	l.Close()

	active := activeSegmentPath(t, src)
	in, err := InspectSegment(active)
	if err != nil || in.Problem != "" {
		t.Fatalf("inspect: %+v %v", in, err)
	}
	full, _ := os.ReadFile(active)
	base := in.BaseOffset

	for cut := 0; cut <= len(full); cut++ {
		dir := copyDir(t, src)
		if err := os.WriteFile(filepath.Join(dir, filepath.Base(active)), full[:cut], 0o644); err != nil {
			t.Fatal(err)
		}
		// Records whose frames end at or before the cut survive.
		want := base
		boundary := cut == 0
		for _, f := range in.Frames {
			end := f.Position + int64(f.Size)
			if end <= int64(cut) {
				want = f.Offset + 1
			}
			if end == int64(cut) {
				boundary = true
			}
		}

		l, err := Open(dir, opts)
		if err != nil {
			t.Fatalf("cut %d: Open: %v", cut, err)
		}
		if l.EndOffset() != want {
			t.Fatalf("cut %d: end = %d, want %d", cut, l.EndOffset(), want)
		}
		if trunc := l.Recovery().Truncation; boundary != (trunc == nil) {
			t.Fatalf("cut %d (boundary=%v): truncation report %+v", cut, boundary, trunc)
		}
		verifyAll(t, l, 0, want)
		end := appendN(t, l, 3, 3)
		verifyAll(t, l, 0, end)
		l.Close()
	}
}

func TestRecoveryRemovesGarbageTail(t *testing.T) {
	dir := t.TempDir()
	l, _ := Open(dir, Options{})
	appendN(t, l, 50, 5)
	l.Close()

	rng := rand.New(rand.NewPCG(1, 2))
	garbage := make([]byte, 777)
	for i := range garbage {
		garbage[i] = byte(rng.UintN(256))
	}
	f, _ := os.OpenFile(activeSegmentPath(t, dir), os.O_APPEND|os.O_WRONLY, 0)
	if _, err := f.Write(garbage); err != nil {
		t.Fatal(err)
	}
	f.Close()

	l = mustOpen(t, dir, Options{})
	trunc := l.Recovery().Truncation
	if trunc == nil || trunc.Bytes != int64(len(garbage)) {
		t.Fatalf("truncation = %+v, want %d bytes removed", trunc, len(garbage))
	}
	if l.EndOffset() != 50 {
		t.Fatalf("end = %d, want 50", l.EndOffset())
	}
	verifyAll(t, l, 0, 50)
}

// A corrupted byte in the active segment truncates the log at the damaged
// record and is reported; records after it are discarded, never served.
func TestRecoveryTruncatesAtCorruptRecordInActiveSegment(t *testing.T) {
	dir := t.TempDir()
	l, _ := Open(dir, Options{})
	appendN(t, l, 40, 1)
	l.Close()

	path := activeSegmentPath(t, dir)
	in, _ := InspectSegment(path)
	victim := in.Frames[25]
	data, _ := os.ReadFile(path)
	data[victim.Position+int64(victim.Size)-1] ^= 0xFF // last byte of record 25's value
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	l = mustOpen(t, dir, Options{})
	trunc := l.Recovery().Truncation
	if trunc == nil || !strings.Contains(trunc.Reason, "checksum") || trunc.Position != victim.Position {
		t.Fatalf("truncation = %+v, want checksum failure at %d", trunc, victim.Position)
	}
	if l.EndOffset() != 25 {
		t.Fatalf("end = %d, want 25", l.EndOffset())
	}
	verifyAll(t, l, 0, 25)
}

func TestRecoveryRebuildsInvalidSealedIndex(t *testing.T) {
	dir := t.TempDir()
	opts := Options{SegmentBytes: 2048, IndexIntervalBytes: 100}
	l, _ := Open(dir, opts)
	appendN(t, l, 200, 2)
	l.Close()

	files, _ := SegmentFiles(dir)
	idx := strings.TrimSuffix(files[0], logSuffix) + indexSuffix
	if err := os.WriteFile(idx, []byte("this is not an index"), 0o644); err != nil {
		t.Fatal(err)
	}

	l = mustOpen(t, dir, opts)
	if l.Recovery().RebuiltIndexes != 1 {
		t.Fatalf("rebuilt = %d, want 1", l.Recovery().RebuiltIndexes)
	}
	verifyAll(t, l, 0, 200)
}

func TestRecoveryIgnoresStaleActiveIndex(t *testing.T) {
	dir := t.TempDir()
	opts := Options{IndexIntervalBytes: 64}
	l, _ := Open(dir, opts)
	appendN(t, l, 100, 1)
	l.Close()
	idx := strings.TrimSuffix(activeSegmentPath(t, dir), logSuffix) + indexSuffix
	if err := os.WriteFile(idx, []byte{0, 0, 0, 1, 0xFF, 0xFF, 0xFF, 0xFF}, 0o644); err != nil { // points past EOF
		t.Fatal(err)
	}

	l = mustOpen(t, dir, opts)
	for off := range int64(100) {
		recs, err := l.Read(off, 1, 1)
		if err != nil || len(recs) != 1 {
			t.Fatalf("Read(%d): %v", off, err)
		}
		checkRecord(t, recs[0], off)
	}
}

// Sealed segments were fsynced when rolled, so damage there is not a torn
// write. Open must refuse to start rather than silently drop records.
func TestSealedSegmentTruncationFailsOpen(t *testing.T) {
	dir := t.TempDir()
	opts := Options{SegmentBytes: 2048}
	l, _ := Open(dir, opts)
	appendN(t, l, 200, 2)
	l.Close()

	files, _ := SegmentFiles(dir)
	fi, _ := os.Stat(files[1])
	if err := os.Truncate(files[1], fi.Size()-10); err != nil {
		t.Fatal(err)
	}

	_, err := Open(dir, opts)
	var ce *CorruptionError
	if !errors.Is(err, ErrCorrupt) || !errors.As(err, &ce) || ce.Segment != files[1] {
		t.Fatalf("Open err = %v, want CorruptionError for %s", err, files[1])
	}
}

func TestSealedSegmentGapFailsOpen(t *testing.T) {
	dir := t.TempDir()
	opts := Options{SegmentBytes: 2048}
	l, _ := Open(dir, opts)
	appendN(t, l, 200, 2)
	l.Close()
	files, _ := SegmentFiles(dir)
	os.Remove(files[1]) // the log now jumps from segment 0 to segment 2

	if _, err := Open(dir, opts); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open err = %v, want ErrCorrupt for missing segment", err)
	}
}

// Damage inside a sealed segment that Open does not scan is caught by the
// checksum when read, and surfaced as corruption instead of bad data.
func TestCorruptionDetectedOnRead(t *testing.T) {
	dir := t.TempDir()
	opts := Options{SegmentBytes: 8192, IndexIntervalBytes: 256}
	l, _ := Open(dir, opts)
	appendN(t, l, 300, 3)
	l.Close()

	files, _ := SegmentFiles(dir)
	in, _ := InspectSegment(files[0])
	victim := in.Frames[2] // before the first index entry: not scanned at open
	data, _ := os.ReadFile(files[0])
	data[victim.Position+12] ^= 0x01
	if err := os.WriteFile(files[0], data, 0o644); err != nil {
		t.Fatal(err)
	}

	l = mustOpen(t, dir, opts)
	_, err := l.Read(victim.Offset, 1<<20, 10)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Read err = %v, want ErrCorrupt", err)
	}
	// Records in other segments remain readable.
	verifyAll(t, l, in.Frames[len(in.Frames)-1].Offset+1, l.EndOffset())
}

func TestRecoveryCleansCrashLeftovers(t *testing.T) {
	dir := t.TempDir()
	l, _ := Open(dir, Options{SegmentBytes: 2048})
	appendN(t, l, 100, 2)
	l.Close()

	orphan := segmentPath(dir, 99999, indexSuffix)
	tmp := filepath.Join(dir, "something.tmp")
	if err := os.WriteFile(orphan, []byte{1, 2, 3}, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(tmp, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	l = mustOpen(t, dir, Options{SegmentBytes: 2048})
	for _, p := range []string{orphan, tmp} {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("%s was not removed", p)
		}
	}
	verifyAll(t, l, 0, 100)
}

// A crash just after a roll leaves an empty newest segment; it must become
// the active segment with no data loss.
func TestRecoveryWithEmptyActiveSegmentAfterRoll(t *testing.T) {
	dir := t.TempDir()
	l, _ := Open(dir, Options{SegmentBytes: 2048})
	end := appendN(t, l, 100, 2)
	l.Close()
	files, _ := SegmentFiles(dir)
	if err := os.Truncate(files[len(files)-1], 0); err != nil { // power loss wiped the unsynced active data
		t.Fatal(err)
	}
	in, _ := InspectSegment(files[len(files)-1])

	l = mustOpen(t, dir, Options{SegmentBytes: 2048})
	if l.EndOffset() != in.BaseOffset || l.EndOffset() > end {
		t.Fatalf("end = %d, want segment base %d", l.EndOffset(), in.BaseOffset)
	}
	verifyAll(t, l, 0, l.EndOffset())
	appendN(t, l, 5, 5)
	verifyAll(t, l, 0, l.EndOffset())
}

func TestInspectSegmentReportsProblem(t *testing.T) {
	dir := t.TempDir()
	l, _ := Open(dir, Options{})
	appendN(t, l, 10, 10)
	l.Close()
	path := activeSegmentPath(t, dir)
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if _, err := f.Write([]byte{0, 0, 0, 200, 1}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	in, err := InspectSegment(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(in.Frames) != 10 || in.Problem == "" || in.FileBytes-in.ValidBytes != 5 {
		t.Fatalf("inspection = frames %d, problem %q, invalid bytes %d", len(in.Frames), in.Problem, in.FileBytes-in.ValidBytes)
	}
	if _, err := InspectSegment(filepath.Join(dir, "nope.txt")); err == nil {
		t.Fatal("non-segment path accepted")
	}
}
