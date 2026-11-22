package logstore

import (
	"bytes"
	"os"
	"testing"

	"pgregory.net/rapid"
)

// TestLogMatchesModel drives the log with random appends, reads, syncs,
// restarts, and power-loss crashes, checking it against an in-memory model.
//
// Crash model: everything up to the last fsync survives; the active segment
// may lose or garble any suffix after the fsynced position.
func TestLogMatchesModel(t *testing.T) {
	root := t.TempDir()
	rapid.Check(t, func(rt *rapid.T) {
		dir, err := os.MkdirTemp(root, "prop")
		if err != nil {
			rt.Fatal(err)
		}
		opts := Options{
			SegmentBytes:       rapid.Int64Range(1024, 8192).Draw(rt, "segmentBytes"),
			IndexIntervalBytes: rapid.Int64Range(1, 512).Draw(rt, "indexInterval"),
		}
		l, err := Open(dir, opts)
		if err != nil {
			rt.Fatal(err)
		}
		defer func() { l.Close() }()
		var model [][]byte

		reopen := func() {
			var err error
			if l, err = Open(dir, opts); err != nil {
				rt.Fatalf("reopen: %v", err)
			}
		}
		check := func() {
			if got := l.EndOffset(); got != int64(len(model)) {
				rt.Fatalf("end = %d, model has %d records", got, len(model))
			}
			recs, err := l.Read(0, 1<<30, len(model)+1)
			if err != nil {
				rt.Fatalf("read all: %v", err)
			}
			if len(recs) != len(model) {
				rt.Fatalf("read %d records, model has %d", len(recs), len(model))
			}
			for i, r := range recs {
				if r.Offset != int64(i) || !bytes.Equal(r.Value, model[i]) {
					rt.Fatalf("record %d mismatch: offset %d value %q, want %q", i, r.Offset, r.Value, model[i])
				}
			}
		}

		rt.Repeat(map[string]func(*rapid.T){
			"append": func(rt *rapid.T) {
				vals := rapid.SliceOfN(rapid.SliceOfN(rapid.Byte(), 0, 400), 1, 6).Draw(rt, "values")
				recs := make([]Record, len(vals))
				for i, v := range vals {
					recs[i] = Record{Value: v}
				}
				first, _, err := l.Append(recs)
				if err != nil {
					rt.Fatal(err)
				}
				if first != int64(len(model)) {
					rt.Fatalf("first offset %d, want %d", first, len(model))
				}
				model = append(model, vals...)
			},
			"sync": func(rt *rapid.T) {
				if err := l.Sync(l.EndOffset()); err != nil {
					rt.Fatal(err)
				}
			},
			"read": func(rt *rapid.T) {
				off := rapid.Int64Range(0, int64(len(model))).Draw(rt, "offset")
				n := rapid.IntRange(1, 20).Draw(rt, "max")
				recs, err := l.Read(off, 1<<20, n)
				if err != nil {
					rt.Fatal(err)
				}
				want := min(n, len(model)-int(off))
				if len(recs) != want {
					rt.Fatalf("Read(%d, max %d) = %d records, want %d", off, n, len(recs), want)
				}
				for i, r := range recs {
					if !bytes.Equal(r.Value, model[int(off)+i]) {
						rt.Fatalf("offset %d value mismatch", off+int64(i))
					}
				}
			},
			"restart": func(rt *rapid.T) {
				if err := l.Close(); err != nil {
					rt.Fatal(err)
				}
				reopen()
				check()
			},
			"powerLoss": func(rt *rapid.T) {
				synced := l.SyncedOffset()
				l.Close() // flushes file contents; the cut below models what power loss would drop
				files, err := SegmentFiles(dir)
				if err != nil {
					rt.Fatal(err)
				}
				active := files[len(files)-1]
				in, err := InspectSegment(active)
				if err != nil {
					rt.Fatal(err)
				}
				minCut := in.FileBytes
				switch {
				case synced <= in.BaseOffset:
					minCut = 0
				default:
					for _, f := range in.Frames {
						if f.Offset == synced {
							minCut = f.Position
						}
					}
				}
				cut := rapid.Int64Range(minCut, in.FileBytes).Draw(rt, "cut")
				if err := os.Truncate(active, cut); err != nil {
					rt.Fatal(err)
				}
				if rapid.Bool().Draw(rt, "garbage") {
					junk := rapid.SliceOfN(rapid.Byte(), 1, 64).Draw(rt, "junk")
					f, _ := os.OpenFile(active, os.O_APPEND|os.O_WRONLY, 0)
					if _, err := f.Write(junk); err != nil {
						rt.Fatal(err)
					}
					f.Close()
				}
				reopen()
				end := l.EndOffset()
				if end < synced {
					rt.Fatalf("lost fsynced records: end %d < synced %d", end, synced)
				}
				if end > int64(len(model)) {
					rt.Fatalf("end %d beyond appended records %d", end, len(model))
				}
				model = model[:end]
				check()
			},
		})
		check()
	})
}
