package logstore

import (
	"fmt"
	"os"
	"path/filepath"
)

// FrameInfo locates one valid record inside a segment file.
type FrameInfo struct {
	Offset      int64
	TimestampMs int64
	Position    int64
	Size        int
}

// SegmentInspection is a read-only integrity report for one segment file.
type SegmentInspection struct {
	Path       string
	BaseOffset int64
	FileBytes  int64
	// ValidBytes is the length of the prefix made of valid, in-sequence frames.
	ValidBytes int64
	Frames     []FrameInfo
	// Problem describes why bytes after ValidBytes are invalid; empty if none.
	Problem string
}

// InspectSegment validates every frame of a segment log file without
// modifying it. It is intended for diagnostics and corruption reporting.
func InspectSegment(path string) (SegmentInspection, error) {
	base, ok := parseBase(filepath.Base(path), logSuffix)
	if !ok {
		return SegmentInspection{}, fmt.Errorf("logstore: %s is not a segment log file", path)
	}
	f, err := os.Open(path)
	if err != nil {
		return SegmentInspection{}, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return SegmentInspection{}, err
	}
	in := SegmentInspection{Path: path, BaseOffset: base, FileBytes: st.Size()}
	next := base
	end, scanErr := scanFrames(f, 0, st.Size(), func(r *Record, pos int64, n int) error {
		if r.Offset != next {
			return &frameError{reason: fmt.Sprintf("offset %d out of sequence, expected %d", r.Offset, next)}
		}
		next++
		in.Frames = append(in.Frames, FrameInfo{Offset: r.Offset, TimestampMs: r.TimestampMs, Position: pos, Size: n})
		return nil
	})
	in.ValidBytes = end
	if scanErr != nil {
		in.Problem = scanErr.Error()
	}
	return in, nil
}

// SegmentFiles returns the segment log file paths in a partition directory,
// ordered by base offset.
func SegmentFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if _, ok := parseBase(e.Name(), logSuffix); ok {
			out = append(out, filepath.Join(dir, e.Name()))
		}
	}
	return out, nil // ReadDir sorts by name; zero-padded names sort by offset
}
