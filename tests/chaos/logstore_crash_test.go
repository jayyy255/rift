// Package chaos holds crash and fault-injection acceptance tests.
package chaos

import (
	"bufio"
	"bytes"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/jayyy255/rift/internal/logstore"
	"github.com/jayyy255/rift/tests/internal/report"
)

const childEnv = "RIFT_CRASH_CHILD_DIR"

// Small segments make the writer roll often, so crashes regularly land at
// and around segment boundaries.
var crashOpts = logstore.Options{SegmentBytes: 64 << 10, IndexIntervalBytes: 512}

func value(offset int64) []byte {
	return []byte(fmt.Sprintf("record-%d-%s", offset, strings.Repeat(string(rune('a'+offset%26)), int(offset%97))))
}

// TestMain turns the test binary into the crash victim when re-executed with
// childEnv set: it appends and fsyncs forever, printing each acknowledged
// end offset, until the parent kills it.
func TestMain(m *testing.M) {
	if dir := os.Getenv(childEnv); dir != "" {
		os.Exit(runWriter(dir))
	}
	os.Exit(m.Run())
}

func runWriter(dir string) int {
	l, err := logstore.Open(dir, crashOpts)
	if err != nil {
		fmt.Fprintln(os.Stderr, "child open:", err)
		return 3
	}
	out := bufio.NewWriter(os.Stdout)
	rng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 7))
	for {
		// Appending for a random 5-25ms between fsyncs leaves an unsynced
		// tail that a power loss may destroy, so cuts land at arbitrary
		// write points, including across segment rolls.
		window := time.Now().Add(time.Duration(5+rng.IntN(20)) * time.Millisecond)
		for time.Now().Before(window) {
			end := l.EndOffset()
			recs := make([]logstore.Record, 1+rng.IntN(8))
			for i := range recs {
				recs[i] = logstore.Record{Key: []byte(fmt.Sprint(end + int64(i))), Value: value(end + int64(i))}
			}
			first, _, err := l.Append(recs)
			if err != nil || first != end {
				fmt.Fprintln(os.Stderr, "child append:", first, end, err)
				return 4
			}
		}
		newEnd := l.EndOffset()
		if err := l.Sync(newEnd); err != nil {
			fmt.Fprintln(os.Stderr, "child sync:", err)
			return 5
		}
		// Acknowledge only after fsync, exactly like a FSYNCED publish.
		fmt.Fprintf(out, "ACK %d\n", newEnd)
		out.Flush()
	}
}

type iteration struct {
	Iteration     int   `json:"iteration"`
	KilledAfterMs int64 `json:"killed_after_ms"`
	// AckedEnd is the highest end offset acknowledged by this iteration's
	// writer; DurableEnd also counts records fsynced by earlier iterations.
	AckedEnd       int64  `json:"acked_end_offset"`
	DurableEnd     int64  `json:"durable_end_offset"`
	PowerLoss      string `json:"power_loss"`
	CutBytes       int64  `json:"cut_bytes"`
	RecoveredEnd   int64  `json:"recovered_end_offset"`
	TruncatedBytes int64  `json:"recovery_truncated_bytes"`
	Segments       int    `json:"segments"`
	Verified       int64  `json:"records_verified"`
}

// TestR1_CrashAtArbitraryWritePoint is the R1 exit test: "crash at an
// arbitrary write point; restart without invalid committed records".
//
// Each iteration SIGKILLs a writer at a random moment, then models power
// loss by cutting the active segment anywhere after the last acknowledged
// (fsynced) record and sometimes appending garbage, as a torn write would.
// Recovery must then (1) open without error, (2) retain every acknowledged
// record, (3) serve only checksum-valid, contiguous records with the exact
// bytes written, and (4) accept new appends at the recovered end. Iterations
// chain: each writer continues on the previous recovered log.
func TestR1_CrashAtArbitraryWritePoint(t *testing.T) {
	iterations := 25
	if testing.Short() {
		iterations = 6
	}
	if v, err := strconv.Atoi(os.Getenv("RIFT_CRASH_ITERATIONS")); err == nil && v > 0 {
		iterations = v
	}
	dir := t.TempDir()
	seed := uint64(time.Now().UnixNano())
	t.Logf("seed %d", seed)
	rng := rand.New(rand.NewPCG(seed, 1))

	var (
		results    []iteration
		durable    int64 // every record below this offset is known to be fsynced
		damaged    int
		verifiedTo int64
	)
	for it := 1; it <= iterations; it++ {
		res := iteration{Iteration: it}
		res.AckedEnd, res.KilledAfterMs = runAndKill(t, dir, time.Duration(80+rng.IntN(200))*time.Millisecond)
		durable = max(durable, res.AckedEnd)
		res.DurableEnd = durable

		res.PowerLoss, res.CutBytes = simulatePowerLoss(t, dir, durable, rng)
		if res.PowerLoss != "process-crash" {
			damaged++
		}

		l, err := logstore.Open(dir, crashOpts)
		if err != nil {
			t.Fatalf("iteration %d: recovery failed: %v", it, err)
		}
		rep := l.Recovery()
		res.RecoveredEnd, res.Segments = l.EndOffset(), rep.Segments
		if rep.Truncation != nil {
			res.TruncatedBytes = rep.Truncation.Bytes
		}
		if res.RecoveredEnd < durable {
			t.Fatalf("iteration %d: lost acknowledged records: recovered end %d < durable end %d", it, res.RecoveredEnd, durable)
		}
		// Damage is confined to the active segment, so each iteration checks
		// the recent region; the whole log is verified once at the end.
		res.Verified = verifyLog(t, l, it, max(l.StartOffset(), verifiedTo-20000))
		verifiedTo = res.RecoveredEnd
		if err := l.Close(); err != nil { // Close fsyncs the probe record verifyLog appended
			t.Fatal(err)
		}
		durable = res.RecoveredEnd + 1
		results = append(results, res)
		t.Logf("iteration %2d: killed after %3dms, durable %6d, %-13s cut %6dB, recovered %6d, truncated %5dB, %3d segments",
			it, res.KilledAfterMs, res.DurableEnd, res.PowerLoss, res.CutBytes, res.RecoveredEnd, res.TruncatedBytes, res.Segments)
	}
	final, err := logstore.Open(dir, crashOpts)
	if err != nil {
		t.Fatalf("final open: %v", err)
	}
	total := verifyLog(t, final, iterations+1, final.StartOffset())
	final.Close()
	t.Logf("final check: all %d records in %d segments are contiguous and intact", total, final.Recovery().Segments)
	if durable <= int64(iterations) {
		t.Fatal("writers acknowledged almost nothing; the test exercised nothing")
	}
	if damaged < iterations/4 {
		t.Errorf("only %d of %d iterations simulated power loss damage", damaged, iterations)
	}
	report.Write(t, "r1-logstore-crash", map[string]any{
		"test":                   "TestR1_CrashAtArbitraryWritePoint",
		"passed":                 !t.Failed(),
		"seed":                   seed,
		"iterations":             results,
		"damaged":                damaged,
		"final_verified_records": total,
		"invariants": []string{
			"log opens after every crash",
			"every acknowledged (fsynced) record survives",
			"recovered records are contiguous, checksum-valid, and byte-identical",
			"appends resume at the recovered end offset",
		},
	})
}

// runAndKill starts a writer child, kills it with SIGKILL after d, and
// returns the highest acknowledged end offset it printed.
func runAndKill(t *testing.T, dir string, d time.Duration) (ackedEnd, ranMs int64) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(), childEnv+"="+dir)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	acks := make(chan int64, 1)
	go func() {
		var maxAck int64
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			if v, ok := strings.CutPrefix(sc.Text(), "ACK "); ok {
				n, _ := strconv.ParseInt(v, 10, 64)
				maxAck = max(maxAck, n)
			}
		}
		acks <- maxAck
	}()
	start := time.Now()
	time.Sleep(d)
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	ackedEnd = <-acks
	err = cmd.Wait()
	if ws, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); !ok || !ws.Signaled() {
		t.Fatalf("writer exited on its own (%v): %s", err, stderr.String())
	}
	return ackedEnd, time.Since(start).Milliseconds()
}

// simulatePowerLoss damages only bytes that were never acknowledged: data
// after the last fsynced record may be missing, garbled, or zero-filled after
// power loss.
func simulatePowerLoss(t *testing.T, dir string, durableEnd int64, rng *rand.Rand) (string, int64) {
	t.Helper()
	files, err := logstore.SegmentFiles(dir)
	if err != nil || len(files) == 0 {
		t.Fatalf("segments: %v %v", files, err)
	}
	active := files[len(files)-1]
	in, err := logstore.InspectSegment(active)
	if err != nil {
		t.Fatal(err)
	}
	// Smallest position the cut may reach: the end of the last durable record.
	minCut := int64(0)
	if durableEnd > in.BaseOffset {
		minCut = in.FileBytes
		for _, f := range in.Frames {
			if f.Offset == durableEnd-1 {
				minCut = f.Position + int64(f.Size)
			}
		}
	}
	switch mode := rng.IntN(5); {
	case mode == 0 || minCut >= in.FileBytes:
		return "process-crash", 0
	case mode == 4:
		// Some filesystems persist later blocks but not earlier ones,
		// leaving zero-filled holes inside the unsynced region.
		start := minCut + rng.Int64N(in.FileBytes-minCut)
		n := min(1+rng.Int64N(4096), in.FileBytes-start)
		f, err := os.OpenFile(active, os.O_WRONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.WriteAt(make([]byte, n), start); err != nil {
			t.Fatal(err)
		}
		f.Close()
		return "zeroed-hole", n
	case mode == 1:
		cut := minCut + rng.Int64N(in.FileBytes-minCut+1)
		if err := os.Truncate(active, cut); err != nil {
			t.Fatal(err)
		}
		return "truncate", in.FileBytes - cut
	default:
		cut := minCut + rng.Int64N(in.FileBytes-minCut+1)
		if err := os.Truncate(active, cut); err != nil {
			t.Fatal(err)
		}
		junk := make([]byte, 1+rng.IntN(300))
		for i := range junk {
			junk[i] = byte(rng.UintN(256))
		}
		f, err := os.OpenFile(active, os.O_APPEND|os.O_WRONLY, 0)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write(junk); err != nil {
			t.Fatal(err)
		}
		f.Close()
		return "torn-garbage", in.FileBytes - cut
	}
}

// verifyLog reads the entire log and checks every record's offset, key, and
// value. It returns the number of records verified.
func verifyLog(t *testing.T, l *logstore.Log, it int, from int64) int64 {
	t.Helper()
	next, end := from, l.EndOffset()
	for next < end {
		recs, err := l.Read(next, 1<<20, 5000)
		if err != nil {
			t.Fatalf("iteration %d: read at %d: %v", it, next, err)
		}
		for _, r := range recs {
			if r.Offset != next {
				t.Fatalf("iteration %d: offset gap: got %d, want %d", it, r.Offset, next)
			}
			if !bytes.Equal(r.Value, value(next)) || string(r.Key) != fmt.Sprint(next) {
				t.Fatalf("iteration %d: offset %d holds wrong data", it, next)
			}
			next++
		}
	}
	if _, _, err := l.Append([]logstore.Record{{Key: []byte(fmt.Sprint(end)), Value: value(end)}}); err != nil {
		t.Fatalf("iteration %d: append after recovery: %v", it, err)
	}
	if got, _ := l.Read(end, 1<<20, 1); len(got) != 1 || got[0].Offset != end {
		t.Fatalf("iteration %d: new append not readable at %d", it, end)
	}
	return next - from
}
