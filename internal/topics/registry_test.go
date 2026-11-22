package topics

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jayyy255/rift/internal/logstore"
)

func openRegistry(t *testing.T, dir string, mutate ...func(*Config)) *Registry {
	t.Helper()
	cfg := Config{DataDir: dir, RetentionCheckInterval: -1, FlushInterval: -1, Logger: slog.New(slog.DiscardHandler)}
	for _, m := range mutate {
		m(&cfg)
	}
	r, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { r.Close() })
	return r
}

func rec(v string) logstore.Record { return logstore.Record{Value: []byte(v)} }

func TestCreateListGetDelete(t *testing.T) {
	r := openRegistry(t, t.TempDir())
	if _, err := r.Create("orders", CreateOptions{Partitions: 3}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Create("audit", CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	list := r.List()
	if len(list) != 2 || list[0].Metadata().Name != "audit" || list[1].Partitions() != 3 {
		t.Fatalf("List = %v", list)
	}
	if list[0].Partitions() != 1 || list[0].Metadata().ReplicationFactor != 1 {
		t.Fatalf("defaults not applied: %+v", list[0].Metadata())
	}
	if _, err := r.Create("orders", CreateOptions{}); !errors.Is(err, ErrTopicExists) {
		t.Fatalf("duplicate create: %v", err)
	}
	if err := r.Delete("orders"); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Get("orders"); !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("Get after delete: %v", err)
	}
	if err := r.Delete("orders"); !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("second delete: %v", err)
	}
	// The name is reusable and the new topic starts empty.
	tp, err := r.Create("orders", CreateOptions{Partitions: 1})
	if err != nil {
		t.Fatal(err)
	}
	if l, _ := tp.Log(0); l.EndOffset() != 0 {
		t.Fatalf("recreated topic has end offset %d", l.EndOffset())
	}
}

func TestCreateValidation(t *testing.T) {
	r := openRegistry(t, t.TempDir())
	var inv *InvalidArgumentError
	for _, tc := range []struct {
		name string
		o    CreateOptions
	}{
		{"", CreateOptions{}},
		{".hidden", CreateOptions{}},
		{"a/b", CreateOptions{}},
		{"ok", CreateOptions{Partitions: -1}},
		{"ok", CreateOptions{Partitions: MaxPartitions + 1}},
		{"ok", CreateOptions{RetentionMs: -2}},
		{"ok", CreateOptions{ReplicationFactor: -1}},
	} {
		if _, err := r.Create(tc.name, tc.o); !errors.As(err, &inv) {
			t.Errorf("Create(%q, %+v) = %v, want InvalidArgumentError", tc.name, tc.o, err)
		}
	}
	if _, err := r.Create("ok", CreateOptions{ReplicationFactor: 3}); !errors.Is(err, ErrReplicationUnavailable) {
		t.Errorf("replication factor 3: %v", err)
	}
}

func TestTopicsAndDataSurviveRestart(t *testing.T) {
	dir := t.TempDir()
	r := openRegistry(t, dir)
	tp, _ := r.Create("events", CreateOptions{Partitions: 2, RetentionMs: 60000})
	for i := range 10 {
		if _, _, err := tp.Append(int32(i%2), []logstore.Record{rec(fmt.Sprint(i))}); err != nil {
			t.Fatal(err)
		}
	}
	r.Close()

	r = openRegistry(t, dir)
	tp, err := r.Get("events")
	if err != nil {
		t.Fatal(err)
	}
	if tp.Metadata().RetentionMs != 60000 {
		t.Fatalf("metadata lost: %+v", tp.Metadata())
	}
	if got := len(r.Recoveries()); got != 2 {
		t.Fatalf("recoveries = %d, want 2", got)
	}
	for p := range int32(2) {
		recs, err := tp.Read(p, 0, 1<<20, 100)
		if err != nil || len(recs) != 5 {
			t.Fatalf("partition %d: %d records, %v", p, len(recs), err)
		}
	}
}

func TestStartupRemovesInterruptedCreateAndDelete(t *testing.T) {
	dir := t.TempDir()
	r := openRegistry(t, dir)
	if _, err := r.Create("keep", CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	r.Close()
	topicsDir := filepath.Join(dir, "topics")
	for _, d := range []string{".creating-half-123", ".deleted-gone-456"} {
		if err := os.MkdirAll(filepath.Join(topicsDir, d, "0"), 0o755); err != nil {
			t.Fatal(err)
		}
	}

	r = openRegistry(t, dir)
	entries, _ := os.ReadDir(topicsDir)
	if len(entries) != 1 || entries[0].Name() != "keep" {
		t.Fatalf("topics dir after startup: %v", entries)
	}
	if len(r.List()) != 1 {
		t.Fatalf("topics = %v", r.List())
	}
}

func TestChoosePartition(t *testing.T) {
	r := openRegistry(t, t.TempDir())
	tp, _ := r.Create("t", CreateOptions{Partitions: 4})
	seen := map[int32]int{}
	for range 400 {
		seen[tp.ChoosePartition(nil)]++
	}
	for p := range int32(4) {
		if seen[p] != 100 {
			t.Fatalf("round-robin uneven: %v", seen)
		}
	}
	k := []byte("customer-17")
	first := tp.ChoosePartition(k)
	for range 50 {
		if tp.ChoosePartition(k) != first {
			t.Fatal("keyed routing not stable")
		}
	}
	if _, err := tp.Log(4); !errors.Is(err, ErrPartitionNotFound) {
		t.Fatalf("Log(4): %v", err)
	}
}

func TestChangedWakesOnAppendAndDelete(t *testing.T) {
	r := openRegistry(t, t.TempDir())
	tp, _ := r.Create("t", CreateOptions{})
	ch := tp.Changed()
	if _, _, err := tp.Append(0, []logstore.Record{rec("x")}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("append did not wake waiter")
	}
	ch = tp.Changed()
	if err := r.Delete("t"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("delete did not wake waiter")
	}
	if !tp.Deleted() {
		t.Fatal("Deleted() = false")
	}
	if _, _, err := tp.Append(0, []logstore.Record{rec("y")}); !errors.Is(err, ErrTopicNotFound) {
		t.Fatalf("append to deleted topic: %v", err)
	}
}

func TestRetentionUsesTopicOverrides(t *testing.T) {
	var deleted []string
	r := openRegistry(t, t.TempDir(), func(c *Config) {
		c.Log.SegmentBytes = 1024
		c.RetentionMaxBytes = 4096 // broker default
		c.OnRetention = func(topic string, _ int32, _ logstore.RetentionResult) { deleted = append(deleted, topic) }
	})
	def, _ := r.Create("default", CreateOptions{})
	unlimited, _ := r.Create("unlimited", CreateOptions{RetentionBytes: -1})
	small, _ := r.Create("small", CreateOptions{RetentionBytes: 2048})
	payload := make([]byte, 200)
	for range 100 {
		for _, tp := range []*Topic{def, unlimited, small} {
			if _, _, err := tp.Append(0, []logstore.Record{{Value: payload}}); err != nil {
				t.Fatal(err)
			}
		}
	}
	r.ApplyRetention()

	size := func(tp *Topic) int64 { l, _ := tp.Log(0); return l.Stats().SizeBytes }
	if s := size(unlimited); s < 20000 {
		t.Errorf("unlimited topic shrank to %d", s)
	}
	if s := size(def); s > 4096+1024 {
		t.Errorf("default topic is %d bytes, want about 4096", s)
	}
	if s := size(small); s > 2048+1024 {
		t.Errorf("small topic is %d bytes, want about 2048", s)
	}
	if len(deleted) != 2 {
		t.Errorf("OnRetention calls = %v", deleted)
	}
}

func TestBackgroundFlushSyncsWrittenData(t *testing.T) {
	r := openRegistry(t, t.TempDir(), func(c *Config) { c.FlushInterval = 10 * time.Millisecond })
	tp, _ := r.Create("t", CreateOptions{})
	if _, _, err := tp.Append(0, []logstore.Record{rec("a"), rec("b")}); err != nil {
		t.Fatal(err)
	}
	l, _ := tp.Log(0)
	deadline := time.Now().Add(2 * time.Second)
	for l.SyncedOffset() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("synced offset stuck at %d", l.SyncedOffset())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestClosedRegistry(t *testing.T) {
	r := openRegistry(t, t.TempDir())
	if _, err := r.Create("t", CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	r.Close()
	if _, err := r.Get("t"); !errors.Is(err, ErrClosed) {
		t.Errorf("Get: %v", err)
	}
	if _, err := r.Create("u", CreateOptions{}); !errors.Is(err, ErrClosed) {
		t.Errorf("Create: %v", err)
	}
}
