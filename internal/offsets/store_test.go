package offsets

import (
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
)

func TestCommitGetAndReload(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Commit("analytics", "orders", map[int32]int64{0: 10, 1: 4}); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit("analytics", "orders", map[int32]int64{1: 7}); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit("billing", "orders", map[int32]int64{0: 1}); err != nil {
		t.Fatal(err)
	}

	want := map[int32]int64{0: 10, 1: 7}
	if got := s.Get("analytics", "orders"); !reflect.DeepEqual(got, want) {
		t.Fatalf("Get = %v, want %v", got, want)
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := s2.Get("analytics", "orders"); !reflect.DeepEqual(got, want) {
		t.Fatalf("after reload Get = %v, want %v", got, want)
	}
	if len(s2.Snapshot()) != 3 {
		t.Fatalf("snapshot = %v", s2.Snapshot())
	}
	if got := s2.Get("nobody", "orders"); len(got) != 0 {
		t.Fatalf("unknown group = %v", got)
	}
}

func TestCommitValidation(t *testing.T) {
	s, _ := Open(t.TempDir())
	if err := s.Commit("../evil", "t", map[int32]int64{0: 1}); err == nil {
		t.Error("path-like group accepted")
	}
	if err := s.Commit("g", "t", map[int32]int64{0: -1}); err == nil {
		t.Error("negative offset accepted")
	}
	if err := s.Commit("g", "t", map[int32]int64{-1: 1}); err == nil {
		t.Error("negative partition accepted")
	}
}

func TestDeleteTopicAndClamp(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	if err := s.Commit("a", "orders", map[int32]int64{0: 50, 1: 5}); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit("b", "orders", map[int32]int64{0: 20}); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit("a", "other", map[int32]int64{0: 3}); err != nil {
		t.Fatal(err)
	}

	changed, err := s.Clamp("orders", 0, 30)
	if err != nil || !reflect.DeepEqual(changed, []string{"a"}) {
		t.Fatalf("Clamp = %v, %v", changed, err)
	}
	if got := s.Get("a", "orders")[0]; got != 30 {
		t.Fatalf("clamped offset = %d", got)
	}
	if err := s.DeleteTopic("orders"); err != nil {
		t.Fatal(err)
	}
	s2, _ := Open(dir)
	if len(s2.Get("a", "orders")) != 0 || len(s2.Get("b", "orders")) != 0 {
		t.Fatal("deleted topic offsets survived reload")
	}
	if s2.Get("a", "other")[0] != 3 {
		t.Fatal("unrelated topic offsets lost")
	}
}

func TestOpenRemovesInterruptedCommitAndRejectsCorruptFile(t *testing.T) {
	dir := t.TempDir()
	s, _ := Open(dir)
	if err := s.Commit("g", "t", map[int32]int64{0: 9}); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "g.json.tmp"), []byte("{partial"), 0o644); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(dir)
	if err != nil || s2.Get("g", "t")[0] != 9 {
		t.Fatalf("reload after interrupted commit: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "g.json.tmp")); !os.IsNotExist(err) {
		t.Fatal("tmp file not removed")
	}
	if err := os.WriteFile(filepath.Join(dir, "g.json"), []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); err == nil {
		t.Fatal("corrupt group file accepted")
	}
}

func TestConcurrentCommits(t *testing.T) {
	s, _ := Open(t.TempDir())
	var wg sync.WaitGroup
	for p := range int32(8) {
		wg.Go(func() {
			for off := range int64(20) {
				if err := s.Commit("g", "t", map[int32]int64{p: off + 1}); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()
	got := s.Get("g", "t")
	for p := range int32(8) {
		if got[p] != 20 {
			t.Fatalf("partition %d = %d, want 20", p, got[p])
		}
	}
}
