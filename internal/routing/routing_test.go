package routing

import (
	"fmt"
	"testing"

	"pgregory.net/rapid"
)

func TestPartitionForKeyIsStable(t *testing.T) {
	// Golden values, computed with an independent FNV-1a implementation.
	// These must never change (see package docs).
	golden := []struct {
		key  string
		n    int32
		want int32
	}{
		{"", 6, 1}, {"customer-17", 6, 2}, {"order-1", 3, 1}, {"a", 1, 0}, {"user-42", 12, 3},
	}
	for _, g := range golden {
		if got := PartitionForKey([]byte(g.key), g.n); got != g.want {
			t.Errorf("PartitionForKey(%q, %d) = %d, want %d", g.key, g.n, got, g.want)
		}
	}
}

func TestPartitionForKeyProperties(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		key := rapid.SliceOf(rapid.Byte()).Draw(t, "key")
		n := rapid.Int32Range(1, 1024).Draw(t, "n")
		p := PartitionForKey(key, n)
		if p < 0 || p >= n {
			t.Fatalf("partition %d outside [0, %d)", p, n)
		}
		if PartitionForKey(key, n) != p {
			t.Fatal("not deterministic")
		}
	})
}

func TestPartitionForKeySpreadsKeys(t *testing.T) {
	const n, keys = 8, 80000
	var counts [n]int
	for i := range keys {
		counts[PartitionForKey([]byte(fmt.Sprintf("key-%d", i)), n)]++
	}
	for p, c := range counts {
		if c < keys/n*9/10 || c > keys/n*11/10 {
			t.Errorf("partition %d got %d keys, want about %d", p, c, keys/n)
		}
	}
}
