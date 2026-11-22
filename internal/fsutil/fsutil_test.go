package fsutil

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWriteFileAtomicReplacesContents(t *testing.T) {
	path := filepath.Join(t.TempDir(), "f.json")
	for _, content := range []string{"first", "second, longer contents", "3"} {
		if err := WriteFileAtomic(path, []byte(content)); err != nil {
			t.Fatal(err)
		}
		got, _ := os.ReadFile(path)
		if string(got) != content {
			t.Fatalf("got %q, want %q", got, content)
		}
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatal("temporary file left behind")
	}
}

func TestWriteFileAtomicMissingDir(t *testing.T) {
	if err := WriteFileAtomic(filepath.Join(t.TempDir(), "nope", "f"), []byte("x")); err == nil {
		t.Fatal("expected error for missing directory")
	}
}
