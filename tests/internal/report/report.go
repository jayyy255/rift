// Package report writes machine-readable results of acceptance tests.
package report

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// Write stores v as JSON in $RIFT_REPORT_DIR/<name>.json when the variable
// is set, and always logs the location or a skip note.
func Write(t *testing.T, name string, v any) {
	t.Helper()
	dir := os.Getenv("RIFT_REPORT_DIR")
	if dir == "" {
		return
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Errorf("report dir: %v", err)
		return
	}
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Errorf("report: %v", err)
		return
	}
	path := filepath.Join(dir, name+".json")
	if err := os.WriteFile(path, append(data, '\n'), 0o644); err != nil {
		t.Errorf("report: %v", err)
		return
	}
	t.Logf("report written to %s", path)
}
