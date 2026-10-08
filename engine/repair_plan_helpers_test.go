package engine

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCompactManifestReadable(t *testing.T) {
	dir := t.TempDir()
	if !compactManifestReadable(dir) {
		t.Fatal("absent manifest must be readable")
	}
	p := compactManifestPath(dir)
	if err := os.WriteFile(p, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	if compactManifestReadable(dir) {
		t.Fatal("corrupt manifest must be unreadable")
	}
	// A non-ENOENT read failure (path is a directory) must not be treated as absent.
	os.Remove(p)
	if err := os.Mkdir(p, 0o755); err != nil {
		t.Fatal(err)
	}
	if compactManifestReadable(dir) {
		t.Fatal("unreadable manifest must block, not pass")
	}
}

func TestReadSidxFile(t *testing.T) {
	p := filepath.Join(t.TempDir(), "sidx_a.json")
	if _, ok := readSidxFile(p); ok {
		t.Fatal("missing file must be not ok")
	}
	os.WriteFile(p, []byte("x"), 0o644)
	if _, ok := readSidxFile(p); ok {
		t.Fatal("corrupt file must be not ok")
	}
}
