package server

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type tarEnt struct {
	name     string
	typ      byte
	body     string
	linkname string
}

func buildArchive(t *testing.T, ents []tarEnt) *bytes.Reader {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for _, e := range ents {
		typ := e.typ
		if typ == 0 {
			typ = tar.TypeReg
		}
		size := int64(len(e.body))
		if typ != tar.TypeReg {
			size = 0
		}
		h := &tar.Header{Name: e.name, Typeflag: typ, Mode: 0o644, Size: size, Linkname: e.linkname}
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if size > 0 {
			if _, err := tw.Write([]byte(e.body)); err != nil {
				t.Fatal(err)
			}
		}
	}
	_ = tw.Close()
	_ = gz.Close()
	return bytes.NewReader(buf.Bytes())
}

func dirTree(t *testing.T, root string) map[string]string {
	t.Helper()
	out := map[string]string{}
	_ = filepath.Walk(root, func(p string, fi os.FileInfo, err error) error {
		if err != nil {
			return nil //nolint:nilerr // best-effort walk
		}
		rel, _ := filepath.Rel(root, p)
		out[rel] = fi.Mode().String()
		if fi.Mode().IsRegular() {
			b, _ := os.ReadFile(p)
			out[rel] += string(b)
		}
		return nil
	})
	return out
}

// C-4: every hostile archive errors, leaves the target untouched and leaves no
// temp directory behind.
func TestExtractSnapshotRejectsHostileArchives(t *testing.T) {
	good := tarEnt{name: "c/seg_000001.ndjson", body: "{}\n"}
	cases := map[string][]tarEnt{
		"dotdot":         {good, {name: "../x", body: "x"}},
		"abs":            {good, {name: "/abs", body: "x"}},
		"nested-escape":  {good, {name: "a/../../b", body: "x"}},
		"symlink":        {good, {name: "c/seg_000002.ndjson", typ: tar.TypeSymlink, linkname: "/etc/passwd"}},
		"hardlink":       {good, {name: "c/seg_000002.ndjson", typ: tar.TypeLink, linkname: "c/seg_000001.ndjson"}},
		"chardev":        {good, {name: "c/seg_000002.ndjson", typ: tar.TypeChar}},
		"fifo":           {good, {name: "c/seg_000002.ndjson", typ: tar.TypeFifo}},
		"dir-entry":      {good, {name: "c/", typ: tar.TypeDir}},
		"depth3":         {good, {name: "c/d/seg_000001.ndjson", body: "x"}},
		"unknown-root":   {good, {name: "passwd", body: "x"}},
		"unknown-file":   {good, {name: "c/evil.sh", body: "x"}},
		"duplicate":      {good, good},
		"dot-collection": {good, {name: "./seg_000001.ndjson", body: "x"}},
		"backslash":      {good, {name: `c\seg_000002.ndjson`, body: "x"}},
		"reserved-coll":  {good, {name: "xtx.foo/seg_000001.ndjson", body: "x"}},
	}
	for name, ents := range cases {
		t.Run(name, func(t *testing.T) {
			parent := t.TempDir()
			target := filepath.Join(parent, "data")
			if err := os.Mkdir(target, 0o755); err != nil {
				t.Fatal(err)
			}
			pre := filepath.Join(target, "keep")
			if err := os.WriteFile(pre, []byte("k"), 0o644); err != nil {
				t.Fatal(err)
			}
			before := dirTree(t, parent)
			if err := extractSnapshot(buildArchive(t, ents), target); err == nil {
				t.Fatal("hostile archive accepted")
			}
			after := dirTree(t, parent)
			if len(before) != len(after) {
				t.Fatalf("tree changed: before=%v after=%v", before, after)
			}
			for k, v := range before {
				if after[k] != v {
					t.Fatalf("%q changed", k)
				}
			}
		})
	}
}

func TestExtractSnapshotLimits(t *testing.T) {
	oldE, oldS := maxSnapshotEntries, maxSnapshotFileSize
	t.Cleanup(func() { maxSnapshotEntries, maxSnapshotFileSize = oldE, oldS })

	parent := t.TempDir()
	target := filepath.Join(parent, "data")

	maxSnapshotEntries = 2
	ents := []tarEnt{{name: "c/seg_000001.ndjson", body: "a"}, {name: "c/seg_000002.ndjson", body: "b"}, {name: "c/seg_000003.ndjson", body: "c"}}
	if err := extractSnapshot(buildArchive(t, ents), target); err == nil {
		t.Fatal("entry limit not enforced")
	}
	maxSnapshotEntries = oldE
	maxSnapshotFileSize = 4
	if err := extractSnapshot(buildArchive(t, []tarEnt{{name: "c/seg_000001.ndjson", body: "toolarge"}}), target); err == nil {
		t.Fatal("size limit not enforced")
	}
	entries, _ := os.ReadDir(parent)
	if len(entries) != 0 {
		t.Fatalf("residue after rejected archives: %v", entries)
	}
}

func TestExtractSnapshotTruncatedLeavesNothing(t *testing.T) {
	parent := t.TempDir()
	target := filepath.Join(parent, "data")
	full := buildArchive(t, []tarEnt{{name: "c/seg_000001.ndjson", body: strings.Repeat("x", 4096)}})
	b := make([]byte, full.Len())
	_, _ = full.Read(b)
	if err := extractSnapshot(bytes.NewReader(b[:len(b)/2]), target); err == nil {
		t.Fatal("truncated archive accepted")
	}
	entries, _ := os.ReadDir(parent)
	if len(entries) != 0 {
		t.Fatalf("residue: %v", entries)
	}
}

func TestExtractSnapshotAcceptsValidArchive(t *testing.T) {
	parent := t.TempDir()
	target := filepath.Join(parent, "data")
	ents := []tarEnt{
		{name: "xtx.format", body: "{}"},
		{name: "users/seg_000001.ndjson", body: "{}\n"},
		{name: "users/meta.json", body: "{}"},
		{name: "users/sidx_age.json", body: "{}"},
		{name: "legacy name/seg_000001.ndjson", body: "{}\n"},
	}
	if err := extractSnapshot(buildArchive(t, ents), target); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{"xtx.format", "users/seg_000001.ndjson", "users/meta.json", "legacy name/seg_000001.ndjson"} {
		if _, err := os.Stat(filepath.Join(target, filepath.FromSlash(f))); err != nil {
			t.Errorf("%s: %v", f, err)
		}
	}
	if left, _ := filepath.Glob(filepath.Join(parent, ".data.restore-*")); len(left) != 0 {
		t.Errorf("temp dir left: %v", left)
	}
}

// C-5: a stale temp directory from a crashed extraction is removed next time.
func TestExtractSnapshotRemovesStaleTemp(t *testing.T) {
	parent := t.TempDir()
	stale := filepath.Join(parent, ".data.restore-123")
	if err := os.MkdirAll(filepath.Join(stale, "c"), 0o700); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(parent, "data")
	if err := extractSnapshot(buildArchive(t, []tarEnt{{name: "c/seg_000001.ndjson", body: "x"}}), target); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("stale temp dir still present: %v", err)
	}
}
