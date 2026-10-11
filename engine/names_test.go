package engine

import (
	"compress/gzip"
	"errors"
	"github.com/srjn45/scriva/store"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// hostileNames is the D1 N-1 corpus: every entry must be refused by every
// engine entry point that takes a collection name.
var hostileNames = []string{
	"", ".", "..", "a/b", `a\b`, "../x", "/abs", `C:\x`, `\\h\s`, "a\x00b",
	"a b", "é", "e\u0301", "\u00e9", strings.Repeat("a", 65),
	"xtx", "xtx.foo", "XTX.foo", "CON", "nul", "Collections", "replication", "LOCK",
	"a.b", ".hidden", "-lead", "_lead", "trail.", "com1",
}

// snapshotTree returns relative path -> mode/size/target for everything under
// root, without following symlinks.
func snapshotTree(t *testing.T, root string) map[string]string {
	t.Helper()
	out := map[string]string{}
	_ = filepath.Walk(root, func(p string, fi os.FileInfo, err error) error {
		if err != nil {
			return nil //nolint:nilerr // best-effort walk
		}
		rel, _ := filepath.Rel(root, p)
		desc := fi.Mode().String()
		if fi.Mode().IsRegular() {
			b, _ := os.ReadFile(p)
			desc += ":" + string(b)
		}
		if fi.Mode()&os.ModeSymlink != 0 {
			tgt, _ := os.Readlink(p)
			desc += "->" + tgt
		}
		out[rel] = desc
		return nil
	})
	return out
}

func sameTree(t *testing.T, what string, before, after map[string]string) {
	t.Helper()
	for k, v := range before {
		if after[k] != v {
			t.Errorf("%s: %q changed: %q -> %q", what, k, v, after[k])
		}
	}
	for k := range after {
		if _, ok := before[k]; !ok {
			t.Errorf("%s: new entry %q appeared", what, k)
		}
	}
}

func TestHostileNamesRefusedEverywhere(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "root")
	// A decoy tree beside the root that a traversal would hit.
	for _, d := range []string{"x", "abs", "b"} {
		if err := os.MkdirAll(filepath.Join(parent, d), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(parent, d, "keep"), []byte("keep"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	db, err := Open(root, CollectionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	before := snapshotTree(t, parent)

	for _, name := range hostileNames {
		check := func(op string, err error) {
			t.Helper()
			if !errors.Is(err, ErrInvalidName) {
				t.Errorf("%s(%q): got %v, want ErrInvalidName", op, name, err)
			}
		}
		_, err := db.CreateCollection(name)
		check("CreateCollection", err)
		_, err = db.CreateCollectionWithDefaultTTL(name, 1)
		check("CreateCollectionWithDefaultTTL", err)
		_, err = db.CollectionWithConfig(name, CollectionConfig{})
		check("CollectionWithConfig", err)
		_, err = db.Collection(name)
		check("Collection", err)
		check("DropCollection", db.DropCollection(name))
		check("Compact", db.Compact(name))
		_, err = OpenCollection(name, root, CollectionConfig{})
		check("OpenCollection", err)
		_, err = db.BeginXTx(t.Context(), []string{name}, XTxOptions{})
		if !errors.Is(err, ErrInvalidName) && (name != "" || !errors.Is(err, ErrXTxInvalid)) {
			t.Errorf("BeginXTx(%q): got %v", name, err)
		}
	}
	sameTree(t, "hostile names", before, snapshotTree(t, parent))
}

func TestHostileNamesInCommitXTx(t *testing.T) {
	parent := t.TempDir()
	db, err := Open(filepath.Join(parent, "root"), CollectionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	before := snapshotTree(t, parent)
	for _, name := range []string{"../x", "a/b", "/abs", "xtx"} {
		_, err := db.CommitXTx("", []XTxOp{{Collection: name, Op: store.OpInsert, Data: map[string]any{"a": 1}}})
		if !errors.Is(err, ErrInvalidName) {
			t.Errorf("CommitXTx(%q): %v", name, err)
		}
	}
	sameTree(t, "commit xtx", before, snapshotTree(t, parent))
}

func TestValidNamesAccepted(t *testing.T) {
	db, err := Open(t.TempDir(), CollectionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	for _, n := range []string{"users", "A", "a-b_c", "0", strings.Repeat("a", 64)} {
		if _, err := db.CreateCollection(n); err != nil {
			t.Errorf("CreateCollection(%q): %v", n, err)
		}
	}
}

// N-2: uniqueness is case-insensitive, lookup case-exact.
func TestCaseFoldUniqueness(t *testing.T) {
	root := t.TempDir()
	db, err := Open(root, CollectionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.CreateCollection("users"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateCollection("Users"); !errors.Is(err, ErrCollectionExists) {
		t.Fatalf("Users: %v", err)
	}
	if _, err := db.CollectionWithConfig("USERS", CollectionConfig{}); !errors.Is(err, ErrCollectionExists) {
		t.Fatalf("CollectionWithConfig USERS: %v", err)
	}
	if _, err := db.Collection("Users"); err == nil {
		t.Fatal("lookup must be case-exact")
	}
	if _, err := db.CreateCollection("users"); !errors.Is(err, ErrCollectionExists) {
		t.Fatalf("exact duplicate: %v", err)
	}
	ents, _ := os.ReadDir(root)
	n := 0
	for _, e := range ents {
		if e.IsDir() {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("want 1 collection dir, got %d", n)
	}
}

// N-3: legacy-named directories load, work, cannot be created, can be dropped.
func TestLegacyNamesOperable(t *testing.T) {
	root := t.TempDir()
	legacy := []string{"legacy name", "Ünï", "a.b"}
	{
		db, err := Open(root, CollectionConfig{})
		if err != nil {
			t.Fatal(err)
		}
		_ = db.Close()
	}
	for _, n := range legacy {
		if err := os.Mkdir(filepath.Join(root, n), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	var gotLegacy, gotSkipped int
	db, err := Open(root, CollectionConfig{OnRootNames: func(l, s int) { gotLegacy, gotSkipped = l, s }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if gotLegacy != 3 || gotSkipped != 0 {
		t.Errorf("OnRootNames = %d,%d", gotLegacy, gotSkipped)
	}
	for _, n := range legacy {
		c, err := db.Collection(n)
		if err != nil {
			t.Fatalf("Collection(%q): %v", n, err)
		}
		if _, _, err := c.Insert(map[string]any{"k": "v"}); err != nil {
			t.Fatalf("insert into %q: %v", n, err)
		}
		if _, err := db.CreateCollection(n); !errors.Is(err, ErrInvalidName) {
			t.Errorf("CreateCollection(%q): %v", n, err)
		}
	}
	if err := db.DropCollection("a.b"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "a.b")); !os.IsNotExist(err) {
		t.Errorf("a.b still on disk: %v", err)
	}
	for _, n := range []string{"legacy name", "Ünï"} {
		if _, err := os.Stat(filepath.Join(root, n)); err != nil {
			t.Errorf("%q removed: %v", n, err)
		}
	}
}

// N-4: a symlink root entry is skipped and can never be created or dropped.
func TestSymlinkRootEntrySkipped(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation needs privileges on windows")
	}
	parent := t.TempDir()
	outside := filepath.Join(parent, "elsewhere")
	if err := os.MkdirAll(outside, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outside, "seg_000001.ndjson"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(parent, "root")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "evil")); err != nil {
		t.Fatal(err)
	}
	before := snapshotTree(t, outside)
	var skipped int
	db, err := Open(root, CollectionConfig{OnRootNames: func(_, s int) { skipped = s }})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if skipped != 1 {
		t.Errorf("skipped = %d", skipped)
	}
	if len(db.ListCollections()) != 0 {
		t.Errorf("symlink was loaded: %v", db.ListCollections())
	}
	if _, err := db.CreateCollection("evil"); !errors.Is(err, ErrUnsafePath) {
		t.Errorf("create evil: %v", err)
	}
	if _, err := db.CollectionWithConfig("evil", CollectionConfig{}); !errors.Is(err, ErrUnsafePath) {
		t.Errorf("CollectionWithConfig evil: %v", err)
	}
	if err := db.DropCollection("evil"); err == nil {
		t.Error("drop evil succeeded")
	}
	sameTree(t, "symlink target", before, snapshotTree(t, outside))
}

// N-5: a reserved root directory still refuses open.
func TestReservedRootDirRefusesOpen(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "xtx.foo"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(root, CollectionConfig{}); !errors.Is(err, ErrReservedName) {
		t.Fatalf("Open: %v", err)
	}
}

// C-1: a collection directory swapped for a symlink after Open.
func TestDropRefusesSwappedSymlink(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation needs privileges on windows")
	}
	parent := t.TempDir()
	outside := filepath.Join(parent, "outside")
	if err := os.MkdirAll(outside, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outside, "precious"), []byte("p"), 0o644); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(parent, "root")
	db, err := Open(root, CollectionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := db.CreateCollection("c"); err != nil {
		t.Fatal(err)
	}
	before := snapshotTree(t, outside)
	// Swap the real directory for a symlink, as a local attacker could.
	real := filepath.Join(db.dataDir, "c")
	if err := os.Rename(real, real+".moved"); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, real); err != nil {
		t.Fatal(err)
	}
	if err := db.DropCollection("c"); !errors.Is(err, ErrUnsafePath) {
		t.Fatalf("drop: %v", err)
	}
	sameTree(t, "outside tree", before, snapshotTree(t, outside))
	if err := os.Remove(real); err != nil { // restore for clean Close
		t.Fatal(err)
	}
	_ = os.Rename(real+".moved", real)
}

// C-1b: once a collection has been opened, later I/O remains anchored to the
// opened directory rather than its mutable pathname. This is deliberately a
// post-open swap: checking Lstat again before each operation would still leave
// a check/use race here.
func TestOpenCollectionIOIgnoresPostOpenSymlinkSwap(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("descriptor-rooted collection paths use /proc on Linux")
	}
	parent := t.TempDir()
	outside := filepath.Join(parent, "outside")
	if err := os.Mkdir(outside, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outside, "precious"), []byte("p"), 0o644); err != nil {
		t.Fatal(err)
	}
	db, err := Open(filepath.Join(parent, "root"), CollectionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	c, err := db.CreateCollection("c")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.Insert(map[string]any{"before": true}); err != nil {
		t.Fatal(err)
	}
	before := snapshotTree(t, outside)
	real := filepath.Join(db.dataDir, "c")
	if err := os.Rename(real, real+".moved"); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, real); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.Insert(map[string]any{"after": true}); err != nil {
		t.Fatalf("write after swap: %v", err)
	}
	var archive strings.Builder
	if err := db.SnapshotTo(&archive); err != nil {
		t.Fatalf("snapshot after swap: %v", err)
	}
	sameTree(t, "outside tree", before, snapshotTree(t, outside))
	if err := os.Remove(real); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(real+".moved", real); err != nil {
		t.Fatal(err)
	}
}

// C-2: a symlinked root works and files appear only under its target.
func TestSymlinkedRootAllowed(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation needs privileges on windows")
	}
	parent := t.TempDir()
	realRoot := filepath.Join(parent, "real")
	if err := os.Mkdir(realRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(parent, "link")
	if err := os.Symlink(realRoot, link); err != nil {
		t.Fatal(err)
	}
	db, err := Open(link, CollectionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	c, err := db.CreateCollection("c")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.Insert(map[string]any{"a": 1}); err != nil {
		t.Fatal(err)
	}
	if err := db.DropCollection("c"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.CreateCollection("d"); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	if _, err := os.Stat(filepath.Join(realRoot, "d")); err != nil {
		t.Fatalf("collection not under real root: %v", err)
	}
}

// C-3: export never follows a symlink file.
func TestSnapshotSkipsSymlinkFiles(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink creation needs privileges on windows")
	}
	parent := t.TempDir()
	secret := filepath.Join(parent, "secret.txt")
	if err := os.WriteFile(secret, []byte("TOP-SECRET-BYTES"), 0o644); err != nil {
		t.Fatal(err)
	}
	db, err := Open(filepath.Join(parent, "root"), CollectionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	c, err := db.CreateCollection("c")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.Insert(map[string]any{"a": 1}); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(secret, filepath.Join(db.dataDir, "c", "seg_000099.ndjson")); err != nil {
		t.Fatal(err)
	}
	var buf strings.Builder
	if err := db.SnapshotTo(&buf); err != nil {
		t.Fatal(err)
	}
	// The archive is gzip; the secret would only be visible after inflating,
	// so inflate and search.
	if got := inflateString(t, buf.String()); strings.Contains(got, "TOP-SECRET-BYTES") || strings.Contains(got, "seg_000099") {
		t.Fatal("snapshot followed a symlink file")
	}
}

func inflateString(t *testing.T, gzipped string) string {
	t.Helper()
	zr, err := gzip.NewReader(strings.NewReader(gzipped))
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(zr)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
