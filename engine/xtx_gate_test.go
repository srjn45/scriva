package engine

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func writeFormat(t *testing.T, dir string, f xtxFormat) {
	t.Helper()
	b, err := encodeXTxFormat(f)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, xtxFormatFile), b, 0o644); err != nil {
		t.Fatal(err)
	}
}

func okFormat() xtxFormat {
	return xtxFormat{Format: xtxFormatName, MinReader: 1, Features: []string{"journal", "stamped-v2"}, CreatedBy: "test"}
}

func rootFiles(t *testing.T, dir string) []string {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range ents {
		out = append(out, e.Name())
	}
	sort.Strings(out)
	return out
}

func TestXTxFormatGate(t *testing.T) {
	cases := map[string]struct {
		mut  func(*xtxFormat)
		want error
	}{
		"ok":          {func(*xtxFormat) {}, nil},
		"too-new":     {func(f *xtxFormat) { f.MinReader = 99 }, ErrFormatTooNew},
		"feature":     {func(f *xtxFormat) { f.Features = append(f.Features, "quantum") }, ErrXTxUnsupported},
		"wrong-name":  {func(f *xtxFormat) { f.Format = "other" }, ErrXTxUnsupported},
		"zero-reader": {func(f *xtxFormat) { f.MinReader = 0 }, ErrXTxUnsupported},
	}
	for name, c := range cases {
		dir := t.TempDir()
		f := okFormat()
		c.mut(&f)
		writeFormat(t, dir, f)
		_, err := readXTxFormat(dir)
		if c.want == nil && err != nil || c.want != nil && !errors.Is(err, c.want) {
			t.Errorf("%s: got %v, want %v", name, err, c.want)
		}
	}
	// A too-new format is reported precisely even if its checksum is not ours.
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, xtxFormatFile),
		[]byte(`{"format":"scriva-xtx","min_reader":99,"future":"x","crc":1}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := readXTxFormat(dir); !errors.Is(err, ErrFormatTooNew) {
		t.Errorf("future format: %v", err)
	}
	// Corruption: flip every bit of a valid file; none may pass as valid.
	dir = t.TempDir()
	writeFormat(t, dir, okFormat())
	good, _ := os.ReadFile(filepath.Join(dir, xtxFormatFile))
	for i := range good {
		for bit := 0; bit < 8; bit++ {
			b := append([]byte(nil), good...)
			b[i] ^= 1 << bit
			f, err := decodeXTxFormat(b)
			if err == nil {
				// The only tolerated flip is the trailing newline (optional).
				if i == len(good)-1 && f != nil {
					continue
				}
				t.Fatalf("flip byte %d bit %d accepted", i, bit)
			}
		}
	}
	if _, err := decodeXTxFormat([]byte("not json\n")); !errors.Is(err, ErrXTxFormatCorrupt) {
		t.Errorf("garbage: %v", err)
	}
}

func TestXTxLegacyRootUntouched(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, CollectionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	col, err := db.CreateCollection("c")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := col.Insert(map[string]any{"a": 1}); err != nil {
		t.Fatal(err)
	}
	if db.xtxJournal != nil {
		t.Fatal("legacy root grew a journal")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	for _, n := range rootFiles(t, dir) {
		if isReservedXTxName(n) {
			t.Fatalf("legacy root created %s", n)
		}
	}
	db, err = Open(dir, CollectionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
}

func TestXTxOpenRefusesTooNewFormat(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, CollectionConfig{})
	_, _ = db.CreateCollection("c")
	_ = db.Close()
	f := okFormat()
	f.MinReader = 99
	writeFormat(t, dir, f)
	if _, err := Open(dir, CollectionConfig{}); !errors.Is(err, ErrFormatTooNew) {
		t.Fatalf("Open: %v, want ErrFormatTooNew", err)
	}
	// The failed open released the directory lock.
	if err := os.Remove(filepath.Join(dir, xtxFormatFile)); err != nil {
		t.Fatal(err)
	}
	db, err := Open(dir, CollectionConfig{})
	if err != nil {
		t.Fatalf("reopen after fixing: %v", err)
	}
	_ = db.Close()
}

func TestXTxOpenWithJournal(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, CollectionConfig{})
	_, _ = db.CreateCollection("c")
	_ = db.Close()
	j, err := openOrCreateXTxJournal(dir, xtxOptions{}, "test")
	if err != nil {
		t.Fatal(err)
	}
	tx, err := j.commit("k", testParts("c"))
	if err != nil {
		t.Fatal(err)
	}
	_ = j.close()

	// A stale tmp from a crashed checkpoint is removed at open.
	if err := os.WriteFile(filepath.Join(dir, xtxJournalTmpFile), []byte("junk"), 0o644); err != nil {
		t.Fatal(err)
	}
	db, err = Open(dir, CollectionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if db.xtxJournal == nil || db.xtxJournal.status(tx) != XTxCommitted {
		t.Fatal("journal not loaded at open")
	}
	if _, err := os.Stat(filepath.Join(dir, xtxJournalTmpFile)); !os.IsNotExist(err) {
		t.Error("stale tmp survived open")
	}
	// Snapshot refuses rather than silently dropping the journal.
	if err := db.SnapshotTo(&bytes.Buffer{}); !errors.Is(err, ErrXTxUnsupported) {
		t.Errorf("SnapshotTo: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// Repair refuses a root whose XTx state it cannot yet preserve.
	if _, err := Repair(context.Background(), dir, RepairOptions{}); !errors.Is(err, ErrXTxUnsupported) {
		t.Errorf("Repair: %v", err)
	}
}

func TestXTxOpenFailsClosedOnCorruptJournal(t *testing.T) {
	dir := t.TempDir()
	if _, err := openOrCreateXTxJournal(dir, xtxOptions{}, "test"); err != nil {
		t.Fatal(err)
	}
	j, _ := openXTxJournal(dir, xtxOptions{})
	for i := 0; i < 3; i++ {
		if _, err := j.commit("", testParts("c")); err != nil {
			t.Fatal(err)
		}
	}
	_ = j.close()
	path := filepath.Join(dir, xtxJournalFile)
	b, _ := os.ReadFile(path)
	lines := bytes.SplitAfter(b, []byte("\n"))
	lines[2][12] ^= 0x01 // middle record
	if err := os.WriteFile(path, bytes.Join(lines, nil), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := Open(dir, CollectionConfig{})
	if !errors.Is(err, ErrXTxJournalCorrupt) || !errors.Is(err, ErrIntegrity) {
		t.Fatalf("Open: %v", err)
	}
	// Open must not have modified the journal.
	if after, _ := os.ReadFile(path); !bytes.Equal(after, bytes.Join(lines, nil)) {
		t.Error("failed open rewrote the journal")
	}
	// And released the lock (a second attempt reports the same error, not "locked").
	if _, err := Open(dir, CollectionConfig{}); !errors.Is(err, ErrXTxJournalCorrupt) {
		t.Fatalf("second Open: %v", err)
	}
}

func TestXTxOpenJournalWithoutFormatUnsupported(t *testing.T) {
	dir := t.TempDir()
	j, err := createXTxJournal(dir, xtxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_ = j.close()
	if _, err := Open(dir, CollectionConfig{}); !errors.Is(err, ErrXTxUnsupported) {
		t.Fatalf("Open: %v", err)
	}
}

func TestXTxReservedNames(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, CollectionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range []string{"xtx.journal", "xtx.anything", "xtx."} {
		if _, err := db.CreateCollection(n); !errors.Is(err, ErrReservedName) {
			t.Errorf("CreateCollection(%q): %v", n, err)
		}
		if _, err := db.CollectionWithConfig(n, CollectionConfig{}); !errors.Is(err, ErrReservedName) {
			t.Errorf("CollectionWithConfig(%q): %v", n, err)
		}
		if _, err := os.Stat(filepath.Join(dir, n)); err == nil {
			t.Errorf("rejected name %q left a directory", n)
		}
	}
	if _, err := db.CreateCollection("xtx"); err != nil { // no dot: allowed
		t.Errorf("collection xtx: %v", err)
	}
	_ = db.Close()

	// A pre-existing reserved root directory refuses the open.
	if err := os.Mkdir(filepath.Join(dir, "xtx.journal.d"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, CollectionConfig{}); !errors.Is(err, ErrReservedName) {
		t.Fatalf("Open with reserved dir: %v", err)
	}
	_ = os.Remove(filepath.Join(dir, "xtx.journal.d"))
	db, err = Open(dir, CollectionConfig{})
	if err != nil {
		t.Fatalf("Open after removing dir: %v", err)
	}
	_ = db.Close()
}
