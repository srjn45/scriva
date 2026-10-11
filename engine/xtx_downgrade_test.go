package engine

// Downgrade and unsupported-version behavior for a data directory that holds
// cross-collection transaction state (design §11). The fence has to keep a
// binary that cannot interpret stamped runs from opening, verifying or
// repairing them, and must never turn an unknown commit outcome into an abort
// by dropping the evidence that decides it.
//
// Every directory is produced by lcPopulate (a fixed write sequence, no
// random input), so a failure naming the case is a complete reproduction.
// Each refusal case also requires that the refused call changed no byte.

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/srjn45/scriva/store"
)

// dgDirs returns a cleanly closed transactional directory and the crash image
// (a prepared-but-undecided and a decided-but-unapplied transaction).
func dgDirs(t *testing.T) (clean, crash string) {
	t.Helper()
	root := t.TempDir()
	clean = filepath.Join(root, "data")
	_, img := lcPopulate(t, clean, root)
	return clean, img.crash
}

// dgStampedLines returns every stamped line of every collection in dir.
func dgStampedLines(t *testing.T, dir string) [][]byte {
	t.Helper()
	var out [][]byte
	for _, col := range lcCols {
		for _, seg := range lcSegments(t, dir, col) {
			for _, l := range lcLines(t, seg) {
				if bytes.Contains(l.text, []byte(`"tx":{`)) {
					out = append(out, l.text)
				}
			}
		}
	}
	return out
}

// TestXTxDowngrade_PreFenceEntryFence is layer L1 (§11.2): the entry decoder
// every pre-fence binary uses must reject every stamped line, so such a binary
// fails closed on first read instead of treating a stamped (possibly
// undecided) entry as an ordinary visible one.
func TestXTxDowngrade_PreFenceEntryFence(t *testing.T) {
	t.Parallel()
	clean, crash := dgDirs(t)
	for name, dir := range map[string]string{"clean": clean, "crash-image": crash} {
		lines := dgStampedLines(t, dir)
		if len(lines) == 0 {
			t.Fatalf("%s: no stamped lines found in a transactional directory", name)
		}
		for _, l := range lines {
			if _, err := store.Decode(l); !errors.Is(err, store.ErrCorruptEntry) {
				t.Fatalf("%s: legacy decoder accepted a stamped line (err=%v): %s", name, err, l)
			}
		}
	}
}

// TestXTxDowngrade_RefusedWithoutMutation: a root whose xtx.format this binary
// cannot satisfy is refused by Open, VerifyDir and Repair alike, with the
// precise error, and no byte of the directory (segments, journal, indexes) is
// touched by any of the three.
func TestXTxDowngrade_RefusedWithoutMutation(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		mut  func(*xtxFormat)
		want error
	}{
		{"min-reader-too-new", func(f *xtxFormat) { f.MinReader = xtxReaderLevel + 1 }, ErrFormatTooNew},
		{"unknown-feature", func(f *xtxFormat) { f.Features = append(f.Features, "future-feature") }, ErrXTxUnsupported},
	}
	for _, c := range cases {
		for _, img := range []string{"clean", "crash-image"} {
			t.Run(c.name+"/"+img, func(t *testing.T) {
				t.Parallel()
				clean, crash := dgDirs(t)
				dir := clean
				if img == "crash-image" {
					dir = crash
				}
				f := okFormat()
				c.mut(&f)
				writeFormat(t, dir, f)
				before := lcTreeHash(t, dir)

				if _, err := Open(dir, lcCfg()); !errors.Is(err, c.want) {
					t.Fatalf("Open: %v, want %v", err, c.want)
				}
				// VerifyDir reports rather than fails: the root must not read as clean.
				rep, err := VerifyDir(context.Background(), dir, VerifyOptions{})
				if err != nil {
					t.Fatalf("VerifyDir: %v", err)
				}
				if rep.Clean() || len(lcFindings(rep)) == 0 {
					t.Fatal("VerifyDir reported a root it cannot interpret as clean")
				}
				if _, err := Repair(context.Background(), dir, RepairOptions{BackupDir: t.TempDir()}); !errors.Is(err, c.want) {
					t.Fatalf("Repair: %v, want %v", err, c.want)
				}
				if after := lcTreeHash(t, dir); after != before {
					t.Fatal("a refused call modified the directory")
				}
				// A later binary that does satisfy the format can still use it.
				writeFormat(t, dir, okFormat())
				if _, err := Open(dir, lcCfg()); err != nil {
					t.Fatalf("Open after restoring a satisfiable format: %v", err)
				}
			})
		}
	}
}

// TestXTxDowngrade_LostEvidenceNeverAborts: removing the journal (the only
// record of decisions) while stamped runs remain must refuse, not presume
// abort and discard runs that may have been committed.
func TestXTxDowngrade_LostEvidenceNeverAborts(t *testing.T) {
	t.Parallel()
	for _, img := range []string{"clean", "crash-image"} {
		t.Run(img, func(t *testing.T) {
			t.Parallel()
			clean, crash := dgDirs(t)
			dir := clean
			if img == "crash-image" {
				dir = crash
			}
			if err := os.Remove(filepath.Join(dir, xtxJournalFile)); err != nil {
				t.Fatal(err)
			}
			before := lcTreeHash(t, dir)
			if _, err := Open(dir, lcCfg()); !errors.Is(err, ErrXTxJournalMissing) {
				t.Fatalf("Open without journal: %v, want ErrXTxJournalMissing", err)
			}
			if _, err := Repair(context.Background(), dir, RepairOptions{BackupDir: t.TempDir()}); err == nil {
				t.Fatal("Repair accepted a transactional root with no journal")
			}
			if after := lcTreeHash(t, dir); after != before {
				t.Fatal("a refused call modified the directory")
			}
		})
	}
}

// TestXTxDowngrade_JournalWithoutFormat: a journal with no xtx.format (the
// format file deleted, or a root assembled by hand) is not a legacy root.
func TestXTxDowngrade_JournalWithoutFormat(t *testing.T) {
	t.Parallel()
	clean, _ := dgDirs(t)
	if err := os.Remove(filepath.Join(clean, xtxFormatFile)); err != nil {
		t.Fatal(err)
	}
	before := lcTreeHash(t, clean)
	if _, err := Open(clean, lcCfg()); !errors.Is(err, ErrXTxUnsupported) {
		t.Fatalf("Open: %v, want ErrXTxUnsupported", err)
	}
	if after := lcTreeHash(t, clean); after != before {
		t.Fatal("refused open modified the directory")
	}
}

// TestXTxDowngrade_StampedRunsWithoutXTxFiles: both xtx.* files gone but
// stamped runs remain. The directory must not be silently adopted as a legacy
// root whose stamped entries are ordinary visible documents.
func TestXTxDowngrade_StampedRunsWithoutXTxFiles(t *testing.T) {
	t.Parallel()
	_, crash := dgDirs(t)
	for _, n := range []string{xtxFormatFile, xtxJournalFile} {
		if err := os.Remove(filepath.Join(crash, n)); err != nil {
			t.Fatal(err)
		}
	}
	before := lcTreeHash(t, crash)
	db, err := Open(crash, lcCfg())
	if err == nil {
		_ = db.Close()
		t.Fatal("Open accepted a directory with stamped runs and no xtx.format/xtx.journal")
	}
	t.Logf("Open refused: %v", err)
	if after := lcTreeHash(t, crash); after != before {
		t.Fatal("refused open modified the directory")
	}
}
