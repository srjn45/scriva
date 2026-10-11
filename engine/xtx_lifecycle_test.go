package engine

// Directory lifecycle of a data directory that holds cross-collection
// transaction state: fresh, upgraded from the pre-transaction format, moved,
// copied (at rest and while live) and backup-restored. Each condition is
// exercised both on a cleanly closed directory and on a crash image that
// contains a prepared-but-undecided transaction, a decided-but-unapplied one
// and a handle with staged writes (see lcPopulate). The damaged condition is
// in xtx_damage_test.go.
//
// These tests have no random input: every state is produced by a fixed write
// sequence and captured at a named commit-path hook, so a failure message that
// names the directory condition is a complete reproduction recipe.

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

// lcUseAgain proves a recovered directory is fully usable: it commits one more
// cross-collection transaction and a plain write, then reopens and re-checks.
func lcUseAgain(t *testing.T, dir string, m *lcModel, where string) {
	t.Helper()
	db, err := Open(dir, lcCfg())
	if err != nil {
		t.Fatalf("%s: reopen for new writes: %v", where, err)
	}
	b := &lcBuilder{t: t, db: db, m: m, n: 1000}
	b.commit("post-"+where, "accounts", "ledger", "orders")
	b.plain("audit")
	lcCheck(t, db, m, where+" (after new transaction)")
	// Forced compaction rewrites sealed segments that hold stamped runs; it
	// must not change what any transaction looks like, now or after a reopen.
	for _, name := range lcCols {
		c := b.col(name)
		if err := c.rotateSegment(); err != nil {
			t.Fatalf("%s: rotate %s: %v", where, name, err)
		}
		if err := c.CompactNow(); err != nil {
			t.Fatalf("%s: compact %s: %v", where, name, err)
		}
	}
	lcCheck(t, db, m, where+" (after compaction)")
	if err := db.Close(); err != nil {
		t.Fatalf("%s: close: %v", where, err)
	}
	lcMustOpen(t, dir, m, where+" (after new transaction, reopened)")
}

func lcHasXTxFiles(dir string) bool {
	for _, n := range []string{xtxFormatFile, xtxJournalFile} {
		if _, err := os.Stat(filepath.Join(dir, n)); err == nil {
			return true
		}
	}
	return false
}

func TestXTxLifecycle_Fresh(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	dir := filepath.Join(root, "data")
	m, img := lcPopulate(t, dir, root)

	if !lcHasXTxFiles(dir) {
		t.Fatal("a directory used transactionally must carry xtx.format and xtx.journal")
	}
	lcMustOpen(t, dir, m, "fresh, clean close")
	lcUseAgain(t, dir, m, "fresh")

	// The crash image is opened for the first time here: recovery must abort
	// the prepared transaction everywhere and apply the decided one everywhere.
	sig := lcMustOpen(t, img.crash, img.crashModel, "fresh, crash image")
	if want := "t-decided=true"; !bytes.Contains([]byte(sig), []byte(want)) {
		t.Fatalf("crash image: %q missing from %q", want, sig)
	}
	lcUseAgain(t, img.crash, img.crashModel, "fresh-crash-image")
}

// lcLegacyFixture is a data directory written by the released v1.4.0 engine,
// the last version before cross-collection transactions. It was produced by
// running that tag's engine (Open with SegmentMaxSize 400, three collections,
// a secondary index on "kind", 8 inserts, one update of id 2, one delete of
// id 3 per collection, clean Close) and is committed as-is. It is an input
// only: tests copy it before opening.
const lcLegacyFixture = "testdata/upgrade/v1.4.0"

var lcLegacyCols = []string{"accounts", "audit", "ledger"}

// lcCheckLegacy asserts the v1.4.0 documents are exactly as that version left them.
func lcCheckLegacy(t *testing.T, dir, where string) {
	t.Helper()
	db, err := Open(dir, lcCfg())
	if err != nil {
		t.Fatalf("%s: open: %v", where, err)
	}
	defer func() { _ = db.Close() }()
	for _, name := range lcLegacyCols {
		c, err := db.Collection(name)
		if err != nil {
			t.Fatalf("%s: collection %s: %v", where, name, err)
		}
		for id := uint64(1); id <= 8; id++ {
			rec, err := c.Get(id)
			if id == 3 {
				if err == nil {
					t.Fatalf("%s: %s/3 was deleted by v1.4.0 but is readable", where, name)
				}
				continue
			}
			if err != nil {
				t.Fatalf("%s: %s/%d: %v", where, name, id, err)
			}
			wantKind, wantN := "legacy", float64(id-1)
			if id == 2 {
				wantKind, wantN = "legacy-updated", 2
			}
			if rec.Data["kind"] != wantKind || rec.Data["col"] != name || rec.Data["n"] != wantN {
				t.Fatalf("%s: %s/%d = %v", where, name, id, rec.Data)
			}
		}
		ids, ok := c.IndexLookup("kind", "legacy")
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		if want := []uint64{1, 4, 5, 6, 7, 8}; !ok || len(ids) != len(want) {
			t.Fatalf("%s: %s secondary index kind=legacy = %v (ok=%v), want %v", where, name, ids, ok, want)
		}
	}
}

func TestXTxLifecycle_Upgraded(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	dir := filepath.Join(root, "data")
	lcCopyTree(t, lcLegacyFixture, dir)
	if lcHasXTxFiles(dir) {
		t.Fatal("fixture is not a pre-transaction directory")
	}

	// The current engine verifies and opens the old directory without
	// converting it: until the first transaction it stays a legacy root.
	lcMustVerifyClean(t, dir, "legacy fixture before first open")
	legacySegs := map[string]string{}
	for _, name := range lcLegacyCols {
		for _, seg := range lcSegments(t, dir, name) {
			b, err := os.ReadFile(seg)
			if err != nil {
				t.Fatal(err)
			}
			legacySegs[seg] = sha256Hex(b)
		}
	}
	lcCheckLegacy(t, dir, "legacy fixture opened by current engine")
	if lcHasXTxFiles(dir) {
		t.Fatal("opening a legacy root wrote xtx.* files before any transaction")
	}

	// First transactional use upgrades the root in place.
	m, img := lcPopulate(t, dir, root)
	if !lcHasXTxFiles(dir) {
		t.Fatal("first transaction did not create xtx.format / xtx.journal")
	}
	f, err := readXTxFormat(dir)
	if err != nil || f == nil || f.MinReader != 1 {
		t.Fatalf("xtx.format after upgrade: %+v, %v", f, err)
	}
	// With compaction off, segments sealed by v1.4.0 are not rewritten by the
	// upgrade (the last one was the active segment and is appended to, so only
	// its prefix is kept).
	for seg, want := range legacySegs {
		b, err := os.ReadFile(seg)
		if err != nil {
			t.Fatal(err)
		}
		if sha256Hex(b) != want {
			// Only a formerly active segment may have grown.
			old, rerr := os.ReadFile(filepath.Join(lcLegacyFixture, lcRel(dir, seg)))
			if rerr != nil || !bytes.HasPrefix(b, old) {
				t.Fatalf("upgrade rewrote v1.4.0 segment %s", lcRel(dir, seg))
			}
		}
	}

	lcMustOpen(t, dir, m, "upgraded, clean close")
	lcCheckLegacy(t, dir, "upgraded, clean close")
	lcUseAgain(t, dir, m, "upgraded")
	lcCheckLegacy(t, dir, "upgraded, after new transaction")

	lcMustOpen(t, img.crash, img.crashModel, "upgraded, crash image")
	lcCheckLegacy(t, img.crash, "upgraded, crash image")
	lcUseAgain(t, img.crash, img.crashModel, "upgraded-crash-image")
	lcCheckLegacy(t, img.crash, "upgraded, crash image, after new transaction")
}

func TestXTxLifecycle_Moved(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	dir := filepath.Join(root, "data")
	m, img := lcPopulate(t, dir, root)

	move := func(src, where string) string {
		dst := filepath.Join(root, "relocated", where, "deeper", "data-moved")
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(src, dst); err != nil {
			t.Fatalf("%s: rename: %v", where, err)
		}
		return dst
	}

	// Cleanly closed directory, never reopened at the old path.
	moved := move(dir, "clean")
	want := lcMustOpen(t, moved, m, "moved, clean close")

	// Moved again after having been opened (and its indexes re-persisted) at
	// the intermediate path.
	movedAgain := move(moved, "again")
	if got := lcMustOpen(t, movedAgain, m, "moved twice"); got != want {
		t.Fatalf("visibility changed by a move:\n before %s\n after  %s", want, got)
	}

	// Reached through a symlink.
	link := filepath.Join(root, "link")
	if err := os.Symlink(movedAgain, link); err == nil {
		if got := lcMustOpen(t, link, m, "moved, opened through a symlink"); got != want {
			t.Fatalf("visibility differs through a symlink:\n direct %s\n link   %s", want, got)
		}
	}
	lcUseAgain(t, movedAgain, m, "moved")

	// Crash image moved before its first recovery: recovery runs at the new path.
	movedImg := move(img.crash, "crash")
	lcMustOpen(t, movedImg, img.crashModel, "moved, crash image")
	lcUseAgain(t, movedImg, img.crashModel, "moved-crash-image")
}

func TestXTxLifecycle_Copied(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	dir := filepath.Join(root, "data")
	m, img := lcPopulate(t, dir, root)

	// Copy at rest: the copy and the original are independent databases with
	// the same transactions.
	cp := filepath.Join(root, "copy-at-rest")
	lcCopyTree(t, dir, cp)
	cpModel := m.clone()
	orig := lcMustOpen(t, dir, m, "copied, original")
	if got := lcMustOpen(t, cp, cpModel, "copied, copy at rest"); got != orig {
		t.Fatalf("copy shows different transactions:\n original %s\n copy     %s", orig, got)
	}
	// Diverge the copy; the original must not see it.
	lcUseAgain(t, cp, cpModel, "copied-copy")
	if got := lcMustOpen(t, dir, m, "copied, original after the copy diverged"); got != orig {
		t.Fatalf("original changed after writing to its copy:\n before %s\n after  %s", orig, got)
	}

	// Copy taken while live, mid-commit (the crash image), and a copy of that
	// copy taken before either was recovered: both recover to the same state.
	second := filepath.Join(root, "copy-of-live-copy")
	lcCopyTree(t, img.crash, second)
	a := lcMustOpen(t, img.crash, img.crashModel, "copied while live")
	b := lcMustOpen(t, second, img.crashModel.clone(), "copy of a live copy")
	if a != b {
		t.Fatalf("two copies of one live image recovered differently:\n first  %s\n second %s", a, b)
	}

	// Copy of an already recovered image, then both used independently.
	third := filepath.Join(root, "copy-after-recovery")
	lcCopyTree(t, img.crash, third)
	thirdModel := img.crashModel.clone()
	lcUseAgain(t, third, thirdModel, "copied-after-recovery")
	if got := lcMustOpen(t, img.crash, img.crashModel, "recovered image after its copy diverged"); got != a {
		t.Fatalf("recovered image changed after writing to its copy")
	}
}

// TestXTxLifecycle_LiveFileCopyOrder pins what a non-atomic file-level copy of
// a running database yields, for both copy orders. Neither can show part of a
// transaction; the two differ in whether the copy opens at all.
func TestXTxLifecycle_LiveFileCopyOrder(t *testing.T) {
	t.Parallel()
	for _, journalFirst := range []bool{true, false} {
		name := "collections_then_journal"
		if journalFirst {
			name = "journal_then_collections"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			dir := filepath.Join(root, "data")
			db, err := Open(dir, lcCfg())
			if err != nil {
				t.Fatal(err)
			}
			b := &lcBuilder{t: t, db: db, m: &lcModel{plain: map[string]map[uint64]float64{}}}
			for _, c := range lcCols {
				if _, err := db.CreateCollection(c); err != nil {
					t.Fatal(err)
				}
				b.plain(c)
			}
			b.commit("before-copy", "accounts", "audit", "ledger")

			cp := filepath.Join(root, "copy")
			copyRoot := func() {
				for _, n := range []string{xtxFormatFile, xtxJournalFile} {
					data, err := os.ReadFile(filepath.Join(dir, n))
					if err != nil {
						t.Fatal(err)
					}
					if err := os.MkdirAll(cp, 0o755); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(filepath.Join(cp, n), data, 0o644); err != nil {
						t.Fatal(err)
					}
				}
			}
			copyCols := func() {
				for _, c := range lcCols {
					lcCopyTree(t, filepath.Join(dir, c), filepath.Join(cp, c))
				}
			}

			// A transaction commits between the two halves of the copy.
			if journalFirst {
				copyRoot()
				b.commit("during-copy", "accounts", "orders")
				copyCols()
			} else {
				copyCols()
				b.commit("during-copy", "accounts", "orders")
				copyRoot()
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}

			m := b.m.clone()
			if journalFirst {
				// The copied journal predates the COMMIT: the copied runs are
				// undecided and the copy is the database as of the journal copy.
				m.setWant(lcAborted, "during-copy")
				lcMustOpen(t, cp, m, "live file copy, journal first")
				return
			}
			// The copied journal has a COMMIT whose runs the collections
			// (copied earlier) do not contain. That is refused, never opened
			// with one participant's worth of data.
			for i := 0; i < 2; i++ {
				_, err := lcOpenCheck(t, cp, lcCfg(), m, "live file copy, journal last")
				if !errors.Is(err, ErrXTxIncomplete) {
					t.Fatalf("open %d of a journal-last copy: want ErrXTxIncomplete, got %v", i, err)
				}
			}
			rep, err := VerifyDir(context.Background(), cp, VerifyOptions{})
			if err != nil || !rep.Has(CodeXTxParticipantMissing) {
				t.Fatalf("VerifyDir of a journal-last copy: %v, %v", lcFindings(rep), err)
			}
		})
	}
}

func lcSnapshot(t *testing.T, db *DB) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := db.SnapshotTo(&buf); err != nil {
		t.Fatalf("SnapshotTo: %v", err)
	}
	return buf.Bytes()
}

func TestXTxLifecycle_BackupRestored(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	dir := filepath.Join(root, "data")
	m, img := lcPopulate(t, dir, root)

	// 1. Online backup of a database at rest, restored by plain extraction
	// (the documented restore: tar xzf into an empty --data directory).
	db, err := Open(dir, lcCfg())
	if err != nil {
		t.Fatal(err)
	}
	archive := lcSnapshot(t, db)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	restored := filepath.Join(root, "restored")
	extractTarGz(t, archive, restored)
	want := lcMustOpen(t, dir, m, "backup source")
	if got := lcMustOpen(t, restored, m.clone(), "backup-restored"); got != want {
		t.Fatalf("restore shows different transactions:\n source   %s\n restored %s", want, got)
	}
	lcUseAgain(t, restored, m.clone(), "backup-restored")

	// 2. Backup of a database that has just recovered a crash image: the
	// archive carries the recovery's own ABORT, and the restore agrees.
	db, err = Open(img.crash, lcCfg())
	if err != nil {
		t.Fatal(err)
	}
	lcCheck(t, db, img.crashModel, "recovered crash image before backup")
	archive = lcSnapshot(t, db)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	restoredImg := filepath.Join(root, "restored-crash-image")
	extractTarGz(t, archive, restoredImg)
	lcMustOpen(t, restoredImg, img.crashModel.clone(), "backup-restored crash image")

	// 3. Backup requested while a transaction is prepared and while it is
	// decided but unapplied. SnapshotTo must not produce an archive until the
	// transaction has finished, and the archive must then hold all of it.
	for _, point := range []struct {
		name string
		at   XTxHookPoint
	}{{"prepared", XTxHookPrepared}, {"decided_unapplied", XTxHookDecided}} {
		t.Run("backup_during_"+point.name, func(t *testing.T) {
			live := filepath.Join(t.TempDir(), "data")
			cfg := lcCfg()
			reached, release := make(chan struct{}), make(chan struct{})
			SetTestXTxHook(&cfg, func(p XTxHookPoint, key, _ string) {
				if p == point.at && key == "in-flight" {
					close(reached)
					<-release
				}
			})
			db, err := Open(live, cfg)
			if err != nil {
				t.Fatal(err)
			}
			b := &lcBuilder{t: t, db: db, m: &lcModel{plain: map[string]map[uint64]float64{}}}
			for _, c := range lcCols {
				if _, err := db.CreateCollection(c); err != nil {
					t.Fatal(err)
				}
				b.plain(c)
			}
			b.commit("settled", "accounts", "audit", "ledger")
			tx, ops := b.stage("in-flight", "audit", "ledger", "orders")

			committed := make(chan error, 1)
			go func() {
				res, err := db.CommitXTx(tx.key, ops)
				if err == nil {
					tx.txid = res.TxID
				}
				committed <- err
			}()
			<-reached

			snap := make(chan []byte, 1)
			snapErr := make(chan error, 1)
			go func() {
				var buf bytes.Buffer
				if err := db.SnapshotTo(&buf); err != nil {
					snapErr <- err
					return
				}
				snap <- buf.Bytes()
			}()
			select {
			case <-snap:
				t.Fatalf("SnapshotTo completed while a transaction was %s", point.name)
			case err := <-snapErr:
				t.Fatalf("SnapshotTo failed while a transaction was %s: %v", point.name, err)
			case <-time.After(100 * time.Millisecond):
			}
			close(release)
			if err := <-committed; err != nil {
				t.Fatalf("commit: %v", err)
			}
			tx.want = lcCommitted
			var archive []byte
			select {
			case archive = <-snap:
			case err := <-snapErr:
				t.Fatalf("SnapshotTo: %v", err)
			case <-time.After(30 * time.Second):
				t.Fatal("SnapshotTo did not finish after the transaction completed")
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			dst := filepath.Join(t.TempDir(), "restored")
			extractTarGz(t, archive, dst)
			lcMustOpen(t, dst, b.m, "backup taken while a transaction was "+point.name)
		})
	}
}
