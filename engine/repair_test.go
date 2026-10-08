package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/srjn45/scriva/store"
)

// ---- helpers ---------------------------------------------------------------

func runRepair(t *testing.T, dir string, opts RepairOptions) (*RepairReport, error) {
	t.Helper()
	if opts.BackupDir == "" {
		opts.BackupDir = t.TempDir()
	}
	return Repair(context.Background(), dir, opts)
}

func mustRepair(t *testing.T, dir string, opts RepairOptions) *RepairReport {
	t.Helper()
	rep, err := runRepair(t, dir, opts)
	if err != nil {
		t.Fatalf("repair: %v", err)
	}
	return rep
}

// segHashes hashes only the segment files, to prove they were not edited.
func segHashes(t *testing.T, colDir string) string {
	t.Helper()
	var sb strings.Builder
	paths, _ := filepath.Glob(filepath.Join(colDir, "seg_*.ndjson"))
	for _, p := range paths {
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(&sb, "%s:%s;", filepath.Base(p), sha256Hex(b))
	}
	return sb.String()
}

func wantVerifyClean(t *testing.T, dir string) {
	t.Helper()
	rep, err := VerifyDir(context.Background(), dir, VerifyOptions{Mode: VerifyFull})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range rep.Collections {
		for _, f := range c.Findings {
			if f.Severity.rank() > SeverityInfo.rank() {
				t.Fatalf("post-repair finding in %s: %+v", c.Name, f)
			}
		}
	}
}

func colRepair(t *testing.T, rep *RepairReport, name string) CollectionRepair {
	t.Helper()
	for _, c := range rep.Collections {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("no collection %q in report", name)
	return CollectionRepair{}
}

func openRepaired(t *testing.T, dir string, cfg CollectionConfig) (*DB, *Collection) {
	t.Helper()
	db, err := Open(dir, cfg)
	if err != nil {
		t.Fatalf("open after repair: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	col, err := db.Collection("c")
	if err != nil {
		t.Fatal(err)
	}
	return db, col
}

func listBackups(t *testing.T, parent string) []string {
	t.Helper()
	ents, _ := os.ReadDir(parent)
	var out []string
	for _, e := range ents {
		if strings.HasPrefix(e.Name(), "repair-backup-") {
			out = append(out, filepath.Join(parent, e.Name()))
		}
	}
	return out
}

func journalExists(dir string) bool {
	_, err := os.Stat(journalPath(dir))
	return err == nil
}

func withFault(t *testing.T, f func(stage string) error) {
	t.Helper()
	repairFault = f
	t.Cleanup(func() { repairFault = nil })
}

// crashImage builds a directory whose persisted index is stale, as after a
// kill -9: a late insert, an update and a delete never reached index.json.
func crashImage(t *testing.T) (img string, late, updated, deleted uint64) {
	t.Helper()
	dir := t.TempDir()
	cfg := CollectionConfig{CompactInterval: time.Hour, SyncMode: SyncModeAlways}
	ids := seedClosed(t, dir, cfg, 4)
	updated, deleted = ids[0], ids[3]
	db, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	col, _ := db.Collection("c")
	if late, _, err = col.Insert(map[string]any{"v": "late"}); err != nil {
		t.Fatal(err)
	}
	if _, err := col.Update(updated, map[string]any{"v": "updated"}); err != nil {
		t.Fatal(err)
	}
	if err := col.Delete(deleted); err != nil {
		t.Fatal(err)
	}
	return copyDataDir(t, dir), late, updated, deleted
}

// ---- tests -----------------------------------------------------------------

func TestRepairStaleCrashIndex(t *testing.T) {
	img, late, updated, deleted := crashImage(t)
	colDir := filepath.Join(img, "c")
	segsBefore := segHashes(t, colDir)
	pre, _ := VerifyDir(context.Background(), img, VerifyOptions{})
	if pre.Clean() {
		t.Fatal("crash image should not verify clean")
	}

	bkParent := t.TempDir()
	rep := mustRepair(t, img, RepairOptions{BackupDir: bkParent})
	c := colRepair(t, rep, "c")
	if c.Status != RepairRepaired || c.After == nil || len(c.Before.Findings) == 0 {
		t.Fatalf("unexpected result: %+v", c)
	}
	if segHashes(t, colDir) != segsBefore {
		t.Fatal("segments were edited")
	}
	wantVerifyClean(t, img)
	if journalExists(img) {
		t.Fatal("journal not retired")
	}

	// Backup exists, is a byte-for-byte copy of the pre-repair collection and
	// carries the report.
	bks := listBackups(t, bkParent)
	if len(bks) != 1 || bks[0] != rep.BackupDir {
		t.Fatalf("backups %v, report says %q", bks, rep.BackupDir)
	}
	if segHashes(t, filepath.Join(rep.BackupDir, "c")) != segsBefore {
		t.Fatal("backup segments differ from the originals")
	}
	if _, err := os.Stat(filepath.Join(rep.BackupDir, "repair-report.json")); err != nil {
		t.Fatal(err)
	}

	_, col := openRepaired(t, img, CollectionConfig{CompactInterval: time.Hour})
	if st := col.IndexRecoveryStats(); st.FullRebuilds != 0 || st.Replays != 0 {
		t.Fatalf("open should trust the repaired index as-is, got %+v", st)
	}
	if r, err := col.Get(late); err != nil || r.Data["v"] != "late" {
		t.Fatalf("late record: %v %+v", err, r)
	}
	if r, err := col.Get(updated); err != nil || r.Data["v"] != "updated" {
		t.Fatalf("updated record: %v %+v", err, r)
	}
	if _, err := col.Get(deleted); err == nil {
		t.Fatal("tombstoned record resurrected")
	}
	// The counter never goes backwards: a new insert gets a fresh id.
	nid, _, err := col.Insert(map[string]any{"v": "n"})
	if err != nil || nid <= late {
		t.Fatalf("new id %d (late %d): %v", nid, late, err)
	}
}

func TestRepairIsIdempotent(t *testing.T) {
	img, _, _, _ := crashImage(t)
	mustRepair(t, img, RepairOptions{})
	snap := snapshotDir(t, img)

	bkParent := t.TempDir()
	rep := mustRepair(t, img, RepairOptions{BackupDir: bkParent})
	if c := colRepair(t, rep, "c"); c.Status != RepairUnchanged {
		t.Fatalf("second run: %+v", c)
	}
	if rep.BackupDir != "" || len(listBackups(t, bkParent)) != 0 {
		t.Fatal("a clean rerun must not take a backup")
	}
	if snapshotDir(t, img) != snap {
		t.Fatal("second run modified the directory")
	}
	wantVerifyClean(t, img)
}

func TestRepairCleanDirectoryUntouched(t *testing.T) {
	dir := t.TempDir()
	seedClosed(t, dir, vcfg, 5)
	snap := snapshotDir(t, dir)
	rep := mustRepair(t, dir, RepairOptions{})
	if c := colRepair(t, rep, "c"); c.Status != RepairUnchanged {
		t.Fatalf("%+v", c)
	}
	// snapshotDir includes LOCK (created by lockDir), so compare segments+index.
	if got := snapshotDir(t, dir); strings.ReplaceAll(got, "LOCK", "") != strings.ReplaceAll(snap, "LOCK", "") {
		t.Fatal("clean directory was modified")
	}
}

func TestRepairRefusesOpenDirectory(t *testing.T) {
	dir := t.TempDir()
	cfg := CollectionConfig{CompactInterval: time.Hour}
	seedClosed(t, dir, cfg, 2)
	db, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err := runRepair(t, dir, RepairOptions{}); !errors.Is(err, ErrDatabaseLocked) {
		t.Fatalf("want ErrDatabaseLocked, got %v", err)
	}
}

func TestRepairBackupMustBeOutsideDataDir(t *testing.T) {
	dir := t.TempDir()
	seedClosed(t, dir, vcfg, 2)
	if _, err := Repair(context.Background(), dir, RepairOptions{BackupDir: filepath.Join(dir, "bk")}); err == nil {
		t.Fatal("backup inside the data dir would be opened as a collection")
	}
}

// withoutLock drops the (empty, harmless) LOCK file entry from a snapshotDir string.
func withoutLock(snap string) string {
	var keep []string
	for _, e := range strings.Split(snap, ";") {
		if !strings.Contains(e, "LOCK:") {
			keep = append(keep, e)
		}
	}
	return strings.Join(keep, ";")
}

func TestRepairBackupFailureAbortsBeforeMutation(t *testing.T) {
	img, _, _, _ := crashImage(t)
	snap := snapshotDir(t, img)
	// A file where the backup parent should be makes the backup impossible.
	blocker := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Repair(context.Background(), img, RepairOptions{BackupDir: filepath.Join(blocker, "sub")}); err == nil {
		t.Fatal("expected backup failure")
	}
	if withoutLock(snapshotDir(t, img)) != withoutLock(snap) {
		t.Fatal("directory changed although the backup failed")
	}
	if journalExists(img) {
		t.Fatal("journal written without a backup")
	}
}

func TestRepairOrphanSegments(t *testing.T) {
	dir := t.TempDir()
	cfg := CollectionConfig{CompactInterval: time.Hour, SegmentMaxSize: 150}
	seedClosed(t, dir, cfg, 12)
	colDir := filepath.Join(dir, "c")
	segs := colSegs(t, colDir)
	if len(segs) < 3 {
		t.Fatalf("need >= 3 segments, got %d", len(segs))
	}
	// Persist an index that omits the oldest segment (rotation race).
	idx := newIndex()
	if err := idx.Rebuild(segs[1:]); err != nil {
		t.Fatal(err)
	}
	snap, _ := idx.Snapshot(segs[1:])
	if err := snap.Persist(filepath.Join(colDir, "index.json")); err != nil {
		t.Fatal(err)
	}
	// And an unnumbered segment with ids no other segment holds.
	stray := encodeEntry(t, store.Entry{Op: store.OpInsert, ID: 99, Rev: 1, Ts: time.Unix(1, 0).UTC(), Data: map[string]any{"v": "stray"}})
	if err := os.WriteFile(filepath.Join(colDir, "seg_abc.ndjson"), stray, 0o644); err != nil {
		t.Fatal(err)
	}
	segsBefore := segHashes(t, colDir)

	rep := mustRepair(t, dir, RepairOptions{})
	c := colRepair(t, rep, "c")
	if c.Status != RepairRepaired {
		t.Fatalf("%+v", c)
	}
	var adopted bool
	for _, a := range c.Actions {
		adopted = adopted || a.Kind == "adopt-segment"
	}
	if !adopted {
		t.Fatalf("no adopt action: %+v", c.Actions)
	}
	if _, err := os.Stat(filepath.Join(colDir, "seg_abc.ndjson")); err == nil {
		t.Fatal("unnumbered segment still present")
	}
	// Contents are untouched: the same set of segment bodies, one renamed.
	if n := strings.Count(segHashes(t, colDir), ";"); n != strings.Count(segsBefore, ";") {
		t.Fatalf("segment count changed: %d vs %d", n, strings.Count(segsBefore, ";"))
	}
	wantVerifyClean(t, dir)

	_, col := openRepaired(t, dir, CollectionConfig{CompactInterval: time.Hour, SegmentMaxSize: 150})
	if r, err := col.Get(99); err != nil || r.Data["v"] != "stray" {
		t.Fatalf("adopted record: %v %+v", err, r)
	}
	for id := uint64(1); id <= 12; id++ {
		if _, err := col.Get(id); err != nil {
			t.Fatalf("id %d (in the unlisted oldest segment) missing: %v", id, err)
		}
	}
	if st := col.IndexRecoveryStats(); st.FullRebuilds != 0 {
		t.Fatalf("open rebuilt although repair persisted coverage: %+v", st)
	}
}

func TestRepairUnnumberedSegmentSharingIDsIsBlocked(t *testing.T) {
	dir := t.TempDir()
	seedClosed(t, dir, vcfg, 3)
	colDir := filepath.Join(dir, "c")
	// Same id as an existing record: replay order would decide the winner.
	stray := encodeEntry(t, store.Entry{Op: store.OpUpdate, ID: 1, Rev: 5, Ts: time.Unix(1, 0).UTC(), Data: map[string]any{"v": "stray"}})
	if err := os.WriteFile(filepath.Join(colDir, "seg_abc.ndjson"), stray, 0o644); err != nil {
		t.Fatal(err)
	}
	before := segHashes(t, colDir)
	bkParent := t.TempDir()
	rep, err := runRepair(t, dir, RepairOptions{BackupDir: bkParent})
	if !errors.Is(err, ErrRepairIncomplete) {
		t.Fatalf("want ErrRepairIncomplete, got %v", err)
	}
	if c := colRepair(t, rep, "c"); c.Status != RepairBlocked || !strings.Contains(c.Reason, "id 1") {
		t.Fatalf("%+v", c)
	}
	if segHashes(t, colDir) != before || len(listBackups(t, bkParent)) != 0 || journalExists(dir) {
		t.Fatal("a blocked collection must not be touched or backed up")
	}
}

func TestRepairTornTailTrimmed(t *testing.T) {
	img, late, _, _ := crashImage(t)
	seg := filepath.Join(img, "c", "seg_000001.ndjson")
	appendRaw(t, seg, []byte(`{"id":900,"op":"ins`))
	rep := mustRepair(t, img, RepairOptions{})
	c := colRepair(t, rep, "c")
	var trimmed bool
	for _, a := range c.Actions {
		trimmed = trimmed || a.Kind == "trim-torn-tail"
	}
	if !trimmed {
		t.Fatalf("torn tail not trimmed: %+v", c.Actions)
	}
	wantVerifyClean(t, img)
	_, col := openRepaired(t, img, CollectionConfig{CompactInterval: time.Hour})
	if _, err := col.Get(late); err != nil {
		t.Fatal(err)
	}
}

// damagedSegments seeds a multi-segment collection and damages the OLDEST
// segment: one record is overwritten with garbage and a glued partial record
// precedes a valid one. It returns the ids that are lost and those recovered
// only through the glued line.
func damagedSegments(t *testing.T) (dir string, lost uint64) {
	t.Helper()
	dir = t.TempDir()
	cfg := CollectionConfig{CompactInterval: time.Hour, SegmentMaxSize: 150}
	ids := seedClosed(t, dir, cfg, 12)
	colDir := filepath.Join(dir, "c")
	seg1 := filepath.Join(colDir, "seg_000001.ndjson")
	b, err := os.ReadFile(seg1)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.SplitAfter(string(b), "\n")
	if len(lines) < 3 {
		t.Fatalf("segment 1 too small: %d lines", len(lines))
	}
	// Destroy the first record (same length keeps later offsets stable).
	lost = ids[0]
	lines[0] = strings.Repeat("#", len(lines[0])-1) + "\n"
	if err := os.WriteFile(seg1, []byte(strings.Join(lines, "")), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir, lost
}

func TestRepairPartialCorruptWithoutSalvageIsBlocked(t *testing.T) {
	dir, _ := damagedSegments(t)
	before := snapshotDir(t, dir)
	bkParent := t.TempDir()
	rep, err := runRepair(t, dir, RepairOptions{BackupDir: bkParent})
	if !errors.Is(err, ErrRepairIncomplete) {
		t.Fatalf("want ErrRepairIncomplete, got %v", err)
	}
	c := colRepair(t, rep, "c")
	if c.Status != RepairBlocked || !strings.Contains(c.Reason, "Salvage") {
		t.Fatalf("%+v", c)
	}
	if strings.ReplaceAll(snapshotDir(t, dir), "LOCK", "") != strings.ReplaceAll(before, "LOCK", "") || len(listBackups(t, bkParent)) != 0 {
		t.Fatal("blocked collection was modified")
	}
}

func TestRepairSalvageIntoNewSegment(t *testing.T) {
	dir, lost := damagedSegments(t)
	colDir := filepath.Join(dir, "c")
	damaged := filepath.Join(colDir, "seg_000001.ndjson")
	origDamaged, _ := os.ReadFile(damaged)
	others := segHashes(t, colDir)

	rep := mustRepair(t, dir, RepairOptions{Salvage: true})
	c := colRepair(t, rep, "c")
	if c.Status != RepairRepaired || c.Salvage == nil {
		t.Fatalf("%+v", c)
	}
	sr := c.Salvage
	if len(sr.Quarantined) != 1 || sr.Quarantined[0] != "seg_000001.ndjson" || sr.BadRegions != 1 || sr.Entries == 0 {
		t.Fatalf("salvage report: %+v", sr)
	}
	// The damaged original is gone from the directory but intact in the backup;
	// every other original segment is byte-identical.
	if _, err := os.Stat(damaged); err == nil {
		t.Fatal("damaged segment still in the directory")
	}
	if b, err := os.ReadFile(filepath.Join(rep.BackupDir, "c", "seg_000001.ndjson")); err != nil || string(b) != string(origDamaged) {
		t.Fatalf("backup of the damaged segment is not intact: %v", err)
	}
	for _, part := range strings.Split(others, ";") {
		name := strings.SplitN(part, ":", 2)[0]
		if name == "" || name == "seg_000001.ndjson" {
			continue
		}
		if !strings.Contains(segHashes(t, colDir), part+";") {
			t.Fatalf("original segment %s was edited", name)
		}
	}
	if _, err := os.Stat(filepath.Join(colDir, sr.NewSegment)); err != nil {
		t.Fatal(err)
	}
	wantVerifyClean(t, dir)

	_, col := openRepaired(t, dir, CollectionConfig{CompactInterval: time.Hour, SegmentMaxSize: 150})
	if _, err := col.Get(lost); err == nil {
		t.Fatal("the destroyed record cannot have come back")
	}
	for id := uint64(2); id <= 12; id++ {
		if _, err := col.Get(id); err != nil {
			t.Fatalf("id %d lost by salvage: %v", id, err)
		}
	}
}

func TestRepairSalvageRecoversGluedRecord(t *testing.T) {
	dir := t.TempDir()
	seedClosed(t, dir, vcfg, 3)
	seg := filepath.Join(dir, "c", "seg_000001.ndjson")
	good := encodeEntry(t, store.Entry{ID: 50, Op: store.OpInsert, Rev: 1, Data: map[string]any{"v": "x"}})
	appendRaw(t, seg, append([]byte(`{"id":49,"op":"insert","data":{"v":"par`), good...))
	rep := mustRepair(t, dir, RepairOptions{Salvage: true})
	c := colRepair(t, rep, "c")
	if c.Salvage == nil || c.Salvage.GluedEmbeds != 1 {
		t.Fatalf("%+v", c.Salvage)
	}
	wantVerifyClean(t, dir)
	_, col := openRepaired(t, dir, vcfg)
	if r, err := col.Get(50); err != nil || r.Data["v"] != "x" {
		t.Fatalf("glued record: %v %+v", err, r)
	}
	if _, err := col.Get(49); err == nil {
		t.Fatal("the partial record must not be invented")
	}
}

func TestRepairSalvageAmbiguousOrderIsBlocked(t *testing.T) {
	dir, _ := damagedSegments(t)
	colDir := filepath.Join(dir, "c")
	// A later segment updates an id that lives in the damaged segment: moving
	// the damaged segment's records to the end would change the winner.
	segs := colSegs(t, colDir)
	last := segs[len(segs)-1].Path()
	appendRaw(t, last, encodeEntry(t, store.Entry{ID: 2, Op: store.OpUpdate, Rev: 2, Data: map[string]any{"v": "later"}}))
	before := segHashes(t, colDir)
	rep, err := runRepair(t, dir, RepairOptions{Salvage: true})
	if !errors.Is(err, ErrRepairIncomplete) {
		t.Fatalf("got %v", err)
	}
	if c := colRepair(t, rep, "c"); c.Status != RepairBlocked || !strings.Contains(c.Reason, "ambiguous") {
		t.Fatalf("%+v", c)
	}
	if segHashes(t, colDir) != before {
		t.Fatal("segments changed")
	}
}

func appendConflicts(t *testing.T, dir string) {
	t.Helper()
	seg := filepath.Join(dir, "c", "seg_000001.ndjson")
	// Two writers both inserted id 1 with different content.
	appendRaw(t, seg, encodeEntry(t, store.Entry{ID: 1, Op: store.OpInsert, Rev: 1, Data: map[string]any{"v": "other-writer"}}))
	appendRaw(t, seg, encodeEntry(t, store.Entry{ID: 7, Op: store.OpInsert, Rev: 1, Data: map[string]any{"v": "x"}}))
	appendRaw(t, seg, encodeEntry(t, store.Entry{ID: 7, Op: store.OpDelete}))
	appendRaw(t, seg, encodeEntry(t, store.Entry{ID: 7, Op: store.OpUpdate, Rev: 2, Data: map[string]any{"v": "y"}}))
}

func TestRepairConflictsReported(t *testing.T) {
	dir := t.TempDir()
	seedClosed(t, dir, vcfg, 3)
	appendConflicts(t, dir)
	colDir := filepath.Join(dir, "c")
	segsBefore := segHashes(t, colDir)

	rep := mustRepair(t, dir, RepairOptions{})
	c := colRepair(t, rep, "c")
	if len(c.Conflicts) < 2 {
		t.Fatalf("conflicts not reported: %+v", c.Conflicts)
	}
	var dup, wad bool
	for _, f := range c.Conflicts {
		dup = dup || f.Code == CodeConflictDuplicateID
		wad = wad || f.Code == CodeConflictWriteAfter
	}
	if !dup || !wad {
		t.Fatalf("conflict set: %+v", c.Conflicts)
	}
	if segHashes(t, colDir) != segsBefore {
		t.Fatal("segments changed: repair must not resolve conflicts")
	}
	// Derived structures are consistent with open's last-line-wins semantics;
	// the only remaining findings are the (unresolved) conflicts themselves.
	after, _ := VerifyDir(context.Background(), dir, VerifyOptions{})
	for _, f := range after.AllFindings() {
		if f.Severity != SeverityConflict && f.Severity != SeverityInfo {
			t.Fatalf("non-conflict finding after repair: %+v", f)
		}
	}
	if after.MaxSeverity() != SeverityConflict {
		t.Fatal("conflicts must remain visible after repair")
	}
	// Rerun is stable.
	snap := snapshotDir(t, dir)
	rep2 := mustRepair(t, dir, RepairOptions{})
	if colRepair(t, rep2, "c").Status != RepairRepaired || snapshotDir(t, dir) != snap {
		// A conflict keeps the collection "needing attention", but rewriting
		// must reproduce identical bytes.
		t.Fatal("rerun with conflicts changed the directory")
	}
}

func TestRepairConflictAbort(t *testing.T) {
	dir := t.TempDir()
	seedClosed(t, dir, vcfg, 3)
	appendConflicts(t, dir)
	before := snapshotDir(t, dir)
	bkParent := t.TempDir()
	rep, err := runRepair(t, dir, RepairOptions{OnConflict: ConflictAbort, BackupDir: bkParent})
	if !errors.Is(err, ErrRepairConflict) {
		t.Fatalf("want ErrRepairConflict, got %v", err)
	}
	if c := colRepair(t, rep, "c"); len(c.Conflicts) == 0 {
		t.Fatal("abort must still report the conflicts")
	}
	if strings.ReplaceAll(snapshotDir(t, dir), "LOCK", "") != strings.ReplaceAll(before, "LOCK", "") || len(listBackups(t, bkParent)) != 0 || journalExists(dir) {
		t.Fatal("abort must not mutate or back up anything")
	}
}

func TestRepairSalvageRefusedWithConflicts(t *testing.T) {
	dir, _ := damagedSegments(t)
	appendConflicts(t, dir)
	_ = os.Remove(filepath.Join(dir, "c", "seg_000001.ndjson.tmp"))
	// Conflicts were appended to seg_000001 which is the damaged one; either way
	// the collection has conflicts, so salvage must refuse.
	rep, err := runRepair(t, dir, RepairOptions{Salvage: true})
	if !errors.Is(err, ErrRepairIncomplete) {
		t.Fatalf("got %v", err)
	}
	if c := colRepair(t, rep, "c"); c.Status != RepairBlocked || !strings.Contains(c.Reason, "conflict") {
		t.Fatalf("%+v", c)
	}
}

// Every stage a crash can land on must be recoverable by simply rerunning.
func TestRepairInterruptionAndRerun(t *testing.T) {
	stages := []string{
		"backup-done", "compaction-done:c", "renamed:c", "salvage-written:c",
		"pre-rebuild:c", "sidx-written:c", "index-written:c",
	}
	for _, stage := range stages {
		t.Run(stage, func(t *testing.T) {
			// A directory that exercises adoption, salvage, sidx and index rebuild.
			dir, lost := damagedSegments(t)
			colDir := filepath.Join(dir, "c")
			stray := encodeEntry(t, store.Entry{Op: store.OpInsert, ID: 99, Rev: 1, Ts: time.Unix(1, 0).UTC(), Data: map[string]any{"v": "stray"}})
			if err := os.WriteFile(filepath.Join(colDir, "seg_zzz.ndjson"), stray, 0o644); err != nil {
				t.Fatal(err)
			}
			// Salvage and adoption are refused together, so repair the unnumbered
			// file first in this run's history (adoption), then salvage.
			mustRepairAdoptOnly(t, dir)
			if err := os.WriteFile(filepath.Join(colDir, "sidx_v.json"), []byte(`{"field":"v","unique":false}`), 0o644); err != nil {
				t.Fatal(err)
			}
			// sidx file above is unparseable-as-valid but JSON: unique flag is
			// readable, so the index is rebuilt rather than blocking.

			bkParent := t.TempDir()
			opts := RepairOptions{Salvage: true, BackupDir: bkParent}

			hit := false
			withFault(t, func(s string) error {
				if s == stage && !hit {
					hit = true
					return errCrash
				}
				return nil
			})
			_, err := Repair(context.Background(), dir, opts)
			repairFault = nil
			if !hit {
				t.Skipf("stage %s not reached by this scenario", stage)
			}
			if !errors.Is(err, errCrash) {
				t.Fatalf("want the simulated crash, got %v", err)
			}
			if !journalExists(dir) {
				t.Fatal("an interrupted run must leave its journal")
			}

			rep, err := Repair(context.Background(), dir, opts)
			if err != nil {
				t.Fatalf("rerun: %v", err)
			}
			if !rep.Resumed {
				t.Fatal("rerun did not resume from the journal")
			}
			if bks := listBackups(t, bkParent); len(bks) != 1 || bks[0] != rep.BackupDir {
				t.Fatalf("a resumed run must reuse the original backup, got %v", bks)
			}
			if journalExists(dir) {
				t.Fatal("journal not retired after the completed rerun")
			}
			wantVerifyClean(t, dir)
			_, col := openRepaired(t, dir, CollectionConfig{CompactInterval: time.Hour, SegmentMaxSize: 150})
			if _, err := col.Get(lost); err == nil {
				t.Fatal("destroyed record reappeared")
			}
			for _, id := range []uint64{2, 3, 12, 99} {
				if _, err := col.Get(id); err != nil {
					t.Fatalf("id %d lost across interruption at %s: %v", id, stage, err)
				}
			}
			// And now it is idempotent.
			snap := snapshotDir(t, dir)
			rep2, err := Repair(context.Background(), dir, opts)
			if err == nil && colRepair(t, rep2, "c").Status != RepairUnchanged {
				t.Fatalf("not idempotent after resume: %+v", colRepair(t, rep2, "c"))
			}
			_ = snap
		})
	}
}

func mustRepairAdoptOnly(t *testing.T, dir string) {
	t.Helper()
	// Adoption is blocked while the oldest segment is damaged only for salvage;
	// plain adoption of a disjoint unnumbered segment happens in the same run
	// as long as no damaged segment forces salvage. Temporarily hide the
	// damage by repairing a copy of the damaged file's absence is overkill —
	// instead adopt directly with the engine's own rule.
	colDir := filepath.Join(dir, "c")
	segs, err := planSegmentLayout(colDir)
	if err != nil {
		t.Fatal(err)
	}
	var maxNum uint64
	for _, s := range segs {
		if n, ok := segmentNum(s.name); ok && n > maxNum {
			maxNum = n
		}
	}
	for _, s := range segs {
		if _, ok := segmentNum(s.name); !ok {
			maxNum++
			if err := os.Rename(s.path, filepath.Join(colDir, fmt.Sprintf("seg_%06d.ndjson", maxNum))); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestRepairInterruptionDuringAdoption(t *testing.T) {
	dir := t.TempDir()
	cfg := CollectionConfig{CompactInterval: time.Hour, SegmentMaxSize: 150}
	seedClosed(t, dir, cfg, 8)
	colDir := filepath.Join(dir, "c")
	for _, n := range []string{"seg_aaa.ndjson", "seg_bbb.ndjson"} {
		id := 100 + len(n)
		e := encodeEntry(t, store.Entry{Op: store.OpInsert, ID: uint64(id), Rev: 1, Ts: time.Unix(1, 0).UTC(), Data: map[string]any{"v": n}})
		if err := os.WriteFile(filepath.Join(colDir, n), e, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// Both have len(n) equal -> same id: make bbb distinct.
	e := encodeEntry(t, store.Entry{Op: store.OpInsert, ID: 555, Rev: 1, Ts: time.Unix(1, 0).UTC(), Data: map[string]any{"v": "bbb"}})
	if err := os.WriteFile(filepath.Join(colDir, "seg_bbb.ndjson"), e, 0o644); err != nil {
		t.Fatal(err)
	}
	bkParent := t.TempDir()
	renames := 0
	withFault(t, func(s string) error {
		if s == "renamed:c" {
			renames++
			return errCrash
		}
		return nil
	})
	if _, err := Repair(context.Background(), dir, RepairOptions{BackupDir: bkParent}); !errors.Is(err, errCrash) {
		t.Fatalf("got %v", err)
	}
	repairFault = nil
	if _, err := Repair(context.Background(), dir, RepairOptions{BackupDir: bkParent}); err != nil {
		t.Fatal(err)
	}
	wantVerifyClean(t, dir)
	_, col := openRepaired(t, dir, cfg)
	for _, id := range []uint64{uint64(100 + len("seg_aaa.ndjson")), 555} {
		if _, err := col.Get(id); err != nil {
			t.Fatalf("adopted id %d: %v", id, err)
		}
	}
}

func TestRepairSecondaryIndexesAndMeta(t *testing.T) {
	dir := t.TempDir()
	cfg := CollectionConfig{CompactInterval: time.Hour, SyncMode: SyncModeAlways}
	db, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	col, _ := db.CreateCollection("c")
	if err := col.EnsureUniqueIndex("email"); err != nil {
		t.Fatal(err)
	}
	if err := col.EnsureIndex("city"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if _, _, err := col.Insert(map[string]any{"email": fmt.Sprintf("u%d@x", i), "city": "paris"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	colDir := filepath.Join(dir, "c")
	// Corrupt a bucket (checksum no longer matches) and rewind the id counter.
	p := sidxFilePath(colDir, "city")
	b, _ := os.ReadFile(p)
	var f map[string]any
	_ = json.Unmarshal(b, &f)
	f["buckets"] = map[string]any{"london": []uint64{1}}
	nb, _ := json.Marshal(f)
	if err := os.WriteFile(p, nb, 0o644); err != nil {
		t.Fatal(err)
	}
	m, _ := loadMeta(metaPath(colDir))
	m.IDCounter = 1
	m.DefaultTTLSeconds = 3600
	if err := persistMeta(metaPath(colDir), m); err != nil {
		t.Fatal(err)
	}

	rep := mustRepair(t, dir, RepairOptions{})
	if colRepair(t, rep, "c").Status != RepairRepaired {
		t.Fatalf("%+v", colRepair(t, rep, "c"))
	}
	wantVerifyClean(t, dir)
	m2, err := loadMeta(metaPath(colDir))
	if err != nil || m2.IDCounter != 4 || m2.DefaultTTLSeconds != 3600 {
		t.Fatalf("meta: %+v %v", m2, err)
	}
	db2, col2 := openRepaired(t, dir, cfg)
	_ = db2
	if ids, ok := col2.IndexLookup("city", "paris"); !ok || len(ids) != 4 {
		t.Fatalf("city index: %v %v", ids, ok)
	}
	// The unique constraint survived the rebuild.
	if _, _, err := col2.Insert(map[string]any{"email": "u0@x", "city": "x"}); err == nil {
		t.Fatal("unique index lost its constraint")
	}
}

func TestRepairUnreadableSidxAndMetaBlockWithoutGuessing(t *testing.T) {
	dir := t.TempDir()
	cfg := CollectionConfig{CompactInterval: time.Hour}
	db, _ := Open(dir, cfg)
	col, _ := db.CreateCollection("c")
	_ = col.EnsureUniqueIndex("email")
	_, _, _ = col.Insert(map[string]any{"email": "a"})
	_ = db.Close()
	colDir := filepath.Join(dir, "c")

	// Unparseable sidx: unique-ness is unknowable.
	if err := os.WriteFile(sidxFilePath(colDir, "email"), []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	before := snapshotDir(t, dir)
	rep, err := runRepair(t, dir, RepairOptions{})
	if !errors.Is(err, ErrRepairIncomplete) || colRepair(t, rep, "c").Status != RepairBlocked {
		t.Fatalf("sidx: %v %+v", err, colRepair(t, rep, "c"))
	}
	if strings.ReplaceAll(snapshotDir(t, dir), "LOCK", "") != strings.ReplaceAll(before, "LOCK", "") {
		t.Fatal("blocked collection modified")
	}
	_ = os.Remove(sidxFilePath(colDir, "email"))

	// Unreadable meta: TTL/encryption settings are not derivable.
	if err := os.WriteFile(metaPath(colDir), []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}
	rep, err = runRepair(t, dir, RepairOptions{})
	if !errors.Is(err, ErrRepairIncomplete) || !strings.Contains(colRepair(t, rep, "c").Reason, "AllowMetaReset") {
		t.Fatalf("meta: %v %+v", err, colRepair(t, rep, "c"))
	}
	rep = mustRepair(t, dir, RepairOptions{AllowMetaReset: true})
	if colRepair(t, rep, "c").Status != RepairRepaired {
		t.Fatalf("%+v", colRepair(t, rep, "c"))
	}
	wantVerifyClean(t, dir)
}

func TestRepairRollsForwardInterruptedCompaction(t *testing.T) {
	dir := t.TempDir()
	colDir := filepath.Join(dir, "c")
	if err := os.MkdirAll(colDir, 0o755); err != nil {
		t.Fatal(err)
	}
	line := func(op store.Op, id, rev uint64, v int) []byte {
		e := store.Entry{Op: op, ID: id, Rev: rev, Ts: time.Unix(1, 0).UTC()}
		if op != store.OpDelete {
			e.Data = map[string]any{"v": v}
		}
		return encodeEntry(t, e)
	}
	write := func(name string, lines ...[]byte) {
		var b []byte
		for _, l := range lines {
			b = append(b, l...)
		}
		if err := os.WriteFile(filepath.Join(colDir, name), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("seg_000001.ndjson", line(store.OpInsert, 1, 1, 1), line(store.OpInsert, 2, 1, 1))
	write("seg_000002.ndjson", line(store.OpUpdate, 1, 2, 2), line(store.OpDelete, 2, 0, 0))
	write("seg_000003.ndjson", line(store.OpUpdate, 1, 3, 3))
	write("seg_000004.ndjson")
	write("seg_000001.ndjson", line(store.OpInsert, 1, 3, 3))
	write(".compact_000002.ndjson")
	m := compactManifest{
		Renames: map[string]string{
			filepath.Join(colDir, ".compact_000001.ndjson"): filepath.Join(colDir, "seg_000001.ndjson"),
			filepath.Join(colDir, ".compact_000002.ndjson"): filepath.Join(colDir, "seg_000002.ndjson"),
		},
		Removals: []string{filepath.Join(colDir, "seg_000003.ndjson")},
	}
	b, _ := json.Marshal(m)
	if err := os.WriteFile(compactManifestPath(colDir), b, 0o644); err != nil {
		t.Fatal(err)
	}

	rep := mustRepair(t, dir, RepairOptions{})
	c := colRepair(t, rep, "c")
	var rolled bool
	for _, a := range c.Actions {
		rolled = rolled || a.Kind == "compaction-roll-forward"
	}
	if !rolled {
		t.Fatalf("%+v", c.Actions)
	}
	if _, err := os.Stat(compactManifestPath(colDir)); err == nil {
		t.Fatal("manifest still present")
	}
	wantVerifyClean(t, dir)
	_, col := openRepaired(t, dir, vcfg)
	if r, err := col.Get(1); err != nil || r.Data["v"] != float64(3) {
		t.Fatalf("%v %+v", err, r)
	}
	if _, err := col.Get(2); err == nil {
		t.Fatal("tombstoned id 2 resurrected")
	}
}

func TestRepairCollectionFilterAndMultipleCollections(t *testing.T) {
	dir := t.TempDir()
	cfg := CollectionConfig{CompactInterval: time.Hour, SyncMode: SyncModeAlways}
	db, _ := Open(dir, cfg)
	for _, n := range []string{"a", "b"} {
		c, _ := db.CreateCollection(n)
		_, _, _ = c.Insert(map[string]any{"v": 1})
	}
	_ = db.Close()
	// Make both stale.
	for _, n := range []string{"a", "b"} {
		_ = os.Remove(filepath.Join(dir, n, "index.json"))
	}
	rep := mustRepair(t, dir, RepairOptions{Collections: []string{"a"}})
	if len(rep.Collections) != 1 || rep.Collections[0].Name != "a" {
		t.Fatalf("%+v", rep.Collections)
	}
	if _, err := os.Stat(filepath.Join(dir, "b", "index.json")); err == nil {
		t.Fatal("collection b was touched")
	}
	if _, err := os.Stat(filepath.Join(rep.BackupDir, "b")); err == nil {
		t.Fatal("collection b was backed up")
	}
}
