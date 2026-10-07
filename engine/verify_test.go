package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/srjn45/scriva/store"
)

var vcfg = CollectionConfig{CompactInterval: time.Hour}

func verifyCol(t *testing.T, dir string, mode VerifyMode) CollectionReport {
	t.Helper()
	rep, err := VerifyDir(context.Background(), dir, VerifyOptions{Mode: mode})
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Collections) != 1 {
		t.Fatalf("want 1 collection, got %d", len(rep.Collections))
	}
	return rep.Collections[0]
}

func hasCode(cr CollectionReport, code FindingCode) bool {
	for _, f := range cr.Findings {
		if f.Code == code {
			return true
		}
	}
	return false
}

func codesOf(cr CollectionReport) []FindingCode {
	var out []FindingCode
	for _, f := range cr.Findings {
		out = append(out, f.Code)
	}
	return out
}

func wantCodes(t *testing.T, cr CollectionReport, codes ...FindingCode) {
	t.Helper()
	for _, c := range codes {
		if !hasCode(cr, c) {
			t.Fatalf("missing finding %q; got %v", c, codesOf(cr))
		}
	}
}

func wantClean(t *testing.T, cr CollectionReport) {
	t.Helper()
	for _, f := range cr.Findings {
		if f.Severity.rank() > SeverityInfo.rank() {
			t.Fatalf("expected clean, got %+v", f)
		}
	}
}

func appendRaw(t *testing.T, path string, b []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write(b); err != nil {
		t.Fatal(err)
	}
}

func encodeEntry(t *testing.T, e store.Entry) []byte {
	t.Helper()
	if e.Ts.IsZero() {
		e.Ts = time.Now().UTC()
	}
	b, err := store.Encode(e)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func colSegs(t *testing.T, colDir string) []*Segment {
	t.Helper()
	paths, _ := filepath.Glob(filepath.Join(colDir, "seg_*.ndjson"))
	var segs []*Segment
	for _, p := range paths {
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		segs = append(segs, openSealedSegment(p, fi.Size()))
	}
	return sortSegments(segs)
}

func TestVerifyCleanOfflineAndOnline(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir, vcfg)
	if err != nil {
		t.Fatal(err)
	}
	col, _ := db.CreateCollection("c")
	if err := col.EnsureUniqueIndex("email"); err != nil {
		t.Fatal(err)
	}
	var ids []uint64
	for i := 0; i < 20; i++ {
		id, _, err := col.Insert(map[string]any{"email": fmt.Sprintf("u%d@x", i), "n": i})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if _, err := col.Update(ids[1], map[string]any{"email": "new@x", "n": 99}); err != nil {
		t.Fatal(err)
	}
	if err := col.Delete(ids[2]); err != nil {
		t.Fatal(err)
	}

	for _, mode := range []VerifyMode{VerifyQuick, VerifyFull} {
		rep, err := db.Verify(context.Background(), VerifyOptions{Mode: mode})
		if err != nil {
			t.Fatal(err)
		}
		if !rep.Clean() || !rep.Online {
			t.Fatalf("%s online not clean: %+v", mode, rep.AllFindings())
		}
		if !rep.Has(CodeLockHeldByThisProc) {
			t.Fatalf("lock state not reported: %+v", rep.Findings)
		}
		crep, err := col.Verify(context.Background(), VerifyOptions{Mode: mode})
		if err != nil || !crep.Clean() {
			t.Fatalf("collection verify: %v %+v", err, crep)
		}
	}
	rep, _ := col.Verify(context.Background(), VerifyOptions{Mode: VerifyFull})
	if st := rep.Collections[0].Stats; st.LiveRecords != 19 || st.SecondaryIndexes != 1 || st.IndexEntries != 19 {
		t.Fatalf("stats %+v", st)
	}

	// Offline while this process holds the lock: reported, not fatal.
	off, err := VerifyDir(context.Background(), dir, VerifyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if !off.Has(CodeLockHeld) {
		t.Fatalf("held lock not reported: %+v", off.Findings)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []VerifyMode{VerifyQuick, VerifyFull} {
		cr := verifyCol(t, dir, mode)
		wantClean(t, cr)
		if len(cr.Findings) != 0 {
			t.Fatalf("%s: unexpected findings %v", mode, codesOf(cr))
		}
	}
	off, _ = VerifyDir(context.Background(), dir, VerifyOptions{})
	if off.Has(CodeLockHeld) || !off.Clean() || off.MaxSeverity() != "" {
		t.Fatalf("closed dir: %+v", off.AllFindings())
	}
}

func TestVerifyStaleIndexAfterCrash(t *testing.T) {
	dir := t.TempDir()
	cfg := CollectionConfig{CompactInterval: time.Hour, SyncMode: SyncModeAlways}
	ids := seedClosed(t, dir, cfg, 4)
	a, d := ids[0], ids[3]

	db, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	col, _ := db.Collection("c")
	b, _, err := col.Insert(map[string]any{"v": "late"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := col.Update(a, map[string]any{"v": "updated"}); err != nil {
		t.Fatal(err)
	}
	if err := col.Delete(d); err != nil {
		t.Fatal(err)
	}
	img := copyDataDir(t, dir) // crash image: no Close, no persist

	q := verifyCol(t, img, VerifyQuick)
	wantCodes(t, q, CodeIndexStaleTail)
	if q.Findings[0].Severity != SeverityRepairableIndex {
		t.Fatalf("severity %v", q.Findings[0].Severity)
	}

	f := verifyCol(t, img, VerifyFull)
	wantCodes(t, f, CodeIndexStaleTail, CodeIndexMissingRecord, CodeIndexStaleRecord, CodeIndexResurrected)
	got := map[FindingCode]uint64{}
	for _, fd := range f.Findings {
		got[fd.Code] = fd.Location.ID
	}
	if got[CodeIndexMissingRecord] != b || got[CodeIndexStaleRecord] != a || got[CodeIndexResurrected] != d {
		t.Fatalf("wrong ids: %v (b=%d a=%d d=%d)", got, b, a, d)
	}
}

func TestVerifyOrphanSegmentUnlisted(t *testing.T) {
	dir := t.TempDir()
	cfg := CollectionConfig{CompactInterval: time.Hour, SegmentMaxSize: 150}
	seedClosed(t, dir, cfg, 12)
	colDir := filepath.Join(dir, "c")
	segs := colSegs(t, colDir)
	if len(segs) < 3 {
		t.Fatalf("need >= 3 segments, got %d", len(segs))
	}
	// Persist an index that omits the oldest segment, as after a rotation race.
	idx := newIndex()
	if err := idx.Rebuild(segs[1:]); err != nil {
		t.Fatal(err)
	}
	snap, err := idx.Snapshot(segs[1:])
	if err != nil {
		t.Fatal(err)
	}
	if err := snap.Persist(filepath.Join(colDir, "index.json")); err != nil {
		t.Fatal(err)
	}
	q := verifyCol(t, dir, VerifyQuick)
	wantCodes(t, q, CodeIndexSegmentUnlisted)
	f := verifyCol(t, dir, VerifyFull)
	wantCodes(t, f, CodeIndexSegmentUnlisted, CodeIndexMissingRecord)
}

func TestVerifyWrongAndDanglingOffsets(t *testing.T) {
	dir := t.TempDir()
	ids := seedClosed(t, dir, vcfg, 6)
	colDir := filepath.Join(dir, "c")
	segs := colSegs(t, colDir)
	ip := filepath.Join(colDir, "index.json")
	idx := newIndex()
	if err := idx.Load(ip); err != nil {
		t.Fatal(err)
	}
	// ids[0] -> ids[1]'s record (valid boundary, wrong identity);
	// ids[2] -> mid-line offset.
	idx.entries[ids[0]] = idx.entries[ids[1]]
	mid := idx.entries[ids[2]]
	mid.Offset += 3
	idx.entries[ids[2]] = mid
	snap, _ := idx.Snapshot(segs)
	if err := snap.Persist(ip); err != nil {
		t.Fatal(err)
	}

	f := verifyCol(t, dir, VerifyFull)
	wantCodes(t, f, CodeIndexWrongRecord, CodeIndexDanglingOffset)
	q := verifyCol(t, dir, VerifyQuick)
	wantCodes(t, q, CodeIndexSpotCheckFailed)
	n := 0
	for _, fd := range q.Findings {
		if fd.Code == CodeIndexSpotCheckFailed {
			n++
		}
	}
	if n != 2 {
		t.Fatalf("want 2 spot-check failures, got %d", n)
	}
}

func TestVerifyGluedLineAndTornTail(t *testing.T) {
	dir := t.TempDir()
	seedClosed(t, dir, vcfg, 3)
	seg := filepath.Join(dir, "c", "seg_000001.ndjson")
	good := encodeEntry(t, store.Entry{ID: 50, Op: store.OpInsert, Rev: 1, Data: map[string]any{"v": "x"}})
	appendRaw(t, seg, append([]byte(`{"id":49,"op":"insert","data":{"v":"par`), good...))
	cr := verifyCol(t, dir, VerifyFull)
	wantCodes(t, cr, CodeSegmentGluedLine)
	for _, f := range cr.Findings {
		if f.Code == CodeSegmentGluedLine && f.Severity != SeverityDataCorruption {
			t.Fatalf("severity %v", f.Severity)
		}
	}

	// A torn final line on the newest segment is informational.
	dir2 := t.TempDir()
	seedClosed(t, dir2, vcfg, 3)
	appendRaw(t, filepath.Join(dir2, "c", "seg_000001.ndjson"), []byte(`{"id":9,"op":"ins`))
	for _, mode := range []VerifyMode{VerifyQuick, VerifyFull} {
		cr := verifyCol(t, dir2, mode)
		wantCodes(t, cr, CodeSegmentTornTail)
		for _, f := range cr.Findings {
			if f.Code == CodeSegmentTornTail && f.Severity != SeverityInfo {
				t.Fatalf("torn tail severity %v", f.Severity)
			}
		}
	}
}

func TestVerifyHistoryConflicts(t *testing.T) {
	dir := t.TempDir()
	seedClosed(t, dir, vcfg, 2)
	seg := filepath.Join(dir, "c", "seg_000001.ndjson")
	// Two writers both inserted id 1 with different content.
	appendRaw(t, seg, encodeEntry(t, store.Entry{ID: 1, Op: store.OpInsert, Rev: 1, Data: map[string]any{"v": "other-writer"}}))
	// Revision regression on id 2: rev 2 then rev 2 with other data, then rev 1.
	appendRaw(t, seg, encodeEntry(t, store.Entry{ID: 2, Op: store.OpUpdate, Rev: 2, Data: map[string]any{"v": "a"}}))
	appendRaw(t, seg, encodeEntry(t, store.Entry{ID: 2, Op: store.OpUpdate, Rev: 2, Data: map[string]any{"v": "b"}}))
	appendRaw(t, seg, encodeEntry(t, store.Entry{ID: 2, Op: store.OpUpdate, Rev: 1, Data: map[string]any{"v": "c"}}))
	// Reuse and write-after-delete.
	appendRaw(t, seg, encodeEntry(t, store.Entry{ID: 7, Op: store.OpInsert, Rev: 1, Data: map[string]any{"v": "x"}}))
	appendRaw(t, seg, encodeEntry(t, store.Entry{ID: 7, Op: store.OpDelete}))
	appendRaw(t, seg, encodeEntry(t, store.Entry{ID: 7, Op: store.OpUpdate, Rev: 2, Data: map[string]any{"v": "y"}}))
	appendRaw(t, seg, encodeEntry(t, store.Entry{ID: 8, Op: store.OpInsert, Rev: 1, Data: map[string]any{"v": "x"}}))
	appendRaw(t, seg, encodeEntry(t, store.Entry{ID: 8, Op: store.OpDelete}))
	appendRaw(t, seg, encodeEntry(t, store.Entry{ID: 8, Op: store.OpInsert, Rev: 1, Data: map[string]any{"v": "z"}}))
	// An identical re-delivery is benign.
	appendRaw(t, seg, encodeEntry(t, store.Entry{ID: 9, Op: store.OpInsert, Rev: 1, Data: map[string]any{"v": "same"}}))
	appendRaw(t, seg, encodeEntry(t, store.Entry{ID: 9, Op: store.OpInsert, Rev: 1, Data: map[string]any{"v": "same"}}))

	cr := verifyCol(t, dir, VerifyFull)
	wantCodes(t, cr, CodeConflictDuplicateID, CodeConflictRevision, CodeConflictWriteAfter, CodeConflictIDReuse, CodeDuplicateIdentical)
	for _, f := range cr.Findings {
		switch f.Code {
		case CodeConflictDuplicateID, CodeConflictRevision, CodeConflictWriteAfter, CodeConflictIDReuse:
			if f.Severity != SeverityConflict {
				t.Fatalf("%s severity %v", f.Code, f.Severity)
			}
		case CodeDuplicateIdentical:
			if f.Severity != SeverityInfo || f.Location.ID != 9 {
				t.Fatalf("identical dup: %+v", f)
			}
		}
	}
	rev := 0
	for _, f := range cr.Findings {
		if f.Code == CodeConflictRevision {
			rev++
		}
	}
	if rev != 2 {
		t.Fatalf("want 2 revision conflicts, got %d", rev)
	}
	rep, _ := VerifyDir(context.Background(), dir, VerifyOptions{})
	if rep.MaxSeverity() != SeverityConflict || rep.Clean() {
		t.Fatalf("max severity %v", rep.MaxSeverity())
	}
}

func TestVerifySecondaryIndexFindings(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, vcfg)
	col, _ := db.CreateCollection("c")
	var ids []uint64
	for i := 0; i < 5; i++ {
		id, _, _ := col.Insert(map[string]any{"color": fmt.Sprintf("c%d", i)})
		ids = append(ids, id)
	}
	if err := col.EnsureIndex("color"); err != nil {
		t.Fatal(err)
	}
	if err := col.Delete(ids[4]); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	colDir := filepath.Join(dir, "c")
	p := sidxFilePath(colDir, "color")
	s := newSecondaryIndex("color", false)
	if err := s.Load(p); err != nil {
		t.Fatal(err)
	}
	s.remove(ids[0])              // missing entry
	s.add("ghost", 777)           // extra entry (no such record)
	s.update(ids[1], "elsewhere") // wrong bucket
	s.add("c4", ids[4])           // resurrected delete
	if err := persistSecondary(s, p, colSegs(t, colDir)); err != nil {
		t.Fatal(err)
	}
	cr := verifyCol(t, dir, VerifyFull)
	wantCodes(t, cr, CodeSidxMissingEntry, CodeSidxExtraEntry, CodeSidxWrongBucket)
	extra := 0
	for _, f := range cr.Findings {
		if f.Code == CodeSidxExtraEntry {
			extra++
		}
	}
	if extra != 2 {
		t.Fatalf("want 2 extra entries, got %d", extra)
	}
	// Quick mode only sees what the index itself shows: nothing wrong here.
	wantClean(t, verifyCol(t, dir, VerifyQuick))

	// Corrupt sidx file.
	if err := os.WriteFile(p, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	wantCodes(t, verifyCol(t, dir, VerifyQuick), CodeSidxUnreadable)
}

func TestVerifyUniqueViolation(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, vcfg)
	col, _ := db.CreateCollection("c")
	if err := col.EnsureUniqueIndex("email"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := col.Insert(map[string]any{"email": "a@x"}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// A second writer put a different record with the same unique value.
	appendRaw(t, filepath.Join(dir, "c", "seg_000001.ndjson"),
		encodeEntry(t, store.Entry{ID: 2, Op: store.OpInsert, Rev: 1, Data: map[string]any{"email": "a@x"}}))
	cr := verifyCol(t, dir, VerifyFull)
	wantCodes(t, cr, CodeSidxUniqueViolation)
	for _, f := range cr.Findings {
		if f.Code == CodeSidxUniqueViolation && (f.Severity != SeverityConflict || f.Location.Field != "email") {
			t.Fatalf("%+v", f)
		}
	}

	// Quick mode sees a violation that is already inside the persisted index.
	colDir := filepath.Join(dir, "c")
	p := sidxFilePath(colDir, "email")
	s := newSecondaryIndex("email", true)
	if err := s.Load(p); err != nil {
		t.Fatal(err)
	}
	s.add("a@x", 2)
	if err := persistSecondary(s, p, colSegs(t, colDir)); err != nil {
		t.Fatal(err)
	}
	wantCodes(t, verifyCol(t, dir, VerifyQuick), CodeSidxUniqueViolation)
}

func TestVerifyFileSetFindings(t *testing.T) {
	dir := t.TempDir()
	seedClosed(t, dir, vcfg, 3)
	colDir := filepath.Join(dir, "c")
	for name, content := range map[string]string{
		".compact_000001.ndjson": "x",
		"seg_000009.ndjson.bak":  "x",
		"seg_abc.ndjson":         "x",
		"index.json.tmp":         "x",
	} {
		if err := os.WriteFile(filepath.Join(colDir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	cr := verifyCol(t, dir, VerifyQuick)
	wantCodes(t, cr, CodeLeftoverTempFile, CodeOrphanSegmentFile)
	wantClean(t, cr)
	var temps, orphans int
	for _, f := range cr.Findings {
		switch f.Code {
		case CodeLeftoverTempFile:
			temps++
		case CodeOrphanSegmentFile:
			orphans++
		}
	}
	if temps != 2 || orphans != 2 {
		t.Fatalf("temps=%d orphans=%d: %v", temps, orphans, codesOf(cr))
	}

	// A pending manifest is repairable; the temps it names are not "leftover".
	m := compactManifest{Renames: map[string]string{filepath.Join(colDir, ".compact_000001.ndjson"): filepath.Join(colDir, "seg_000001.ndjson")}}
	b, _ := json.Marshal(m)
	if err := os.WriteFile(compactManifestPath(colDir), b, 0o644); err != nil {
		t.Fatal(err)
	}
	cr = verifyCol(t, dir, VerifyQuick)
	wantCodes(t, cr, CodeManifestPending)
	temps = 0
	for _, f := range cr.Findings {
		if f.Code == CodeLeftoverTempFile && strings.HasPrefix(f.Location.Segment, ".compact_") {
			temps++
		}
	}
	if temps != 0 {
		t.Fatalf("manifest-named temp reported as leftover")
	}
	if err := os.WriteFile(compactManifestPath(colDir), []byte("{torn"), 0o644); err != nil {
		t.Fatal(err)
	}
	cr = verifyCol(t, dir, VerifyQuick)
	wantCodes(t, cr, CodeManifestCorrupt)
}

func TestVerifyIndexFileProblems(t *testing.T) {
	dir := t.TempDir()
	seedClosed(t, dir, vcfg, 3)
	ip := filepath.Join(dir, "c", "index.json")

	// v1 index: coverage unknown.
	idx := newIndex()
	if err := idx.Load(ip); err != nil {
		t.Fatal(err)
	}
	wantClean(t, verifyCol(t, dir, VerifyQuick))
	v1 := indexFile{Entries: idx.entries}
	pb, _ := json.Marshal(v1.Entries)
	v1.Checksum = sha256Hex(pb)
	b, _ := json.Marshal(v1)
	if err := os.WriteFile(ip, b, 0o644); err != nil {
		t.Fatal(err)
	}
	wantCodes(t, verifyCol(t, dir, VerifyQuick), CodeIndexCoverageUnknown)

	// Truncated segment vs coverage.
	dir2 := t.TempDir()
	seedClosed(t, dir2, vcfg, 3)
	seg := filepath.Join(dir2, "c", "seg_000001.ndjson")
	fb, _ := os.ReadFile(seg)
	if err := os.WriteFile(seg, fb[:len(fb)/2], 0o644); err != nil {
		t.Fatal(err)
	}
	wantCodes(t, verifyCol(t, dir2, VerifyQuick), CodeIndexCoverageMismatch)

	// Missing and garbage index.
	if err := os.WriteFile(filepath.Join(dir2, "c", "index.json"), []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}
	wantCodes(t, verifyCol(t, dir2, VerifyQuick), CodeIndexUnreadable)
	_ = os.Remove(filepath.Join(dir2, "c", "index.json"))
	wantCodes(t, verifyCol(t, dir2, VerifyQuick), CodeIndexMissing)

	// Covered segment gone.
	dir3 := t.TempDir()
	seedClosed(t, dir3, CollectionConfig{CompactInterval: time.Hour, SegmentMaxSize: 150}, 12)
	segs := colSegs(t, filepath.Join(dir3, "c"))
	if err := os.Remove(segs[0].Path()); err != nil {
		t.Fatal(err)
	}
	wantCodes(t, verifyCol(t, dir3, VerifyQuick), CodeIndexCoverageSegmentMissing)
}

func TestVerifyIDCounter(t *testing.T) {
	dir := t.TempDir()
	cfg := CollectionConfig{CompactInterval: time.Hour, SegmentMaxSize: 150}
	seedClosed(t, dir, cfg, 12)
	colDir := filepath.Join(dir, "c")
	wantClean(t, verifyCol(t, dir, VerifyFull))
	m, err := loadMeta(metaPath(colDir))
	if err != nil {
		t.Fatal(err)
	}
	m.IDCounter = 1
	if err := persistMeta(metaPath(colDir), m); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []VerifyMode{VerifyQuick, VerifyFull} {
		wantCodes(t, verifyCol(t, dir, mode), CodeIDCounterBehind)
	}
	_ = os.Remove(metaPath(colDir))
	cr := verifyCol(t, dir, VerifyFull)
	wantCodes(t, cr, CodeMetaMissing)
	wantClean(t, cr)
	if err := os.WriteFile(metaPath(colDir), []byte("nope"), 0o644); err != nil {
		t.Fatal(err)
	}
	wantCodes(t, verifyCol(t, dir, VerifyQuick), CodeMetaUnreadable)
}

func TestVerifyExpiredLive(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, vcfg)
	col, _ := db.CreateCollection("c")
	if _, _, err := col.InsertWithExpiry(map[string]any{"v": 1}, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	rep, err := VerifyDir(context.Background(), dir, VerifyOptions{Now: time.Now().Add(2 * time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if !rep.Has(CodeRecordExpiredLive) || !rep.Clean() {
		t.Fatalf("%+v", rep.AllFindings())
	}
}

func TestVerifyDoesNotModifyDirectory(t *testing.T) {
	dir := t.TempDir()
	seedClosed(t, dir, vcfg, 4)
	colDir := filepath.Join(dir, "c")
	appendRaw(t, filepath.Join(colDir, "seg_000001.ndjson"), []byte(`{"id":9,"op":"ins`))
	_ = os.WriteFile(filepath.Join(colDir, ".compact_x"), []byte("x"), 0o644)
	before := snapshotDir(t, dir)
	for _, mode := range []VerifyMode{VerifyQuick, VerifyFull} {
		verifyCol(t, dir, mode)
	}
	if after := snapshotDir(t, dir); after != before {
		t.Fatalf("verify modified the directory:\nbefore %s\nafter  %s", before, after)
	}
}

func snapshotDir(t *testing.T, dir string) string {
	t.Helper()
	var sb strings.Builder
	_ = filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		b, _ := os.ReadFile(p)
		fmt.Fprintf(&sb, "%s:%d:%s;", p, len(b), sha256Hex(b))
		return nil
	})
	return sb.String()
}

func TestVerifyFindingCapAndFilter(t *testing.T) {
	dir := t.TempDir()
	seedClosed(t, dir, vcfg, 10)
	// Drop the index entries: every record becomes a missing-record finding.
	colDir := filepath.Join(dir, "c")
	idx := newIndex()
	snap, _ := idx.Snapshot(nil)
	_ = snap.Persist(filepath.Join(colDir, "index.json"))
	rep, err := VerifyDir(context.Background(), dir, VerifyOptions{MaxFindingsPerCode: 3})
	if err != nil {
		t.Fatal(err)
	}
	cr := rep.Collections[0]
	n := 0
	for _, f := range cr.Findings {
		if f.Code == CodeIndexMissingRecord {
			n++
		}
	}
	if n != 3 || cr.Truncated[CodeIndexMissingRecord] == 0 {
		t.Fatalf("cap not applied: n=%d truncated=%v", n, cr.Truncated)
	}
	if rep2, _ := VerifyDir(context.Background(), dir, VerifyOptions{Collections: []string{"nope"}}); len(rep2.Collections) != 0 {
		t.Fatalf("filter ignored")
	}
}

func TestVerifyOnlineSeesIndexDivergence(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, vcfg)
	defer db.Close()
	col, _ := db.CreateCollection("c")
	var ids []uint64
	for i := 0; i < 5; i++ {
		id, _, _ := col.Insert(map[string]any{"v": i})
		ids = append(ids, id)
	}
	// Corrupt the live in-memory index: drop one, add a phantom.
	col.index.Delete(ids[0])
	col.index.Set(999, IndexEntry{SegmentPath: col.active.Path(), Offset: 0, Rev: 1})
	rep, err := col.Verify(context.Background(), VerifyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	cr := rep.Collections[0]
	wantCodes(t, cr, CodeIndexMissingRecord, CodeIndexWrongRecord)
	qrep, _ := col.Verify(context.Background(), VerifyOptions{Mode: VerifyQuick})
	wantCodes(t, qrep.Collections[0], CodeIndexSpotCheckFailed)
}

func TestVerifyOnlineConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, CollectionConfig{CompactInterval: time.Hour, SegmentMaxSize: 4096})
	defer db.Close()
	col, _ := db.CreateCollection("c")
	if err := col.EnsureIndex("g"); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			id, _, err := col.Insert(map[string]any{"g": i % 7, "pad": strings.Repeat("p", 64)})
			if err != nil {
				return
			}
			if i%3 == 0 {
				_, _ = col.Update(id, map[string]any{"g": (i + 1) % 7})
			}
			if i%5 == 0 {
				_ = col.Delete(id)
			}
		}
	}()
	for i := 0; i < 15; i++ {
		rep, err := db.Verify(context.Background(), VerifyOptions{Mode: VerifyFull})
		if err != nil {
			t.Fatal(err)
		}
		if !rep.Clean() {
			close(stop)
			wg.Wait()
			t.Fatalf("online verify under writes found %+v", rep.AllFindings())
		}
		if i%4 == 0 {
			_ = col.CompactNow()
		}
	}
	close(stop)
	wg.Wait()
}

func TestVerifyContextCancelled(t *testing.T) {
	dir := t.TempDir()
	seedClosed(t, dir, vcfg, 3)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := VerifyDir(ctx, dir, VerifyOptions{}); err == nil {
		t.Fatal("want context error")
	}
}

func TestVerifyClosedCollection(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(dir, vcfg)
	col, _ := db.CreateCollection("c")
	_ = db.Close()
	if _, err := col.Verify(context.Background(), VerifyOptions{}); err == nil {
		t.Fatal("want error on closed collection")
	}
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
