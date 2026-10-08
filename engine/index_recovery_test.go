package engine

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// copyDataDir snapshots dir (a crash image: whatever is on disk right now,
// no Close) into a fresh temp dir and returns it.
func copyDataDir(t *testing.T, dir string) string {
	t.Helper()
	dst := filepath.Join(t.TempDir(), "img")
	err := filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(dir, p)
		if info.IsDir() {
			return os.MkdirAll(filepath.Join(dst, rel), 0o755)
		}
		if info.Name() == "LOCK" || info.Name() == ".lock" {
			return nil
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(dst, rel), b, 0o644)
	})
	if err != nil {
		t.Fatal(err)
	}
	return dst
}

type recoveryEvent struct {
	kind  string
	bytes int64
}

type recoveryRecorder struct {
	mu     sync.Mutex
	events []recoveryEvent
}

func (r *recoveryRecorder) hook(_, kind string, bytes int64, _ time.Duration) {
	r.mu.Lock()
	r.events = append(r.events, recoveryEvent{kind, bytes})
	r.mu.Unlock()
}

func (r *recoveryRecorder) kinds() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for _, e := range r.events {
		out = append(out, e.kind)
	}
	return out
}

// seedClosed creates collection "c" with n records, closes cleanly (persisting
// a v2 index with coverage), and returns the ids.
func seedClosed(t *testing.T, dir string, cfg CollectionConfig, n int) []uint64 {
	t.Helper()
	db, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	col, err := db.CreateCollection("c")
	if err != nil {
		t.Fatal(err)
	}
	var ids []uint64
	for i := 0; i < n; i++ {
		id, _, err := col.Insert(map[string]any{"v": fmt.Sprintf("v%d", i), "pad": "xxxxxxxxxxxxxxxxxxxx"})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return ids
}

func openRecovered(t *testing.T, dir string, cfg CollectionConfig) (*DB, *Collection, *recoveryRecorder) {
	t.Helper()
	rec := &recoveryRecorder{}
	cfg.OnIndexRecovery = rec.hook
	db, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	col, err := db.Collection("c")
	if err != nil {
		t.Fatal(err)
	}
	return db, col, rec
}

func dirSegSize(t *testing.T, dir string) int64 {
	t.Helper()
	paths, _ := filepath.Glob(filepath.Join(dir, "c", "seg_*.ndjson"))
	var n int64
	for _, p := range paths {
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatal(err)
		}
		n += fi.Size()
	}
	return n
}

func TestRecoveryCleanReopenNoWork(t *testing.T) {
	dir := t.TempDir()
	cfg := CollectionConfig{CompactInterval: time.Hour}
	seedClosed(t, dir, cfg, 10)
	db, col, rec := openRecovered(t, dir, cfg)
	defer db.Close()
	if s := col.IndexRecoveryStats(); s != (IndexRecoveryStats{}) {
		t.Fatalf("clean reopen did recovery work: %+v", s)
	}
	if k := rec.kinds(); len(k) != 0 {
		t.Fatalf("hook fired on clean reopen: %v", k)
	}
}

// Crash after insert/update/delete on a previously closed dir: tail replay only,
// the deleted record stays deleted and the replayed bytes are just the tail.
func TestRecoveryCrashTailReplay(t *testing.T) {
	dir := t.TempDir()
	cfg := CollectionConfig{CompactInterval: time.Hour, SyncMode: SyncModeAlways}
	ids := seedClosed(t, dir, cfg, 20)
	before := dirSegSize(t, dir)

	db, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	col, _ := db.Collection("c")
	nw, _, err := col.Insert(map[string]any{"v": "new"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := col.Update(ids[0], map[string]any{"v": "upd"}); err != nil {
		t.Fatal(err)
	}
	if err := col.Delete(ids[1]); err != nil {
		t.Fatal(err)
	}
	tail := dirSegSize(t, dir) - before
	img := copyDataDir(t, dir) // crash image: no Close
	_ = db.Close()

	db2, col2, rec := openRecovered(t, img, cfg)
	defer db2.Close()
	s := col2.IndexRecoveryStats()
	if s.Replays != 1 || s.FullRebuilds != 0 || s.SpotCheckFailures != 0 || s.ReplayedBytes != tail {
		t.Fatalf("stats %+v, want 1 replay of %d bytes and no rebuild", s, tail)
	}
	if k := rec.kinds(); len(k) != 1 || k[0] != IndexRecoveryReplay || rec.events[0].bytes != tail {
		t.Fatalf("hook events %+v", rec.events)
	}
	if _, err := col2.Get(ids[1]); err == nil {
		t.Fatal("deleted record resurrected")
	}
	r, err := col2.Get(ids[0])
	if err != nil || r.Data["v"] != "upd" || r.Rev != 2 {
		t.Fatalf("updated record: %+v %v", r, err)
	}
	if r, err := col2.Get(nw); err != nil || r.Data["v"] != "new" {
		t.Fatalf("new record: %+v %v", r, err)
	}
	if got := col2.Stats().RecordCount; got != 20 {
		t.Fatalf("count %d, want 20", got)
	}
	// Recovered index was persisted: a second open is a no-op.
	_ = db2.Close()
	db3, col3, _ := openRecovered(t, img, cfg)
	defer db3.Close()
	if s := col3.IndexRecoveryStats(); s != (IndexRecoveryStats{}) {
		t.Fatalf("reopen after recovery still did work: %+v", s)
	}
}

// Crash right after rotation: the new, unlisted segment is replayed in full.
func TestRecoveryCrashAfterRotation(t *testing.T) {
	dir := t.TempDir()
	cfg := CollectionConfig{CompactInterval: time.Hour, SyncMode: SyncModeAlways, SegmentMaxSize: 400}
	seedClosed(t, dir, cfg, 4)

	db, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	col, _ := db.Collection("c")
	want := map[uint64]map[string]any{}
	for i := 0; i < 20; i++ {
		id, _, err := col.Insert(map[string]any{"v": i, "pad": "xxxxxxxxxxxxxxxxxxxx"})
		if err != nil {
			t.Fatal(err)
		}
		want[id] = map[string]any{"v": i, "pad": "xxxxxxxxxxxxxxxxxxxx"}
	}
	img := copyDataDir(t, dir)
	_ = db.Close()
	if n, _ := filepath.Glob(filepath.Join(img, "c", "seg_*.ndjson")); len(n) < 3 {
		t.Fatalf("expected rotation, got %d segments", len(n))
	}

	db2, col2, _ := openRecovered(t, img, cfg)
	defer db2.Close()
	s := col2.IndexRecoveryStats()
	if s.Replays != 1 || s.FullRebuilds != 0 {
		t.Fatalf("stats %+v, want one replay", s)
	}
	if got := col2.Stats().RecordCount; got != 24 {
		t.Fatalf("count %d, want 24", got)
	}
	for id := range want {
		if _, err := col2.Get(id); err != nil {
			t.Fatalf("get %d: %v", id, err)
		}
	}
}

// A torn last line (crash mid-write) is trimmed and the rest of the tail replays.
func TestRecoveryTornTail(t *testing.T) {
	dir := t.TempDir()
	cfg := CollectionConfig{CompactInterval: time.Hour, SyncMode: SyncModeAlways}
	ids := seedClosed(t, dir, cfg, 5)
	db, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	col, _ := db.Collection("c")
	nw, _, _ := col.Insert(map[string]any{"v": "kept"})
	img := copyDataDir(t, dir)
	_ = db.Close()

	paths, _ := filepath.Glob(filepath.Join(img, "c", "seg_*.ndjson"))
	f, err := os.OpenFile(paths[len(paths)-1], os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.WriteString(`{"op":"insert","id":99,"da`)
	_ = f.Close()

	db2, col2, _ := openRecovered(t, img, cfg)
	defer db2.Close()
	if s := col2.IndexRecoveryStats(); s.Replays != 1 || s.FullRebuilds != 0 {
		t.Fatalf("stats %+v", s)
	}
	if got := col2.Stats().RecordCount; got != 6 {
		t.Fatalf("count %d, want 6", got)
	}
	if _, err := col2.Get(nw); err != nil {
		t.Fatal(err)
	}
	_ = ids
}

// A v1 index (no coverage, absolute paths) is treated as stale: one rebuild,
// rewritten as v2, and the next open does no work.
func TestRecoveryV1Upgrade(t *testing.T) {
	dir := t.TempDir()
	cfg := CollectionConfig{CompactInterval: time.Hour}
	ids := seedClosed(t, dir, cfg, 8)

	ip := filepath.Join(dir, "c", "index.json")
	idx := newIndex()
	if err := idx.Load(ip); err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(idx.entries)
	sum := sha256.Sum256(b)
	v1, _ := json.Marshal(indexFile{Entries: idx.entries, Checksum: hex.EncodeToString(sum[:])})
	if err := os.WriteFile(ip, v1, 0o644); err != nil {
		t.Fatal(err)
	}

	db, col, rec := openRecovered(t, dir, cfg)
	if s := col.IndexRecoveryStats(); s.FullRebuilds != 1 || s.Replays != 0 {
		t.Fatalf("stats %+v, want exactly one rebuild", s)
	}
	if k := rec.kinds(); len(k) != 1 || k[0] != IndexRecoveryRebuild {
		t.Fatalf("events %v", k)
	}
	for _, id := range ids {
		if _, err := col.Get(id); err != nil {
			t.Fatal(err)
		}
	}
	_ = db.Close()
	db2, col2, _ := openRecovered(t, dir, cfg)
	defer db2.Close()
	if s := col2.IndexRecoveryStats(); s != (IndexRecoveryStats{}) {
		t.Fatalf("second open after upgrade did work: %+v", s)
	}
}

// Anything that breaks coverage forces a full rebuild, never a trusted index.
func TestRecoveryCoverageViolationsRebuild(t *testing.T) {
	cfg := CollectionConfig{CompactInterval: time.Hour, SegmentMaxSize: 300}
	cases := []struct {
		name   string
		damage func(t *testing.T, colDir string)
	}{
		{"covered bytes altered (fingerprint)", func(t *testing.T, d string) {
			p := filepath.Join(d, "seg_000001.ndjson")
			b, _ := os.ReadFile(p)
			// Same length, still valid JSON: only the checksum can notice.
			b = bytes.Replace(b, []byte("xxxxxxxx"), []byte("yyyyyyyy"), 1)
			_ = os.WriteFile(p, b, 0o644)
		}},
		{"covered segment truncated", func(t *testing.T, d string) {
			p := filepath.Join(d, "seg_000001.ndjson")
			b, _ := os.ReadFile(p)
			// cut at an earlier line boundary
			cut := 0
			for i, c := range b {
				if c == '\n' && i < len(b)-2 {
					cut = i + 1
					break
				}
			}
			_ = os.WriteFile(p, b[:cut], 0o644)
		}},
		{"covered segment missing", func(t *testing.T, d string) {
			_ = os.Remove(filepath.Join(d, "seg_000001.ndjson"))
		}},
		{"unlisted older segment", func(t *testing.T, d string) {
			// An orphan older than the covered range (e.g. compaction aftermath).
			b, _ := os.ReadFile(filepath.Join(d, "seg_000001.ndjson"))
			_ = os.WriteFile(filepath.Join(d, "seg_000000.ndjson"), b, 0o644)
		}},
		{"sealed covered segment grew", func(t *testing.T, d string) {
			f, _ := os.OpenFile(filepath.Join(d, "seg_000001.ndjson"), os.O_APPEND|os.O_WRONLY, 0)
			_, _ = f.WriteString(`{"op":"delete","id":1}` + "\n")
			_ = f.Close()
		}},
		{"corrupt index file", func(t *testing.T, d string) {
			_ = os.WriteFile(filepath.Join(d, "index.json"), []byte(`{"version":2,"entr`), 0o644)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			seedClosed(t, dir, cfg, 12)
			colDir := filepath.Join(dir, "c")
			if n, _ := filepath.Glob(filepath.Join(colDir, "seg_*.ndjson")); len(n) < 2 {
				t.Fatalf("need rotated segments, got %d", len(n))
			}
			tc.damage(t, colDir)
			db, err := Open(dir, cfg)
			if err != nil {
				// Altered bytes fail the entry checksum during the rebuild: the
				// stale index must never be trusted over corrupt data.
				if tc.name != "covered bytes altered (fingerprint)" || !errors.Is(err, ErrIntegrity) {
					t.Fatal(err)
				}
				return
			}
			defer db.Close()
			col, _ := db.Collection("c")
			if col.IndexRecoveryStats().FullRebuilds != 1 {
				t.Fatalf("stats %+v, want one full rebuild", col.IndexRecoveryStats())
			}
		})
	}
}

// A checksum-valid index with coverage that matches the segments but whose
// offsets point mid-record is caught by the spot check.
func TestRecoverySpotCheckWrongOffset(t *testing.T) {
	dir := t.TempDir()
	cfg := CollectionConfig{CompactInterval: time.Hour}
	ids := seedClosed(t, dir, cfg, 5)
	ip := filepath.Join(dir, "c", "index.json")
	idx := newIndex()
	if err := idx.Load(ip); err != nil {
		t.Fatal(err)
	}
	for id, e := range idx.entries {
		e.Offset++
		idx.entries[id] = e
	}
	if err := (&IndexSnapshot{entries: idx.entries, coverage: idx.coverage}).Persist(ip); err != nil {
		t.Fatal(err)
	}
	db, col, rec := openRecovered(t, dir, cfg)
	defer db.Close()
	s := col.IndexRecoveryStats()
	if s.SpotCheckFailures != 1 || s.FullRebuilds != 1 {
		t.Fatalf("stats %+v", s)
	}
	if k := rec.kinds(); len(k) != 2 || k[0] != IndexRecoverySpotCheckFail || k[1] != IndexRecoveryRebuild {
		t.Fatalf("events %v", k)
	}
	for _, id := range ids {
		if _, err := col.Get(id); err != nil {
			t.Fatalf("get %d after rebuild: %v", id, err)
		}
	}
}

// Numeric segment ordering: seg_1000000 sorts after seg_999999.
func TestSortSegmentsNumeric(t *testing.T) {
	mk := func(n string) *Segment { return openSealedSegment("/d/seg_"+n+".ndjson", 0) }
	got := sortSegments([]*Segment{mk("1000000"), mk("999999"), mk("000002")})
	want := []string{"seg_000002.ndjson", "seg_999999.ndjson", "seg_1000000.ndjson"}
	for i, s := range got {
		if filepath.Base(s.Path()) != want[i] {
			t.Fatalf("order[%d]=%s want %s", i, filepath.Base(s.Path()), want[i])
		}
	}
}
