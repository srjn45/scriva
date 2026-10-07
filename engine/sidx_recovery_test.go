package engine

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/srjn45/scriva/query"
)

func sidxCfg() CollectionConfig {
	return CollectionConfig{CompactInterval: time.Hour, SyncMode: SyncModeAlways}
}

// seedSidx creates collection "c" with n records {v: i, name: nI}, indexes v
// (numeric, non-unique) and name (unique), and closes cleanly.
func seedSidx(t *testing.T, dir string, n int) []uint64 {
	t.Helper()
	db, err := Open(dir, sidxCfg())
	if err != nil {
		t.Fatal(err)
	}
	col, err := db.CreateCollection("c")
	if err != nil {
		t.Fatal(err)
	}
	var ids []uint64
	for i := 0; i < n; i++ {
		id, _, err := col.Insert(map[string]any{"v": float64(i), "name": nameOf(i)})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := col.EnsureIndex("v"); err != nil {
		t.Fatal(err)
	}
	if err := col.EnsureUniqueIndex("name"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return ids
}

func nameOf(i int) string { return "n" + string(rune('a'+i%26)) + string(rune('a'+i/26)) }

func sortedIDs(ids []uint64) []uint64 {
	out := make([]uint64, 0, len(ids))
	out = append(out, ids...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// requireSidxMatchesScan asserts every secondary-index answer equals the
// index-free full-scan answer for the same data.
func requireSidxMatchesScan(t *testing.T, col *Collection, maxV int) {
	t.Helper()
	for v := 0; v <= maxV; v++ {
		got, ok := col.IndexLookup("v", toIndexKey(float64(v)))
		if !ok {
			t.Fatal("index v missing")
		}
		want := scanIDs(t, col, &query.FieldFilter{Field: "v", Op: query.OpEq, Value: toIndexKey(float64(v))})
		if len(got) == 0 && len(want) == 0 {
			continue
		}
		if !reflect.DeepEqual(sortedIDs(got), want) {
			t.Fatalf("v=%d: index %v scan %v", v, sortedIDs(got), want)
		}
	}
	for _, op := range []query.Op{query.OpGt, query.OpGte, query.OpLt, query.OpLte} {
		for _, q := range []string{"-1", "0", "3", "7", "100"} {
			f := rangeFilter("v", op, q)
			got, err := col.Scan(f) // served by the index
			if err != nil {
				t.Fatal(err)
			}
			col.sidxMu.Lock()
			saved := col.sidxMap
			col.sidxMap = map[string]*SecondaryIndex{}
			col.sidxMu.Unlock()
			want := scanIDs(t, col, f)
			col.sidxMu.Lock()
			col.sidxMap = saved
			col.sidxMu.Unlock()
			var gotIDs []uint64
			for _, r := range got {
				gotIDs = append(gotIDs, r.ID)
			}
			if !reflect.DeepEqual(sortedIDs(gotIDs), want) {
				t.Fatalf("range %s %s: index %v scan %v", op, q, sortedIDs(gotIDs), want)
			}
		}
	}
}

func TestSidxCrashTailReplay(t *testing.T) {
	dir := t.TempDir()
	ids := seedSidx(t, dir, 20)

	db, err := Open(dir, sidxCfg())
	if err != nil {
		t.Fatal(err)
	}
	col, _ := db.Collection("c")
	if _, _, err := col.Insert(map[string]any{"v": float64(5), "name": "fresh"}); err != nil {
		t.Fatal(err)
	}
	if _, err := col.Update(ids[0], map[string]any{"v": float64(99), "name": nameOf(0)}); err != nil {
		t.Fatal(err)
	}
	if err := col.Delete(ids[1]); err != nil {
		t.Fatal(err)
	}
	img := copyDataDir(t, dir) // crash: sidx files predate the tail
	_ = db.Close()

	db2, col2, _ := openRecovered(t, img, sidxCfg())
	defer db2.Close()
	s := col2.IndexRecoveryStats()
	if s.SecondaryReplays != 2 || s.SecondaryRebuilds != 0 {
		t.Fatalf("stats %+v, want 2 sidx replays and no rebuild", s)
	}
	requireSidxMatchesScan(t, col2, 100)
	if got, _ := col2.IndexLookup("v", "99"); len(got) != 1 || got[0] != ids[0] {
		t.Fatalf("updated value lookup: %v", got)
	}
	if got, _ := col2.IndexLookup("v", "1"); len(got) != 0 {
		t.Fatalf("deleted record still indexed: %v", got)
	}

	// Recovery persisted fresh coverage: the next open does no work.
	_ = db2.Close()
	db3, col3, _ := openRecovered(t, img, sidxCfg())
	defer db3.Close()
	if s := col3.IndexRecoveryStats(); s != (IndexRecoveryStats{}) {
		t.Fatalf("reopen after recovery did work: %+v", s)
	}
}

func TestSidxCrashAfterRotationReplaysNewSegment(t *testing.T) {
	dir := t.TempDir()
	seedSidx(t, dir, 10)
	cfg := sidxCfg()
	cfg.SegmentMaxSize = 400
	db, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	col, _ := db.Collection("c")
	for i := 0; i < 30; i++ {
		if _, _, err := col.Insert(map[string]any{"v": float64(i % 7), "name": "x" + nameOf(i)}); err != nil {
			t.Fatal(err)
		}
	}
	img := copyDataDir(t, dir)
	_ = db.Close()
	if m, _ := filepath.Glob(filepath.Join(img, "c", "seg_*.ndjson")); len(m) < 2 {
		t.Skip("no rotation happened")
	}
	db2, col2, _ := openRecovered(t, img, cfg)
	defer db2.Close()
	if s := col2.IndexRecoveryStats(); s.SecondaryRebuilds != 0 || s.SecondaryReplays != 2 {
		t.Fatalf("stats %+v", s)
	}
	requireSidxMatchesScan(t, col2, 10)
}

func TestSidxUniqueEnforcedAfterReplay(t *testing.T) {
	dir := t.TempDir()
	ids := seedSidx(t, dir, 5)
	db, err := Open(dir, sidxCfg())
	if err != nil {
		t.Fatal(err)
	}
	col, _ := db.Collection("c")
	if err := col.Delete(ids[0]); err != nil { // frees nameOf(0)
		t.Fatal(err)
	}
	if _, _, err := col.Insert(map[string]any{"v": float64(1), "name": "tailonly"}); err != nil {
		t.Fatal(err)
	}
	img := copyDataDir(t, dir)
	_ = db.Close()

	db2, col2, _ := openRecovered(t, img, sidxCfg())
	defer db2.Close()
	if _, _, err := col2.Insert(map[string]any{"v": float64(1), "name": "tailonly"}); !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("tail-written unique value accepted twice: %v", err)
	}
	if _, _, err := col2.Insert(map[string]any{"v": float64(1), "name": nameOf(1)}); !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("covered unique value accepted twice: %v", err)
	}
	if _, _, err := col2.Insert(map[string]any{"v": float64(1), "name": nameOf(0)}); err != nil {
		t.Fatalf("value freed by tail delete rejected: %v", err)
	}
}

func TestSidxV1UpgradeRebuildsAndPersistsV2(t *testing.T) {
	dir := t.TempDir()
	seedSidx(t, dir, 8)
	p := sidxFilePath(filepath.Join(dir, "c"), "v")
	// Rewrite as a legacy v1 file (no version/coverage), as old servers wrote.
	old := newSecondaryIndex("v", false)
	if err := old.Load(p); err != nil {
		t.Fatal(err)
	}
	if !old.coverageKnown {
		t.Fatal("clean close should persist v2 coverage")
	}
	if err := old.Persist(p); err != nil {
		t.Fatal(err)
	}

	db, col, _ := openRecovered(t, dir, sidxCfg())
	s := col.IndexRecoveryStats()
	if s.SecondaryRebuilds != 1 {
		t.Fatalf("stats %+v, want exactly the v1 sidx rebuilt", s)
	}
	requireSidxMatchesScan(t, col, 10)
	_ = db.Close()

	up := newSecondaryIndex("v", false)
	if err := up.Load(p); err != nil || !up.coverageKnown || len(up.coverage) == 0 {
		t.Fatalf("not upgraded to v2: err=%v known=%v cov=%v", err, up.coverageKnown, up.coverage)
	}
}

func TestSidxCoverageViolationsRebuild(t *testing.T) {
	cases := map[string]func(t *testing.T, segDir string){
		"hash mismatch": func(t *testing.T, segDir string) {
			old := newSecondaryIndex("v", false)
			if err := old.Load(sidxFilePath(segDir, "v")); err != nil {
				t.Fatal(err)
			}
			cov := append([]SegmentCoverage(nil), old.coverage...)
			cov[0].Checksum = "00" + cov[0].Checksum[2:] // a validly-sealed file describing other bytes
			if err := old.PersistWithCoverage(sidxFilePath(segDir, "v"), cov); err != nil {
				t.Fatal(err)
			}
		},
		"corrupt file": func(t *testing.T, segDir string) {
			if err := os.WriteFile(sidxFilePath(segDir, "v"), []byte("{not json"), 0o644); err != nil {
				t.Fatal(err)
			}
		},
		"entries without coverage": func(t *testing.T, segDir string) {
			old := newSecondaryIndex("v", false)
			if err := old.Load(sidxFilePath(segDir, "v")); err != nil {
				t.Fatal(err)
			}
			if err := old.PersistWithCoverage(sidxFilePath(segDir, "v"), nil); err != nil {
				t.Fatal(err)
			}
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			seedSidx(t, dir, 8)
			mutate(t, filepath.Join(dir, "c"))
			db, col, _ := openRecovered(t, dir, sidxCfg())
			defer db.Close()
			if s := col.IndexRecoveryStats(); s.SecondaryRebuilds < 1 {
				t.Fatalf("stats %+v, want a sidx rebuild", s)
			}
			requireSidxMatchesScan(t, col, 10)
		})
	}
}
