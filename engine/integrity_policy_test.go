package engine

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/srjn45/scriva/store"
)

// damagedDir seeds a closed collection of n records over several segments and
// then flips bytes inside a record of the first (sealed) segment, so the
// persisted index is no longer trusted and the rebuild meets real corruption.
func damagedDir(t *testing.T) (dir string, cfg CollectionConfig) {
	t.Helper()
	dir = t.TempDir()
	cfg = CollectionConfig{CompactInterval: time.Hour, SegmentMaxSize: 300}
	seedClosed(t, dir, cfg, 12)
	p := filepath.Join(dir, "c", "seg_000001.ndjson")
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	b[20] ^= 0x01 // inside the first record: checksum no longer matches
	if err := os.WriteFile(p, b, 0o644); err != nil {
		t.Fatal(err)
	}
	return dir, cfg
}

func TestIntegrityPolicyDefaultFailsClosed(t *testing.T) {
	for _, pol := range []IntegrityPolicy{"", PolicyFail, PolicyRebuildIndexOnly} {
		t.Run(string(pol), func(t *testing.T) {
			dir, cfg := damagedDir(t)
			cfg.IntegrityPolicy = pol
			var mu sync.Mutex
			var outcomes []string
			cfg.OnIntegrity = func(_ string, _ IntegrityPolicy, outcome string, rep *CollectionReport) {
				mu.Lock()
				defer mu.Unlock()
				outcomes = append(outcomes, outcome)
				if rep == nil || rep.Name != "c" {
					t.Errorf("bad report %+v", rep)
				}
			}
			_, err := Open(dir, cfg)
			if !errors.Is(err, ErrIntegrity) {
				t.Fatalf("Open err = %v, want ErrIntegrity", err)
			}
			var ie *OpenIntegrityError
			if !errors.As(err, &ie) || ie.Report == nil || !ie.Report.Has(CodeSegmentBadRegion) {
				t.Fatalf("want *OpenIntegrityError with bad-region finding, got %v", err)
			}
			if len(outcomes) != 1 || outcomes[0] != IntegrityOutcomeFailed {
				t.Fatalf("outcomes = %v", outcomes)
			}
			// A refused open must release the directory lock and not touch bytes.
			cfg.IntegrityPolicy = PolicyReport
			db, err := Open(dir, cfg)
			if err != nil {
				t.Fatalf("reopen under report: %v", err)
			}
			_ = db.Close()
		})
	}
}

func TestIntegrityPolicyReportSalvages(t *testing.T) {
	dir, cfg := damagedDir(t)
	cfg.IntegrityPolicy = PolicyReport
	var outcome string
	cfg.OnIntegrity = func(_ string, _ IntegrityPolicy, o string, _ *CollectionReport) { outcome = o }
	db, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	col, _ := db.Collection("c")
	if outcome != IntegrityOutcomeReported {
		t.Fatalf("outcome = %q", outcome)
	}
	rep := col.OpenIntegrityReport()
	if rep == nil || !rep.Has(CodeSegmentBadRegion) {
		t.Fatalf("open report = %+v", rep)
	}
	// The 11 intact records stay readable; the damaged one is skipped.
	if n := col.Stats().RecordCount; n != 11 {
		t.Fatalf("records = %d, want 11", n)
	}
}

// An untrusted index over intact segments is still rebuilt automatically under
// the default policy, and the scan reports clean.
func TestIntegrityPolicyIntactRebuildIsAutomatic(t *testing.T) {
	dir := t.TempDir()
	cfg := CollectionConfig{CompactInterval: time.Hour, SegmentMaxSize: 300}
	seedClosed(t, dir, cfg, 12)
	if err := os.WriteFile(filepath.Join(dir, "c", "index.json"), []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}
	var outcome string
	cfg.OnIntegrity = func(_ string, _ IntegrityPolicy, o string, _ *CollectionReport) { outcome = o }
	db, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	col, _ := db.Collection("c")
	if outcome != IntegrityOutcomeClean || col.IndexRecoveryStats().FullRebuilds != 1 {
		t.Fatalf("outcome=%q stats=%+v", outcome, col.IndexRecoveryStats())
	}
}

func TestIntegrityPolicyConflictFailsClosed(t *testing.T) {
	dir := t.TempDir()
	cfg := CollectionConfig{CompactInterval: time.Hour}
	seedClosed(t, dir, cfg, 3)
	// Re-insert live id 1 with different content: ambiguous history.
	e := store.NewInsert(1, map[string]any{"other": "content"})
	e.Rev = 1
	b, err := store.Encode(e)
	if err != nil {
		t.Fatal(err)
	}
	f, _ := os.OpenFile(filepath.Join(dir, "c", "seg_000001.ndjson"), os.O_APPEND|os.O_WRONLY, 0)
	_, _ = f.Write(b)
	_ = f.Close()
	_ = os.Remove(filepath.Join(dir, "c", "index.json"))
	if _, err := Open(dir, cfg); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("err = %v, want ErrIntegrity", err)
	}
	cfg.IntegrityPolicy = PolicyReport
	db, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	col, _ := db.Collection("c")
	if !col.OpenIntegrityReport().Has(CodeConflictDuplicateID) {
		t.Fatalf("report = %+v", col.OpenIntegrityReport())
	}
}

func TestParseIntegrityPolicy(t *testing.T) {
	for in, want := range map[string]IntegrityPolicy{"": PolicyFail, "Fail": PolicyFail, "report": PolicyReport, "rebuild-index-only": PolicyRebuildIndexOnly} {
		if got, err := ParseIntegrityPolicy(in); err != nil || got != want {
			t.Fatalf("%q -> %q, %v", in, got, err)
		}
	}
	if _, err := ParseIntegrityPolicy("nope"); err == nil {
		t.Fatal("want error")
	}
}

func TestOnLockAndPoisonHooks(t *testing.T) {
	dir := t.TempDir()
	var res []string
	cfg := CollectionConfig{CompactInterval: time.Hour, OnLock: func(_, r string) { res = append(res, r) }}
	db, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := Open(dir, cfg); !errors.Is(err, ErrDatabaseLocked) {
		t.Fatalf("second open err = %v", err)
	}
	if len(res) != 2 || res[0] != LockAcquired || res[1] != LockContended {
		t.Fatalf("lock results = %v", res)
	}
}

func TestOnAppendHook(t *testing.T) {
	dir := t.TempDir()
	var n, b int
	cfg := CollectionConfig{CompactInterval: time.Hour, OnAppend: func(_ string, bytes int, err error) {
		if err == nil {
			n++
			b += bytes
		}
	}}
	db, err := Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	col, _ := db.CreateCollection("c")
	if _, _, err := col.Insert(map[string]any{"a": 1}); err != nil {
		t.Fatal(err)
	}
	if n != 1 || b == 0 {
		t.Fatalf("appends=%d bytes=%d", n, b)
	}
}
