package engine

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// sealedFixture builds a closed collection with several sealed segments, each
// larger than the legacy 64 KiB tail window, a secondary index on "v", and
// returns the id of the first record (which sits at offset 0 of segment 1).
func sealedFixture(t *testing.T) (data string, first uint64) {
	t.Helper()
	data = t.TempDir()
	cfg := CollectionConfig{SegmentMaxSize: 90 << 10, IndexPersistInterval: -1, CompactInterval: time.Hour, SyncMode: SyncModeAlways}
	c, err := OpenCollection("c", data, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.EnsureIndex("v"); err != nil {
		t.Fatal(err)
	}
	pad := strings.Repeat("p", 1000)
	for i := 0; i < 300; i++ {
		id, _, err := c.Insert(map[string]any{"v": "aaaa", "pad": pad})
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			first = id
		}
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	return data, first
}

func TestSealedCoverageIsFullChecksum(t *testing.T) {
	data, _ := sealedFixture(t)
	idx := loadedCoverage(t, filepath.Join(data, "c"))
	if len(idx.coverage) < 3 {
		t.Fatalf("want several segments, got %+v", idx.coverage)
	}
	for _, cv := range idx.coverage {
		if cv.Checksum == "" || cv.Tail != "" {
			t.Fatalf("every covered segment (sealed or active) must carry a full checksum and no tail-only proof: %+v", cv)
		}
	}
}

// TestSealedSegmentSameSizeEditDetected changes an indexed field of an early,
// non-tail record of a sealed segment without changing its id, offset or the
// segment size, and proves neither the primary nor the secondary index can
// serve the stale state after reopen.
func TestSealedSegmentSameSizeEditDetected(t *testing.T) {
	data, first := sealedFixture(t)
	seg := filepath.Join(data, "c", "seg_000001.ndjson")
	b, err := os.ReadFile(seg)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) <= coverageTailBytesLegacy+len(b[:bytes.IndexByte(b, 10)])+1 {
		t.Fatalf("segment too small (%d) to place the edit outside the legacy tail window", len(b))
	}
	line := b[:bytes.IndexByte(b, '\n')]
	edited := bytes.Replace(line, []byte(`"aaaa"`), []byte(`"bbbb"`), 1)
	if bytes.Equal(line, edited) || len(line) != len(edited) {
		t.Fatalf("edit did not apply: %s", line)
	}
	var probe map[string]any
	if json.Unmarshal(edited, &probe) != nil {
		t.Fatal("edited record must remain valid JSON")
	}
	copy(b, edited)
	if err := os.WriteFile(seg, b, 0o644); err != nil {
		t.Fatal(err)
	}

	c, err := OpenCollection("c", data, CollectionConfig{SegmentMaxSize: 90 << 10, IndexPersistInterval: -1, CompactInterval: time.Hour})
	if err != nil {
		if !errors.Is(err, ErrIntegrity) {
			t.Fatalf("open must either rebuild or fail with a typed integrity error, got %v", err)
		}
		return
	}
	defer func() { _ = c.Close() }()

	if c.IndexRecoveryStats().FullRebuilds == 0 || c.IndexRecoveryStats().SecondaryRebuilds == 0 {
		t.Fatalf("a same-size edit of sealed bytes must force primary and secondary rebuilds: %+v", c.IndexRecoveryStats())
	}
	for _, id := range lookup(t, c, "aaaa") {
		if id == first {
			t.Fatal("stale secondary index still maps the edited record to its old value")
		}
	}
	found := false
	for _, id := range lookup(t, c, "bbbb") {
		found = found || id == first
	}
	if !found {
		t.Fatal("rebuilt secondary index does not reflect the edited record")
	}
	rec, err := c.Get(first)
	if err != nil || rec.Data["v"] != "bbbb" {
		t.Fatalf("primary index inconsistent with segment: %+v %v", rec, err)
	}
}

func lookup(t *testing.T, c *Collection, v string) []uint64 {
	t.Helper()
	ids, ok := c.IndexLookup("v", v)
	if !ok {
		t.Fatal("secondary index missing")
	}
	return ids
}

// coverageTailBytesLegacy is the tail window older releases fingerprinted.
const coverageTailBytesLegacy = 64 << 10

// A Tail-only coverage file (older release) proves nothing about the prefix:
// the index is rebuilt once and rewritten with full checksums.
func TestLegacyTailOnlyCoverageForcesRebuildOnce(t *testing.T) {
	data, _ := sealedFixture(t)
	p := filepath.Join(data, "c", "index.json")
	idx := newIndex()
	if err := idx.Load(p); err != nil {
		t.Fatal(err)
	}
	snap := &IndexSnapshot{entries: idx.entries, coverage: func() []SegmentCoverage {
		cov := idx.Coverage()
		for i := range cov {
			cov[i].Tail, cov[i].Checksum = "deadbeef", ""
		}
		return cov
	}()}
	if err := snap.Persist(p); err != nil {
		t.Fatal(err)
	}

	c, err := OpenCollection("c", data, CollectionConfig{SegmentMaxSize: 90 << 10, IndexPersistInterval: -1, CompactInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if c.IndexRecoveryStats().FullRebuilds != 1 {
		t.Fatalf("legacy coverage must force exactly one rebuild: %+v", c.IndexRecoveryStats())
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c2, err := OpenCollection("c", data, CollectionConfig{SegmentMaxSize: 90 << 10, IndexPersistInterval: -1, CompactInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c2.Close() }()
	if s := c2.IndexRecoveryStats(); s.FullRebuilds != 0 || s.SecondaryRebuilds != 0 {
		t.Fatalf("after the rewrite a clean reopen must not rebuild: %+v", s)
	}
}
