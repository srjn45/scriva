package engine

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/srjn45/scriva/store"
)

func writeSalvageFixture(t *testing.T, chunks ...[]byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "seg_000001.ndjson")
	if err := os.WriteFile(path, bytes.Join(chunks, nil), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func encodedSalvageEntry(t *testing.T, id uint64) []byte {
	t.Helper()
	b, err := store.Encode(store.NewInsert(id, map[string]any{"id": id}))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestScanSegmentTolerantReportsAndContinues(t *testing.T) {
	first := encodedSalvageEntry(t, 1)
	last := encodedSalvageEntry(t, 3)
	path := writeSalvageFixture(t, first, []byte("not json\n"), last)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	report, err := ScanSegmentTolerant(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Entries) != 2 || report.Entries[0].Entry.ID != 1 || report.Entries[1].Entry.ID != 3 {
		t.Fatalf("entries = %#v, want ids 1 and 3", report.Entries)
	}
	if len(report.BadRegions) != 1 || report.BadRegions[0].Reason != BadRegionUndecodableJSON {
		t.Fatalf("bad regions = %#v", report.BadRegions)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("tolerant scan mutated its segment")
	}
}

func TestScanSegmentTolerantRecoversGluedSuccessfulAppend(t *testing.T) {
	first := encodedSalvageEntry(t, 1)
	third := encodedSalvageEntry(t, 3)
	// This is the exact shape produced by a failed partial append followed by a
	// later successful append when no rollback occurs: one physical line holds
	// a torn JSON prefix and a complete record.
	partial := []byte(`{"id":2,"op":"insert","ts":"2026-01-01T00:00:00Z","data":`)
	path := writeSalvageFixture(t, first, partial, third)

	report, err := ScanSegmentTolerant(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Entries) != 2 || report.Entries[1].Entry.ID != 3 {
		t.Fatalf("entries = %#v, want recovered id 3", report.Entries)
	}
	recovered := report.Entries[1]
	if !recovered.RecoveredFromGlued || recovered.Offset != int64(len(first)+len(partial)) || recovered.Length != int64(len(third)) {
		t.Fatalf("recovered entry = %#v", recovered)
	}
	if len(report.BadRegions) != 1 || report.BadRegions[0].Reason != BadRegionGluedLines || report.BadRegions[0].Length != int64(len(partial)) {
		t.Fatalf("bad regions = %#v", report.BadRegions)
	}
}

func TestScanSegmentTolerantClassifiesTailOversizeAndWrongOp(t *testing.T) {
	wrong := []byte(`{"id":9,"op":"bogus"}` + "\n")
	tooLarge := append(bytes.Repeat([]byte{'x'}, maxScanTokenSize), '\n')
	path := writeSalvageFixture(t, wrong, tooLarge, []byte(`{"id":10`))
	report, err := ScanSegmentTolerant(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Entries) != 0 || len(report.BadRegions) != 3 {
		t.Fatalf("report = %#v", report)
	}
	got := []BadRegionReason{report.BadRegions[0].Reason, report.BadRegions[1].Reason, report.BadRegions[2].Reason}
	want := []BadRegionReason{BadRegionWrongOp, BadRegionOversizedLine, BadRegionTornTailLine}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("reason[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestScanSegmentTolerantInvalidUTF8(t *testing.T) {
	path := writeSalvageFixture(t, []byte{'"', 0xff, '"', '\n'})
	report, err := ScanSegmentTolerant(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.BadRegions) != 1 || report.BadRegions[0].Reason != BadRegionUndecodableJSON {
		t.Fatalf("bad regions = %#v", report.BadRegions)
	}
}

func FuzzScanSegmentTolerant(f *testing.F) {
	f.Add([]byte(`{"id":1,"op":"insert"}` + "\n"))
	f.Add([]byte{0xff, '\n', '{', '}'})
	f.Add([]byte(`{"id":1,"op":"insert"}{"id":2,"op":"delete"}` + "\n"))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 128*1024 {
			data = data[:128*1024]
		}
		path := writeSalvageFixture(t, data)
		before, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		report, err := ScanSegmentTolerant(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range report.Entries {
			if entry.Offset < 0 || entry.Length <= 0 || entry.Offset+entry.Length > int64(len(data)) {
				t.Fatalf("invalid entry extent %#v for %d bytes", entry, len(data))
			}
			if err := validSegmentOp(entry.Entry.Op); err != nil {
				t.Fatalf("invalid salvaged op: %v", err)
			}
		}
		after, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(before, after) {
			t.Fatal("tolerant scan mutated fuzz input")
		}
	})
}
