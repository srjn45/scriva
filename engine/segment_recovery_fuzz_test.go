//nolint:errcheck
package engine

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/srjn45/scriva/store"
)

// writeEntries appends n insert entries (ids 1..n) to a fresh active segment at
// path and returns, for each entry, the exclusive end byte offset of its line
// (i.e. one past its terminating '\n'). These offsets are captured from the
// segment's real on-disk size after each Append so they account for the exact
// encoded length — including the variable-width timestamp — that recovery sees.
func writeEntries(t *testing.T, path string, n int, payload []byte) []int64 {
	t.Helper()
	seg, err := openActiveSegment(path)
	if err != nil {
		t.Fatalf("openActiveSegment: %v", err)
	}
	lineEnds := make([]int64, n)
	for i := 0; i < n; i++ {
		e := store.NewInsert(uint64(i+1), map[string]any{
			"p": string(payload),
			"i": i,
		})
		if _, err := seg.Append(e); err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
		lineEnds[i] = seg.Size()
	}
	if err := seg.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return lineEnds
}

// survivorCount returns how many complete lines remain after truncating the
// file to truncTo bytes: every line whose terminating newline lands strictly
// before truncTo (i.e. whose exclusive end is <= truncTo) is fully preserved.
func survivorCount(lineEnds []int64, truncTo int64) int {
	k := 0
	for _, end := range lineEnds {
		if end <= truncTo {
			k++
		}
	}
	return k
}

// assertRecovered truncates path to truncTo, reopens it through the crash-
// recovery path, and asserts the recovered segment holds exactly the complete
// lines that preceded the truncation point and accepts a fresh append.
func assertRecovered(t *testing.T, path string, lineEnds []int64, truncTo int64) {
	t.Helper()

	if err := os.Truncate(path, truncTo); err != nil {
		t.Fatalf("truncate to %d: %v", truncTo, err)
	}

	seg, err := openActiveSegment(path)
	if err != nil {
		t.Fatalf("openActiveSegment after truncate to %d: %v", truncTo, err)
	}
	defer seg.Close()

	want := survivorCount(lineEnds, truncTo)

	entries, err := seg.ScanAll()
	if err != nil {
		t.Fatalf("ScanAll after recovery (truncTo=%d): %v", truncTo, err)
	}
	if len(entries) != want {
		t.Fatalf("survivors after truncTo=%d: got %d want %d", truncTo, len(entries), want)
	}
	// Every surviving line must be the original entry in order and decode cleanly.
	for i, e := range entries {
		if e.ID != uint64(i+1) {
			t.Fatalf("survivor[%d] id=%d want %d (truncTo=%d)", i, e.ID, i+1, truncTo)
		}
		if e.Op != store.OpInsert {
			t.Fatalf("survivor[%d] op=%q want insert", i, e.Op)
		}
	}

	// A fresh append after recovery must succeed and be independently readable.
	freshID := uint64(len(entries) + 1)
	off, err := seg.Append(store.NewInsert(freshID, map[string]any{"fresh": true}))
	if err != nil {
		t.Fatalf("Append after recovery (truncTo=%d): %v", truncTo, err)
	}
	got, err := seg.ReadAt(off)
	if err != nil {
		t.Fatalf("ReadAt fresh entry (truncTo=%d): %v", truncTo, err)
	}
	if got.ID != freshID {
		t.Fatalf("fresh entry id=%d want %d", got.ID, freshID)
	}

	all, err := seg.ScanAll()
	if err != nil {
		t.Fatalf("ScanAll after fresh append: %v", err)
	}
	if len(all) != want+1 {
		t.Fatalf("entries after fresh append: got %d want %d", len(all), want+1)
	}
}

// FuzzSegmentRecovery drives the partial-line recovery path
// (recoverPartialLine / openActiveSegment) with fuzzed entry counts, payloads,
// and truncation offsets. For any truncation point it must: never panic, keep
// every complete line written before the cut, and accept a fresh append.
func FuzzSegmentRecovery(f *testing.F) {
	// Explicit seeds spanning the interesting truncation regimes.
	f.Add(uint8(3), []byte("payload"), int64(0))   // truncate to empty
	f.Add(uint8(3), []byte("payload"), int64(10))  // mid first line
	f.Add(uint8(5), []byte(""), int64(1<<30))      // beyond EOF -> no truncation
	f.Add(uint8(8), []byte("hello world"), int64(7))
	f.Add(uint8(1), []byte("x"), int64(3))
	f.Add(uint8(12), []byte(`{"nested":"json"}`), int64(123))

	f.Fuzz(func(t *testing.T, nb uint8, payload []byte, truncOff int64) {
		n := int(nb%16) + 1 // 1..16 entries
		if len(payload) > 4096 {
			payload = payload[:4096]
		}

		path := filepath.Join(t.TempDir(), "seg_000001.ndjson")
		lineEnds := writeEntries(t, path, n, payload)
		size := lineEnds[n-1]

		// Map the fuzzed offset into [0, size] (a crash can only ever leave us
		// with a prefix of the file).
		truncTo := int64(uint64(truncOff) % uint64(size+1))

		assertRecovered(t, path, lineEnds, truncTo)
	})
}

// TestSegmentRecoveryTable exercises the recovery boundaries deterministically:
// exact line boundaries, mid-line cuts, full file, and total truncation.
func TestSegmentRecoveryTable(t *testing.T) {
	const n = 6
	payload := []byte("the quick brown fox")

	// Recompute line ends fresh per case so each gets an untouched file.
	cases := []struct {
		name string
		// pick chooses the truncation offset from the recorded line ends.
		pick func(lineEnds []int64) int64
	}{
		{"empty", func(le []int64) int64 { return 0 }},
		{"first-line-boundary", func(le []int64) int64 { return le[0] }},
		{"third-line-boundary", func(le []int64) int64 { return le[2] }},
		{"mid-third-line", func(le []int64) int64 { return le[1] + 1 }},
		{"one-before-eof", func(le []int64) int64 { return le[n-1] - 1 }},
		{"full-file", func(le []int64) int64 { return le[n-1] }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "seg_000001.ndjson")
			lineEnds := writeEntries(t, path, n, payload)
			assertRecovered(t, path, lineEnds, tc.pick(lineEnds))
		})
	}
}
