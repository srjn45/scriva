package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/srjn45/filedbv2/store"
)

// ErrIndexStale is returned by Load when the persisted index checksum does
// not match, indicating the index must be rebuilt from segment files.
var ErrIndexStale = errors.New("index: checksum mismatch — rebuild required")

// IndexEntry records the location of the latest version of a record, plus its
// current revision so callers can read the rev without a segment read.
type IndexEntry struct {
	SegmentPath string `json:"segment"`
	Offset      int64  `json:"offset"`
	// Rev is the record's current revision (1 on insert, +1 per update). It is
	// omitted when zero so an index.json written before revisions existed still
	// verifies its checksum and loads unchanged.
	Rev uint64 `json:"rev,omitempty"`
	// ExpiresAt is the record's TTL deadline as a Unix nanosecond timestamp
	// (0 = never). Mirrored from the segment entry so a read can drop an expired
	// record without touching disk. Omitted when zero for backward-compatible
	// index.json files.
	ExpiresAt int64 `json:"expires_at,omitempty"`
}

// indexFile is the on-disk representation persisted to index.json.
type indexFile struct {
	Entries  map[uint64]IndexEntry `json:"entries"`
	Checksum string                `json:"checksum"`
}

// Index is the in-memory id → location map for a single collection.
type Index struct {
	mu      sync.RWMutex
	entries map[uint64]IndexEntry
}

// newIndex creates an empty index.
func newIndex() *Index {
	return &Index{entries: make(map[uint64]IndexEntry)}
}

// Set records or updates the location of id.
func (idx *Index) Set(id uint64, entry IndexEntry) {
	idx.mu.Lock()
	idx.entries[id] = entry
	idx.mu.Unlock()
}

// Get returns the location of id, or false if not present.
func (idx *Index) Get(id uint64) (IndexEntry, bool) {
	idx.mu.RLock()
	e, ok := idx.entries[id]
	idx.mu.RUnlock()
	return e, ok
}

// Delete removes id from the index (called on delete operations).
func (idx *Index) Delete(id uint64) {
	idx.mu.Lock()
	delete(idx.entries, id)
	idx.mu.Unlock()
}

// Len returns the number of live records tracked by the index.
func (idx *Index) Len() int {
	idx.mu.RLock()
	n := len(idx.entries)
	idx.mu.RUnlock()
	return n
}

// Persist serialises the index to path with an embedded SHA-256 checksum.
func (idx *Index) Persist(path string) error {
	idx.mu.RLock()
	snapshot := make(map[uint64]IndexEntry, len(idx.entries))
	for k, v := range idx.entries {
		snapshot[k] = v
	}
	idx.mu.RUnlock()

	payload, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("index: marshal: %w", err)
	}

	sum := sha256.Sum256(payload)
	file := indexFile{
		Entries:  snapshot,
		Checksum: hex.EncodeToString(sum[:]),
	}

	b, err := json.Marshal(file)
	if err != nil {
		return fmt.Errorf("index: marshal file: %w", err)
	}

	// Write atomically and durably (temp file → fsync → rename → fsync dir).
	if err := writeFileAtomic(path, b, 0o644); err != nil {
		return fmt.Errorf("index: persist: %w", err)
	}
	return nil
}

// Load reads a persisted index from path and verifies its checksum.
// Returns ErrIndexStale on checksum mismatch; the caller should Rebuild.
func (idx *Index) Load(path string) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("index: read %q: %w", path, err)
	}

	var file indexFile
	if err := json.Unmarshal(b, &file); err != nil {
		return fmt.Errorf("index: unmarshal: %w", err)
	}

	// Verify checksum.
	payload, err := json.Marshal(file.Entries)
	if err != nil {
		return fmt.Errorf("index: re-marshal for checksum: %w", err)
	}
	sum := sha256.Sum256(payload)
	if hex.EncodeToString(sum[:]) != file.Checksum {
		return ErrIndexStale
	}

	idx.mu.Lock()
	idx.entries = file.Entries
	idx.mu.Unlock()
	return nil
}

// segmentsValid reports whether every index entry points inside one of the
// given segment files (path → size). An entry referencing a missing file — or
// an offset at or past its end — means the persisted index describes a segment
// layout that no longer exists on disk (e.g. it was written by a Close() that
// raced a compaction swap) and must be rebuilt even though its checksum is
// intact.
func (idx *Index) segmentsValid(sizes map[string]int64) bool {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	for _, e := range idx.entries {
		size, ok := sizes[e.SegmentPath]
		if !ok || e.Offset >= size {
			return false
		}
	}
	return true
}

// Rebuild constructs the index by replaying all entries from the provided
// segments in order. The latest entry for each id wins. Revisions are recomputed
// by replay order — each surviving insert/update bumps a per-id counter — but
// never fall below a revision already recorded in the entry itself, so a
// compacted record (whose full write history was collapsed into a single line
// that still carries its latest rev) keeps that rev instead of resetting to 1.
// A delete clears the counter so a re-inserted id restarts at rev 1.
func (idx *Index) Rebuild(segments []*Segment) error {
	fresh := make(map[uint64]IndexEntry)

	for _, seg := range segments {
		entries, err := seg.ScanAll()
		if err != nil {
			return fmt.Errorf("index: rebuild scan %q: %w", seg.Path(), err)
		}

		// Re-scan with byte offsets so we know each entry's position.
		offsets, err := scanOffsets(seg)
		if err != nil {
			return err
		}

		for i, e := range entries {
			switch e.Op {
			case store.OpInsert, store.OpUpdate:
				rev := fresh[e.ID].Rev + 1
				if e.Rev > rev {
					rev = e.Rev
				}
				fresh[e.ID] = IndexEntry{SegmentPath: seg.Path(), Offset: offsets[i], Rev: rev, ExpiresAt: e.ExpiresAt}
			case store.OpDelete:
				delete(fresh, e.ID)
			}
		}
	}

	idx.mu.Lock()
	idx.entries = fresh
	idx.mu.Unlock()
	return nil
}

// scanOffsets returns the byte offset of each entry in a segment.
func scanOffsets(seg *Segment) ([]int64, error) {
	f, err := os.Open(seg.Path())
	if err != nil {
		return nil, fmt.Errorf("index: scanOffsets open %q: %w", seg.Path(), err)
	}
	defer func() { _ = f.Close() }()

	var offsets []int64
	var pos int64
	buf := make([]byte, 1)

	// Efficient line-boundary detection.
	lineStart := int64(0)
	inLine := false
	readBuf := make([]byte, 32*1024)

	for {
		n, err := f.Read(readBuf)
		for i := 0; i < n; i++ {
			_ = buf
			if !inLine {
				lineStart = pos + int64(i)
				inLine = true
			}
			if readBuf[i] == '\n' {
				offsets = append(offsets, lineStart)
				inLine = false
			}
		}
		pos += int64(n)
		if err != nil {
			break
		}
	}
	return offsets, nil
}
