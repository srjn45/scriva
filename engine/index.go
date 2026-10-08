package engine

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/srjn45/scriva/store"
)

// ErrIndexStale is returned by Load when the persisted index checksum does
// not match, indicating the index must be rebuilt from segment files.
var ErrIndexStale = errors.New("index: checksum mismatch — rebuild required")

// ErrIndexCorrupt is returned when an indexed offset points to an entry whose
// record identity disagrees with the index.
var ErrIndexCorrupt = errors.New("engine: index corrupt")

// IntegrityError records an identity mismatch between an index entry and the
// on-disk segment data it points to. It wraps ErrIndexCorrupt so callers can check
// errors.Is(err, ErrIndexCorrupt) as well as inspect the details via errors.As.
type IntegrityError struct {
	ID          uint64
	FoundID     uint64
	SegmentPath string
	Offset      int64
}

func (e *IntegrityError) Error() string {
	return fmt.Sprintf("collection: integrity error: index for id %d points to id %d in %q at offset %d: %v",
		e.ID, e.FoundID, e.SegmentPath, e.Offset, ErrIndexCorrupt)
}

func (e *IntegrityError) Unwrap() error {
	return ErrIndexCorrupt
}

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
	// Epoch is the encryption-policy epoch the record was written under, mirrored
	// from the segment entry so migration completeness can be judged from the
	// in-memory index alone (no segment reads): a collection is functionally
	// migrated when every live entry is at the current epoch. Omitted when zero for
	// backward-compatible index.json files.
	Epoch uint64 `json:"epoch,omitempty"`
}

// indexFormatV2 is the self-describing on-disk index format. v1 files (the
// bare {entries, checksum} shape with absolute segment paths) carry no version
// field and are still accepted by Load.
const indexFormatV2 = 2

// SegmentCoverage records how much of one segment a persisted index describes:
// the segment's base name, the number of bytes covered, and the SHA-256 of
// exactly those bytes. A later task uses it to decide whether the index is
// current, needs a tail replay, or must be rebuilt.
//
// Checksum (full SHA-256) is recorded for the segment that was active at
// capture time; sealed, immutable segments record only Tail — the SHA-256 of
// the last coverageTailBytes covered bytes — so persisting and re-validating a
// large data set costs O(segments), not O(bytes). Index files written before
// Tail existed carry Checksum only and are verified by the full hash. Either
// way a mismatch forces a rebuild, and the bounded identity spot-check still
// runs.
type SegmentCoverage struct {
	Segment  string `json:"segment"`
	Size     int64  `json:"size"`
	Checksum string `json:"checksum"`
	Tail     string `json:"tail,omitempty"`
}

// coverageTailBytes is how many trailing covered bytes the tail fingerprint
// hashes.
const coverageTailBytes = 64 << 10

// indexPayload is the canonical (checksummed) body of a v2 index file. Encoding
// is deterministic: struct fields marshal in declaration order and map keys are
// sorted by encoding/json.
type indexPayload struct {
	Version  int                   `json:"version"`
	Entries  map[uint64]IndexEntry `json:"entries"`
	Coverage []SegmentCoverage     `json:"coverage"`
}

// indexFile is the on-disk representation persisted to index.json. Version 0
// (absent) is the legacy v1 layout, whose checksum covers only Entries.
type indexFile struct {
	Version  int                   `json:"version,omitempty"`
	Entries  map[uint64]IndexEntry `json:"entries"`
	Coverage []SegmentCoverage     `json:"coverage,omitempty"`
	Checksum string                `json:"checksum"`
}

// IndexSnapshot is a point-in-time copy of the index entries together with the
// segment coverage they correspond to. Take it under the collection lock so the
// two are consistent, then persist it without holding the lock.
type IndexSnapshot struct {
	entries  map[uint64]IndexEntry
	coverage []SegmentCoverage
}

// Index is the in-memory id → location map for a single collection.
type Index struct {
	mu      sync.RWMutex
	entries map[uint64]IndexEntry
	// coverage is what the last successful Load reported (nil for v1 files or
	// an index that was never loaded).
	coverage []SegmentCoverage
	// coverageKnown is true when the last Load read a v2 file, i.e. coverage
	// (possibly empty) is authoritative. False for v1 files and never-loaded
	// indexes: "coverage unknown", which recovery treats as stale.
	coverageKnown bool
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

// countAtEpoch returns the number of live records and, of those, how many were
// written at the given epoch. It answers migration completeness (every live
// record at the current epoch) from the in-memory index alone, with no segment
// reads.
func (idx *Index) countAtEpoch(epoch uint64) (total, atEpoch int) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	total = len(idx.entries)
	for _, e := range idx.entries {
		if e.Epoch == epoch {
			atEpoch++
		}
	}
	return total, atEpoch
}

// Coverage returns the segment coverage recorded by the last Load, or nil when
// the file was a v1 index (which carries none).
func (idx *Index) Coverage() []SegmentCoverage {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return append([]SegmentCoverage(nil), idx.coverage...)
}

// Snapshot copies the entries and captures coverage for segs. Callers must hold
// the lock that stops writes to segs so the coverage matches the entries.
func (idx *Index) Snapshot(segs []*Segment) (*IndexSnapshot, error) {
	// segs end with the active segment; earlier ones are sealed and get the
	// cheap tail fingerprint.
	sealed := make(map[*Segment]bool, len(segs))
	for i := 0; i+1 < len(segs); i++ {
		sealed[segs[i]] = true
	}
	return idx.snapshotCached(segs, sealed, nil)
}

// snapshotCached is Snapshot for a periodic persister: coverage of sealed
// (immutable) segments is memoized in cache keyed by path and size, so a
// steady-state persist hashes only the bounded active segment instead of the
// whole data set. segs must list sealed segments followed by the active one;
// only the entries of segs that are in sealed are cached. A nil cache disables
// caching. The cache must be cleared whenever sealed segment files are replaced.
func (idx *Index) snapshotCached(segs []*Segment, sealed map[*Segment]bool, cache map[string]SegmentCoverage) (*IndexSnapshot, error) {
	cov := make([]SegmentCoverage, 0, len(segs))
	for _, seg := range segs {
		if cache != nil && sealed[seg] {
			if c, ok := cache[seg.Path()]; ok && c.Size == seg.Size() {
				cov = append(cov, c)
				continue
			}
		}
		c, err := captureCoverage(seg, !sealed[seg])
		if err != nil {
			return nil, err
		}
		if cache != nil && sealed[seg] {
			cache[seg.Path()] = c
		}
		cov = append(cov, c)
	}
	sort.Slice(cov, func(i, j int) bool { return cov[i].Segment < cov[j].Segment })
	idx.mu.RLock()
	snap := make(map[uint64]IndexEntry, len(idx.entries))
	for k, v := range idx.entries {
		snap[k] = v
	}
	idx.mu.RUnlock()
	return &IndexSnapshot{entries: snap, coverage: cov}, nil
}

// captureCoverage records the first Size() bytes of seg: always the tail
// fingerprint, plus the full SHA-256 when full is set (the active segment).
func captureCoverage(seg *Segment, full bool) (SegmentCoverage, error) {
	size := seg.Size()
	f, err := os.Open(seg.Path())
	if err != nil {
		return SegmentCoverage{}, fmt.Errorf("index: coverage open %q: %w", seg.Path(), err)
	}
	defer func() { _ = f.Close() }()
	cv := SegmentCoverage{Segment: filepath.Base(seg.Path()), Size: size}
	if full {
		h := sha256.New()
		if _, err := io.CopyN(h, f, size); err != nil {
			return SegmentCoverage{}, fmt.Errorf("index: coverage hash %q: %w", seg.Path(), err)
		}
		cv.Checksum = hex.EncodeToString(h.Sum(nil))
	}
	tail, err := tailFingerprint(f, size)
	if err != nil {
		return SegmentCoverage{}, fmt.Errorf("index: coverage tail %q: %w", seg.Path(), err)
	}
	cv.Tail = tail
	return cv, nil
}

// tailFingerprint hashes the last coverageTailBytes of the first size bytes of f.
func tailFingerprint(f *os.File, size int64) (string, error) {
	off := size - coverageTailBytes
	if off < 0 {
		off = 0
	}
	h := sha256.New()
	n, err := io.Copy(h, io.NewSectionReader(f, off, size-off))
	if err != nil {
		return "", err
	}
	if n != size-off {
		return "", io.ErrUnexpectedEOF // file shorter than the covered size
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// Persist serialises the index to path in the v2 format without segment
// coverage. Use Snapshot + IndexSnapshot.Persist to record coverage.
func (idx *Index) Persist(path string) error {
	idx.mu.RLock()
	snap := make(map[uint64]IndexEntry, len(idx.entries))
	for k, v := range idx.entries {
		snap[k] = v
	}
	idx.mu.RUnlock()
	return (&IndexSnapshot{entries: snap}).Persist(path)
}

// Persist writes the snapshot to path as a v2 index. Segment paths are stored
// relative to path's directory so the data directory can be moved.
func (s *IndexSnapshot) Persist(path string) error {
	dir := filepath.Dir(path)
	rel := make(map[uint64]IndexEntry, len(s.entries))
	for id, e := range s.entries {
		e.SegmentPath = relSegmentPath(dir, e.SegmentPath)
		rel[id] = e
	}
	cov := s.coverage
	if cov == nil {
		cov = []SegmentCoverage{}
	}
	payload, err := json.Marshal(indexPayload{Version: indexFormatV2, Entries: rel, Coverage: cov})
	if err != nil {
		return fmt.Errorf("index: marshal: %w", err)
	}
	// The file is the payload with the checksum field appended, so it is
	// built from the single marshal above (same keys, same order) and Load can
	// hash the raw bytes without re-marshalling.
	sum := sha256.Sum256(payload)
	b := make([]byte, 0, len(payload)+len(checksumKey)+sha256.Size*2+3)
	b = append(b, payload[:len(payload)-1]...)
	b = append(b, checksumKey...)
	b = append(b, '"')
	b = append(b, hex.EncodeToString(sum[:])...)
	b = append(b, '"', '}')

	// Write atomically and durably (temp file → fsync → rename → fsync dir).
	if err := writeFileAtomic(path, b, 0o644); err != nil {
		return fmt.Errorf("index: persist: %w", err)
	}
	return nil
}

// relSegmentPath returns p relative to dir, or its base name when p is not
// under dir (segments always live directly in the collection directory).
func relSegmentPath(dir, p string) string {
	if !filepath.IsAbs(p) {
		return filepath.ToSlash(p)
	}
	if r, err := filepath.Rel(dir, p); err == nil && !strings.HasPrefix(r, "..") {
		return filepath.ToSlash(r)
	}
	return filepath.Base(p)
}

// resolveSegmentPath maps a stored segment path to an absolute path under dir.
// Absolute paths (v1 files) are reduced to their base name: they name a
// location that may no longer exist if the directory was moved.
func resolveSegmentPath(dir, p string) string {
	p = filepath.FromSlash(p)
	if filepath.IsAbs(p) || strings.HasPrefix(p, "/") {
		p = filepath.Base(p)
	}
	return filepath.Join(dir, p)
}

// Load reads a persisted index from path (v1 or v2) and verifies its checksum.
// Segment paths are resolved against path's directory. Returns ErrIndexStale on
// checksum mismatch or an unknown format version; the caller should Rebuild.
func (idx *Index) Load(path string) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("index: read %q: %w", path, err)
	}

	var file indexFile
	if err := json.Unmarshal(b, &file); err != nil {
		return fmt.Errorf("index: unmarshal: %w", err)
	}

	if file.Version == indexFormatV2 && rawChecksumOK(b, file.Checksum) {
		return idx.install(path, file)
	}

	var payload []byte
	switch file.Version {
	case 0: // v1: checksum over the entries map alone.
		payload, err = json.Marshal(file.Entries)
	case indexFormatV2:
		cov := file.Coverage
		if cov == nil {
			cov = []SegmentCoverage{}
		}
		payload, err = json.Marshal(indexPayload{Version: indexFormatV2, Entries: file.Entries, Coverage: cov})
	default:
		return ErrIndexStale
	}
	if err != nil {
		return fmt.Errorf("index: re-marshal for checksum: %w", err)
	}
	sum := sha256.Sum256(payload)
	if hex.EncodeToString(sum[:]) != file.Checksum {
		return ErrIndexStale
	}

	return idx.install(path, file)
}

// checksumKey is the literal that precedes the checksum value in a persisted
// v2 file (see IndexSnapshot.Persist).
const checksumKey = `,"checksum":`

// rawChecksumOK reports whether the SHA-256 of b with its trailing checksum
// field replaced by a closing brace equals want, without re-marshalling the
// entries. A false result is not an error: callers fall back to the canonical
// re-marshal check, so this only ever accepts byte-exact canonical files.
func rawChecksumOK(b []byte, want string) bool {
	suffix := checksumKey + `"` + want + `"}`
	if want == "" || !bytes.HasSuffix(b, []byte(suffix)) {
		return false
	}
	h := sha256.New()
	h.Write(b[:len(b)-len(suffix)])
	h.Write([]byte{'}'})
	return hex.EncodeToString(h.Sum(nil)) == want
}

// install resolves segment paths against path's directory and adopts file's
// contents as the in-memory index.
func (idx *Index) install(path string, file indexFile) error {
	dir := filepath.Dir(path)
	entries := make(map[uint64]IndexEntry, len(file.Entries))
	for id, e := range file.Entries {
		e.SegmentPath = resolveSegmentPath(dir, e.SegmentPath)
		entries[id] = e
	}

	idx.mu.Lock()
	idx.entries = entries
	idx.coverage = file.Coverage
	idx.coverageKnown = file.Version == indexFormatV2
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
	for _, seg := range sortSegments(segments) {
		if err := applyEntries(fresh, seg, 0); err != nil {
			return err
		}
	}
	idx.mu.Lock()
	idx.entries = fresh
	idx.mu.Unlock()
	return nil
}

// applyEntries replays seg's records from byte offset from (a record boundary)
// onto m, with the semantics Rebuild and tail replay share: each insert/update
// bumps the per-id revision (never below the revision the line carries), a
// delete removes the entry so it is not resurrected, last writer wins.
func applyEntries(m map[uint64]IndexEntry, seg *Segment, from int64) error {
	err := seg.ScanFromOffset(from, func(off int64, e store.Entry) error {
		applyOne(m, seg.Path(), off, e)
		return nil
	})
	if err != nil {
		return fmt.Errorf("index: replay %q from %d: %w", seg.Path(), from, err)
	}
	return nil
}

// applyOne folds a single record at (path, off) onto m; see applyEntries.
func applyOne(m map[uint64]IndexEntry, path string, off int64, e store.Entry) {
	switch e.Op {
	case store.OpInsert, store.OpUpdate:
		rev := m[e.ID].Rev + 1
		if e.Rev > rev {
			rev = e.Rev
		}
		m[e.ID] = IndexEntry{SegmentPath: path, Offset: off, Rev: rev, ExpiresAt: e.ExpiresAt, Epoch: e.Epoch}
	case store.OpDelete:
		delete(m, e.ID)
	}
}

// segmentNum parses N from a seg_N.ndjson path.
func segmentNum(path string) (uint64, bool) {
	var n uint64
	if _, err := fmt.Sscanf(filepath.Base(path), "seg_%d.ndjson", &n); err != nil {
		return 0, false
	}
	return n, true
}

// sortSegments returns segs ordered by numeric segment id (not lexical name, so
// seg_1000000 sorts after seg_999999). Unparseable names sort last, by name.
func sortSegments(segs []*Segment) []*Segment {
	out := append([]*Segment(nil), segs...)
	sort.SliceStable(out, func(i, j int) bool {
		ni, oki := segmentNum(out[i].Path())
		nj, okj := segmentNum(out[j].Path())
		switch {
		case oki && okj && ni != nj:
			return ni < nj
		case oki != okj:
			return oki
		}
		return out[i].Path() < out[j].Path()
	})
	return out
}
