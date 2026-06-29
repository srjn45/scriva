package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/srjn45/filedbv2/internal/store"
)

// SecondaryIndex is an in-memory inverted index: field-value → set of IDs.
// It makes eq-filter queries O(1) instead of O(n) full-segment scan.
//
// Concurrency model:
//   - add/remove/update/rebuild are called while the Collection's write lock is
//     held, but they also acquire s.mu.Lock so that Persist (called outside the
//     Collection lock) can safely snapshot the data.
//   - Lookup is called under the Collection's read lock and takes s.mu.RLock.
//   - Persist takes s.mu.RLock for a safe snapshot.
type SecondaryIndex struct {
	field   string
	mu      sync.RWMutex
	buckets map[string]map[uint64]struct{} // value → id set
	reverse map[uint64]string             // id → value (fast removal on update/delete)
}

// sidxFile is the on-disk format persisted to sidx_<field>.json.
type sidxFile struct {
	Field    string              `json:"field"`
	Buckets  map[string][]uint64 `json:"buckets"`
	Checksum string              `json:"checksum"`
}

func newSecondaryIndex(field string) *SecondaryIndex {
	return &SecondaryIndex{
		field:   field,
		buckets: make(map[string]map[uint64]struct{}),
		reverse: make(map[uint64]string),
	}
}

// add records id under value. Goroutine-safe.
func (s *SecondaryIndex) add(value string, id uint64) {
	s.mu.Lock()
	if s.buckets[value] == nil {
		s.buckets[value] = make(map[uint64]struct{})
	}
	s.buckets[value][id] = struct{}{}
	s.reverse[id] = value
	s.mu.Unlock()
}

// remove deletes id from whatever bucket it occupies (using reverse map).
// Goroutine-safe.
func (s *SecondaryIndex) remove(id uint64) {
	s.mu.Lock()
	value, ok := s.reverse[id]
	if ok {
		bucket := s.buckets[value]
		delete(bucket, id)
		if len(bucket) == 0 {
			delete(s.buckets, value)
		}
		delete(s.reverse, id)
	}
	s.mu.Unlock()
}

// update atomically moves id from its old bucket to newValue's bucket.
// Goroutine-safe.
func (s *SecondaryIndex) update(id uint64, newValue string) {
	s.mu.Lock()
	if oldValue, ok := s.reverse[id]; ok {
		bucket := s.buckets[oldValue]
		delete(bucket, id)
		if len(bucket) == 0 {
			delete(s.buckets, oldValue)
		}
	}
	if s.buckets[newValue] == nil {
		s.buckets[newValue] = make(map[uint64]struct{})
	}
	s.buckets[newValue][id] = struct{}{}
	s.reverse[id] = newValue
	s.mu.Unlock()
}

// Lookup returns all IDs whose indexed field equals value. Goroutine-safe.
func (s *SecondaryIndex) Lookup(value string) []uint64 {
	s.mu.RLock()
	bucket := s.buckets[value]
	if len(bucket) == 0 {
		s.mu.RUnlock()
		return nil
	}
	ids := make([]uint64, 0, len(bucket))
	for id := range bucket {
		ids = append(ids, id)
	}
	s.mu.RUnlock()
	return ids
}

// rebuild reconstructs the index by replaying entries from segs.
// Must be called while the Collection write lock is held.
func (s *SecondaryIndex) rebuild(segs []*Segment) error {
	type rec struct {
		data    map[string]any
		deleted bool
	}
	latest := make(map[uint64]rec)
	for _, seg := range segs {
		entries, err := seg.ScanAll()
		if err != nil {
			return fmt.Errorf("sidx rebuild: scan %q: %w", seg.Path(), err)
		}
		for _, e := range entries {
			switch e.Op {
			case store.OpInsert, store.OpUpdate:
				latest[e.ID] = rec{data: e.Data}
			case store.OpDelete:
				latest[e.ID] = rec{deleted: true}
			}
		}
	}

	fresh := make(map[string]map[uint64]struct{})
	freshRev := make(map[uint64]string)
	for id, r := range latest {
		if r.deleted {
			continue
		}
		if val, ok := r.data[s.field]; ok {
			key := toIndexKey(val)
			if fresh[key] == nil {
				fresh[key] = make(map[uint64]struct{})
			}
			fresh[key][id] = struct{}{}
			freshRev[id] = key
		}
	}

	s.mu.Lock()
	s.buckets = fresh
	s.reverse = freshRev
	s.mu.Unlock()
	return nil
}

// Persist writes the index to path with an embedded SHA-256 checksum.
func (s *SecondaryIndex) Persist(path string) error {
	s.mu.RLock()
	bucketsJSON := make(map[string][]uint64, len(s.buckets))
	for val, ids := range s.buckets {
		slice := make([]uint64, 0, len(ids))
		for id := range ids {
			slice = append(slice, id)
		}
		bucketsJSON[val] = slice
	}
	s.mu.RUnlock()

	payload, err := json.Marshal(bucketsJSON)
	if err != nil {
		return fmt.Errorf("sidx: marshal: %w", err)
	}
	sum := sha256.Sum256(payload)

	f := sidxFile{
		Field:    s.field,
		Buckets:  bucketsJSON,
		Checksum: hex.EncodeToString(sum[:]),
	}
	b, err := json.Marshal(f)
	if err != nil {
		return fmt.Errorf("sidx: marshal file: %w", err)
	}

	if err := writeFileAtomic(path, b, 0o644); err != nil {
		return fmt.Errorf("sidx: persist: %w", err)
	}
	return nil
}

// Load reads a persisted index from path and verifies its checksum.
func (s *SecondaryIndex) Load(path string) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var f sidxFile
	if err := json.Unmarshal(b, &f); err != nil {
		return fmt.Errorf("sidx: unmarshal: %w", err)
	}

	payload, err := json.Marshal(f.Buckets)
	if err != nil {
		return fmt.Errorf("sidx: checksum marshal: %w", err)
	}
	sum := sha256.Sum256(payload)
	if hex.EncodeToString(sum[:]) != f.Checksum {
		return fmt.Errorf("sidx %q: checksum mismatch — rebuild required", s.field)
	}

	fresh := make(map[string]map[uint64]struct{}, len(f.Buckets))
	freshRev := make(map[uint64]string)
	for val, ids := range f.Buckets {
		bucket := make(map[uint64]struct{}, len(ids))
		for _, id := range ids {
			bucket[id] = struct{}{}
			freshRev[id] = val
		}
		fresh[val] = bucket
	}

	s.mu.Lock()
	s.buckets = fresh
	s.reverse = freshRev
	s.mu.Unlock()
	return nil
}

// toIndexKey converts a record field value to its bucket key string.
func toIndexKey(v any) string {
	return fmt.Sprintf("%v", v)
}

// filterValueToIndexKey decodes a JSON-encoded filter comparison value and
// converts it to the same string representation used by toIndexKey.
func filterValueToIndexKey(filterValue string) string {
	var v any
	if err := json.Unmarshal([]byte(filterValue), &v); err != nil {
		return filterValue
	}
	return fmt.Sprintf("%v", v)
}

// sidxFilePath returns the disk path for a secondary index on field.
func sidxFilePath(dir, field string) string {
	return fmt.Sprintf("%s/sidx_%s.json", dir, field)
}
