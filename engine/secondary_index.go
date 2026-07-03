package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"

	"github.com/srjn45/filedbv2/query"
	"github.com/srjn45/filedbv2/store"
)

// ErrDuplicateKey is returned when a write would map a value already held by a
// different live record on a unique secondary index. Callers can match it with
// errors.Is; it is wrapped with the offending field and value for context.
var ErrDuplicateKey = errors.New("engine: duplicate key")

// SecondaryIndex is an in-memory inverted index: field-value → set of IDs.
// It makes eq-filter queries O(1) instead of O(n) full-segment scan, and — via
// an ordered view of its distinct keys — accelerates range predicates
// (gt/gte/lt/lte) so they read O(matches) rather than the whole collection.
//
// Concurrency model:
//   - add/remove/update/rebuild are called while the Collection's write lock is
//     held, but they also acquire s.mu.Lock so that Persist (called outside the
//     Collection lock) can safely snapshot the data.
//   - Lookup / LookupRange are called under the Collection's read lock and take
//     s.mu.RLock.
//   - Persist takes s.mu.RLock for a safe snapshot.
type SecondaryIndex struct {
	field   string
	unique  bool // when true, a value may map to at most one live id
	mu      sync.RWMutex
	buckets map[string]map[uint64]struct{} // value → id set
	reverse map[uint64]string              // id → value (fast removal on update/delete)

	// Ordered view for range queries. kind records the observed value type of
	// the field; sorted holds the distinct bucket keys in ascending order under
	// that type. Both are maintained only while the field is homogeneous — once
	// numbers and strings are mixed, kind becomes indexMixed and range queries
	// fall back to the full scan (a numeric/lexical order is undefined there).
	kind   indexKind
	sorted []orderedKey
}

// indexKind classifies the value type stored in an index, which decides how its
// keys are ordered for range queries.
type indexKind uint8

const (
	indexEmpty   indexKind = iota // no values observed yet
	indexNumeric                  // all values are JSON numbers (float64)
	indexString                   // all values are strings (or non-numeric scalars)
	indexMixed                    // both numbers and strings — range ordering undefined
)

func (k indexKind) String() string {
	switch k {
	case indexNumeric:
		return "numeric"
	case indexString:
		return "string"
	case indexMixed:
		return "mixed"
	default:
		return ""
	}
}

func parseIndexKind(s string) indexKind {
	switch s {
	case "numeric":
		return indexNumeric
	case "string":
		return indexString
	case "mixed":
		return indexMixed
	default:
		return indexEmpty
	}
}

// orderedKey pairs a bucket key string with its typed value so the sorted view
// can order numerically or lexically as appropriate.
type orderedKey struct {
	key string
	val any // float64 for numeric indexes, string otherwise
}

// valKind reports the index kind of a single value and the typed value used for
// ordering. Numbers order numerically; everything else orders as a string.
func valKind(v any) (indexKind, any) {
	switch x := v.(type) {
	case float64:
		return indexNumeric, x
	case string:
		return indexString, x
	default:
		return indexString, fmt.Sprintf("%v", x)
	}
}

// mergeKind combines an index's current kind with a newly observed value's kind.
func mergeKind(cur, next indexKind) indexKind {
	if cur == indexEmpty {
		return next
	}
	if cur == next {
		return cur
	}
	return indexMixed
}

// valForKind reconstructs the typed ordering value for a bucket key given a
// homogeneous kind (used when rebuilding the sorted view from string keys).
func valForKind(kind indexKind, key string) any {
	if kind == indexNumeric {
		f, _ := strconv.ParseFloat(key, 64)
		return f
	}
	return key
}

func newSecondaryIndex(field string, unique bool) *SecondaryIndex {
	return &SecondaryIndex{
		field:   field,
		unique:  unique,
		buckets: make(map[string]map[uint64]struct{}),
		reverse: make(map[uint64]string),
	}
}

// orderLessKey reports whether a sorts before b in the ordered view.
func orderLessKey(a, b orderedKey) bool {
	if c := query.Compare(a.val, b.val); c != 0 {
		return c < 0
	}
	return a.key < b.key
}

// searchSorted returns the position of target in the sorted view (or where it
// would be inserted). Callers under s.mu must hold it.
func (s *SecondaryIndex) searchSorted(target orderedKey) int {
	return sort.Search(len(s.sorted), func(i int) bool {
		return !orderLessKey(s.sorted[i], target)
	})
}

// orderedInsert adds a brand-new bucket key to the sorted view. No-op once the
// index is mixed. Must be called under s.mu.
func (s *SecondaryIndex) orderedInsert(key string, val any) {
	if s.kind == indexMixed {
		return
	}
	ok := orderedKey{key: key, val: val}
	i := s.searchSorted(ok)
	if i < len(s.sorted) && s.sorted[i].key == key {
		return // already present
	}
	s.sorted = append(s.sorted, orderedKey{})
	copy(s.sorted[i+1:], s.sorted[i:])
	s.sorted[i] = ok
}

// orderedRemove drops an emptied bucket key from the sorted view. No-op once the
// index is mixed. Must be called under s.mu.
func (s *SecondaryIndex) orderedRemove(key string) {
	if s.kind == indexMixed {
		return
	}
	target := orderedKey{key: key, val: valForKind(s.kind, key)}
	i := s.searchSorted(target)
	if i < len(s.sorted) && s.sorted[i].key == key {
		s.sorted = append(s.sorted[:i], s.sorted[i+1:]...)
	}
}

// observe folds a newly indexed value's kind into the index. If it makes the
// field heterogeneous, the sorted view is dropped (range serving disabled).
// Must be called under s.mu.
func (s *SecondaryIndex) observe(val any) {
	vk, _ := valKind(val)
	s.kind = mergeKind(s.kind, vk)
	if s.kind == indexMixed {
		s.sorted = nil
	}
}

// sidxFile is the on-disk format persisted to sidx_<field>.json.
type sidxFile struct {
	Field    string              `json:"field"`
	Unique   bool                `json:"unique,omitempty"`
	Kind     string              `json:"kind,omitempty"` // "numeric"|"string"|"mixed"; absent = legacy
	Buckets  map[string][]uint64 `json:"buckets"`
	Checksum string              `json:"checksum"`
}

// add records id under value. Goroutine-safe.
func (s *SecondaryIndex) add(value any, id uint64) {
	key := toIndexKey(value)
	_, normVal := valKind(value)
	s.mu.Lock()
	newBucket := s.buckets[key] == nil
	if newBucket {
		s.buckets[key] = make(map[uint64]struct{})
	}
	s.buckets[key][id] = struct{}{}
	s.reverse[id] = key
	s.observe(value)
	if newBucket {
		s.orderedInsert(key, normVal)
	}
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
			s.orderedRemove(value)
		}
		delete(s.reverse, id)
	}
	s.mu.Unlock()
}

// update atomically moves id from its old bucket to newValue's bucket.
// Goroutine-safe.
func (s *SecondaryIndex) update(id uint64, newValue any) {
	newKey := toIndexKey(newValue)
	_, normVal := valKind(newValue)
	s.mu.Lock()
	if oldKey, ok := s.reverse[id]; ok {
		bucket := s.buckets[oldKey]
		delete(bucket, id)
		if len(bucket) == 0 {
			delete(s.buckets, oldKey)
			s.orderedRemove(oldKey)
		}
	}
	newBucket := s.buckets[newKey] == nil
	if newBucket {
		s.buckets[newKey] = make(map[uint64]struct{})
	}
	s.buckets[newKey][id] = struct{}{}
	s.reverse[id] = newKey
	s.observe(newValue)
	if newBucket {
		s.orderedInsert(newKey, normVal)
	}
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

// conflict reports whether value is already mapped to a live id other than the
// given one. Used to enforce uniqueness before a write is applied. It only
// considers other ids, so re-writing a record's own existing value is allowed.
// Goroutine-safe.
func (s *SecondaryIndex) conflict(value string, id uint64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for existing := range s.buckets[value] {
		if existing != id {
			return true
		}
	}
	return false
}

// LookupRange returns the candidate IDs whose indexed field satisfies a range
// predicate (op ∈ {gt,gte,lt,lte}) against queryVal, and whether the index could
// serve the query. It serves the query only when the field is homogeneous and
// its type matches the query value's type; otherwise it returns ok=false and the
// caller falls back to a full scan. The returned ids are a candidate set that
// the caller must still validate with the filter — for a homogeneous field they
// already match, but re-checking keeps results identical to the scan path.
func (s *SecondaryIndex) LookupRange(op query.Op, queryVal any) (ids []uint64, ok bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	switch s.kind {
	case indexEmpty:
		return nil, true // no indexed values → no matches, but the index answered
	case indexMixed:
		return nil, false // ordering undefined → let the caller full-scan
	}
	qk, qv := valKind(queryVal)
	if qk != s.kind {
		return nil, false // comparing across types → full-scan for scan-identical results
	}

	n := len(s.sorted)
	var lo, hi int
	switch op {
	case query.OpGt:
		lo = sort.Search(n, func(i int) bool { return query.Compare(s.sorted[i].val, qv) > 0 })
		hi = n
	case query.OpGte:
		lo = sort.Search(n, func(i int) bool { return query.Compare(s.sorted[i].val, qv) >= 0 })
		hi = n
	case query.OpLt:
		lo = 0
		hi = sort.Search(n, func(i int) bool { return query.Compare(s.sorted[i].val, qv) >= 0 })
	case query.OpLte:
		lo = 0
		hi = sort.Search(n, func(i int) bool { return query.Compare(s.sorted[i].val, qv) > 0 })
	default:
		return nil, false
	}

	for _, entry := range s.sorted[lo:hi] {
		for id := range s.buckets[entry.key] {
			ids = append(ids, id)
		}
	}
	return ids, true
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
	freshKind := indexEmpty
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
			vk, _ := valKind(val)
			freshKind = mergeKind(freshKind, vk)
		}
	}

	s.mu.Lock()
	s.buckets = fresh
	s.reverse = freshRev
	s.kind = freshKind
	s.sorted = buildSorted(fresh, freshKind)
	s.mu.Unlock()
	return nil
}

// buildSorted materialises the ordered view from a bucket map for a homogeneous
// kind. Returns nil when the kind is mixed/empty (range serving disabled).
func buildSorted(buckets map[string]map[uint64]struct{}, kind indexKind) []orderedKey {
	if kind == indexMixed || kind == indexEmpty {
		return nil
	}
	sorted := make([]orderedKey, 0, len(buckets))
	for key := range buckets {
		sorted = append(sorted, orderedKey{key: key, val: valForKind(kind, key)})
	}
	sort.Slice(sorted, func(i, j int) bool { return orderLessKey(sorted[i], sorted[j]) })
	return sorted
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
	kind := s.kind.String()
	unique := s.unique
	s.mu.RUnlock()

	payload, err := json.Marshal(bucketsJSON)
	if err != nil {
		return fmt.Errorf("sidx: marshal: %w", err)
	}
	sum := sha256.Sum256(payload)

	f := sidxFile{
		Field:    s.field,
		Unique:   unique,
		Kind:     kind,
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

	// Restore the unique flag from parsed metadata before the checksum check so
	// it is preserved even when the caller has to rebuild from a stale file
	// (rebuild only touches the buckets, not this flag).
	s.mu.Lock()
	s.unique = f.Unique
	s.mu.Unlock()

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

	kind := parseIndexKind(f.Kind)
	if kind == indexEmpty && len(fresh) > 0 {
		// Legacy file without a persisted kind: infer it from the keys so range
		// queries still work. A field of numeric-looking strings would be read
		// as numeric until the next rebuild — a rare, self-correcting edge.
		kind = inferKind(fresh)
	}

	s.mu.Lock()
	s.buckets = fresh
	s.reverse = freshRev
	s.kind = kind
	s.sorted = buildSorted(fresh, kind)
	s.mu.Unlock()
	return nil
}

// inferKind guesses a homogeneous kind from bucket keys: numeric if every key
// parses as a float, otherwise string.
func inferKind(buckets map[string]map[uint64]struct{}) indexKind {
	for key := range buckets {
		if _, err := strconv.ParseFloat(key, 64); err != nil {
			return indexString
		}
	}
	return indexNumeric
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

// filterValueTyped decodes a JSON-encoded filter comparison value to its typed
// Go value (float64/string/…) for range comparison. Invalid JSON is treated as
// a plain string.
func filterValueTyped(filterValue string) any {
	var v any
	if err := json.Unmarshal([]byte(filterValue), &v); err != nil {
		return filterValue
	}
	return v
}

// sidxFilePath returns the disk path for a secondary index on field.
func sidxFilePath(dir, field string) string {
	return fmt.Sprintf("%s/sidx_%s.json", dir, field)
}
