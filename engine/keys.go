package engine

import (
	"errors"
	"fmt"
	"time"
)

// KeyField is the reserved data field that stores a caller-supplied string
// primary key. It is populated only by the keyed API (InsertWithKey,
// UpdateByKey) and enforced unique by a mandatory secondary index created
// lazily on the first keyed write. Because the key lives in the record's data
// it round-trips through segments, compaction, index rebuild, and reopen for
// free, and surfaces naturally in Watch events.
const KeyField = "_key"

// ErrReservedField is returned by Insert and Update when the supplied data sets
// a reserved field (currently _key) directly. String keys are settable only
// through the keyed API. Callers can match it with errors.Is.
var ErrReservedField = errors.New("engine: reserved field")

// ErrKeyNotFound is returned by FindByKey, UpdateByKey, and DeleteByKey when no
// live record carries the given string key. Callers can match it with
// errors.Is.
var ErrKeyNotFound = errors.New("engine: key not found")

// reservedFieldErr builds the ErrReservedField error with guidance on the
// correct API to use.
func reservedFieldErr() error {
	return fmt.Errorf("%w: %q is settable only via InsertWithKey/UpdateByKey", ErrReservedField, KeyField)
}

// InsertWithKey inserts data under the caller-supplied string key. It stamps the
// key into the reserved _key field, ensures the collection's unique _key index
// exists (created lazily on the first keyed write), and appends the record. A
// key already held by a live record is rejected with ErrDuplicateKey. Supplying
// _key inside data is rejected with ErrReservedField — the key argument is the
// only way to set it.
func (c *Collection) InsertWithKey(key string, data map[string]any) (uint64, time.Time, error) {
	if _, ok := data[KeyField]; ok {
		return 0, time.Time{}, reservedFieldErr()
	}
	if err := c.ensureKeyIndex(); err != nil {
		return 0, time.Time{}, err
	}
	return c.insert(stampKey(data, key))
}

// FindByKey returns the data and timestamp for the record carrying key. The
// lookup is O(1) via the unique _key index. A missing key yields
// ErrKeyNotFound.
func (c *Collection) FindByKey(key string) (map[string]any, time.Time, error) {
	id, err := c.resolveKey(key)
	if err != nil {
		return nil, time.Time{}, err
	}
	return c.FindByID(id)
}

// UpdateByKey overwrites the data for the record carrying key, preserving the
// key itself. Supplying _key inside data is rejected with ErrReservedField; a
// missing key yields ErrKeyNotFound.
func (c *Collection) UpdateByKey(key string, data map[string]any) (time.Time, error) {
	if _, ok := data[KeyField]; ok {
		return time.Time{}, reservedFieldErr()
	}
	id, err := c.resolveKey(key)
	if err != nil {
		return time.Time{}, err
	}
	return c.update(id, stampKey(data, key))
}

// DeleteByKey removes the record carrying key. A missing key yields
// ErrKeyNotFound.
func (c *Collection) DeleteByKey(key string) error {
	id, err := c.resolveKey(key)
	if err != nil {
		return err
	}
	return c.Delete(id)
}

// ensureKeyIndex lazily creates the mandatory unique index on the reserved _key
// field. EnsureUniqueIndex is idempotent, so this is cheap after the first call.
func (c *Collection) ensureKeyIndex() error {
	return c.EnsureUniqueIndex(KeyField)
}

// resolveKey returns the uint64 id of the live record carrying key. It relies on
// the unique _key index, which maps a key to at most one id, so the lookup is
// O(1). A missing key (or an index that does not exist because no keyed write
// has happened yet) yields ErrKeyNotFound.
func (c *Collection) resolveKey(key string) (uint64, error) {
	ids, ok := c.IndexLookup(KeyField, key)
	if !ok || len(ids) == 0 {
		return 0, fmt.Errorf("%w: %q", ErrKeyNotFound, key)
	}
	// The _key index is unique, so at most one id maps to the key.
	return ids[0], nil
}

// stampKey returns a copy of data with the reserved _key field set to key. The
// input map is never mutated, so a keyed write does not leak the reserved field
// back into the caller's map.
func stampKey(data map[string]any, key string) map[string]any {
	out := make(map[string]any, len(data)+1)
	for k, v := range data {
		out[k] = v
	}
	out[KeyField] = key
	return out
}
