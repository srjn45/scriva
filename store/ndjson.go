// Package store handles low-level NDJSON encoding and decoding for FileDB
// segment entries. Each entry is a single JSON object terminated by a newline.
package store

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"time"
)

// Op represents the type of operation recorded in a segment entry.
type Op string

const (
	OpInsert Op = "insert"
	OpUpdate Op = "update"
	OpDelete Op = "delete"
)

// ErrCorruptEntry is returned by Decode when an entry carries a crc that does
// not match its contents — evidence of on-disk bit-rot. It wraps the id so
// callers can report which record failed.
var ErrCorruptEntry = errors.New("store: entry checksum mismatch")

// crc32cTable is the Castagnoli (CRC32C) polynomial table, hardware-accelerated
// on most platforms. Used for per-entry integrity checks.
var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// Entry is one line in a segment file. It captures the operation, the record
// id, a timestamp, and — for insert/update — the full record data.
type Entry struct {
	ID uint64    `json:"id"`
	Op Op        `json:"op"`
	Ts time.Time `json:"ts"`
	// Rev is the record's monotonic revision: 1 on insert, +1 on each update.
	// It is omitted when zero, so segment lines written before revisions existed
	// decode as rev 0 — fully backward compatible.
	Rev  uint64         `json:"rev,omitempty"`
	Data map[string]any `json:"data,omitempty"` // nil for OpDelete
	// CRC is an optional CRC32C (Castagnoli) checksum over the entry's id, op,
	// rev, and canonical data. It is written by Encode and verified by Decode
	// when present. A nil CRC marks a legacy line written before checksums
	// existed; such lines are decoded without verification for backward
	// compatibility.
	CRC *uint32 `json:"crc,omitempty"`
}

// checksum computes the CRC32C over the entry's id, op, rev, and canonical
// data. The timestamp and the crc field itself are excluded so the value is
// stable across encode/decode round-trips. Rev bytes are folded in only when
// non-zero, so a legacy line (rev 0, written before revisions existed) hashes
// exactly as it did originally and its stored crc still verifies. Data is
// canonicalised via json.Marshal, which sorts map keys deterministically.
func checksum(e Entry) (uint32, error) {
	h := crc32.New(crc32cTable)
	var idbuf [8]byte
	binary.LittleEndian.PutUint64(idbuf[:], e.ID)
	_, _ = h.Write(idbuf[:])
	_, _ = h.Write([]byte(e.Op))
	if e.Rev != 0 {
		var revbuf [8]byte
		binary.LittleEndian.PutUint64(revbuf[:], e.Rev)
		_, _ = h.Write(revbuf[:])
	}
	if e.Data != nil {
		b, err := json.Marshal(e.Data)
		if err != nil {
			return 0, fmt.Errorf("store: checksum entry id=%d: %w", e.ID, err)
		}
		_, _ = h.Write(b)
	}
	return h.Sum32(), nil
}

// Encode serialises e as a JSON object followed by a newline, stamping it with
// a CRC32C checksum. The returned slice is ready to be appended directly to a
// segment file.
func Encode(e Entry) ([]byte, error) {
	sum, err := checksum(e)
	if err != nil {
		return nil, err
	}
	e.CRC = &sum
	b, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("store: encode entry id=%d: %w", e.ID, err)
	}
	return append(b, '\n'), nil
}

// Decode parses a single JSON line (the trailing newline is ignored). If the
// entry carries a crc, its contents are verified against it; a mismatch returns
// ErrCorruptEntry. Legacy lines without a crc are returned unverified.
func Decode(line []byte) (Entry, error) {
	var e Entry
	if err := json.Unmarshal(line, &e); err != nil {
		return Entry{}, fmt.Errorf("store: decode entry: %w", err)
	}
	if e.CRC != nil {
		want := *e.CRC
		got, err := checksum(e)
		if err != nil {
			return Entry{}, err
		}
		if got != want {
			return Entry{}, fmt.Errorf("%w: id=%d (want %08x, got %08x)", ErrCorruptEntry, e.ID, want, got)
		}
	}
	return e, nil
}

// NewInsert returns an Entry for an insert operation.
func NewInsert(id uint64, data map[string]any) Entry {
	return Entry{ID: id, Op: OpInsert, Ts: time.Now().UTC(), Data: data}
}

// NewUpdate returns an Entry for an update operation.
func NewUpdate(id uint64, data map[string]any) Entry {
	return Entry{ID: id, Op: OpUpdate, Ts: time.Now().UTC(), Data: data}
}

// NewDelete returns an Entry for a delete tombstone (no data).
func NewDelete(id uint64) Entry {
	return Entry{ID: id, Op: OpDelete, Ts: time.Now().UTC()}
}
