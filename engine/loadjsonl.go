package engine

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

// LoadJSONL bulk-loads newline-delimited JSON (NDJSON) records from r, inserting
// each one through the normal write path: every record is appended to a segment,
// indexed (primary and every secondary index, including the unique _key index),
// emitted to Watch subscribers, and covered by the collection's durability/sync
// mode — exactly as an individual Insert/InsertWithKey would be. It returns the
// number of records loaded.
//
// keyField selects how records are addressed:
//
//   - keyField == ""   — each record is inserted with an engine-assigned uint64
//     id (plain Insert semantics). A record that carries the reserved _key field
//     is rejected with ErrReservedField.
//   - keyField != ""   — the named field is read from each record and used as the
//     record's caller-supplied string key (InsertWithKey semantics): the value
//     must be present and a string, and a key already held by another record —
//     whether already committed or appearing twice within this load — is rejected
//     with ErrDuplicateKey.
//
// The load is atomic: every line is parsed and validated first, then the whole
// batch is applied under a single write-lock critical section (reusing CommitTx),
// so a malformed line, a missing/non-string key, a reserved-field violation, or a
// duplicate key aborts the entire load before anything is written — the segments
// and indexes are left exactly as they were, with no partial application. Parse
// errors report the 1-based physical line number of the offending line. Blank and
// whitespace-only lines are skipped, so a trailing newline is not an error.
func (c *Collection) LoadJSONL(r io.Reader, keyField string) (n int, err error) {
	// Ensure the mandatory unique _key index exists before validating so a
	// duplicate key is caught by CommitTx's uniqueness pre-check. ensureKeyIndex
	// acquires c.mu internally, so it must run before we build the batch.
	if keyField != "" {
		if err := c.ensureKeyIndex(); err != nil {
			return 0, err
		}
	}

	// Parse phase: decode and validate every record up front. Nothing is written
	// until the whole input is known-good, which keeps the load all-or-nothing.
	datas, err := c.parseJSONL(r, keyField)
	if err != nil {
		return 0, err
	}
	if len(datas) == 0 {
		return 0, nil
	}

	// Apply phase: stage every record as an insert and commit the batch atomically
	// under one write lock. CommitTx pre-validates uniqueness across the batch and
	// against committed data, appends each entry, updates the primary and secondary
	// indexes, honours the sync mode, and emits one Watch event per record — so the
	// bulk path is indistinguishable from a run of individual inserts.
	ts := time.Now().UTC()
	ops := make([]txOp, len(datas))
	for i, d := range datas {
		ops[i] = txOp{kind: txOpInsert, id: c.ReserveID(), data: d, ts: ts}
	}
	if err := c.CommitTx(ops); err != nil {
		return 0, err
	}
	return len(datas), nil
}

// parseJSONL reads r line by line, decoding each non-blank line into a record and
// applying the keyField contract. It returns the fully-prepared per-record data
// maps (with _key stamped for keyed loads) or the first error, tagged with the
// 1-based physical line number.
func (c *Collection) parseJSONL(r io.Reader, keyField string) ([]map[string]any, error) {
	br := bufio.NewReader(r)
	var datas []map[string]any
	lineNo := 0

	for {
		line, readErr := br.ReadString('\n')
		if len(line) > 0 {
			lineNo++
			if trimmed := strings.TrimSpace(line); trimmed != "" {
				data, err := c.prepareRecord(trimmed, keyField, lineNo)
				if err != nil {
					return nil, err
				}
				datas = append(datas, data)
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				break
			}
			return nil, fmt.Errorf("collection: load: read line %d: %w", lineNo+1, readErr)
		}
	}
	return datas, nil
}

// prepareRecord decodes one NDJSON line and returns the data map to insert,
// enforcing the reserved-field and keyField rules. lineNo is the 1-based physical
// line number, used only for error context.
func (c *Collection) prepareRecord(line, keyField string, lineNo int) (map[string]any, error) {
	var rec map[string]any
	if err := json.Unmarshal([]byte(line), &rec); err != nil {
		return nil, fmt.Errorf("collection: load: line %d: %w", lineNo, err)
	}

	if keyField == "" {
		// Plain insert: reject smuggled reserved fields, mirroring Insert.
		if _, ok := rec[KeyField]; ok {
			return nil, fmt.Errorf("collection: load: line %d: %w", lineNo, reservedFieldErr())
		}
		return rec, nil
	}

	// Keyed insert: the named field supplies the record's string key.
	v, ok := rec[keyField]
	if !ok {
		return nil, fmt.Errorf("collection: load: line %d: missing key field %q", lineNo, keyField)
	}
	key, ok := v.(string)
	if !ok {
		return nil, fmt.Errorf("collection: load: line %d: key field %q must be a string, got %T", lineNo, keyField, v)
	}
	// Reject a stray _key that is not itself the key field, mirroring InsertWithKey.
	if keyField != KeyField {
		if _, ok := rec[KeyField]; ok {
			return nil, fmt.Errorf("collection: load: line %d: %w", lineNo, reservedFieldErr())
		}
	}
	return stampKey(rec, key), nil
}
