package engine

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"time"

	"github.com/srjn45/scriva/store"
)

// TxStamp is the `tx` object carried by a stamped segment entry (design §3.1).
type TxStamp struct {
	T string `json:"t"` // txid
	I uint32 `json:"i"` // 0-based index within this participant's run
	N uint32 `json:"n"` // total ops in this participant's run
}

// stampedEntry is a transaction-aware segment entry: a store.Entry plus a Tx stamp (§3.1).
// The JSON field order is fixed for deterministic wire encoding.
type stampedEntry struct {
	ID        uint64         `json:"id"`
	Op        store.Op       `json:"op"`
	Ts        string         `json:"ts"`
	Rev       uint64         `json:"rev,omitempty"`
	Data      map[string]any `json:"data,omitempty"`
	Epoch     uint64         `json:"epoch,omitempty"`
	ExpiresAt int64          `json:"expires_at,omitempty"`
	Tx        TxStamp        `json:"tx"`
	CRC       uint32         `json:"crc"`
}

// v1InputBytes reproduces the byte sequence that legacy store.Encode feeds to CRC32C.
func v1InputBytes(id uint64, op store.Op, rev uint64, expires int64, epoch uint64, data map[string]any) ([]byte, error) {
	var b bytes.Buffer
	var u [8]byte
	binary.LittleEndian.PutUint64(u[:], id)
	b.Write(u[:])
	b.WriteString(string(op))
	if rev != 0 {
		binary.LittleEndian.PutUint64(u[:], rev)
		b.Write(u[:])
	}
	if expires != 0 {
		binary.LittleEndian.PutUint64(u[:], uint64(expires))
		b.Write(u[:])
	}
	if epoch != 0 {
		binary.LittleEndian.PutUint64(u[:], epoch)
		b.Write(u[:])
	}
	if data != nil {
		j, err := json.Marshal(data)
		if err != nil {
			return nil, err
		}
		b.Write(j)
	}
	return b.Bytes(), nil
}

// checksumV1 computes the legacy entry checksum.
func checksumV1(id uint64, op store.Op, rev uint64, expires int64, epoch uint64, data map[string]any) (uint32, error) {
	in, err := v1InputBytes(id, op, rev, expires, epoch, data)
	if err != nil {
		return 0, err
	}
	return crc32.Checksum(in, xtxCRCTable), nil
}

// checksumV2 computes the domain-separated checksum of design §3.2:
// CRC32C("XTX1\x00" ‖ byte(len(tx.t)) ‖ tx.t ‖ u32le(tx.i) ‖ u32le(tx.n) ‖ v1 input)
func checksumV2(id uint64, op store.Op, rev uint64, expires int64, epoch uint64, data map[string]any, tx TxStamp) (uint32, error) {
	if l := len(tx.T); l == 0 || l > xtxMaxTxIDLen {
		return 0, fmt.Errorf("%w: txid length %d", ErrBadStamp, l)
	}
	in, err := v1InputBytes(id, op, rev, expires, epoch, data)
	if err != nil {
		return 0, err
	}
	var b bytes.Buffer
	b.WriteString("XTX1\x00")
	b.WriteByte(byte(len(tx.T)))
	b.WriteString(tx.T)
	var u [4]byte
	binary.LittleEndian.PutUint32(u[:], tx.I)
	b.Write(u[:])
	binary.LittleEndian.PutUint32(u[:], tx.N)
	b.Write(u[:])
	b.Write(in)
	return crc32.Checksum(b.Bytes(), xtxCRCTable), nil
}

// encodeStamped serializes a stamped entry as one NDJSON line with a v2 checksum.
func encodeStamped(e stampedEntry) ([]byte, error) {
	if e.Tx.N == 0 || e.Tx.I >= e.Tx.N {
		return nil, fmt.Errorf("%w: i=%d n=%d", ErrBadStamp, e.Tx.I, e.Tx.N)
	}
	sum, err := checksumV2(e.ID, e.Op, e.Rev, e.ExpiresAt, e.Epoch, e.Data, e.Tx)
	if err != nil {
		return nil, err
	}
	e.CRC = sum
	b, err := json.Marshal(e)
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

// parseStamped decodes one NDJSON line and validates it under the v2 checksum rules.
// Spliced stamps (v1 checksums) and corrupt lines are rejected.
func parseStamped(line []byte) (stampedEntry, error) {
	var raw struct {
		stampedEntry
		CRC *uint32 `json:"crc"`
	}
	if err := json.Unmarshal(bytes.TrimRight(line, "\n"), &raw); err != nil {
		return stampedEntry{}, fmt.Errorf("engine: decode stamped: %w", err)
	}
	e := raw.stampedEntry
	if e.Tx.T == "" {
		return stampedEntry{}, fmt.Errorf("%w: no tx object", ErrBadStamp)
	}
	if e.Tx.N == 0 || e.Tx.I >= e.Tx.N {
		return stampedEntry{}, fmt.Errorf("%w: i=%d n=%d", ErrBadStamp, e.Tx.I, e.Tx.N)
	}
	if raw.CRC == nil {
		return stampedEntry{}, ErrMissingCRC
	}
	e.CRC = *raw.CRC
	v2, err := checksumV2(e.ID, e.Op, e.Rev, e.ExpiresAt, e.Epoch, e.Data, e.Tx)
	if err != nil {
		return stampedEntry{}, err
	}
	if v2 != e.CRC {
		if v1, err := checksumV1(e.ID, e.Op, e.Rev, e.ExpiresAt, e.Epoch, e.Data); err == nil && v1 == e.CRC {
			return stampedEntry{}, ErrV1CRCOnStamp
		}
		return stampedEntry{}, fmt.Errorf("%w: id=%d", store.ErrCorruptEntry, e.ID)
	}
	return e, nil
}

// decodeSegmentLine decodes either a plain entry or a stamped entry.
func decodeSegmentLine(line []byte) (store.Entry, *TxStamp, error) {
	trimmed := bytes.TrimRight(line, "\n")
	if bytes.Contains(trimmed, []byte(`"tx"`)) {
		var probe struct {
			Tx *TxStamp `json:"tx"`
		}
		if err := json.Unmarshal(trimmed, &probe); err == nil && probe.Tx != nil && probe.Tx.T != "" {
			se, err := parseStamped(trimmed)
			if err != nil {
				return store.Entry{}, nil, err
			}
			var ts time.Time
			if se.Ts != "" {
				ts, _ = time.Parse(time.RFC3339Nano, se.Ts)
			}
			entry := store.Entry{
				ID:        se.ID,
				Op:        se.Op,
				Ts:        ts,
				Rev:       se.Rev,
				Data:      se.Data,
				Epoch:     se.Epoch,
				ExpiresAt: se.ExpiresAt,
				CRC:       &se.CRC,
			}
			return entry, &se.Tx, nil
		}
	}
	e, err := store.Decode(line)
	if err != nil {
		return store.Entry{}, nil, err
	}
	return e, nil, nil
}

// entryVisible reports whether an entry is visible given the decisions map.
// A plain entry (tx == nil or empty) is always visible.
// A stamped entry is visible iff its txid is committed.
func entryVisible(tx *TxStamp, decisions map[string]string) bool {
	if tx == nil || tx.T == "" {
		return true
	}
	if decisions == nil {
		return false
	}
	d := decisions[tx.T]
	return d == xtxKindCommit || d == "retire-commit"
}
