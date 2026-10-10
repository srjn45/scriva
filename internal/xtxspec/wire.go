package xtxspec

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"sort"

	"github.com/srjn45/scriva/store"
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Errors returned by the parsers.
var (
	ErrBadCRC       = errors.New("xtxspec: checksum mismatch")
	ErrV1CRCOnStamp = errors.New("xtxspec: stamped entry carries a v1 checksum")
	ErrMissingCRC   = errors.New("xtxspec: stamped entry has no crc")
	ErrBadStamp     = errors.New("xtxspec: invalid tx stamp")
)

// MaxTxIDLen is the §3.1 bound on tx.t.
const MaxTxIDLen = 64

// Tx is the `tx` object of a stamped entry (design §3.1).
type Tx struct {
	T string `json:"t"` // txid
	I uint32 `json:"i"` // index within this participant's run
	N uint32 `json:"n"` // run length
}

// Stamped is a transaction-aware segment entry: a store.Entry plus a Tx stamp.
// The JSON field order is fixed so that encoding is byte-deterministic.
type Stamped struct {
	ID        uint64         `json:"id"`
	Op        store.Op       `json:"op"`
	Ts        string         `json:"ts"`
	Rev       uint64         `json:"rev,omitempty"`
	Data      map[string]any `json:"data,omitempty"`
	Epoch     uint64         `json:"epoch,omitempty"`
	ExpiresAt int64          `json:"expires_at,omitempty"`
	Tx        Tx             `json:"tx"`
	CRC       uint32         `json:"crc"`
}

// v1Input reproduces exactly the byte sequence store's checksum() feeds to
// CRC32C (id, op, rev?, expires_at?, epoch?, data?). A test pins it to
// store.Encode so the two cannot drift.
func v1Input(id uint64, op store.Op, rev uint64, expires int64, epoch uint64, data map[string]any) ([]byte, error) {
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

// ChecksumV1 is the legacy entry checksum (what every released binary computes).
func ChecksumV1(e Stamped) (uint32, error) {
	in, err := v1Input(e.ID, e.Op, e.Rev, e.ExpiresAt, e.Epoch, e.Data)
	if err != nil {
		return 0, err
	}
	return crc32.Checksum(in, crcTable), nil
}

// ChecksumV2 is the domain-separated checksum of design §3.2:
//
//	CRC32C("XTX1\x00" ‖ u8(len(tx.t)) ‖ tx.t ‖ u32le(i) ‖ u32le(n) ‖ v1 input)
//
// The length prefix is one byte because tx.t is bounded by MaxTxIDLen.
func ChecksumV2(e Stamped) (uint32, error) {
	if l := len(e.Tx.T); l == 0 || l > MaxTxIDLen {
		return 0, fmt.Errorf("%w: txid length %d", ErrBadStamp, l)
	}
	in, err := v1Input(e.ID, e.Op, e.Rev, e.ExpiresAt, e.Epoch, e.Data)
	if err != nil {
		return 0, err
	}
	var b bytes.Buffer
	b.WriteString("XTX1\x00")
	b.WriteByte(byte(len(e.Tx.T)))
	b.WriteString(e.Tx.T)
	var u [4]byte
	binary.LittleEndian.PutUint32(u[:], e.Tx.I)
	b.Write(u[:])
	binary.LittleEndian.PutUint32(u[:], e.Tx.N)
	b.Write(u[:])
	b.Write(in)
	return crc32.Checksum(b.Bytes(), crcTable), nil
}

// EncodeStamped serialises e as one NDJSON line with a v2 checksum.
func EncodeStamped(e Stamped) ([]byte, error) {
	if e.Tx.N == 0 || e.Tx.I >= e.Tx.N {
		return nil, fmt.Errorf("%w: i=%d n=%d", ErrBadStamp, e.Tx.I, e.Tx.N)
	}
	sum, err := ChecksumV2(e)
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

// ParseStamped decodes one line and verifies it under the v2 rules: a line
// whose crc only verifies under v1 is rejected (a spliced stamp).
func ParseStamped(line []byte) (Stamped, error) {
	var raw struct {
		Stamped
		CRC *uint32 `json:"crc"`
	}
	if err := json.Unmarshal(bytes.TrimRight(line, "\n"), &raw); err != nil {
		return Stamped{}, fmt.Errorf("xtxspec: decode stamped: %w", err)
	}
	e := raw.Stamped
	if e.Tx.T == "" {
		return Stamped{}, fmt.Errorf("%w: no tx object", ErrBadStamp)
	}
	if e.Tx.N == 0 || e.Tx.I >= e.Tx.N {
		return Stamped{}, fmt.Errorf("%w: i=%d n=%d", ErrBadStamp, e.Tx.I, e.Tx.N)
	}
	if raw.CRC == nil {
		return Stamped{}, ErrMissingCRC
	}
	e.CRC = *raw.CRC
	v2, err := ChecksumV2(e)
	if err != nil {
		return Stamped{}, err
	}
	if v2 != e.CRC {
		if v1, err := ChecksumV1(e); err == nil && v1 == e.CRC {
			return Stamped{}, ErrV1CRCOnStamp
		}
		return Stamped{}, fmt.Errorf("%w: id=%d", ErrBadCRC, e.ID)
	}
	return e, nil
}

// ---------------------------------------------------------------- digests

// RunOp is the part of an op that the participant digest pins (§4.2).
type RunOp struct {
	ID  uint64
	Op  store.Op
	Rev uint64
}

// ParticipantDigest is `d`: SHA-256 over u32le(i)‖u64le(id)‖op‖0x00‖u64le(rev).
// It deliberately excludes data, epoch and offsets.
func ParticipantDigest(run []RunOp) string {
	h := sha256.New()
	var u4 [4]byte
	var u8 [8]byte
	for i, o := range run {
		binary.LittleEndian.PutUint32(u4[:], uint32(i))
		h.Write(u4[:])
		binary.LittleEndian.PutUint64(u8[:], o.ID)
		h.Write(u8[:])
		h.Write([]byte(o.Op))
		h.Write([]byte{0})
		binary.LittleEndian.PutUint64(u8[:], o.Rev)
		h.Write(u8[:])
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

// Part is one participant entry of a commit record.
type Part struct {
	C string `json:"c"`
	N uint32 `json:"n"`
	D string `json:"d"`
}

// SetDigest is `pd`: SHA-256 over name‖0x00‖u32le(n)‖d for each part, in
// canonical (bytewise-ascending name) order. parts must already be canonical.
func SetDigest(parts []Part) string {
	h := sha256.New()
	var u4 [4]byte
	for _, p := range parts {
		h.Write([]byte(p.C))
		h.Write([]byte{0})
		binary.LittleEndian.PutUint32(u4[:], p.N)
		h.Write(u4[:])
		h.Write([]byte(p.D))
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

// CanonicalParts sorts parts bytewise by name and rejects duplicates.
func CanonicalParts(parts []Part) ([]Part, error) {
	out := append([]Part(nil), parts...)
	sort.Slice(out, func(i, j int) bool { return out[i].C < out[j].C })
	for i := 1; i < len(out); i++ {
		if out[i].C == out[i-1].C {
			return nil, fmt.Errorf("xtxspec: duplicate participant %q", out[i].C)
		}
	}
	return out, nil
}
