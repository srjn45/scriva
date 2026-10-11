package engine

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/srjn45/scriva/store"
)

// Replication protocol versions, negotiated per Replicate stream
// (docs/design-xtx-replication.md §2).
const (
	// ReplicationProtocolV1 is the legacy protocol: one ReplicationRecord per
	// committed entry, one LSN per entry. A follower that advertises nothing is
	// treated as v1.
	ReplicationProtocolV1 uint32 = 1
	// ReplicationProtocolV2 adds the cross-collection transaction envelope: one
	// record, one LSN, many ordered participant operations.
	ReplicationProtocolV2 uint32 = 2
)

// DefaultMaxEnvelopeBytes bounds the published payload (sum of encoded entry
// data) of one transaction envelope when replication is enabled. A commit
// whose envelope would exceed it is rejected with ErrXTxTooLarge before
// anything is written, so a committed transaction can always be published.
const DefaultMaxEnvelopeBytes = 32 << 20

// envelopeVersion is the version stamped into the digest preimage; changing the
// canonical encoding requires a new value.
const envelopeVersion = 2

var (
	// ErrEnvelopeDigest: the envelope's digest does not match its contents
	// (corruption in transit or tampering). The receiver must not apply it.
	ErrEnvelopeDigest = errors.New("engine: replication envelope digest mismatch")
	// ErrEnvelopeInvalid: the envelope is structurally invalid (empty, bad
	// order, unknown op) or could not be decoded.
	ErrEnvelopeInvalid = errors.New("engine: replication envelope is invalid")
)

// TxEnvelopeOp is one participant operation of a replicated transaction: the
// collection it targets and the committed entry (id, op, revision, data,
// expiry, timestamp, encryption epoch) exactly as the leader wrote it.
type TxEnvelopeOp struct {
	Collection string
	Entry      store.Entry

	dataJSON []byte // cached canonical JSON of Entry.Data (nil when Data is nil)
}

// TxEnvelope is a cross-collection transaction shipped as one indivisible
// replication record (protocol v2). Ops are in the leader's canonical commit
// order: participants ascending by collection name (bytewise), each
// participant's operations in staging order. Digest is SHA-256 over the
// canonical encoding of everything except the LSN, which the replication
// stream carries as framing.
type TxEnvelope struct {
	TxID   string
	Key    string // client idempotency key; "" when none
	Ops    []TxEnvelopeOp
	Digest [sha256.Size]byte
}

// canonicalData round-trips data through JSON so it holds exactly the types a
// receiver reconstructs from the wire (float64 numbers, map[string]any), and
// returns the canonical encoding used by the digest.
func canonicalData(data map[string]any) (map[string]any, []byte, error) {
	if data == nil {
		return nil, nil, nil
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return nil, nil, err
	}
	var norm map[string]any
	if err := json.Unmarshal(raw, &norm); err != nil {
		return nil, nil, err
	}
	// json.Marshal sorts map keys, so re-marshalling the normalized form is the
	// canonical byte sequence on both sides.
	canon, err := json.Marshal(norm)
	if err != nil {
		return nil, nil, err
	}
	return norm, canon, nil
}

// prepareEnvelopeOps normalizes ops for publication and returns the payload
// size (sum of canonical data bytes). It does not compute the digest, so it
// can run before the txid is known.
func prepareEnvelopeOps(ops []TxEnvelopeOp) ([]TxEnvelopeOp, int64, error) {
	out := make([]TxEnvelopeOp, len(ops))
	var size int64
	for i, op := range ops {
		norm, canon, err := canonicalData(op.Entry.Data)
		if err != nil {
			return nil, 0, fmt.Errorf("%w: op %d: %v", ErrEnvelopeInvalid, i, err)
		}
		op.Entry.Data = norm
		op.dataJSON = canon
		size += int64(len(canon)) + int64(len(op.Collection)) + 64
		out[i] = op
	}
	return out, size, nil
}

// NewTxEnvelope validates ops, normalizes their data to its wire form and
// seals the envelope with its digest.
func NewTxEnvelope(txid, key string, ops []TxEnvelopeOp) (*TxEnvelope, error) {
	prepared, _, err := prepareEnvelopeOps(ops)
	if err != nil {
		return nil, err
	}
	return sealEnvelope(txid, key, prepared)
}

func sealEnvelope(txid, key string, prepared []TxEnvelopeOp) (*TxEnvelope, error) {
	env := &TxEnvelope{TxID: txid, Key: key, Ops: prepared}
	if err := env.validate(); err != nil {
		return nil, err
	}
	env.Digest = env.computeDigest()
	return env, nil
}

// validate checks structure: a txid, at least one op, known ops, and the
// canonical participant order (collections ascending, each collection's ops
// contiguous).
func (e *TxEnvelope) validate() error {
	if e.TxID == "" {
		return fmt.Errorf("%w: empty txid", ErrEnvelopeInvalid)
	}
	if len(e.Ops) == 0 {
		return fmt.Errorf("%w: no operations", ErrEnvelopeInvalid)
	}
	var last string
	for i, op := range e.Ops {
		if op.Collection == "" {
			return fmt.Errorf("%w: op %d has no collection", ErrEnvelopeInvalid, i)
		}
		switch op.Entry.Op {
		case store.OpInsert, store.OpUpdate, store.OpDelete:
		default:
			return fmt.Errorf("%w: op %d has unknown op %q", ErrEnvelopeInvalid, i, op.Entry.Op)
		}
		if i > 0 && op.Collection < last {
			return fmt.Errorf("%w: op %d collection %q breaks canonical order", ErrEnvelopeInvalid, i, op.Collection)
		}
		last = op.Collection
	}
	seen := map[string]bool{}
	prev := ""
	for i, op := range e.Ops {
		if i > 0 && op.Collection != prev && seen[op.Collection] {
			return fmt.Errorf("%w: collection %q is not contiguous", ErrEnvelopeInvalid, op.Collection)
		}
		seen[op.Collection] = true
		prev = op.Collection
	}
	return nil
}

// computeDigest hashes the canonical encoding:
//
//	"SCRIVA-XTX-ENV\x00" u32le(version) lp(txid) lp(key) u32le(nOps)
//	per op: u32le(index) lp(collection) lp(op) u64le(id) u64le(rev)
//	        i64le(expires_at) i64le(ts.sec) u32le(ts.nsec) u64le(epoch)
//	        byte(hasData) lp(canonical-json(data))
//
// where lp(x) is u32le(len) followed by the bytes.
func (e *TxEnvelope) computeDigest() [sha256.Size]byte {
	h := sha256.New()
	var buf bytes.Buffer
	lp := func(b []byte) {
		var n [4]byte
		binary.LittleEndian.PutUint32(n[:], uint32(len(b)))
		buf.Write(n[:])
		buf.Write(b)
	}
	u32 := func(v uint32) {
		var b [4]byte
		binary.LittleEndian.PutUint32(b[:], v)
		buf.Write(b[:])
	}
	u64 := func(v uint64) {
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], v)
		buf.Write(b[:])
	}
	buf.WriteString("SCRIVA-XTX-ENV\x00")
	u32(envelopeVersion)
	lp([]byte(e.TxID))
	lp([]byte(e.Key))
	u32(uint32(len(e.Ops)))
	for i, op := range e.Ops {
		en := op.Entry
		u32(uint32(i))
		lp([]byte(op.Collection))
		lp([]byte(en.Op))
		u64(en.ID)
		u64(en.Rev)
		u64(uint64(en.ExpiresAt))
		ts := en.Ts.UTC()
		u64(uint64(ts.Unix()))
		u32(uint32(ts.Nanosecond()))
		u64(en.Epoch)
		if en.Data == nil {
			buf.WriteByte(0)
			lp(nil)
		} else {
			buf.WriteByte(1)
			dj := op.dataJSON
			if dj == nil {
				dj, _ = json.Marshal(en.Data)
			}
			lp(dj)
		}
		_, _ = h.Write(buf.Bytes())
		buf.Reset()
	}
	// Flush the header-only case (no ops cannot occur after validate, but keep
	// the function total).
	_, _ = h.Write(buf.Bytes())
	var out [sha256.Size]byte
	copy(out[:], h.Sum(nil))
	return out
}

// Verify recomputes the digest from the envelope's contents and compares it in
// constant time. It also re-checks structure. A receiver must call Verify
// before acting on an envelope.
func (e *TxEnvelope) Verify() error {
	if err := e.validate(); err != nil {
		return err
	}
	want := e.computeDigest()
	if subtle.ConstantTimeCompare(want[:], e.Digest[:]) != 1 {
		return fmt.Errorf("%w: tx %s", ErrEnvelopeDigest, e.TxID)
	}
	return nil
}

// Weight is the number of entries the envelope carries; the replication ring
// and subscriber buffers account an envelope as this many entries.
func (e *TxEnvelope) Weight() int { return len(e.Ops) }

// Participants returns the participant collections in canonical order with the
// number of operations each contributes (the stamp "n" of its run).
func (e *TxEnvelope) Participants() []TxEnvelopeParticipant {
	var out []TxEnvelopeParticipant
	for _, op := range e.Ops {
		if n := len(out); n > 0 && out[n-1].Collection == op.Collection {
			out[n-1].Ops++
			continue
		}
		out = append(out, TxEnvelopeParticipant{Collection: op.Collection, Ops: 1})
	}
	return out
}

// TxEnvelopeParticipant is one participant of an envelope.
type TxEnvelopeParticipant struct {
	Collection string
	Ops        int
}

// envelopeJSON is the self-describing byte encoding of an envelope
// (MarshalBinary); the replication wire uses the protobuf mapping instead.
type envelopeJSON struct {
	V      int              `json:"v"`
	TxID   string           `json:"tx"`
	Key    string           `json:"key,omitempty"`
	Ops    []envelopeOpJSON `json:"ops"`
	Digest string           `json:"digest"`
}

type envelopeOpJSON struct {
	Collection string         `json:"c"`
	Op         store.Op       `json:"op"`
	ID         uint64         `json:"id"`
	Rev        uint64         `json:"rev,omitempty"`
	Data       map[string]any `json:"data,omitempty"`
	HasData    bool           `json:"has_data,omitempty"`
	ExpiresAt  int64          `json:"exp,omitempty"`
	TsSec      int64          `json:"ts_s"`
	TsNsec     int32          `json:"ts_n"`
	Epoch      uint64         `json:"epoch,omitempty"`
}

// MarshalBinary encodes the envelope as one JSON document.
func (e *TxEnvelope) MarshalBinary() ([]byte, error) {
	j := envelopeJSON{V: envelopeVersion, TxID: e.TxID, Key: e.Key, Digest: hex.EncodeToString(e.Digest[:])}
	for _, op := range e.Ops {
		en := op.Entry
		ts := en.Ts.UTC()
		j.Ops = append(j.Ops, envelopeOpJSON{
			Collection: op.Collection, Op: en.Op, ID: en.ID, Rev: en.Rev,
			Data: en.Data, HasData: en.Data != nil, ExpiresAt: en.ExpiresAt,
			TsSec: ts.Unix(), TsNsec: int32(ts.Nanosecond()), Epoch: en.Epoch,
		})
	}
	return json.Marshal(j)
}

// UnmarshalTxEnvelope decodes MarshalBinary output and verifies the digest.
// Truncated, malformed or tampered input yields ErrEnvelopeInvalid or
// ErrEnvelopeDigest.
func UnmarshalTxEnvelope(b []byte) (*TxEnvelope, error) {
	var j envelopeJSON
	dec := json.NewDecoder(bytes.NewReader(b))
	if err := dec.Decode(&j); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrEnvelopeInvalid, err)
	}
	if j.V != envelopeVersion {
		return nil, fmt.Errorf("%w: unsupported version %d", ErrEnvelopeInvalid, j.V)
	}
	raw, err := hex.DecodeString(j.Digest)
	if err != nil || len(raw) != sha256.Size {
		return nil, fmt.Errorf("%w: bad digest field", ErrEnvelopeInvalid)
	}
	env := &TxEnvelope{TxID: j.TxID, Key: j.Key}
	copy(env.Digest[:], raw)
	for _, o := range j.Ops {
		data := o.Data
		if o.HasData && data == nil {
			data = map[string]any{}
		}
		env.Ops = append(env.Ops, TxEnvelopeOp{
			Collection: o.Collection,
			Entry: store.Entry{
				ID: o.ID, Op: o.Op, Rev: o.Rev, Data: data, ExpiresAt: o.ExpiresAt,
				Ts: time.Unix(o.TsSec, int64(o.TsNsec)).UTC(), Epoch: o.Epoch,
			},
		})
	}
	if err := env.Verify(); err != nil {
		return nil, err
	}
	return env, nil
}
