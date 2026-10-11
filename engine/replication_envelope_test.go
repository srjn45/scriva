package engine

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/srjn45/scriva/store"
)

func testEnvelope(t *testing.T) *TxEnvelope {
	t.Helper()
	ts := time.Date(2026, 5, 1, 12, 0, 0, 123456789, time.UTC)
	env, err := NewTxEnvelope("tx-1", "key-1", []TxEnvelopeOp{
		{Collection: "a", Entry: store.Entry{ID: 1, Op: store.OpInsert, Rev: 1, Ts: ts, Data: map[string]any{"n": 1, "s": "x"}}},
		{Collection: "a", Entry: store.Entry{ID: 2, Op: store.OpUpdate, Rev: 3, Ts: ts, Data: map[string]any{"k": []any{1, "two"}}, ExpiresAt: 99}},
		{Collection: "b", Entry: store.Entry{ID: 7, Op: store.OpDelete, Rev: 2, Ts: ts}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return env
}

func TestTxEnvelope_RoundTripAndVerify(t *testing.T) {
	t.Parallel()
	env := testEnvelope(t)
	if err := env.Verify(); err != nil {
		t.Fatal(err)
	}
	b, err := env.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	got, err := UnmarshalTxEnvelope(b)
	if err != nil {
		t.Fatal(err)
	}
	if got.Digest != env.Digest || got.TxID != "tx-1" || got.Key != "key-1" || len(got.Ops) != 3 {
		t.Fatalf("round trip mismatch: %+v", got)
	}
	if got.Ops[2].Entry.Data != nil {
		t.Fatal("delete op grew data")
	}
	parts := got.Participants()
	if len(parts) != 2 || parts[0] != (TxEnvelopeParticipant{"a", 2}) || parts[1] != (TxEnvelopeParticipant{"b", 1}) {
		t.Fatalf("participants: %+v", parts)
	}
}

func TestTxEnvelope_DigestStableAcrossWireTypes(t *testing.T) {
	t.Parallel()
	// A leader may hold int / int64 values; the wire delivers float64. The
	// digest must agree on both sides.
	mk := func(v any) *TxEnvelope {
		env, err := NewTxEnvelope("tx", "", []TxEnvelopeOp{{Collection: "c", Entry: store.Entry{
			ID: 1, Op: store.OpInsert, Rev: 1, Data: map[string]any{"v": v, "big": int64(1) << 60},
		}}})
		if err != nil {
			t.Fatal(err)
		}
		return env
	}
	if mk(5).Digest != mk(float64(5)).Digest {
		t.Fatal("digest differs between int and float64 representations")
	}
}

func TestTxEnvelope_CorruptedDigest(t *testing.T) {
	t.Parallel()
	env := testEnvelope(t)
	env.Digest[0] ^= 0xff
	if err := env.Verify(); !errors.Is(err, ErrEnvelopeDigest) {
		t.Fatalf("got %v, want ErrEnvelopeDigest", err)
	}
}

func TestTxEnvelope_TamperedContents(t *testing.T) {
	t.Parallel()
	for name, mut := range map[string]func(e *TxEnvelope){
		"data":  func(e *TxEnvelope) { e.Ops[0].Entry.Data = map[string]any{"n": float64(3), "s": "x"} },
		"rev":   func(e *TxEnvelope) { e.Ops[1].Entry.Rev++ },
		"id":    func(e *TxEnvelope) { e.Ops[0].Entry.ID++ },
		"txid":  func(e *TxEnvelope) { e.TxID = "tx-2" },
		"key":   func(e *TxEnvelope) { e.Key = "" },
		"coll":  func(e *TxEnvelope) { e.Ops[2].Collection = "c" },
		"drop":  func(e *TxEnvelope) { e.Ops = e.Ops[:2] },
		"ts":    func(e *TxEnvelope) { e.Ops[0].Entry.Ts = e.Ops[0].Entry.Ts.Add(time.Nanosecond) },
		"epoch": func(e *TxEnvelope) { e.Ops[0].Entry.Epoch = 4 },
		"swap-within-collection": func(e *TxEnvelope) {
			e.Ops[0], e.Ops[1] = e.Ops[1], e.Ops[0]
		},
	} {
		t.Run(name, func(t *testing.T) {
			env := testEnvelope(t)
			mut(env)
			if err := env.Verify(); !errors.Is(err, ErrEnvelopeDigest) {
				t.Fatalf("got %v, want ErrEnvelopeDigest", err)
			}
		})
	}
}

func TestTxEnvelope_ReorderedParticipantsInvalid(t *testing.T) {
	t.Parallel()
	env := testEnvelope(t)
	env.Ops[0], env.Ops[2] = env.Ops[2], env.Ops[0] // b, a, a
	if err := env.Verify(); !errors.Is(err, ErrEnvelopeInvalid) {
		t.Fatalf("got %v, want ErrEnvelopeInvalid", err)
	}
	// A builder must refuse to seal a non-canonical order too.
	_, err := NewTxEnvelope("t", "", []TxEnvelopeOp{
		{Collection: "b", Entry: store.Entry{ID: 1, Op: store.OpInsert}},
		{Collection: "a", Entry: store.Entry{ID: 1, Op: store.OpInsert}},
	})
	if !errors.Is(err, ErrEnvelopeInvalid) {
		t.Fatalf("got %v, want ErrEnvelopeInvalid", err)
	}
	_, err = NewTxEnvelope("t", "", []TxEnvelopeOp{
		{Collection: "a", Entry: store.Entry{ID: 1, Op: store.OpInsert}},
		{Collection: "b", Entry: store.Entry{ID: 1, Op: store.OpInsert}},
		{Collection: "a", Entry: store.Entry{ID: 2, Op: store.OpInsert}},
	})
	if !errors.Is(err, ErrEnvelopeInvalid) {
		t.Fatalf("non-contiguous: got %v, want ErrEnvelopeInvalid", err)
	}
}

func TestTxEnvelope_Invalid(t *testing.T) {
	t.Parallel()
	if _, err := NewTxEnvelope("", "", []TxEnvelopeOp{{Collection: "a", Entry: store.Entry{Op: store.OpInsert}}}); !errors.Is(err, ErrEnvelopeInvalid) {
		t.Fatalf("empty txid: %v", err)
	}
	if _, err := NewTxEnvelope("t", "", nil); !errors.Is(err, ErrEnvelopeInvalid) {
		t.Fatalf("no ops: %v", err)
	}
	if _, err := NewTxEnvelope("t", "", []TxEnvelopeOp{{Collection: "a", Entry: store.Entry{Op: "bogus"}}}); !errors.Is(err, ErrEnvelopeInvalid) {
		t.Fatalf("bad op: %v", err)
	}
}

func TestTxEnvelope_TruncatedAndCorruptedBytes(t *testing.T) {
	t.Parallel()
	b, err := testEnvelope(t).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range []int{0, 1, len(b) / 2, len(b) - 1} {
		if _, err := UnmarshalTxEnvelope(b[:n]); !errors.Is(err, ErrEnvelopeInvalid) {
			t.Fatalf("truncated to %d: got %v, want ErrEnvelopeInvalid", n, err)
		}
	}
	// Change a payload value without fixing the digest.
	bad := strings.Replace(string(b), `"s":"x"`, `"s":"y"`, 1)
	if bad == string(b) {
		t.Fatal("mutation did not apply")
	}
	if _, err := UnmarshalTxEnvelope([]byte(bad)); !errors.Is(err, ErrEnvelopeDigest) {
		t.Fatalf("tampered: got %v, want ErrEnvelopeDigest", err)
	}
	// Unknown format version.
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	m["v"] = 9
	vb, _ := json.Marshal(m)
	if _, err := UnmarshalTxEnvelope(vb); !errors.Is(err, ErrEnvelopeInvalid) {
		t.Fatalf("bad version: got %v", err)
	}
}
