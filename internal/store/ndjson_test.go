package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestEncodeDecodeParity(t *testing.T) {
	original := Entry{
		ID:   42,
		Op:   OpInsert,
		Ts:   time.Now().UTC().Truncate(time.Millisecond),
		Data: map[string]any{"userName": "srajan", "score": float64(99)},
	}

	b, err := Encode(original)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	// Encoded bytes should end with newline.
	if b[len(b)-1] != '\n' {
		t.Error("Encode: missing trailing newline")
	}

	decoded, err := Decode(b[:len(b)-1])
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}

	if decoded.ID != original.ID {
		t.Errorf("ID mismatch: got %d want %d", decoded.ID, original.ID)
	}
	if decoded.Op != original.Op {
		t.Errorf("Op mismatch: got %q want %q", decoded.Op, original.Op)
	}
	if decoded.Data["userName"] != original.Data["userName"] {
		t.Errorf("Data mismatch: got %v want %v", decoded.Data, original.Data)
	}
}

func TestDeleteEntryHasNoData(t *testing.T) {
	e := NewDelete(7)
	b, err := Encode(e)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := Decode(b[:len(b)-1])
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Data != nil {
		t.Errorf("expected nil Data for delete entry, got %v", decoded.Data)
	}
	if decoded.Op != OpDelete {
		t.Errorf("expected OpDelete, got %q", decoded.Op)
	}
}

// TestEncodeStampsCRC verifies every encoded line carries a crc key.
func TestEncodeStampsCRC(t *testing.T) {
	b, err := Encode(NewInsert(1, map[string]any{"a": float64(1)}))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(b, []byte(`"crc":`)) {
		t.Errorf("encoded entry is missing crc field: %s", b)
	}

	var e Entry
	if err := json.Unmarshal(b, &e); err != nil {
		t.Fatal(err)
	}
	if e.CRC == nil {
		t.Fatal("decoded CRC is nil")
	}
}

// TestDecodeLegacyLineWithoutCRC verifies backward compatibility: a line
// written before checksums existed (no crc key) decodes without verification.
func TestDecodeLegacyLineWithoutCRC(t *testing.T) {
	legacy := []byte(`{"id":5,"op":"insert","ts":"2026-07-03T00:00:00Z","data":{"k":"v"}}`)
	e, err := Decode(legacy)
	if err != nil {
		t.Fatalf("legacy line should decode without error, got %v", err)
	}
	if e.ID != 5 || e.Op != OpInsert || e.Data["k"] != "v" {
		t.Errorf("legacy decode wrong: %+v", e)
	}
	if e.CRC != nil {
		t.Errorf("legacy line should have nil CRC, got %v", *e.CRC)
	}
}

// TestDecodeDetectsCorruptData flips a byte inside the data payload and asserts
// the checksum catches it — the core bit-rot guarantee.
func TestDecodeDetectsCorruptData(t *testing.T) {
	b, err := Encode(NewInsert(99, map[string]any{"name": "alice"}))
	if err != nil {
		t.Fatal(err)
	}
	line := b[:len(b)-1] // strip newline

	// Flip a byte in the value "alice" -> mutate 'a' to 'b'.
	idx := bytes.Index(line, []byte("alice"))
	if idx < 0 {
		t.Fatal("could not locate payload to corrupt")
	}
	corrupt := make([]byte, len(line))
	copy(corrupt, line)
	corrupt[idx] = 'b'

	_, err = Decode(corrupt)
	if !errors.Is(err, ErrCorruptEntry) {
		t.Fatalf("expected ErrCorruptEntry, got %v", err)
	}
}

// TestDecodeDetectsCorruptID verifies the checksum also covers the id, not just
// the data payload.
func TestDecodeDetectsCorruptID(t *testing.T) {
	b, err := Encode(NewInsert(10, map[string]any{"x": float64(1)}))
	if err != nil {
		t.Fatal(err)
	}
	line := b[:len(b)-1]
	idx := bytes.Index(line, []byte(`"id":10`))
	if idx < 0 {
		t.Fatal("could not locate id")
	}
	corrupt := make([]byte, len(line))
	copy(corrupt, line)
	corrupt[idx+len(`"id":1`)] = '9' // 10 -> 19

	_, err = Decode(corrupt)
	if !errors.Is(err, ErrCorruptEntry) {
		t.Fatalf("expected ErrCorruptEntry for corrupt id, got %v", err)
	}
}

// TestDecodeDetectsCorruptDelete verifies a delete tombstone's id is protected.
func TestDecodeDetectsCorruptDelete(t *testing.T) {
	b, err := Encode(NewDelete(77))
	if err != nil {
		t.Fatal(err)
	}
	line := b[:len(b)-1]
	idx := bytes.Index(line, []byte(`"id":77`))
	if idx < 0 {
		t.Fatal("could not locate id")
	}
	corrupt := make([]byte, len(line))
	copy(corrupt, line)
	corrupt[idx+len(`"id":7`)] = '8' // 77 -> 78

	_, err = Decode(corrupt)
	if !errors.Is(err, ErrCorruptEntry) {
		t.Fatalf("expected ErrCorruptEntry for corrupt delete, got %v", err)
	}
}

// FuzzEntryChecksumDetectsCorruption asserts the invariant that a decoded entry
// never silently returns wrong data: for any single-byte mutation of a valid
// encoded line, Decode must either fail to parse or report ErrCorruptEntry — it
// must never return an entry whose id/op/data differ from the original.
func FuzzEntryChecksumDetectsCorruption(f *testing.F) {
	f.Add(uint64(1), "hello", 5)
	f.Add(uint64(999), "the quick brown fox", 12)
	f.Add(uint64(0), "", 0)
	f.Add(uint64(1<<40), `{"nested":true}`, 3)

	f.Fuzz(func(t *testing.T, id uint64, val string, pos int) {
		b, err := Encode(NewInsert(id, map[string]any{"v": val}))
		if err != nil {
			t.Skip()
		}
		line := b[:len(b)-1]
		if len(line) == 0 {
			t.Skip()
		}
		// Baseline: decode the clean line. JSON encoding is lossy for some
		// inputs (e.g. invalid UTF-8 normalises to U+FFFD), so the canonical
		// round-tripped entry — not the raw input — is the reference point.
		baseline, err := Decode(line)
		if err != nil {
			t.Skip()
		}

		i := pos % len(line)
		if i < 0 {
			i += len(line)
		}
		corrupt := make([]byte, len(line))
		copy(corrupt, line)
		corrupt[i] ^= 0x01 // flip one bit

		got, err := Decode(corrupt)
		if err != nil {
			return // parse failure or ErrCorruptEntry — both acceptable
		}
		// Decode succeeded: the mutation must have been semantically neutral
		// (e.g. inside the ts field or whitespace), so id/op/data must match the
		// clean baseline. Anything else is silent, undetected corruption.
		if got.ID != baseline.ID || got.Op != baseline.Op {
			t.Fatalf("silent corruption: got id=%d op=%q want id=%d op=%q", got.ID, got.Op, baseline.ID, baseline.Op)
		}
		gv, _ := got.Data["v"].(string)
		bv, _ := baseline.Data["v"].(string)
		if gv != bv {
			t.Fatalf("silent data corruption: got %q want %q", gv, bv)
		}
	})
}
