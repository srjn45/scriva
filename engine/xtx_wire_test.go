package engine

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/srjn45/scriva/store"
)

func TestXTxWire_ChecksumV1MatchesStore(t *testing.T) {
	cases := []stampedEntry{
		{ID: 1, Op: store.OpInsert, Rev: 0, Data: map[string]any{"a": 1, "b": "x"}},
		{ID: 9, Op: store.OpDelete, Rev: 5},
		{ID: 3, Op: store.OpUpdate, Rev: 2, Epoch: 4, ExpiresAt: 99, Data: map[string]any{"z": true}},
	}
	for _, c := range cases {
		line, err := store.Encode(store.Entry{ID: c.ID, Op: c.Op, Rev: c.Rev, Epoch: c.Epoch, ExpiresAt: c.ExpiresAt, Data: c.Data})
		if err != nil {
			t.Fatal(err)
		}
		e, err := store.Decode(line)
		if err != nil {
			t.Fatal(err)
		}
		got, err := checksumV1(c.ID, c.Op, c.Rev, c.ExpiresAt, c.Epoch, c.Data)
		if err != nil {
			t.Fatal(err)
		}
		if got != *e.CRC {
			t.Errorf("id=%d: checksumV1=%08x store=%08x", c.ID, got, *e.CRC)
		}
	}
}

func TestXTxWire_EncodeDecodeRoundTrip(t *testing.T) {
	entry := stampedEntry{
		ID:        42,
		Op:        store.OpUpdate,
		Ts:        time.Now().UTC().Format(time.RFC3339Nano),
		Rev:       3,
		Data:      map[string]any{"account": "acc-100", "balance": 500.0},
		Epoch:     2,
		ExpiresAt: 123456789,
		Tx:        TxStamp{T: "4f9c2a7e11b0d3a5-0000000000000012", I: 0, N: 2},
	}

	b, err := encodeStamped(entry)
	if err != nil {
		t.Fatalf("encodeStamped: %v", err)
	}

	decoded, err := parseStamped(b)
	if err != nil {
		t.Fatalf("parseStamped: %v", err)
	}

	if decoded.ID != entry.ID || decoded.Op != entry.Op || decoded.Rev != entry.Rev ||
		decoded.Epoch != entry.Epoch || decoded.ExpiresAt != entry.ExpiresAt ||
		decoded.Tx.T != entry.Tx.T || decoded.Tx.I != entry.Tx.I || decoded.Tx.N != entry.Tx.N {
		t.Fatalf("decoded entry mismatch: got %+v, want %+v", decoded, entry)
	}

	// Also test decodeSegmentLine
	se, tx, err := decodeSegmentLine(b)
	if err != nil {
		t.Fatalf("decodeSegmentLine: %v", err)
	}
	if tx == nil || tx.T != entry.Tx.T || tx.I != 0 || tx.N != 2 {
		t.Fatalf("decodeSegmentLine tx stamp mismatch: got %+v", tx)
	}
	if se.ID != entry.ID || se.Op != entry.Op || se.Rev != entry.Rev {
		t.Fatalf("decodeSegmentLine entry mismatch: got %+v", se)
	}
}

func TestXTxWire_RejectsSplicedV1Stamp(t *testing.T) {
	entry := stampedEntry{
		ID:   1,
		Op:   store.OpInsert,
		Ts:   "2026-10-10T12:00:00Z",
		Rev:  1,
		Data: map[string]any{"sku": "a1", "qty": 2.0},
		Tx:   TxStamp{T: "4f9c2a7e11b0d3a5-0000000000000012", I: 0, N: 2},
	}

	// Compute v1 checksum (as if spliced onto a stamped line)
	v1CRC, err := checksumV1(entry.ID, entry.Op, entry.Rev, entry.ExpiresAt, entry.Epoch, entry.Data)
	if err != nil {
		t.Fatal(err)
	}

	b, err := encodeStamped(entry)
	if err != nil {
		t.Fatal(err)
	}

	// Splice v1 checksum into the encoded line
	correctV2CRC, _ := checksumV2(entry.ID, entry.Op, entry.Rev, entry.ExpiresAt, entry.Epoch, entry.Data, entry.Tx)
	spliced := bytes.Replace(b, []byte(fmt.Sprintf(`"crc":%d`, correctV2CRC)), []byte(fmt.Sprintf(`"crc":%d`, v1CRC)), 1)

	_, err = parseStamped(spliced)
	if !errors.Is(err, ErrV1CRCOnStamp) {
		t.Fatalf("parseStamped spliced: got %v, want ErrV1CRCOnStamp", err)
	}

	_, _, err = decodeSegmentLine(spliced)
	if !errors.Is(err, ErrV1CRCOnStamp) {
		t.Fatalf("decodeSegmentLine spliced: got %v, want ErrV1CRCOnStamp", err)
	}
}

func TestXTxWire_RejectsStrippedStamp(t *testing.T) {
	entry := stampedEntry{
		ID:   1,
		Op:   store.OpInsert,
		Ts:   "2026-10-10T12:00:00Z",
		Rev:  1,
		Data: map[string]any{"sku": "a1", "qty": 2.0},
		Tx:   TxStamp{T: "4f9c2a7e11b0d3a5-0000000000000012", I: 0, N: 2},
	}

	b, err := encodeStamped(entry)
	if err != nil {
		t.Fatal(err)
	}

	// Strip the "tx" object from the JSON
	stripped := bytes.Replace(b, []byte(`"tx":{"t":"4f9c2a7e11b0d3a5-0000000000000012","i":0,"n":2},`), nil, 1)

	// A reader that strips tx must fail v1 checksum because the line carried v2 CRC
	_, err = store.Decode(stripped)
	if !errors.Is(err, store.ErrCorruptEntry) {
		t.Fatalf("store.Decode stripped stamp: got %v, want store.ErrCorruptEntry", err)
	}

	_, _, err = decodeSegmentLine(stripped)
	if !errors.Is(err, store.ErrCorruptEntry) {
		t.Fatalf("decodeSegmentLine stripped stamp: got %v, want store.ErrCorruptEntry", err)
	}
}

func TestXTxWire_RejectsInvalidStamps(t *testing.T) {
	cases := []stampedEntry{
		{ID: 1, Op: store.OpInsert, Tx: TxStamp{T: "", I: 0, N: 1}},                                      // empty txid
		{ID: 1, Op: store.OpInsert, Tx: TxStamp{T: "tx1", I: 1, N: 1}},                                  // i >= n
		{ID: 1, Op: store.OpInsert, Tx: TxStamp{T: "tx1", I: 0, N: 0}},                                  // n == 0
		{ID: 1, Op: store.OpInsert, Tx: TxStamp{T: strings.Repeat("x", xtxMaxTxIDLen+1), I: 0, N: 1}}, // txid too long
	}
	for i, c := range cases {
		if _, err := encodeStamped(c); err == nil {
			t.Errorf("case %d: encodeStamped accepted invalid stamp: %+v", i, c.Tx)
		}
	}
}

func TestXTxWire_EntryVisiblePredicate(t *testing.T) {
	decisions := map[string]string{
		"tx-commit":  xtxKindCommit,
		"tx-abort":   xtxKindAbort,
		"tx-retirec": "retire-commit",
	}

	// Plain entry is always visible
	if !entryVisible(nil, decisions) {
		t.Error("plain nil tx should be visible")
	}
	if !entryVisible(&TxStamp{}, decisions) {
		t.Error("plain empty tx should be visible")
	}

	// Stamped entries
	if !entryVisible(&TxStamp{T: "tx-commit"}, decisions) {
		t.Error("committed tx should be visible")
	}
	if !entryVisible(&TxStamp{T: "tx-retirec"}, decisions) {
		t.Error("retired commit tx should be visible")
	}
	if entryVisible(&TxStamp{T: "tx-abort"}, decisions) {
		t.Error("aborted tx must NOT be visible")
	}
	if entryVisible(&TxStamp{T: "tx-undecided"}, decisions) {
		t.Error("undecided tx must NOT be visible")
	}
	if entryVisible(&TxStamp{T: "tx-commit"}, nil) {
		t.Error("nil decisions map must treat stamped entry as dead")
	}
}

func TestXTxWire_GoldenFixturesCompatibility(t *testing.T) {
	// Read golden fixtures generated by xtxspec
	p := filepath.Join("..", "internal", "xtxspec", "testdata", "entries_run_orders.ndjson")
	b, err := os.ReadFile(p)
	if err != nil {
		t.Skipf("skipping golden fixture test: %v", err)
	}

	lines := bytes.Split(bytes.TrimRight(b, "\n"), []byte("\n"))
	if len(lines) != 2 {
		t.Fatalf("want 2 lines in fixture, got %d", len(lines))
	}

	for i, l := range lines {
		se, tx, err := decodeSegmentLine(l)
		if err != nil {
			t.Fatalf("line %d: decodeSegmentLine failed: %v", i, err)
		}
		if tx == nil || tx.T != "4f9c2a7e11b0d3a5-0000000000000012" || tx.N != 2 || tx.I != uint32(i) {
			t.Fatalf("line %d: unexpected tx stamp %+v", i, tx)
		}
		if se.ID != uint64(i+1) {
			t.Fatalf("line %d: unexpected ID %d", i, se.ID)
		}
	}
}
