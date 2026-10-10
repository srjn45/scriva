package xtxspec

import (
	"bytes"
	"flag"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/srjn45/scriva/store"
)

var update = flag.Bool("update-xtx-fixtures", false, "rewrite testdata fixtures")

const (
	txid  = "4f9c2a7e11b0d3a5-0000000000000012"
	epoch = "4f9c2a7e11b0d3a5"
	ts    = "2026-10-10T12:00:00Z"
)

func stamped(id uint64, op store.Op, rev uint64, i, n uint32, data map[string]any) Stamped {
	return Stamped{ID: id, Op: op, Ts: ts, Rev: rev, Data: data, Tx: Tx{T: txid, I: i, N: n}}
}

func mustBytes(t *testing.T, b []byte, err error) []byte {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func catBytes(parts ...[]byte) []byte { return bytes.Join(parts, nil) }

// fixtures builds every golden file deterministically.
func fixtures(t *testing.T) map[string][]byte {
	t.Helper()
	h, err := EncodeHeader(Header{Magic: Magic, V: 1, Epoch: epoch, Created: ts})
	hb := mustBytes(t, h, err)
	rec := func(r Record) []byte {
		b, err := EncodeRecord(r)
		return mustBytes(t, b, err)
	}
	commit := rec(Record{K: KindCommit, Tx: txid, Key: "order-77", Ts: ts, Parts: []Part{ScenarioParts[1], ScenarioParts[0]}})
	abort := rec(Record{K: KindAbort, Tx: txid, Ts: ts, Why: "client"})
	retire := rec(Record{K: KindRetire, Tx: txid, Ts: ts, O: KindCommit})

	e0, err := EncodeStamped(stamped(1, store.OpInsert, 1, 0, 2, map[string]any{"sku": "a1", "qty": 2}))
	b0 := mustBytes(t, e0, err)
	e1, err := EncodeStamped(stamped(2, store.OpUpdate, 2, 1, 2, map[string]any{"total": 9}))
	b1 := mustBytes(t, e1, err)

	// Spliced: a v1 crc computed without the stamp, then tx added.
	sp := stamped(1, store.OpInsert, 1, 0, 2, map[string]any{"sku": "a1", "qty": 2})
	v1, _ := ChecksumV1(sp)
	spliced := bytes.Replace(b0, []byte(`"crc":`+itoa(sp, t)), []byte(`"crc":`+utoa(v1)), 1)

	// Old-binary view: stamp stripped, v2 crc kept.
	stripped := bytes.Replace(b0, []byte(`"tx":{"t":"`+txid+`","i":0,"n":2},`), nil, 1)

	f, err := EncodeFormat(Format{Format: FormatName, MinReader: 1, Features: []string{"journal", "stamped-v2"}, CreatedBy: "v1.x.y"})
	fb := mustBytes(t, f, err)
	ftoo, err := EncodeFormat(Format{Format: FormatName, MinReader: 99, Features: []string{"journal", "stamped-v2"}, CreatedBy: "v9.0.0"})
	ftb := mustBytes(t, ftoo, err)

	torn := catBytes(hb, commit, commit[:len(commit)/2])
	zero := catBytes(hb, commit, make([]byte, 40), []byte("\n"))
	corrupt := catBytes(hb, bytes.Replace(commit, []byte(`"key":"order-77"`), []byte(`"key":"order-78"`), 1), abort)
	return map[string][]byte{
		"entries_run_orders.ndjson":        catBytes(b0, b1),
		"entry_v1crc_spliced.ndjson":       spliced,
		"entry_stamp_stripped.ndjson":      stripped,
		"journal_clean.ndjson":             catBytes(hb, commit),
		"journal_commit_retire.ndjson":     catBytes(hb, commit, retire),
		"journal_torn_tail.ndjson":         torn,
		"journal_zero_tail.ndjson":         zero,
		"journal_corrupt_middle.ndjson":    corrupt,
		"journal_decision_conflict.ndjson": catBytes(hb, commit, abort),
		"journal_header_only.ndjson":       hb,
		"xtx.format.json":                  fb,
		"xtx.format.too_new.json":          ftb,
	}
}

func itoa(e Stamped, t *testing.T) string {
	c, err := ChecksumV2(e)
	if err != nil {
		t.Fatal(err)
	}
	return utoa(c)
}

func utoa(u uint32) string {
	var b [10]byte
	i := len(b)
	if u == 0 {
		return "0"
	}
	for u > 0 {
		i--
		b[i] = byte('0' + u%10)
		u /= 10
	}
	return string(b[i:])
}

func TestFixturesGolden(t *testing.T) {
	got := fixtures(t)
	if *update {
		for n, b := range got {
			if err := os.WriteFile(filepath.Join("testdata", n), b, 0o644); err != nil {
				t.Fatal(err)
			}
		}
		return
	}
	for n, b := range got {
		want, err := os.ReadFile(filepath.Join("testdata", n))
		if err != nil {
			t.Fatalf("%s: %v (run: go test ./internal/xtxspec -update-xtx-fixtures)", n, err)
		}
		if !bytes.Equal(b, want) {
			t.Errorf("%s drifted from golden fixture; wire format is frozen, regenerate deliberately", n)
		}
	}
	// Determinism: building twice yields identical bytes.
	again := fixtures(t)
	for n, b := range got {
		if !bytes.Equal(b, again[n]) {
			t.Errorf("%s is not deterministic", n)
		}
	}
}

func TestV1ChecksumMatchesStore(t *testing.T) {
	cases := []Stamped{
		stamped(1, store.OpInsert, 0, 0, 1, map[string]any{"a": 1, "b": "x"}),
		stamped(9, store.OpDelete, 5, 0, 1, nil),
		{ID: 3, Op: store.OpUpdate, Rev: 2, Epoch: 4, ExpiresAt: 99, Data: map[string]any{"z": true}, Tx: Tx{T: txid, N: 1}},
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
		got, err := ChecksumV1(c)
		if err != nil {
			t.Fatal(err)
		}
		if got != *e.CRC {
			t.Errorf("id=%d: ChecksumV1=%08x store=%08x", c.ID, got, *e.CRC)
		}
	}
}

// The old-binary fence (L1): a released reader must reject every stamped line.
func TestOldReaderRejectsStampedEntries(t *testing.T) {
	b, err := os.ReadFile("testdata/entries_run_orders.ndjson")
	if err != nil {
		t.Fatal(err)
	}
	lines := bytes.Split(bytes.TrimRight(b, "\n"), []byte("\n"))
	if len(lines) != 2 {
		t.Fatalf("want 2 lines, got %d", len(lines))
	}
	for i, l := range lines {
		if _, err := store.Decode(l); err == nil {
			t.Errorf("line %d: store.Decode accepted a stamped entry (fence broken)", i)
		}
		if _, err := ParseStamped(l); err != nil {
			t.Errorf("line %d: ParseStamped: %v", i, err)
		}
	}
}

func TestStampedRejections(t *testing.T) {
	sp, _ := os.ReadFile("testdata/entry_v1crc_spliced.ndjson")
	if _, err := ParseStamped(sp); err == nil || !strings.Contains(err.Error(), "v1 checksum") {
		t.Errorf("spliced: got %v, want ErrV1CRCOnStamp", err)
	}
	st, _ := os.ReadFile("testdata/entry_stamp_stripped.ndjson")
	if _, err := ParseStamped(st); err == nil {
		t.Error("stripped stamp accepted")
	}
	if _, err := store.Decode(st); err == nil {
		t.Error("store.Decode accepted a stamp-stripped line: stripping would silently commit")
	}
	for _, bad := range []Stamped{
		stamped(1, "insert", 1, 2, 2, nil),
		stamped(1, "insert", 1, 0, 0, nil),
		{ID: 1, Op: "insert", Tx: Tx{T: strings.Repeat("x", 65), N: 1}},
	} {
		if _, err := EncodeStamped(bad); err == nil {
			t.Errorf("EncodeStamped accepted %+v", bad.Tx)
		}
	}
}

func TestJournalScan(t *testing.T) {
	read := func(n string) []byte {
		b, err := os.ReadFile(filepath.Join("testdata", n))
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	j, err := ScanJournal(read("journal_clean.ndjson"))
	if err != nil || len(j.Records) != 1 || j.TornTail || j.Header.Epoch != epoch {
		t.Fatalf("clean: %+v %v", j, err)
	}
	d, err := j.Decisions()
	if err != nil || d[txid] != DecCommit || !EntryVisible(txid, d) {
		t.Fatalf("decisions: %v %v", d, err)
	}
	if len(j.Records[0].Parts) != 2 || j.Records[0].Parts[0].C != "orders" {
		t.Errorf("parts not canonical: %+v", j.Records[0].Parts)
	}
	for _, n := range []string{"journal_torn_tail.ndjson", "journal_zero_tail.ndjson"} {
		raw := read(n)
		j, err := ScanJournal(raw)
		if err != nil || !j.TornTail || len(j.Records) != 1 || j.GoodLen >= len(raw) {
			t.Errorf("%s: want torn tail, got %+v %v", n, j, err)
		}
	}
	if _, err := ScanJournal(read("journal_corrupt_middle.ndjson")); err == nil || !strings.Contains(err.Error(), "journal-corrupt") {
		t.Errorf("corrupt middle: got %v, want fail closed", err)
	}
	j, err = ScanJournal(read("journal_decision_conflict.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.Decisions(); err == nil || !strings.Contains(err.Error(), "decision-conflict") {
		t.Errorf("conflict: got %v", err)
	}
	j, err = ScanJournal(read("journal_commit_retire.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	d, _ = j.Decisions()
	if d[txid] != DecRetireCommit {
		t.Errorf("retire must supersede commit, got %v", d[txid])
	}
	j, err = ScanJournal(read("journal_header_only.ndjson"))
	if err != nil || len(j.Records) != 0 {
		t.Errorf("header only: %+v %v", j, err)
	}
	if _, err := ScanJournal(nil); err == nil {
		t.Error("empty journal accepted")
	}
}

func TestFormatGate(t *testing.T) {
	b, _ := os.ReadFile("testdata/xtx.format.json")
	if _, err := ParseFormat(b); err != nil {
		t.Fatal(err)
	}
	b, _ = os.ReadFile("testdata/xtx.format.too_new.json")
	if _, err := ParseFormat(b); err == nil || !strings.Contains(err.Error(), "newer reader") {
		t.Errorf("too-new: got %v", err)
	}
	if _, err := ParseFormat(bytes.Replace(b, []byte("v9.0.0"), []byte("v9.0.1"), 1)); err == nil {
		t.Error("tampered xtx.format accepted")
	}
}

func TestParticipantDigestExcludesData(t *testing.T) {
	// The digest pins ops, not payload: re-encryption/compaction must not change it.
	if ParticipantDigest(runA) != ScenarioParts[0].D {
		t.Fatal("digest not stable")
	}
	if _, err := CanonicalParts([]Part{{C: "a"}, {C: "a"}}); err == nil {
		t.Error("duplicate participant accepted")
	}
	p, _ := CanonicalParts([]Part{{C: "stock"}, {C: "Orders"}, {C: "orders"}})
	if p[0].C != "Orders" || p[1].C != "orders" || p[2].C != "stock" {
		t.Errorf("not bytewise order: %+v", p)
	}
}

// Every boundary state must resolve to its documented outcome, and no state
// where an ack may exist may resolve to anything but committed (I3).
func TestBoundaryCrashOutcomes(t *testing.T) {
	seen := map[string]bool{}
	for _, b := range Boundaries() {
		if seen[b.ID] {
			t.Fatalf("duplicate boundary %s", b.ID)
		}
		seen[b.ID] = true
		if len(b.States) == 0 {
			t.Errorf("%s has no crash state", b.ID)
		}
		for _, s := range b.States {
			got := Resolve(s.In)
			if got.Row != s.Want.Row || got.Outcome != s.Want.Outcome || got.Code != s.Want.Code {
				t.Errorf("%s %q: got row %d %s %q, want row %d %s %q", b.ID, s.Label,
					got.Row, got.Outcome, got.Code, s.Want.Row, s.Want.Outcome, s.Want.Code)
			}
			if s.MayHaveAck && got.Outcome != OutCommitted {
				t.Errorf("%s %q: acked state resolves to %s (violates I3)", b.ID, s.Label, got.Outcome)
			}
		}
	}
}

// Rows of the §7.2 truth table not reachable from a crash boundary.
func TestTruthTableRemainingRows(t *testing.T) {
	full := map[string]Run{"orders": fullA, "stock": fullB}
	cases := []struct {
		name string
		in   Input
		row  int
		code string
	}{
		{"digest mismatch", committed(map[string]Run{"orders": fullA, "stock": {Complete: true, N: 1, Digest: "x"}}), 5, CodeDigestMismatch},
		{"short run", committed(map[string]Run{"orders": {Complete: true, N: 1, Digest: fullA.Digest}, "stock": fullB}), 5, CodeParticipantMissing},
		{"missing collection", Input{Decision: DecCommit, Parts: ScenarioParts, Evidence: full, Existing: map[string]bool{"orders": true}}, 6, CodeMissingCollection},
		{"foreign run", committed(map[string]Run{"orders": fullA, "stock": fullB, "other": fullB}), 7, CodeForeignRun},
		{"retire abort", Input{Decision: DecRetireAbort, Evidence: full, Existing: existing}, 10, ""},
		{"conflict", Input{Conflict: true, Evidence: full, Existing: existing}, 11, CodeDecisionConflict},
	}
	for _, c := range cases {
		got := Resolve(c.in)
		if got.Row != c.row || got.Code != c.code {
			t.Errorf("%s: got row %d %q, want %d %q", c.name, got.Row, got.Code, c.row, c.code)
		}
	}
}

// Order independence (I5): Resolve must not depend on map iteration or part order.
func TestResolveOrderIndependent(t *testing.T) {
	in := committed(map[string]Run{"orders": fullA, "stock": fullB})
	want := Resolve(in)
	rev := in
	rev.Parts = []Part{ScenarioParts[1], ScenarioParts[0]}
	for i := 0; i < 50; i++ {
		if got := Resolve(rev); got != want {
			t.Fatalf("iteration %d: %v != %v", i, got, want)
		}
	}
}

func TestBoundaryChecklistDocInSync(t *testing.T) {
	doc, err := os.ReadFile("../../docs/design-xtx-fault-boundaries.md")
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range Boundaries() {
		if !bytes.Contains(doc, []byte("| "+b.ID+" |")) {
			t.Errorf("boundary %s missing from docs/design-xtx-fault-boundaries.md", b.ID)
		}
	}
}

// The harness must stay unreachable from normal opens until a later stage
// deliberately wires it in.
func TestNotImportedByProduction(t *testing.T) {
	root := "../.."
	fset := token.NewFileSet()
	err := filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			if info.Name() == ".worktrees" || info.Name() == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") || strings.Contains(p, "internal/xtxspec") {
			return nil
		}
		f, perr := parser.ParseFile(fset, p, nil, parser.ImportsOnly)
		if perr != nil {
			return perr
		}
		for _, im := range f.Imports {
			if strings.Contains(im.Path.Value, "internal/xtxspec") {
				t.Errorf("%s imports xtxspec; the format must stay disabled", p)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
