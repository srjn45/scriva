package engine

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func testParts(names ...string) []xtxPart {
	var ps []xtxPart
	for i, n := range names {
		ps = append(ps, xtxPart{C: n, N: uint32(i + 1), D: partDigest([]xtxOpRef{{ID: uint64(i + 1), Op: "insert", Rev: 1}})})
	}
	return ps
}

func fixedClock() func() time.Time {
	t := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	return func() time.Time { t = t.Add(time.Second); return t }
}

// buildJournal writes a journal with several records and returns its bytes and
// the txids in order: commit(a,b), abort, commit(c) retired.
func buildJournal(t *testing.T, dir string) ([]byte, []string) {
	t.Helper()
	j, err := createXTxJournal(dir, xtxOptions{now: fixedClock()})
	if err != nil {
		t.Fatal(err)
	}
	t1, err := j.commit("k1", testParts("a", "b"))
	if err != nil {
		t.Fatal(err)
	}
	t2, err := j.commit("k2", testParts("c"))
	if err != nil {
		t.Fatal(err)
	}
	if err := j.abort(t2, "k2", "client"); err == nil {
		t.Fatal("abort of committed tx must be refused")
	}
	t3 := formatTxID(j.epoch(), 99)
	if err := j.abort(t3, "k3", "conflict"); err != nil {
		t.Fatal(err)
	}
	if err := j.retire(t2); err != nil {
		t.Fatal(err)
	}
	if err := j.close(); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(dir, xtxJournalFile))
	if err != nil {
		t.Fatal(err)
	}
	return b, []string{t1, t2, t3}
}

func TestXTxTxIDRoundTrip(t *testing.T) {
	id := formatTxID("0123456789abcdef", 0x12)
	if id != "0123456789abcdef-0000000000000012" {
		t.Fatalf("id = %q", id)
	}
	e, s, err := parseTxID(id)
	if err != nil || e != "0123456789abcdef" || s != 0x12 {
		t.Fatalf("parse: %q %d %v", e, s, err)
	}
	for _, bad := range []string{"", "x", id + "0", strings.ToUpper(id), "0123456789abcdef_0000000000000012", "0123456789abcdeg-0000000000000012"} {
		if _, _, err := parseTxID(bad); err == nil {
			t.Errorf("parseTxID(%q) accepted", bad)
		}
	}
}

func TestXTxDigestsExcludeData(t *testing.T) {
	a := partDigest([]xtxOpRef{{1, "insert", 1}, {2, "update", 3}})
	b := partDigest([]xtxOpRef{{1, "insert", 1}, {2, "update", 3}})
	c := partDigest([]xtxOpRef{{2, "update", 3}, {1, "insert", 1}}) // order matters
	if a != b || a == c || len(a) != 64 {
		t.Fatalf("digest properties violated: %s %s %s", a, b, c)
	}
	p1, _ := partsDigest(testParts("a", "b"))
	p2, _ := partsDigest(testParts("a", "c"))
	if p1 == p2 {
		t.Fatal("pd must depend on participant names")
	}
}

func TestXTxLimits(t *testing.T) {
	l := defaultXTxLimits()
	if err := l.check(1000, 16, 64<<20); err != nil {
		t.Fatal(err)
	}
	for _, c := range [][3]int64{{1001, 1, 1}, {1, 17, 1}, {1, 1, 64<<20 + 1}} {
		err := l.check(int(c[0]), int(c[1]), c[2])
		if !errors.Is(err, ErrXTxTooLarge) {
			t.Errorf("check%v = %v, want ErrXTxTooLarge", c, err)
		}
	}
	if err := l.validate(1999); err == nil {
		t.Error("ring < 2 x ops accepted")
	}
	if err := l.validate(2000); err != nil {
		t.Error(err)
	}
	if err := l.validate(0); err != nil { // replication off
		t.Error(err)
	}
	if err := (xtxLimits{}).validate(0); err == nil {
		t.Error("zero limits accepted")
	}
}

func TestXTxJournalRoundTripAndReopen(t *testing.T) {
	dir := t.TempDir()
	data, txs := buildJournal(t, dir)
	if !bytes.HasPrefix(data, []byte(`{"magic":"SCRIVA-XTX","v":1,"epoch":"`)) {
		t.Fatalf("header shape: %.80s", data)
	}
	j, err := openXTxJournal(dir, xtxOptions{})
	if err != nil || j == nil {
		t.Fatal(err)
	}
	defer j.close()
	want := map[string]XTxStatus{txs[0]: XTxCommitted, txs[1]: XTxCommitted, txs[2]: XTxAborted}
	for tx, st := range want {
		if got := j.status(tx); got != st {
			t.Errorf("status(%s) = %s, want %s", tx, got, st)
		}
	}
	if d, _ := j.decision(txs[1]); !d.Retired || d.Outcome != xtxKindCommit {
		t.Errorf("tx2 decision = %+v", d)
	}
	if st, tx := j.statusByKey("k1"); st != XTxCommitted || tx != txs[0] {
		t.Errorf("statusByKey(k1) = %s %s", st, tx)
	}
	if st, _ := j.statusByKey("nope"); st != XTxUnknown {
		t.Errorf("unknown key = %s", st)
	}
	// seq 3 is next after reopen (seqs 1,2 used, 99 seen in the abort record).
	tx, err := j.commit("", testParts("z"))
	if err != nil {
		t.Fatal(err)
	}
	if _, seq, _ := parseTxID(tx); seq != 100 {
		t.Errorf("next seq = %d, want 100 (max seen 99 + 1)", seq)
	}
	// Unknown / foreign txids.
	if st := j.status(formatTxID(j.epoch(), 5000)); st != XTxUnknown {
		t.Errorf("future seq = %s", st)
	}
	if st := j.status(formatTxID("ffffffffffffffff", 1)); st != XTxUnknown {
		t.Errorf("foreign epoch = %s", st)
	}
	if st := j.status(formatTxID(j.epoch(), 50)); st != XTxExpired {
		t.Errorf("allocated-but-unrecorded seq = %s, want EXPIRED", st)
	}
	j.observeSeq(7000)
	tx, _ = j.commit("", testParts("y"))
	if _, seq, _ := parseTxID(tx); seq != 7001 {
		t.Errorf("after observeSeq: seq %d", seq)
	}
}

func TestXTxJournalCommitValidation(t *testing.T) {
	j, err := createXTxJournal(t.TempDir(), xtxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	for name, parts := range map[string][]xtxPart{
		"empty":     nil,
		"unsorted":  {testParts("b")[0], testParts("a")[0]},
		"duplicate": {testParts("a")[0], testParts("a")[0]},
		"zero-n":    {{C: "a", N: 0, D: testParts("a")[0].D}},
		"bad-d":     {{C: "a", N: 1, D: "zz"}},
	} {
		if _, err := j.commit("", parts); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := j.commit(strings.Repeat("k", 129), testParts("a")); !errors.Is(err, ErrXTxTooLarge) {
		t.Errorf("long key: %v", err)
	}
	if _, err := j.commit(strings.Repeat("k", 128), testParts("a")); err != nil {
		t.Errorf("128-byte key: %v", err)
	}
	if err := j.retire(formatTxID(j.epoch(), 77)); err == nil {
		t.Error("retire of undecided tx accepted")
	}
	if err := j.abort(formatTxID("ffffffffffffffff", 1), "", "client"); err == nil {
		t.Error("abort of foreign txid accepted")
	}
	if err := j.abort(formatTxID(j.epoch(), 5), "", "because"); err == nil {
		t.Error("unknown abort reason accepted")
	}
}

// TestXTxParseTruncatedEveryOffset: any prefix of a valid journal parses as
// "a prefix of the records plus (maybe) a torn tail" — never an error, never a
// record that was not fully written.
func TestXTxParseTruncatedEveryOffset(t *testing.T) {
	full, _ := buildJournal(t, t.TempDir())
	ref, err := parseXTxJournal(full)
	if err != nil || ref.Torn || ref.ValidLen != int64(len(full)) {
		t.Fatalf("reference parse: %+v %v", ref, err)
	}
	for k := 0; k <= len(full); k++ {
		p, err := parseXTxJournal(full[:k])
		if err != nil {
			t.Fatalf("truncated at %d: %v", k, err)
		}
		if p.TornCreate {
			if k >= bytes.IndexByte(full, '\n')+1 {
				t.Fatalf("torn-create after a complete header at %d", k)
			}
			continue
		}
		if p.ValidLen > int64(k) || full[p.ValidLen-1] != '\n' {
			t.Fatalf("at %d: ValidLen %d not a record boundary", k, p.ValidLen)
		}
		if p.Torn != (p.ValidLen < int64(k)) {
			t.Fatalf("at %d: Torn=%v ValidLen=%d", k, p.Torn, p.ValidLen)
		}
		if len(p.Records) > len(ref.Records) {
			t.Fatalf("at %d: too many records", k)
		}
		for i, r := range p.Records {
			if r.Tx != ref.Records[i].Tx || r.K != ref.Records[i].K {
				t.Fatalf("at %d: record %d differs", k, i)
			}
		}
	}
}

// TestXTxParseBitFlipEveryBit: every single-bit corruption of a complete
// journal is detected. The one inherent exception (design §4.3) is flipping
// the final record's terminating newline, which is indistinguishable from a
// torn tail and drops exactly that record.
func TestXTxParseBitFlipEveryBit(t *testing.T) {
	full, _ := buildJournal(t, t.TempDir())
	ref, _ := parseXTxJournal(full)
	buf := make([]byte, len(full))
	for i := range full {
		for bit := 0; bit < 8; bit++ {
			copy(buf, full)
			buf[i] ^= 1 << bit
			p, err := parseXTxJournal(buf)
			if err == nil {
				if i == len(full)-1 && p.Torn && len(p.Records) == len(ref.Records)-1 {
					continue
				}
				t.Fatalf("flip byte %d bit %d undetected (records %d torn=%v)", i, bit, len(p.Records), p.Torn)
			}
			if !errors.Is(err, ErrXTxJournalCorrupt) && !errors.Is(err, ErrXTxUnsupported) && !errors.Is(err, ErrXTxDecisionConflict) {
				t.Fatalf("flip byte %d bit %d: untyped error %v", i, bit, err)
			}
		}
	}
}

func TestXTxParseRejectsMalformedRecords(t *testing.T) {
	dir := t.TempDir()
	j, _ := createXTxJournal(dir, xtxOptions{now: fixedClock()})
	ep := j.epoch()
	j.close()
	hdr, _ := os.ReadFile(filepath.Join(dir, xtxJournalFile))
	good := testParts("a", "b")
	pd, _ := partsDigest(good)
	base := func() xtxRecord {
		return xtxRecord{K: xtxKindCommit, Tx: formatTxID(ep, 1), TS: "2026-10-10T12:00:00Z", Parts: good, PD: pd}
	}
	mut := map[string]func(*xtxRecord){
		"bad-kind":      func(r *xtxRecord) { r.K = "prepare" },
		"foreign-epoch": func(r *xtxRecord) { r.Tx = formatTxID("ffffffffffffffff", 1) },
		"bad-txid":      func(r *xtxRecord) { r.Tx = "nope" },
		"bad-ts":        func(r *xtxRecord) { r.TS = "yesterday" },
		"unsorted":      func(r *xtxRecord) { r.Parts = []xtxPart{good[1], good[0]} },
		"pd-mismatch":   func(r *xtxRecord) { r.PD = strings.Repeat("0", 64) },
		"no-parts":      func(r *xtxRecord) { r.Parts, r.PD = nil, "" },
		"commit-why":    func(r *xtxRecord) { r.Why = "io" },
		"abort-reason":  func(r *xtxRecord) { r.K, r.Parts, r.PD, r.Why = xtxKindAbort, nil, "", "meh" },
		"abort-parts":   func(r *xtxRecord) { r.K, r.Why = xtxKindAbort, "io" },
		"retire-o":      func(r *xtxRecord) { r.K, r.Parts, r.PD, r.O = xtxKindRetire, nil, "", "maybe" },
		"long-key":      func(r *xtxRecord) { r.Key = strings.Repeat("k", 129) },
	}
	for name, m := range mut {
		r := base()
		m(&r)
		line, err := encodeXTxRecord(r) // valid checksum: only semantics are wrong
		if err != nil {
			t.Fatal(err)
		}
		_, perr := parseXTxJournal(append(append([]byte{}, hdr...), line...))
		if !errors.Is(perr, ErrXTxJournalCorrupt) {
			t.Errorf("%s: got %v, want ErrXTxJournalCorrupt", name, perr)
		}
	}
	// Sanity: the unmutated record is accepted.
	line, _ := encodeXTxRecord(base())
	if _, err := parseXTxJournal(append(append([]byte{}, hdr...), line...)); err != nil {
		t.Fatalf("valid record rejected: %v", err)
	}
	// Unknown field (a v1 journal's field set is closed) and trailing junk.
	var m map[string]any
	_ = json.Unmarshal(bytes.TrimSpace(line), &m)
	m["extra"] = 1
	xb, _ := json.Marshal(m)
	if _, err := parseXTxJournal(append(append([]byte{}, hdr...), append(xb, '\n')...)); !errors.Is(err, ErrXTxJournalCorrupt) {
		t.Errorf("unknown field: %v", err)
	}
	// Missing checksum.
	delete(m, "extra")
	delete(m, "crc")
	xb, _ = json.Marshal(m)
	if _, err := parseXTxJournal(append(append([]byte{}, hdr...), append(xb, '\n')...)); !errors.Is(err, ErrXTxJournalCorrupt) {
		t.Errorf("missing crc: %v", err)
	}
}

func TestXTxParseHeaderGate(t *testing.T) {
	enc := func(h xtxHeader) []byte { b, _ := encodeXTxHeader(h); return b }
	ok := xtxHeader{Magic: xtxJournalMagic, V: 1, Epoch: "0123456789abcdef", Created: "2026-10-10T12:00:00Z"}
	if _, err := parseXTxJournal(enc(ok)); err != nil {
		t.Fatal(err)
	}
	v2 := ok
	v2.V = 2
	if _, err := parseXTxJournal(enc(v2)); !errors.Is(err, ErrXTxUnsupported) {
		t.Errorf("v2 header: %v", err)
	}
	badMagic := ok
	badMagic.Magic = "SCRIVA-OTHER"
	if _, err := parseXTxJournal(enc(badMagic)); !errors.Is(err, ErrXTxJournalCorrupt) {
		t.Errorf("magic: %v", err)
	}
	if _, err := parseXTxJournal([]byte("garbage line\n")); !errors.Is(err, ErrXTxJournalCorrupt) {
		t.Errorf("garbage header: %v", err)
	}
	if _, err := parseXTxJournal([]byte("garbage no newline")); !errors.Is(err, ErrXTxJournalCorrupt) {
		t.Errorf("garbage unterminated header: %v", err)
	}
	for _, torn := range [][]byte{nil, {}, make([]byte, 40), headerPrefix[:7]} {
		p, err := parseXTxJournal(torn)
		if err != nil || !p.TornCreate {
			t.Errorf("torn creation %q: %+v %v", torn, p, err)
		}
	}
	// A corrupt error also satisfies the generic integrity sentinel.
	_, err := parseXTxJournal([]byte("garbage line\n"))
	if !errors.Is(err, ErrIntegrity) {
		t.Error("corrupt journal must match ErrIntegrity")
	}
}

func TestXTxParseTailRules(t *testing.T) {
	full, _ := buildJournal(t, t.TempDir())
	lines := bytes.SplitAfter(full, []byte("\n"))
	lines = lines[:len(lines)-1] // trailing empty
	if len(lines) != 5 {
		t.Fatalf("fixture lines = %d", len(lines))
	}
	join := func(parts ...[]byte) []byte { return bytes.Join(parts, nil) }

	// Zero-filled newline-terminated final line: torn.
	zeros := append(make([]byte, 30), '\n')
	// (all-zero *content* with newline)
	if p, err := parseXTxJournal(join(full, zeros[:30], []byte("\n"))); err != nil || !p.Torn || len(p.Records) != 4 {
		t.Errorf("zero final line: %+v %v", p, err)
	}
	// Unterminated garbage final line: torn.
	if p, err := parseXTxJournal(join(full, []byte(`{"k":"com`))); err != nil || !p.Torn || len(p.Records) != 4 {
		t.Errorf("unterminated tail: %+v %v", p, err)
	}
	// Complete but corrupt final line: fail closed.
	bad := append([]byte(nil), lines[4]...)
	bad[10] ^= 0x01
	if _, err := parseXTxJournal(join(lines[0], lines[1], lines[2], lines[3], bad)); !errors.Is(err, ErrXTxJournalCorrupt) {
		t.Errorf("corrupt final line: %v", err)
	}
	// Corrupt middle record with good records after: fail closed.
	bad = append([]byte(nil), lines[2]...)
	bad[10] ^= 0x01
	if _, err := parseXTxJournal(join(lines[0], lines[1], bad, lines[3], lines[4])); !errors.Is(err, ErrXTxJournalCorrupt) {
		t.Errorf("corrupt middle record: %v", err)
	}
	// Corrupt middle record in front of a torn tail is still corruption.
	if _, err := parseXTxJournal(join(lines[0], lines[1], bad, []byte(`{"k":`))); !errors.Is(err, ErrXTxJournalCorrupt) {
		t.Errorf("corrupt then torn: %v", err)
	}
	// Lost (zeroed) middle line: fail closed, never presumed abort.
	if _, err := parseXTxJournal(join(lines[0], lines[1], make([]byte, len(lines[2])-1), []byte("\n"), lines[3], lines[4])); !errors.Is(err, ErrXTxJournalCorrupt) {
		t.Errorf("zeroed middle record: %v", err)
	}
}

func TestXTxDecisionConflicts(t *testing.T) {
	dir := t.TempDir()
	j, _ := createXTxJournal(dir, xtxOptions{now: fixedClock()})
	tx, _ := j.commit("k", testParts("a"))
	ep := j.epoch()
	j.close()
	hdr, _ := os.ReadFile(filepath.Join(dir, xtxJournalFile))
	hdrOnly := hdr[:bytes.IndexByte(hdr, '\n')+1]
	commit := xtxRecord{K: xtxKindCommit, Tx: tx, Key: "k", TS: "2026-10-10T12:00:00Z"}
	commit.Parts = testParts("a")
	commit.PD, _ = partsDigest(commit.Parts)
	abort := xtxRecord{K: xtxKindAbort, Tx: tx, Key: "k", TS: commit.TS, Why: "io"}
	other := commit
	other.Parts = testParts("a", "b")
	other.PD, _ = partsDigest(other.Parts)
	retireAbort := xtxRecord{K: xtxKindRetire, Tx: tx, Key: "k", TS: commit.TS, O: xtxKindAbort}
	retireCommit := retireAbort
	retireCommit.O = xtxKindCommit
	diffKey := commit
	diffKey.Key = "other"
	_ = ep

	build := func(rs ...xtxRecord) []byte {
		b := append([]byte(nil), hdrOnly...)
		for _, r := range rs {
			l, err := encodeXTxRecord(r)
			if err != nil {
				t.Fatal(err)
			}
			b = append(b, l...)
		}
		return b
	}
	for name, img := range map[string][]byte{
		"commit+abort":         build(commit, abort),
		"abort+commit":         build(abort, commit),
		"commit+retire-abort":  build(commit, retireAbort),
		"abort+retire-commit":  build(abort, retireCommit),
		"commit+other-commit":  build(commit, other),
		"commit+different-key": build(commit, diffKey),
	} {
		if _, err := parseXTxJournal(img); !errors.Is(err, ErrXTxDecisionConflict) {
			t.Errorf("%s: %v, want ErrXTxDecisionConflict", name, err)
		} else if !errors.Is(err, ErrIntegrity) {
			t.Errorf("%s: conflict must match ErrIntegrity", name)
		}
	}
	for name, img := range map[string][]byte{
		"duplicate commit":          build(commit, commit),
		"duplicate abort":           build(abort, abort),
		"commit+retire(commit)":     build(commit, retireCommit),
		"bare retire(commit)":       build(retireCommit),
		"retire twice":              build(commit, retireCommit, retireCommit),
		"commit after own retire":   build(commit, retireCommit, commit),
		"retire(abort) after abort": build(abort, retireAbort),
	} {
		p, err := parseXTxJournal(img)
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if len(p.Table.byTx) != 1 {
			t.Errorf("%s: %d decisions", name, len(p.Table.byTx))
		}
	}
}

func TestXTxOpenRepairsTornTail(t *testing.T) {
	dir := t.TempDir()
	full, txs := buildJournal(t, dir)
	path := filepath.Join(dir, xtxJournalFile)
	// Cut the last record in half.
	last := bytes.LastIndexByte(full[:len(full)-1], '\n') + 1
	cut := last + (len(full)-last)/2
	if err := os.WriteFile(path, full[:cut], 0o644); err != nil {
		t.Fatal(err)
	}
	j, err := openXTxJournal(dir, xtxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// The retire of txs[1] was the torn record: tx stays committed, unretired.
	if d, _ := j.decision(txs[1]); d.Retired {
		t.Error("torn retire must not count")
	}
	if st, _ := os.Stat(path); st.Size() != int64(last) {
		t.Errorf("file not truncated to %d: %d", last, st.Size())
	}
	// And the journal is appendable afterwards.
	if err := j.retire(txs[1]); err != nil {
		t.Fatal(err)
	}
	j.close()
	j, err = openXTxJournal(dir, xtxOptions{})
	if err != nil {
		t.Fatalf("reopen after repair: %v", err)
	}
	if d, _ := j.decision(txs[1]); !d.Retired {
		t.Error("retire lost")
	}
	j.close()
}

func TestXTxOpenTornCreationRecreates(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, xtxJournalFile)
	if err := os.WriteFile(path, headerPrefix[:10], 0o644); err != nil {
		t.Fatal(err)
	}
	j, err := openXTxJournal(dir, xtxOptions{})
	if err != nil || j == nil {
		t.Fatalf("open: %v", err)
	}
	defer j.close()
	if _, err := j.commit("", testParts("a")); err != nil {
		t.Fatal(err)
	}
	if reopened, err := openXTxJournal(dir, xtxOptions{}); err != nil {
		t.Fatal(err)
	} else if reopened != nil {
		reopened.close()
	}
	b, _ := os.ReadFile(path)
	if _, err := parseXTxJournal(b); err != nil {
		t.Fatal(err)
	}
}

func TestXTxOpenAbsentIsNil(t *testing.T) {
	j, err := openXTxJournal(t.TempDir(), xtxOptions{})
	if j != nil || err != nil {
		t.Fatalf("absent journal: %v %v", j, err)
	}
}

func TestXTxCommitAppendFailureRollsBack(t *testing.T) {
	dir := t.TempDir()
	fs := newFaultFS()
	j, err := createXTxJournal(dir, xtxOptions{wrapFile: fs.wrap, now: fixedClock()})
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	n := fs.count("write")
	fs.failWriteAt(n+1, 25, syscall.ENOSPC) // torn write: 25 bytes reach disk
	tx, err := j.commit("k", testParts("a"))
	if !errors.Is(err, ErrXTxDurability) || tx == "" {
		t.Fatalf("commit: tx=%q err=%v", tx, err)
	}
	if !errors.Is(err, syscall.ENOSPC) {
		t.Errorf("cause lost: %v", err)
	}
	if st := j.status(tx); st == XTxCommitted || st == XTxPending {
		t.Errorf("failed append reports %s", st)
	}
	// Rolled back: journal still usable and file is whole.
	b, _ := os.ReadFile(filepath.Join(dir, xtxJournalFile))
	if _, err := parseXTxJournal(b); err != nil {
		t.Fatalf("file after rollback: %v", err)
	}
	if p, _ := parseXTxJournal(b); p.Torn || len(p.Records) != 0 {
		t.Fatalf("rollback left bytes: %+v", p)
	}
	tx2, err := j.commit("k", testParts("a"))
	if err != nil {
		t.Fatalf("second commit: %v", err)
	}
	if tx2 == tx {
		t.Error("txid reused after failed append")
	}
}

func TestXTxCommitFsyncFailurePoisons(t *testing.T) {
	dir := t.TempDir()
	fs := newFaultFS()
	j, err := createXTxJournal(dir, xtxOptions{wrapFile: fs.wrap})
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	fs.failSyncAt(fs.count("sync")+1, syscall.EIO)
	tx, err := j.commit("k", testParts("a"))
	if !errors.Is(err, ErrXTxOutcomeUnknown) {
		t.Fatalf("commit: %v", err)
	}
	if XTxRetrySafe(err) {
		t.Error("unknown outcome must not be retry-safe")
	}
	// The process must not claim an outcome it does not know.
	if st := j.status(tx); st == XTxCommitted || st == XTxAborted {
		t.Errorf("status after failed fsync = %s", st)
	}
	if _, err := j.commit("k2", testParts("b")); !errors.Is(err, ErrXTxDurability) {
		t.Errorf("poisoned journal accepted a commit: %v", err)
	}
	if err := j.retire(tx); err == nil {
		t.Error("poisoned journal accepted a retire")
	}
	if _, err := j.checkpoint(func(xtxDecision) bool { return true }); err == nil {
		t.Error("poisoned journal accepted a checkpoint")
	}
}

func TestXTxCheckpoint(t *testing.T) {
	dir := t.TempDir()
	j, err := createXTxJournal(dir, xtxOptions{now: fixedClock()})
	if err != nil {
		t.Fatal(err)
	}
	var txs []string
	for i := 0; i < 5; i++ {
		tx, err := j.commit(fmt.Sprintf("k%d", i), testParts("a"))
		if err != nil {
			t.Fatal(err)
		}
		txs = append(txs, tx)
	}
	for _, tx := range txs[:3] {
		if err := j.retire(tx); err != nil {
			t.Fatal(err)
		}
	}
	// Even if asked to drop everything: live txs and the hwm tx survive.
	n, err := j.checkpoint(func(xtxDecision) bool { return true })
	if err != nil || n != 3 {
		t.Fatalf("checkpoint: %d %v", n, err)
	}
	if j.generation() != 1 {
		t.Errorf("generation = %d", j.generation())
	}
	for _, tx := range txs[:3] {
		if _, ok := j.decision(tx); ok {
			t.Errorf("%s survived", tx)
		}
	}
	for _, tx := range txs[3:] {
		if _, ok := j.decision(tx); !ok {
			t.Errorf("live %s dropped", tx)
		}
	}
	// Appends continue on the new file.
	tx6, err := j.commit("", testParts("a"))
	if err != nil {
		t.Fatal(err)
	}
	j.close()
	if _, err := os.Stat(filepath.Join(dir, xtxJournalTmpFile)); !os.IsNotExist(err) {
		t.Error("tmp left behind")
	}
	j, err = openXTxJournal(dir, xtxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer j.close()
	if j.generation() != 1 {
		t.Errorf("reopened generation = %d", j.generation())
	}
	if _, ok := j.decision(tx6); !ok {
		t.Error("post-checkpoint commit lost")
	}
	// Retired high-water-mark record is never dropped, so seq never regresses.
	if err := j.retire(tx6); err != nil {
		t.Fatal(err)
	}
	if _, err := j.checkpoint(func(xtxDecision) bool { return true }); err != nil {
		t.Fatal(err)
	}
	if _, ok := j.decision(tx6); !ok {
		t.Error("high-water-mark record dropped")
	}
	// Nothing to drop: no rewrite.
	g := j.generation()
	if n, _ := j.checkpoint(func(xtxDecision) bool { return false }); n != 0 || j.generation() != g {
		t.Error("no-op checkpoint rewrote the file")
	}
}

// TestXTxCheckpointCrashBoundaries crashes the filesystem after every
// operation of a checkpoint; the file must always be the old or the new
// journal, whole, with every live decision intact.
func TestXTxCheckpointCrashBoundaries(t *testing.T) {
	run := func(crashAt int) (steps int, dir string, live []string) {
		dir = t.TempDir()
		fs := newFaultFS()
		j, err := createXTxJournal(dir, xtxOptions{wrapFile: fs.wrap, renameFn: fs.rename, now: fixedClock()})
		if err != nil {
			t.Fatal(err)
		}
		var txs []string
		for i := 0; i < 4; i++ {
			tx, err := j.commit("", testParts("a"))
			if err != nil {
				t.Fatal(err)
			}
			txs = append(txs, tx)
		}
		_ = j.retire(txs[0])
		_ = j.retire(txs[1])
		base := fs.totalSteps()
		if crashAt >= 0 {
			fs.crashAfter(base + crashAt)
		}
		_, _ = j.checkpoint(func(xtxDecision) bool { return true })
		steps = fs.totalSteps() - base
		fs.kill()
		fs.releaseFDs()
		return steps, dir, txs
	}
	total, _, _ := run(-1)
	if total < 3 {
		t.Fatalf("checkpoint performed %d ops", total)
	}
	for k := 0; k <= total; k++ {
		_, dir, txs := run(k)
		_ = os.Remove(filepath.Join(dir, xtxJournalTmpFile)) // as gateXTxRoot does at open
		j, err := openXTxJournal(dir, xtxOptions{})
		if err != nil {
			t.Fatalf("crash after %d: reopen: %v", k, err)
		}
		for _, tx := range txs[2:] {
			if d, ok := j.decision(tx); !ok || d.Outcome != xtxKindCommit {
				t.Fatalf("crash after %d: live decision %s lost", k, tx)
			}
		}
		g := j.generation()
		_, has0 := j.decision(txs[0])
		if g == 0 && !has0 {
			t.Fatalf("crash after %d: old file missing retired record", k)
		}
		if g == 1 && has0 {
			t.Fatalf("crash after %d: new file kept dropped record", k)
		}
		j.close()
	}
}

func TestXTxCheckpointRenameFailureKeepsOldJournal(t *testing.T) {
	dir := t.TempDir()
	fs := newFaultFS()
	j, _ := createXTxJournal(dir, xtxOptions{wrapFile: fs.wrap, renameFn: fs.rename})
	defer j.close()
	a, _ := j.commit("", testParts("a"))
	b, _ := j.commit("", testParts("a"))
	_ = j.retire(a)
	fs.failRenameAt(fs.count("rename")+1, syscall.EIO)
	if _, err := j.checkpoint(func(xtxDecision) bool { return true }); err == nil {
		t.Fatal("checkpoint ignored rename failure")
	}
	if _, err := os.Stat(filepath.Join(dir, xtxJournalTmpFile)); !os.IsNotExist(err) {
		t.Error("tmp not cleaned after failed rename")
	}
	// Journal remains usable; nothing was lost.
	if err := j.retire(b); err != nil {
		t.Fatalf("journal unusable after failed checkpoint: %v", err)
	}
	if _, ok := j.decision(a); !ok {
		t.Error("decision lost by failed checkpoint")
	}
}

func TestXTxTypedErrors(t *testing.T) {
	cause := errors.New("disk on fire")
	e := xtxErr("tx-1", ErrXTxOutcomeUnknown, cause)
	if !errors.Is(e, ErrXTxOutcomeUnknown) || !errors.Is(e, cause) {
		t.Error("XTxError must match sentinel and cause")
	}
	var xe *XTxError
	if !errors.As(fmt.Errorf("wrapped: %w", e), &xe) || xe.Tx != "tx-1" {
		t.Error("errors.As lost the tx")
	}
	if !strings.Contains(e.Error(), "tx-1") || !strings.Contains(e.Error(), "disk on fire") {
		t.Errorf("message: %s", e)
	}
	safe := map[error]bool{
		ErrXTxConflict: true, ErrXTxDurability: true, ErrXTxOutcomeUnknown: false,
		ErrXTxInProgress: false, ErrXTxTooLarge: false, ErrFormatTooNew: false, ErrXTxUnsupported: false,
	}
	for err, want := range safe {
		if got := XTxRetrySafe(xtxErr("", err, nil)); got != want {
			t.Errorf("XTxRetrySafe(%v) = %v, want %v", err, got, want)
		}
	}
	if XTxRetrySafe(nil) || XTxRetrySafe(errors.New("mystery")) {
		t.Error("unrecognised errors must fail closed")
	}
}
