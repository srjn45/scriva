package engine

// The root coordinator journal of cross-collection transactions (XTx).
//
// Format and semantics are fixed by docs/design-cross-collection-transactions.md
// (§4 journal, §5 ids/limits, §8 status). This file implements only the
// journal itself; it knows nothing about segments or collections. Later stages
// build on this internal API:
//
//	j, _ := openOrCreateXTxJournal(dir, opts)     // gate + torn-tail repair
//	txid, err := j.commit(key, parts)             // S3+S4: append COMMIT, fsync
//	_ = j.abort(txid, key, "recovery")            // advisory / recovery ABORT
//	_ = j.retire(txid)                            // after digests verified
//	d, ok := j.decision(txid)                     // decision table lookup
//	st := j.status(txid) / j.statusByKey(key)     // §8.2
//	j.observeSeq(maxSeqSeenInStampedEntries)      // §5.1 seq restore
//	j.checkpoint(drop)                            // §4.5 atomic GC rewrite
//
// partDigest/partsDigest compute the `d` and `pd` fields. Every mutating call
// fsyncs before returning; a write or fsync failure that cannot be rolled back
// poisons the journal (all later calls fail with ErrXTxDurability) and the only
// way back is reopening, which re-parses the file.

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"
)

const (
	xtxReservedPrefix   = "xtx."
	xtxJournalFile      = "xtx.journal"
	xtxJournalTmpFile   = "xtx.journal.tmp"
	xtxFormatFile       = "xtx.format"
	xtxJournalMagic     = "SCRIVA-XTX"
	xtxJournalVersion   = 1
	xtxMaxKeyLen        = 128 // §8.2
	xtxMaxTxIDLen       = 64  // §3.1
	xtxEpochHexLen      = 16
	xtxSeqHexLen        = 16
	xtxKindCommit       = "commit"
	xtxKindAbort        = "abort"
	xtxKindRetire       = "retire"
	xtxMaxJournalRecord = 16 << 20 // sanity bound on one line
)

var xtxCRCTable = crc32.MakeTable(crc32.Castagnoli)

// xtxAbortReasons is the closed set of ABORT "why" values (§4.2).
var xtxAbortReasons = map[string]bool{"conflict": true, "quota": true, "io": true, "recovery": true, "client": true}

// --- limits (§5.3) ----------------------------------------------------------

// xtxLimits bounds one XTx. Exceeding a limit is ErrXTxTooLarge, reported
// before any write.
type xtxLimits struct {
	MaxOps          int
	MaxParticipants int
	MaxBytes        int64
}

func defaultXTxLimits() xtxLimits {
	return xtxLimits{MaxOps: 1000, MaxParticipants: 16, MaxBytes: 64 << 20}
}

// validate checks the configuration itself (startup validation, §5.3).
func (l xtxLimits) validate(replicationRing int) error {
	if l.MaxOps <= 0 || l.MaxParticipants <= 0 || l.MaxBytes <= 0 {
		return fmt.Errorf("xtx limits must be positive: %+v", l)
	}
	if replicationRing > 0 && replicationRing < 2*l.MaxOps {
		return fmt.Errorf("replication ring %d must be >= 2 x xtx max ops (%d)", replicationRing, 2*l.MaxOps)
	}
	return nil
}

// check reports ErrXTxTooLarge when a transaction exceeds a limit.
func (l xtxLimits) check(ops, participants int, bytes int64) error {
	switch {
	case ops > l.MaxOps:
		return xtxErr("", ErrXTxTooLarge, fmt.Errorf("%d ops > limit %d", ops, l.MaxOps))
	case participants > l.MaxParticipants:
		return xtxErr("", ErrXTxTooLarge, fmt.Errorf("%d participants > limit %d", participants, l.MaxParticipants))
	case bytes > l.MaxBytes:
		return xtxErr("", ErrXTxTooLarge, fmt.Errorf("%d bytes > limit %d", bytes, l.MaxBytes))
	}
	return nil
}

// --- txid (§5.1) ------------------------------------------------------------

func formatTxID(epoch string, seq uint64) string {
	return epoch + "-" + fmt.Sprintf("%016x", seq)
}

// parseTxID splits "<16 hex epoch>-<16 hex seq>".
func parseTxID(tx string) (epoch string, seq uint64, err error) {
	if len(tx) > xtxMaxTxIDLen || len(tx) != xtxEpochHexLen+1+xtxSeqHexLen || tx[xtxEpochHexLen] != '-' {
		return "", 0, fmt.Errorf("malformed txid %q", tx)
	}
	epoch = tx[:xtxEpochHexLen]
	if !isLowerHex(epoch) || !isLowerHex(tx[xtxEpochHexLen+1:]) {
		return "", 0, fmt.Errorf("malformed txid %q", tx)
	}
	seq, err = strconv.ParseUint(tx[xtxEpochHexLen+1:], 16, 64)
	if err != nil {
		return "", 0, fmt.Errorf("malformed txid %q: %w", tx, err)
	}
	return epoch, seq, nil
}

func isLowerHex(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func newXTxEpoch() (string, error) {
	var b [8]byte
	if _, err := io.ReadFull(rand.Reader, b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// --- digests (§4.2) ---------------------------------------------------------

// xtxOpRef identifies one op of a participant's run for its digest.
type xtxOpRef struct {
	ID  uint64
	Op  string
	Rev uint64
}

// partDigest is the participant digest d: SHA-256 over, for i = 0..n-1,
// u32le(i) ‖ u64le(id) ‖ op ‖ 0x00 ‖ u64le(rev). It deliberately excludes
// data, epoch and offsets.
func partDigest(ops []xtxOpRef) string {
	h := sha256.New()
	var b [8]byte
	for i, o := range ops {
		binary.LittleEndian.PutUint32(b[:4], uint32(i))
		h.Write(b[:4])
		binary.LittleEndian.PutUint64(b[:], o.ID)
		h.Write(b[:])
		h.Write([]byte(o.Op))
		h.Write([]byte{0})
		binary.LittleEndian.PutUint64(b[:], o.Rev)
		h.Write(b[:])
	}
	return hex.EncodeToString(h.Sum(nil))
}

// xtxPart is one participant entry of a COMMIT record.
type xtxPart struct {
	C string `json:"c"` // collection name
	N uint32 `json:"n"` // run length
	D string `json:"d"` // participant digest, hex
}

// partsDigest is pd: SHA-256 over name ‖ 0x00 ‖ u32le(n) ‖ d for each part in
// canonical order, with d as its raw 32 bytes. (The design says "‖ d" for the
// hex field; the raw digest bytes are the canonical reading — recorded in the
// PR description.)
func partsDigest(parts []xtxPart) (string, error) {
	h := sha256.New()
	var b [4]byte
	for _, p := range parts {
		d, err := hex.DecodeString(p.D)
		if err != nil || len(d) != sha256.Size {
			return "", fmt.Errorf("participant %q: malformed digest", p.C)
		}
		h.Write([]byte(p.C))
		h.Write([]byte{0})
		binary.LittleEndian.PutUint32(b[:], p.N)
		h.Write(b[:])
		h.Write(d)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// validateParts enforces the canonical participant list: non-empty, sorted
// bytewise by name, no duplicates, n ≥ 1, well-formed digest.
func validateParts(parts []xtxPart) error {
	if len(parts) == 0 {
		return errors.New("commit has no participants")
	}
	for i, p := range parts {
		if p.C == "" || p.N == 0 {
			return fmt.Errorf("participant %d: empty name or zero run length", i)
		}
		if i > 0 && parts[i-1].C >= p.C {
			return fmt.Errorf("participants not strictly ascending at %q", p.C)
		}
		if d, err := hex.DecodeString(p.D); err != nil || len(d) != sha256.Size {
			return fmt.Errorf("participant %q: malformed digest", p.C)
		}
	}
	return nil
}

// --- record framing (§4.1, §4.2) -------------------------------------------

// xtxHeader is line 1 of the journal.
type xtxHeader struct {
	Magic   string  `json:"magic"`
	V       int     `json:"v"`
	Epoch   string  `json:"epoch"`
	Gen     uint64  `json:"gen,omitempty"` // bumped by every checkpoint rewrite (§4.5)
	Created string  `json:"created"`
	CRC     *uint32 `json:"crc,omitempty"`
}

// xtxRecord is one decision record (kinds commit / abort / retire).
type xtxRecord struct {
	K     string    `json:"k"`
	Tx    string    `json:"tx"`
	Key   string    `json:"key"`
	TS    string    `json:"ts"`
	Parts []xtxPart `json:"parts,omitempty"`
	PD    string    `json:"pd,omitempty"`
	Why   string    `json:"why,omitempty"`
	O     string    `json:"o,omitempty"`
	CRC   *uint32   `json:"crc,omitempty"`
}

func crcOf(b []byte) uint32 { return crc32.Checksum(b, xtxCRCTable) }

// encodeXTxLine marshals v (a *xtxHeader or *xtxRecord) with crc computed over
// the canonical JSON with crc omitted, and returns the newline-terminated line.
func encodeXTxHeader(h xtxHeader) ([]byte, error) {
	h.CRC = nil
	b, err := json.Marshal(h)
	if err != nil {
		return nil, err
	}
	c := crcOf(b)
	h.CRC = &c
	b, err = json.Marshal(h)
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

func encodeXTxRecord(r xtxRecord) ([]byte, error) {
	r.CRC = nil
	b, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	c := crcOf(b)
	r.CRC = &c
	b, err = json.Marshal(r)
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

// strictUnmarshal decodes exactly one JSON object, rejecting unknown fields
// and trailing data.
func strictUnmarshal(line []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(line))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	if _, err := dec.Token(); err != io.EOF {
		return errors.New("trailing data after record")
	}
	return nil
}

// decodeXTxHeader parses and verifies line 1. A version this binary does not
// know is ErrXTxUnsupported (checked before the checksum, since a future
// version may define a different one); everything else is corruption.
func decodeXTxHeader(line []byte) (xtxHeader, error) {
	var probe struct {
		Magic string `json:"magic"`
		V     int    `json:"v"`
	}
	if err := json.Unmarshal(line, &probe); err != nil {
		return xtxHeader{}, &journalCorruptError{0, "header does not parse: " + err.Error()}
	}
	if probe.Magic != xtxJournalMagic {
		return xtxHeader{}, &journalCorruptError{0, "bad magic"}
	}
	if probe.V != xtxJournalVersion {
		return xtxHeader{}, xtxErr("", ErrXTxUnsupported, fmt.Errorf("xtx.journal version %d (this binary reads %d)", probe.V, xtxJournalVersion))
	}
	var h xtxHeader
	if err := strictUnmarshal(line, &h); err != nil {
		return xtxHeader{}, &journalCorruptError{0, "header does not parse: " + err.Error()}
	}
	if h.CRC == nil {
		return xtxHeader{}, &journalCorruptError{0, "header has no checksum"}
	}
	want := *h.CRC
	h.CRC = nil
	b, _ := json.Marshal(h)
	if crcOf(b) != want {
		return xtxHeader{}, &journalCorruptError{0, "header checksum mismatch"}
	}
	// encoding/json matches field names case-insensitively; insist on the
	// exact bytes we wrote so a flipped key-case bit cannot pass.
	h.CRC = &want
	if canon, _ := json.Marshal(h); !bytes.Equal(canon, line) {
		return xtxHeader{}, &journalCorruptError{0, "header is not in canonical form"}
	}
	h.CRC = nil
	if len(h.Epoch) != xtxEpochHexLen || !isLowerHex(h.Epoch) {
		return xtxHeader{}, &journalCorruptError{0, "header epoch malformed"}
	}
	if _, err := time.Parse(time.RFC3339Nano, h.Created); err != nil {
		return xtxHeader{}, &journalCorruptError{0, "header created timestamp malformed"}
	}
	return h, nil
}

// decodeXTxRecord parses and fully validates one record line (without '\n').
func decodeXTxRecord(line []byte, epoch string, off int64) (xtxRecord, error) {
	bad := func(format string, a ...any) error {
		return &journalCorruptError{off, fmt.Sprintf(format, a...)}
	}
	var r xtxRecord
	if err := strictUnmarshal(line, &r); err != nil {
		return r, bad("record does not parse: %v", err)
	}
	if r.CRC == nil {
		return r, bad("record has no checksum")
	}
	want := *r.CRC
	r.CRC = nil
	b, _ := json.Marshal(r)
	if crcOf(b) != want {
		return r, bad("record checksum mismatch")
	}
	r.CRC = &want
	if canon, _ := json.Marshal(r); !bytes.Equal(canon, line) {
		return r, bad("record is not in canonical form")
	}
	r.CRC = nil
	if e, _, err := parseTxID(r.Tx); err != nil || e != epoch {
		return r, bad("txid %q malformed or from another journal epoch", r.Tx)
	}
	if len(r.Key) > xtxMaxKeyLen {
		return r, bad("idempotency key longer than %d bytes", xtxMaxKeyLen)
	}
	if _, err := time.Parse(time.RFC3339Nano, r.TS); err != nil {
		return r, bad("timestamp malformed")
	}
	switch r.K {
	case xtxKindCommit:
		if r.Why != "" || r.O != "" {
			return r, bad("commit record carries abort/retire fields")
		}
		if err := validateParts(r.Parts); err != nil {
			return r, bad("%v", err)
		}
		pd, _ := partsDigest(r.Parts)
		if pd != r.PD {
			return r, bad("participant-set digest mismatch")
		}
	case xtxKindAbort:
		if len(r.Parts) != 0 || r.PD != "" || r.O != "" {
			return r, bad("abort record carries commit/retire fields")
		}
		if !xtxAbortReasons[r.Why] {
			return r, bad("unknown abort reason %q", r.Why)
		}
	case xtxKindRetire:
		if len(r.Parts) != 0 || r.PD != "" || r.Why != "" {
			return r, bad("retire record carries commit/abort fields")
		}
		if r.O != xtxKindCommit && r.O != xtxKindAbort {
			return r, bad("retire outcome %q", r.O)
		}
	default:
		return r, bad("unknown record kind %q", r.K)
	}
	return r, nil
}

// --- decision table ---------------------------------------------------------

// xtxDecision is the resolved state of one txid.
type xtxDecision struct {
	Tx      string
	Key     string
	Outcome string // xtxKindCommit | xtxKindAbort
	Retired bool
	Why     string // abort reason
	TS      string
	Parts   []xtxPart // commit only; absent for a bare RETIRE(commit)
	PD      string
}

type xtxTable struct {
	byTx   map[string]*xtxDecision
	byKey  map[string]string
	maxSeq uint64
}

func newXTxTable() *xtxTable {
	return &xtxTable{byTx: map[string]*xtxDecision{}, byKey: map[string]string{}}
}

func conflictErr(tx, why string) error {
	return xtxErr(tx, ErrXTxDecisionConflict, fmt.Errorf("%w: %s", ErrIntegrity, why))
}

// apply folds one validated record into the table. Duplicates with the same
// outcome are idempotent; COMMIT vs ABORT (or a COMMIT with different content)
// is a decision conflict.
func (t *xtxTable) apply(r xtxRecord) error {
	_, seq, _ := parseTxID(r.Tx)
	if seq > t.maxSeq {
		t.maxSeq = seq
	}
	outcome := r.K
	if r.K == xtxKindRetire {
		outcome = r.O
	}
	d := t.byTx[r.Tx]
	if d == nil {
		d = &xtxDecision{Tx: r.Tx, Key: r.Key, Outcome: outcome, TS: r.TS}
		t.byTx[r.Tx] = d
	} else {
		if d.Outcome != outcome {
			return conflictErr(r.Tx, fmt.Sprintf("%s recorded after %s", outcome, d.Outcome))
		}
		if d.Key != r.Key {
			return conflictErr(r.Tx, "idempotency key differs between records of one tx")
		}
	}
	switch r.K {
	case xtxKindCommit:
		if d.Parts != nil || d.PD != "" {
			if d.PD != r.PD {
				return conflictErr(r.Tx, "commit records for one tx differ")
			}
		} else {
			d.Parts, d.PD = r.Parts, r.PD
		}
	case xtxKindAbort:
		if d.Why == "" {
			d.Why = r.Why
		}
	case xtxKindRetire:
		d.Retired = true
	}
	if r.Key != "" {
		if prev, ok := t.byKey[r.Key]; !ok || t.byTx[prev].Outcome != xtxKindCommit || prev == r.Tx {
			t.byKey[r.Key] = r.Tx
		}
	}
	return nil
}

// --- parsing (§4.3) ---------------------------------------------------------

type xtxParsed struct {
	Header   xtxHeader
	Records  []xtxRecord
	ValidLen int64 // length of the valid prefix (header + good records)
	Torn     bool  // bytes beyond ValidLen are a torn tail to discard
	// TornCreate: the file is empty or an unfinished header; the journal was
	// never usable, so it must be re-created.
	TornCreate bool
	Table      *xtxTable
}

// headerPrefix is the beginning of every header line, used to recognise a
// header torn by a crash during creation.
var headerPrefix = []byte(`{"magic":"` + xtxJournalMagic + `"`)

// parseXTxJournal parses a whole journal image per §4.3. It never modifies
// anything. Errors are fail-closed: *journalCorruptError (errors.Is
// ErrXTxJournalCorrupt), ErrXTxUnsupported (unknown version) or
// ErrXTxDecisionConflict.
func parseXTxJournal(data []byte) (*xtxParsed, error) {
	p := &xtxParsed{Table: newXTxTable()}
	if len(data) == 0 || (bytes.IndexByte(data, '\n') < 0 && tornHeaderPrefix(data)) {
		p.TornCreate = true
		return p, nil
	}
	nl := bytes.IndexByte(data, '\n')
	if nl < 0 {
		return nil, &journalCorruptError{0, "header line is not terminated"}
	}
	h, err := decodeXTxHeader(data[:nl])
	if err != nil {
		return nil, err
	}
	p.Header = h
	pos := int64(nl) + 1
	p.ValidLen = pos
	for pos < int64(len(data)) {
		rest := data[pos:]
		i := bytes.IndexByte(rest, '\n')
		if i < 0 {
			// Unterminated final line: torn tail, never acknowledged.
			p.Torn = true
			break
		}
		line := rest[:i]
		next := pos + int64(i) + 1
		if len(line) > xtxMaxJournalRecord {
			return nil, &journalCorruptError{pos, "record too large"}
		}
		if next == int64(len(data)) && allZero(line) {
			p.Torn = true // preallocated / zero-filled final line
			break
		}
		r, err := decodeXTxRecord(line, h.Epoch, pos)
		if err != nil {
			return nil, err
		}
		if err := p.Table.apply(r); err != nil {
			return nil, err
		}
		p.Records = append(p.Records, r)
		pos = next
		p.ValidLen = pos
	}
	return p, nil
}

func tornHeaderPrefix(b []byte) bool {
	if allZero(b) {
		return true
	}
	if len(b) > len(headerPrefix) {
		return bytes.HasPrefix(b, headerPrefix)
	}
	return bytes.Equal(b, headerPrefix[:len(b)])
}

func allZero(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return len(b) > 0
}

// --- the journal ------------------------------------------------------------

// xtxOptions carries the test seams shared with the segment layer.
type xtxOptions struct {
	wrapFile fileWrapper // decorates every file the journal opens (faultFS.wrap)
	renameFn renameFunc  // rename seam for checkpoint/creation
	now      func() time.Time
}

func (o xtxOptions) clock() time.Time {
	if o.now != nil {
		return o.now().UTC()
	}
	return time.Now().UTC()
}

type xtxJournal struct {
	mu       sync.Mutex
	dir      string
	path     string
	opt      xtxOptions
	f        segFile
	hdr      xtxHeader
	size     int64
	nextSeq  uint64
	table    *xtxTable
	records  []xtxRecord       // every valid record, in file order (for checkpoint)
	pending  map[string]string // txid → key, allocated and appended but not yet durable
	poisoned error
	closed   bool
}

// openXTxJournal opens dir/xtx.journal if it exists, repairing a torn tail.
// It returns (nil, nil) when there is no journal — a legacy root, or one where
// XTx was never used. A torn creation is re-created with a fresh epoch (nothing
// could have committed against it).
func openXTxJournal(dir string, opt xtxOptions) (*xtxJournal, error) {
	path := filepath.Join(dir, xtxJournalFile)
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("xtx journal: read: %w", err)
	}
	p, err := parseXTxJournal(data)
	if err != nil {
		return nil, err
	}
	if p.TornCreate {
		return createXTxJournal(dir, opt)
	}
	f, err := openJournalFile(path, opt)
	if err != nil {
		return nil, err
	}
	j := &xtxJournal{
		dir: dir, path: path, opt: opt, f: f, hdr: p.Header,
		size: p.ValidLen, table: p.Table, records: p.Records,
		nextSeq: p.Table.maxSeq + 1, pending: map[string]string{},
	}
	if p.Torn {
		if err := f.Truncate(p.ValidLen); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("xtx journal: truncate torn tail: %w", err)
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("xtx journal: sync after truncating torn tail: %w", err)
		}
	}
	return j, nil
}

func openJournalFile(path string, opt xtxOptions) (segFile, error) {
	osf, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("xtx journal: open: %w", err)
	}
	var f segFile = osf
	if opt.wrapFile != nil {
		f = opt.wrapFile(path, f)
	}
	return f, nil
}

// createXTxJournal writes a fresh journal (header only) with tmp + fsync +
// rename + dir fsync, so a crash leaves no half-created file, and opens it.
// The caller must already have made xtx.format durable (§11.3 order).
func createXTxJournal(dir string, opt xtxOptions) (*xtxJournal, error) {
	epoch, err := newXTxEpoch()
	if err != nil {
		return nil, fmt.Errorf("xtx journal: epoch: %w", err)
	}
	hdr := xtxHeader{Magic: xtxJournalMagic, V: xtxJournalVersion, Epoch: epoch,
		Created: opt.clock().Format(time.RFC3339Nano)}
	line, err := encodeXTxHeader(hdr)
	if err != nil {
		return nil, err
	}
	path := filepath.Join(dir, xtxJournalFile)
	if err := writeJournalAtomic(dir, path, line, opt); err != nil {
		return nil, err
	}
	f, err := openJournalFile(path, opt)
	if err != nil {
		return nil, err
	}
	return &xtxJournal{dir: dir, path: path, opt: opt, f: f, hdr: hdr, size: int64(len(line)),
		nextSeq: 1, table: newXTxTable(), pending: map[string]string{}}, nil
}

// openOrCreateXTxJournal is the writer entry point: it makes xtx.format
// durable first, then opens or creates the journal.
func openOrCreateXTxJournal(dir string, opt xtxOptions, createdBy string) (*xtxJournal, error) {
	if err := ensureXTxFormat(dir, createdBy); err != nil {
		return nil, err
	}
	j, err := openXTxJournal(dir, opt)
	if err != nil || j != nil {
		return j, err
	}
	return createXTxJournal(dir, opt)
}

// writeJournalAtomic writes data to xtx.journal.tmp, fsyncs it, renames it over
// path and fsyncs the directory. On any error before the rename the tmp file
// is removed and the old journal is untouched.
func writeJournalAtomic(dir, path string, data []byte, opt xtxOptions) error {
	tmp := filepath.Join(dir, xtxJournalTmpFile)
	osf, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("xtx journal: create tmp: %w", err)
	}
	var f segFile = osf
	if opt.wrapFile != nil {
		f = opt.wrapFile(tmp, f)
	}
	fail := func(what string, err error) error {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("xtx journal: %s: %w", what, err)
	}
	if n, err := f.Write(data); err != nil {
		return fail("write tmp", err)
	} else if n != len(data) {
		return fail("write tmp", io.ErrShortWrite)
	}
	if err := f.Sync(); err != nil {
		return fail("sync tmp", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("xtx journal: close tmp: %w", err)
	}
	if err := doRename(opt.renameFn, tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("xtx journal: rename: %w", err)
	}
	if err := fsyncDir(dir); err != nil {
		return &postRenameError{fmt.Errorf("xtx journal: sync dir after rename: %w", err)}
	}
	return nil
}

// postRenameError marks a failure after the rename took effect: the new file
// is in place but its directory entry is not known durable.
type postRenameError struct{ error }

func (e *postRenameError) Unwrap() error { return e.error }

// usable returns the error that makes mutation impossible, or nil. Caller holds mu.
func (j *xtxJournal) usable() error {
	if j.closed {
		return errors.New("xtx journal: closed")
	}
	if j.poisoned != nil {
		return xtxErr("", ErrXTxDurability, fmt.Errorf("journal poisoned: %w", j.poisoned))
	}
	return nil
}

func (j *xtxJournal) poison(cause error) {
	if j.poisoned == nil {
		j.poisoned = cause
	}
}

// appendLine writes one whole line with a single write. On failure it rolls
// the file back to the previous size; if that fails the journal is poisoned.
// Caller holds mu.
func (j *xtxJournal) appendLine(line []byte) error {
	n, err := j.f.Write(line)
	if err == nil && n != len(line) {
		err = io.ErrShortWrite
	}
	if err != nil {
		if terr := j.f.Truncate(j.size); terr != nil {
			j.poison(fmt.Errorf("append failed (%v) and rollback failed: %w", err, terr))
		}
		return err
	}
	j.size += int64(len(line))
	return nil
}

// syncLocked fsyncs the journal; a failure poisons it (retrying fsync is not
// proof of durability, §6.3 rule 3). Caller holds mu.
func (j *xtxJournal) syncLocked() error {
	if err := j.f.Sync(); err != nil {
		j.poison(fmt.Errorf("fsync: %w", err))
		return err
	}
	return nil
}

func (j *xtxJournal) newRecord(kind, tx, key string) xtxRecord {
	return xtxRecord{K: kind, Tx: tx, Key: key, TS: j.opt.clock().Format(time.RFC3339Nano)}
}

// allocateTxID allocates the next monotonic txid under journal.mu and records it
// in the in-memory pending map (§5.1, §6.2).
func (j *xtxJournal) allocateTxID(key string) (string, error) {
	if len(key) > xtxMaxKeyLen {
		return "", xtxErr("", ErrXTxTooLarge, fmt.Errorf("idempotency key longer than %d bytes", xtxMaxKeyLen))
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.usable(); err != nil {
		return "", err
	}
	tx := formatTxID(j.hdr.Epoch, j.nextSeq)
	j.nextSeq++
	j.pending[tx] = key
	return tx, nil
}

// commitPrepared appends a COMMIT record for an already-allocated txid and fsyncs (S3+S4).
func (j *xtxJournal) commitPrepared(tx, key string, parts []xtxPart) error {
	if len(key) > xtxMaxKeyLen {
		return xtxErr(tx, ErrXTxTooLarge, fmt.Errorf("idempotency key longer than %d bytes", xtxMaxKeyLen))
	}
	if err := validateParts(parts); err != nil {
		return fmt.Errorf("xtx journal: %w", err)
	}
	pd, err := partsDigest(parts)
	if err != nil {
		return fmt.Errorf("xtx journal: %w", err)
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.usable(); err != nil {
		return err
	}
	r := j.newRecord(xtxKindCommit, tx, key)
	r.Parts = append([]xtxPart(nil), parts...)
	r.PD = pd
	line, err := encodeXTxRecord(r)
	if err != nil {
		return err
	}
	if err := j.appendLine(line); err != nil {
		delete(j.pending, tx)
		return xtxErr(tx, ErrXTxDurability, err)
	}
	if err := j.syncLocked(); err != nil {
		// Not installed in the table: the outcome is whatever recovery finds.
		return xtxErr(tx, ErrXTxOutcomeUnknown, err)
	}
	delete(j.pending, tx)
	return j.installLocked(r)
}

// commit allocates the next txid, appends COMMIT{parts} and fsyncs (S3+S4).
// parts must already be canonical (validateParts). Errors:
//   - validation: plain error, nothing written, seq not consumed
//   - append failure: ErrXTxDurability (aborted-or-unknown per §6.4; the record
//     was rolled back or the journal poisoned, so recovery decides)
//   - fsync failure: ErrXTxOutcomeUnknown — the record may be durable.
//
// A txid is returned in the error where one was allocated.
func (j *xtxJournal) commit(key string, parts []xtxPart) (string, error) {
	tx, err := j.allocateTxID(key)
	if err != nil {
		return "", err
	}
	if err := j.commitPrepared(tx, key, parts); err != nil {
		return tx, err
	}
	return tx, nil
}

func (j *xtxJournal) installLocked(r xtxRecord) error {
	if err := j.table.apply(r); err != nil {
		return err
	}
	j.records = append(j.records, r)
	return nil
}

// abort appends ABORT{why} and fsyncs. It is advisory for transactions that
// never reached COMMIT (§6.4) and the write recovery uses for presumed aborts
// (why "recovery"). Aborting a committed tx is refused: a decided tx is never
// aborted. Idempotent for an already aborted tx.
func (j *xtxJournal) abort(tx, key, why string) error {
	if !xtxAbortReasons[why] {
		return fmt.Errorf("xtx journal: unknown abort reason %q", why)
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.usable(); err != nil {
		return err
	}
	if e, _, err := parseTxID(tx); err != nil || e != j.hdr.Epoch {
		return fmt.Errorf("xtx journal: txid %q does not belong to this journal", tx)
	}
	if d := j.table.byTx[tx]; d != nil {
		if d.Outcome != xtxKindAbort {
			return conflictErr(tx, "abort requested for a committed transaction")
		}
		return nil
	}
	return j.appendDecisionLocked(j.newRecord(xtxKindAbort, tx, key), why)
}

func (j *xtxJournal) appendDecisionLocked(r xtxRecord, why string) error {
	r.Why = why
	line, err := encodeXTxRecord(r)
	if err != nil {
		return err
	}
	if err := j.appendLine(line); err != nil {
		return xtxErr(r.Tx, ErrXTxDurability, err)
	}
	if err := j.syncLocked(); err != nil {
		return xtxErr(r.Tx, ErrXTxOutcomeUnknown, err)
	}
	return j.installLocked(r)
}

// retire appends RETIRE{outcome of the existing decision} (§4.4 step 2). The
// caller must already have verified participant evidence. Retiring an
// undecided tx is refused; retiring twice is idempotent.
func (j *xtxJournal) retire(tx string) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.usable(); err != nil {
		return err
	}
	d := j.table.byTx[tx]
	if d == nil {
		return fmt.Errorf("xtx journal: retire of undecided tx %s", tx)
	}
	if d.Retired {
		return nil
	}
	r := j.newRecord(xtxKindRetire, tx, d.Key)
	r.O = d.Outcome
	line, err := encodeXTxRecord(r)
	if err != nil {
		return err
	}
	if err := j.appendLine(line); err != nil {
		return xtxErr(tx, ErrXTxDurability, err)
	}
	if err := j.syncLocked(); err != nil {
		return xtxErr(tx, ErrXTxOutcomeUnknown, err)
	}
	return j.installLocked(r)
}

// decision returns a copy of the decision for tx, if one is durable.
func (j *xtxJournal) decision(tx string) (xtxDecision, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	d := j.table.byTx[tx]
	if d == nil {
		return xtxDecision{}, false
	}
	c := *d
	c.Parts = append([]xtxPart(nil), d.Parts...)
	return c, true
}

// decisions returns a snapshot of the whole decision table, sorted by txid
// (deterministic for recovery, invariant I5).
func (j *xtxJournal) decisions() []xtxDecision {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := make([]xtxDecision, 0, len(j.table.byTx))
	for _, d := range j.table.byTx {
		c := *d
		c.Parts = append([]xtxPart(nil), d.Parts...)
		out = append(out, c)
	}
	sort.Slice(out, func(a, b int) bool { return out[a].Tx < out[b].Tx })
	return out
}

func statusOf(d *xtxDecision) XTxStatus {
	if d.Outcome == xtxKindCommit {
		return XTxCommitted
	}
	return XTxAborted
}

// status answers §8.2 for a txid:
//
//	durable COMMIT / RETIRE(commit)        COMMITTED
//	durable ABORT / RETIRE(abort)          ABORTED
//	appended, fsync not yet confirmed      PENDING
//	this epoch, seq already allocated,
//	  no record (collected or lost)        EXPIRED  (never claimed safe-to-retry)
//	otherwise                              UNKNOWN
func (j *xtxJournal) status(tx string) XTxStatus {
	j.mu.Lock()
	defer j.mu.Unlock()
	if d := j.table.byTx[tx]; d != nil {
		return statusOf(d)
	}
	if _, ok := j.pending[tx]; ok {
		return XTxPending
	}
	if e, seq, err := parseTxID(tx); err == nil && e == j.hdr.Epoch && seq < j.nextSeq {
		return XTxExpired
	}
	return XTxUnknown
}

// statusByKey answers §8.2 for an idempotency key. A key with a COMMIT
// anywhere reports COMMITTED. A key never seen (or whose records were
// garbage-collected beyond the retention window) is UNKNOWN: the retention
// floor, not this call, is what keeps that safe.
func (j *xtxJournal) statusByKey(key string) (XTxStatus, string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if key == "" {
		return XTxUnknown, ""
	}
	if tx, ok := j.table.byKey[key]; ok {
		return statusOf(j.table.byTx[tx]), tx
	}
	for tx, k := range j.pending {
		if k == key {
			return XTxPending, tx
		}
	}
	return XTxUnknown, ""
}

// observeSeq raises the next txid sequence above seq (§5.1: restore from
// max(journal, stamped entries) + 1).
func (j *xtxJournal) observeSeq(seq uint64) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if seq >= j.nextSeq {
		j.nextSeq = seq + 1
	}
}

// epoch returns the journal epoch (txid prefix).
func (j *xtxJournal) epoch() string {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.hdr.Epoch
}

// generation returns the checkpoint generation (0 for a never-rewritten file).
func (j *xtxJournal) generation() uint64 {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.hdr.Gen
}

// checkpoint rewrites the journal without the records of every tx for which
// drop returns true (§4.5): xtx.journal.tmp is written and fsynced with a
// header of the same epoch and generation+1, renamed over xtx.journal, and the
// directory fsynced. A crash leaves the old or the new file whole. drop is
// only consulted for retired decisions — a live decision is never discarded.
// It returns the number of transactions dropped.
func (j *xtxJournal) checkpoint(drop func(xtxDecision) bool) (int, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := j.usable(); err != nil {
		return 0, err
	}
	// The record holding the highest seq is never dropped: it is the
	// high-water mark that keeps a reopened journal from reissuing a txid.
	hwm := formatTxID(j.hdr.Epoch, j.table.maxSeq)
	dropTx := map[string]bool{}
	for tx, d := range j.table.byTx {
		if d.Retired && tx != hwm && drop(*d) {
			dropTx[tx] = true
		}
	}
	if len(dropTx) == 0 {
		return 0, nil
	}
	hdr := j.hdr
	hdr.Gen++
	buf, err := encodeXTxHeader(hdr)
	if err != nil {
		return 0, err
	}
	var kept []xtxRecord
	table := newXTxTable()
	for _, r := range j.records {
		if dropTx[r.Tx] {
			continue
		}
		line, err := encodeXTxRecord(r)
		if err != nil {
			return 0, err
		}
		buf = append(buf, line...)
		kept = append(kept, r)
		if err := table.apply(r); err != nil {
			return 0, err
		}
	}
	if err := writeJournalAtomic(j.dir, j.path, buf, j.opt); err != nil {
		var pr *postRenameError
		if errors.As(err, &pr) {
			j.poison(err) // renamed but not known durable: fail closed
		}
		// Otherwise it failed before the rename took effect: old file intact.
		return 0, err
	}
	nf, err := openJournalFile(j.path, j.opt)
	if err != nil {
		j.poison(err)
		return 0, err
	}
	_ = j.f.Close()
	j.f, j.hdr, j.size, j.records = nf, hdr, int64(len(buf)), kept
	j.table = table
	return len(dropTx), nil
}

// close releases the file. It does not fsync: every mutation already did.
func (j *xtxJournal) close() error {
	if j == nil {
		return nil
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.closed {
		return nil
	}
	j.closed = true
	return j.f.Close()
}
