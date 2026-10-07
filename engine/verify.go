package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Severity ranks how serious a verification finding is. The order matters:
// each level is strictly worse than the one before it.
type Severity string

const (
	// SeverityInfo is informational: expected leftovers or state that open
	// handles automatically (a torn tail the next open trims, a held lock).
	SeverityInfo Severity = "info"
	// SeverityRepairableIndex means a derived structure (primary or secondary
	// index, id counter, interrupted compaction swap) disagrees with the
	// segments. The segments are the source of truth, so rebuilding from them
	// fixes it without losing data.
	SeverityRepairableIndex Severity = "repairable-index"
	// SeverityDataCorruption means segment bytes themselves are damaged.
	SeverityDataCorruption Severity = "data-corruption"
	// SeverityConflict means the record history is ambiguous (duplicate ids,
	// conflicting revisions, unique violations) and must not be resolved
	// automatically.
	SeverityConflict Severity = "conflict"
)

func (s Severity) rank() int {
	switch s {
	case SeverityInfo:
		return 1
	case SeverityRepairableIndex:
		return 2
	case SeverityDataCorruption:
		return 3
	case SeverityConflict:
		return 4
	}
	return 0
}

// FindingCode is the stable, machine-readable identity of a finding.
type FindingCode string

const (
	// Segment bytes.
	CodeSegmentTornTail     FindingCode = "segment-torn-tail"
	CodeSegmentBadRegion    FindingCode = "segment-bad-region"
	CodeSegmentGluedLine    FindingCode = "segment-glued-line"
	CodeSegmentUnreadable   FindingCode = "segment-unreadable"
	CodeOrphanSegmentFile   FindingCode = "orphan-segment-file"
	CodeLeftoverTempFile    FindingCode = "leftover-temp-file"
	CodeManifestPending     FindingCode = "compaction-manifest-pending"
	CodeManifestCorrupt     FindingCode = "compaction-manifest-corrupt"
	CodeLockHeld            FindingCode = "lock-held"
	CodeLockHeldByThisProc  FindingCode = "lock-held-by-this-process"
	CodeLockProbeFailed     FindingCode = "lock-probe-failed"
	CodeMetaMissing         FindingCode = "meta-missing"
	CodeMetaUnreadable      FindingCode = "meta-unreadable"
	CodeIDCounterBehind     FindingCode = "idcounter-behind"
	CodeRecordExpiredLive   FindingCode = "record-expired-live"
	CodeDuplicateIdentical  FindingCode = "duplicate-record-identical"
	CodeConflictDuplicateID FindingCode = "conflict-duplicate-id"
	CodeConflictIDReuse     FindingCode = "conflict-id-reuse-after-delete"
	CodeConflictWriteAfter  FindingCode = "conflict-write-after-delete"
	CodeConflictRevision    FindingCode = "conflict-revision-regression"

	// Primary index.
	CodeIndexMissing                FindingCode = "index-missing"
	CodeIndexUnreadable             FindingCode = "index-unreadable"
	CodeIndexCoverageUnknown        FindingCode = "index-coverage-unknown"
	CodeIndexCoverageMismatch       FindingCode = "index-coverage-mismatch"
	CodeIndexCoverageSegmentMissing FindingCode = "index-coverage-segment-missing"
	CodeIndexStaleTail              FindingCode = "index-stale-tail"
	CodeIndexSegmentUnlisted        FindingCode = "index-segment-unlisted"
	CodeIndexSpotCheckFailed        FindingCode = "index-spotcheck-failed"
	CodeIndexMissingRecord          FindingCode = "index-missing-record"
	CodeIndexStaleRecord            FindingCode = "index-stale-record"
	CodeIndexWrongRecord            FindingCode = "index-wrong-record"
	CodeIndexDanglingOffset         FindingCode = "index-dangling-offset"
	CodeIndexDanglingEntry          FindingCode = "index-dangling-entry"
	CodeIndexResurrected            FindingCode = "index-resurrected-delete"

	// Secondary indexes.
	CodeSidxUnreadable             FindingCode = "sidx-unreadable"
	CodeSidxCoverageUnknown        FindingCode = "sidx-coverage-unknown"
	CodeSidxCoverageMismatch       FindingCode = "sidx-coverage-mismatch"
	CodeSidxCoverageSegmentMissing FindingCode = "sidx-coverage-segment-missing"
	CodeSidxStaleTail              FindingCode = "sidx-stale-tail"
	CodeSidxSegmentUnlisted        FindingCode = "sidx-segment-unlisted"
	CodeSidxMissingEntry           FindingCode = "sidx-missing-entry"
	CodeSidxExtraEntry             FindingCode = "sidx-extra-entry"
	CodeSidxWrongBucket            FindingCode = "sidx-wrong-bucket"
	CodeSidxUniqueViolation        FindingCode = "sidx-unique-violation"
)

// VerifyMode selects how deep verification goes.
type VerifyMode string

const (
	// VerifyQuick checks persisted-index coverage and fingerprints, a bounded
	// identity spot-check of index entries, structural files, the id counter
	// and the newest segment only. It never reads every segment.
	VerifyQuick VerifyMode = "quick"
	// VerifyFull additionally scans every segment tolerantly and compares the
	// primary and secondary indexes against the ground truth rebuilt from it.
	VerifyFull VerifyMode = "full"
)

// DefaultMaxFindingsPerCode caps how many findings of one code are retained
// per collection; the remainder are counted in CollectionReport.Truncated.
const DefaultMaxFindingsPerCode = 1000

// VerifyOptions configures Verify. The zero value is a full check of every
// collection.
type VerifyOptions struct {
	Mode VerifyMode
	// Collections restricts the check to these collection names (nil = all).
	Collections []string
	// MaxFindingsPerCode overrides DefaultMaxFindingsPerCode (<= 0 = default).
	MaxFindingsPerCode int
	// Now is the clock used for expiry findings (zero = time.Now()).
	Now time.Time
}

func (o VerifyOptions) normalized() VerifyOptions {
	if o.Mode == "" {
		o.Mode = VerifyFull
	}
	if o.MaxFindingsPerCode <= 0 {
		o.MaxFindingsPerCode = DefaultMaxFindingsPerCode
	}
	if o.Now.IsZero() {
		o.Now = time.Now()
	}
	return o
}

func (o VerifyOptions) wants(name string) bool {
	if len(o.Collections) == 0 {
		return true
	}
	for _, n := range o.Collections {
		if n == name {
			return true
		}
	}
	return false
}

// Location pins a finding to a place on disk. Segment is a base name; Offset
// is meaningful only when Segment is set; ID and Field are zero/empty when not
// applicable (record ids start at 1).
type Location struct {
	Segment string `json:"segment,omitempty"`
	Offset  int64  `json:"offset,omitempty"`
	ID      uint64 `json:"id,omitempty"`
	Field   string `json:"field,omitempty"`
}

// Finding is one verification result.
type Finding struct {
	Severity   Severity    `json:"severity"`
	Code       FindingCode `json:"code"`
	Collection string      `json:"collection,omitempty"`
	Location   Location    `json:"location"`
	Message    string      `json:"message"`
}

// VerifyStats summarizes what a collection check looked at.
type VerifyStats struct {
	Segments         int   `json:"segments"`
	SegmentBytes     int64 `json:"segment_bytes"`
	Entries          int   `json:"entries"`      // valid entries scanned (full mode)
	LiveRecords      int   `json:"live_records"` // live records in the ground truth (full mode)
	BadRegions       int   `json:"bad_regions"`
	IndexEntries     int   `json:"index_entries"`
	SecondaryIndexes int   `json:"secondary_indexes"`
}

// CollectionReport is the verification result for one collection.
type CollectionReport struct {
	Name     string     `json:"name"`
	Mode     VerifyMode `json:"mode"`
	Stats    VerifyStats
	Findings []Finding `json:"findings"`
	// Truncated counts findings dropped per code once MaxFindingsPerCode was hit.
	Truncated map[FindingCode]int `json:"truncated,omitempty"`
}

// IntegrityReport is the structured result of Verify. It is a read-only
// observation: nothing in it was repaired or resolved.
type IntegrityReport struct {
	Mode VerifyMode `json:"mode"`
	// Online is true when the check ran against an open handle's consistent
	// in-memory snapshot, false for an offline directory check (which also
	// validates the persisted index/sidx/meta files).
	Online      bool               `json:"online"`
	Findings    []Finding          `json:"findings,omitempty"` // database-level (lock state)
	Collections []CollectionReport `json:"collections"`
}

// AllFindings returns database-level then per-collection findings.
func (r *IntegrityReport) AllFindings() []Finding {
	out := append([]Finding(nil), r.Findings...)
	for _, c := range r.Collections {
		out = append(out, c.Findings...)
	}
	return out
}

// MaxSeverity returns the worst severity present ("" when there are no findings).
func (r *IntegrityReport) MaxSeverity() Severity {
	var worst Severity
	for _, f := range r.AllFindings() {
		if f.Severity.rank() > worst.rank() {
			worst = f.Severity
		}
	}
	return worst
}

// Clean reports whether there is nothing above informational severity.
func (r *IntegrityReport) Clean() bool { return r.MaxSeverity().rank() <= SeverityInfo.rank() }

// Has reports whether any finding carries code.
func (r *IntegrityReport) Has(code FindingCode) bool {
	for _, f := range r.AllFindings() {
		if f.Code == code {
			return true
		}
	}
	return false
}

// Codes returns the distinct finding codes present, sorted.
func (r *IntegrityReport) Codes() []FindingCode {
	seen := map[FindingCode]bool{}
	for _, f := range r.AllFindings() {
		seen[f.Code] = true
	}
	out := make([]FindingCode, 0, len(seen))
	for c := range seen {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// findingSink collects findings with a per-code cap.
type findingSink struct {
	collection string
	max        int
	counts     map[FindingCode]int
	truncated  map[FindingCode]int
	out        []Finding
}

func newFindingSink(collection string, max int) *findingSink {
	return &findingSink{collection: collection, max: max, counts: map[FindingCode]int{}, truncated: map[FindingCode]int{}}
}

func (s *findingSink) add(sev Severity, code FindingCode, loc Location, format string, args ...any) {
	s.counts[code]++
	if s.counts[code] > s.max {
		s.truncated[code]++
		return
	}
	s.out = append(s.out, Finding{Severity: sev, Code: code, Collection: s.collection, Location: loc, Message: fmt.Sprintf(format, args...)})
}

// Verify checks the collection's integrity online. It takes a consistent
// snapshot of the in-memory index, secondary indexes and segment extents, then
// verifies them against the segment bytes without blocking writers: appends
// only extend files past the snapshotted sizes, and compaction is excluded for
// the duration. It never modifies anything.
func (c *Collection) Verify(ctx context.Context, opts VerifyOptions) (*IntegrityReport, error) {
	opts = opts.normalized()
	rep := &IntegrityReport{Mode: opts.Mode, Online: true}
	cr, err := c.verifyOnline(ctx, opts)
	if err != nil {
		return nil, err
	}
	rep.Collections = []CollectionReport{*cr}
	return rep, nil
}

// Verify checks every open collection (or those named in opts.Collections)
// online. See Collection.Verify.
func (db *DB) Verify(ctx context.Context, opts VerifyOptions) (*IntegrityReport, error) {
	opts = opts.normalized()
	rep := &IntegrityReport{Mode: opts.Mode, Online: true}
	rep.Findings = append(rep.Findings, Finding{
		Severity: SeverityInfo, Code: CodeLockHeldByThisProc,
		Message: "the data directory lock is held by this process (the one running Verify)",
	})

	db.mu.RLock()
	names := make([]string, 0, len(db.collections))
	for n := range db.collections {
		if opts.wants(n) {
			names = append(names, n)
		}
	}
	cols := make(map[string]*Collection, len(names))
	for _, n := range names {
		cols[n] = db.collections[n]
	}
	db.mu.RUnlock()
	sort.Strings(names)

	for _, n := range names {
		cr, err := cols[n].verifyOnline(ctx, opts)
		if err != nil {
			return nil, fmt.Errorf("verify collection %q: %w", n, err)
		}
		rep.Collections = append(rep.Collections, *cr)
	}
	return rep, nil
}

// VerifyDir verifies a data directory offline, without opening it: it reads
// segments and the persisted index/sidx/meta/manifest files exactly as they sit
// on disk (it does not run open-time recovery, so a stale persisted index is
// reported rather than silently fixed). It is read-only and never takes the
// directory lock; a lock held by a live process is itself reported, and in that
// case findings may reflect in-flight writes.
func VerifyDir(ctx context.Context, dataDir string, opts VerifyOptions) (*IntegrityReport, error) {
	opts = opts.normalized()
	rep := &IntegrityReport{Mode: opts.Mode}

	held, err := probeDirLock(dataDir)
	switch {
	case err != nil:
		rep.Findings = append(rep.Findings, Finding{Severity: SeverityInfo, Code: CodeLockProbeFailed,
			Message: fmt.Sprintf("could not determine lock state: %v", err)})
	case held:
		rep.Findings = append(rep.Findings, Finding{Severity: SeverityInfo, Code: CodeLockHeld,
			Message: "the data directory is open in another process; persisted files may legitimately lag its in-memory state"})
	}

	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return nil, fmt.Errorf("verify: read %q: %w", dataDir, err)
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() && opts.wants(e.Name()) {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	for _, n := range names {
		cr, err := verifyOffline(ctx, filepath.Join(dataDir, n), n, opts)
		if err != nil {
			return nil, fmt.Errorf("verify collection %q: %w", n, err)
		}
		rep.Collections = append(rep.Collections, *cr)
	}
	return rep, nil
}

var errVerifyClosed = errors.New("engine: verify: collection is closed")

func (c *Collection) verifyOnline(ctx context.Context, opts VerifyOptions) (*CollectionReport, error) {
	// compactMu excludes compaction (which replaces sealed files) for the whole
	// check; lock order is compactMu → mu.
	c.compactMu.Lock()
	defer c.compactMu.Unlock()
	if c.closeDone {
		return nil, errVerifyClosed
	}

	c.mu.RLock()
	all := make([]*Segment, 0, len(c.sealed)+1)
	all = append(all, c.sealed...)
	all = append(all, c.active)
	segs := make([]vseg, 0, len(all))
	for _, s := range all {
		n, _ := segmentNum(s.Path())
		segs = append(segs, vseg{seg: s, path: s.Path(), name: filepath.Base(s.Path()), num: n, size: s.Size()})
	}
	c.index.mu.RLock()
	entries := make(map[uint64]IndexEntry, len(c.index.entries))
	for id, e := range c.index.entries {
		entries[id] = e
	}
	c.index.mu.RUnlock()
	var views []sidxView
	c.sidxMu.RLock()
	fields := make([]string, 0, len(c.sidxMap))
	for f := range c.sidxMap {
		fields = append(fields, f)
	}
	sort.Strings(fields)
	for _, f := range fields {
		sn := c.sidxMap[f].snapshot()
		views = append(views, sidxView{field: f, unique: sn.unique, buckets: sn.buckets})
	}
	c.sidxMu.RUnlock()
	counter := c.idSeq.Load()
	c.mu.RUnlock()

	in := &verifyInput{
		name: c.name, dir: c.dir, opts: opts,
		segs: segs, hasIndex: true, index: entries,
		sidx:      views,
		idCounter: counter, idCounterKnown: true, idCounterLive: true,
	}
	sink := newFindingSink(c.name, opts.MaxFindingsPerCode)
	listDirFindings(c.dir, in, sink, true)
	if err := runChecks(ctx, in, sink); err != nil {
		return nil, err
	}
	return in.report(sink), nil
}

func verifyOffline(ctx context.Context, dir, name string, opts VerifyOptions) (*CollectionReport, error) {
	in := &verifyInput{name: name, dir: dir, opts: opts, persisted: true}
	sink := newFindingSink(name, opts.MaxFindingsPerCode)
	listDirFindings(dir, in, sink, false)
	loadPersisted(in, sink)
	if err := runChecks(ctx, in, sink); err != nil {
		return nil, err
	}
	return in.report(sink), nil
}

func (in *verifyInput) report(sink *findingSink) *CollectionReport {
	cr := &CollectionReport{Name: in.name, Mode: in.opts.Mode, Stats: in.stats, Findings: sink.out}
	if cr.Findings == nil {
		cr.Findings = []Finding{}
	}
	if len(sink.truncated) > 0 {
		cr.Truncated = sink.truncated
	}
	return cr
}
