package engine

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/srjn45/scriva/store"
)

// Repair errors. The *RepairReport returned alongside them is always non-nil
// once the run got past argument validation and locking.
var (
	// ErrRepairConflict is returned under ConflictAbort when any selected
	// collection has ambiguous record history. Nothing was modified.
	ErrRepairConflict = errors.New("engine: repair aborted: conflicting record history")
	// ErrRepairIncomplete is returned when at least one collection was left
	// untouched because repairing it would require guessing (see
	// CollectionRepair.Reason). The other collections were still repaired.
	ErrRepairIncomplete = errors.New("engine: repair incomplete: some collections need operator action")
)

// repairFault is a test seam: it is called at named points of a repair and a
// non-nil return aborts the run there, simulating a crash. Nil in production.
var repairFault func(stage string) error

func repairPoint(stage string) error {
	if repairFault != nil {
		return repairFault(stage)
	}
	return nil
}

const repairJournalFilename = "REPAIR_JOURNAL.json"

// ConflictPolicy says what Repair does with ambiguous record history
// (duplicate ids, conflicting revisions, unique violations). Repair never
// resolves a conflict itself under either policy.
type ConflictPolicy string

const (
	// ConflictReport (default) rebuilds derived structures with exactly the
	// semantics open uses (last line wins) and lists every conflict in the
	// report. Salvage is refused for a collection that has conflicts.
	ConflictReport ConflictPolicy = "report"
	// ConflictAbort stops before any mutation (before the backup) if any
	// selected collection has a conflict, returning ErrRepairConflict.
	ConflictAbort ConflictPolicy = "abort"
)

// RepairOptions configures Repair. The zero value repairs every collection
// that needs it, reports conflicts, and does not salvage.
type RepairOptions struct {
	// Collections restricts the repair to these names (nil = all).
	Collections []string
	// BackupDir is the parent directory in which the timestamped backup
	// (repair-backup-<UTC time>) is created. It must not be inside the data
	// directory. Default: the data directory's parent.
	BackupDir string
	// OnConflict selects the conflict policy (default ConflictReport).
	OnConflict ConflictPolicy
	// Salvage allows moving the valid records of segments that contain
	// damaged regions into a NEW segment, when doing so provably cannot change
	// what any id resolves to. The damaged originals are never deleted: they
	// are moved, byte-for-byte, into <collection>/quarantine/<run>/ (a
	// subdirectory open, verify and rebuild never read) together with a
	// MANIFEST.json of sizes and SHA-256 hashes, and the verified backup
	// keeps a second copy. Without Salvage a
	// collection with damaged segment bytes is left untouched and reported.
	Salvage bool
	// AllowMetaReset permits rewriting an unreadable meta.json without its
	// non-derivable settings (default TTL, encryption). Without it such a
	// collection is left untouched, because those settings would be guessed.
	AllowMetaReset bool
	// Now is the clock used for the backup name and expiry findings
	// (zero = time.Now()).
	Now time.Time
}

// RepairStatus is the outcome for one collection.
type RepairStatus string

const (
	RepairUnchanged RepairStatus = "unchanged" // nothing above informational severity
	RepairRepaired  RepairStatus = "repaired"
	RepairBlocked   RepairStatus = "blocked" // untouched: fixing it would require guessing
)

// RepairAction is one change Repair made (or, for a Blocked collection, none).
type RepairAction struct {
	Kind   string `json:"kind"`
	Target string `json:"target,omitempty"`
	Detail string `json:"detail,omitempty"`
}

// SalvageReport describes a salvage into a new segment.
type SalvageReport struct {
	NewSegment  string   `json:"new_segment"`
	Quarantined []string `json:"quarantined"` // damaged originals moved out of the replay set (never deleted)
	// QuarantineDir is where the originals now live, relative to the
	// collection directory (quarantine/<run>); it holds a MANIFEST.json.
	QuarantineDir string `json:"quarantine_dir,omitempty"`
	Entries       int    `json:"entries"`
	GluedEmbeds   int    `json:"glued_entries"` // entries recovered after corrupt bytes on the same line
	BadRegions    int    `json:"bad_regions"`   // damaged regions whose contents are lost
}

// CollectionRepair is the result for one collection.
type CollectionRepair struct {
	Name      string            `json:"name"`
	Status    RepairStatus      `json:"status"`
	Reason    string            `json:"reason,omitempty"`
	Before    *CollectionReport `json:"before"`
	After     *CollectionReport `json:"after,omitempty"`
	Conflicts []Finding         `json:"conflicts,omitempty"`
	Actions   []RepairAction    `json:"actions,omitempty"`
	Salvage   *SalvageReport    `json:"salvage,omitempty"`
}

// RepairReport is the structured result of Repair.
type RepairReport struct {
	Dir string `json:"dir"`
	// BackupDir is the verified backup taken before any mutation ("" when
	// nothing needed repair).
	BackupDir string `json:"backup_dir,omitempty"`
	// Resumed is true when a journal from an interrupted run was continued.
	Resumed     bool               `json:"resumed,omitempty"`
	Collections []CollectionRepair `json:"collections"`
}

// Blocked lists the collections left untouched.
func (r *RepairReport) Blocked() []string {
	var out []string
	for _, c := range r.Collections {
		if c.Status == RepairBlocked {
			out = append(out, c.Name)
		}
	}
	return out
}

// repairJournal is the restart record. It pins the backup (taken once, before
// the first mutation, and never retaken: a later backup would capture a
// half-repaired directory) and records the multi-step plans whose inputs
// disappear as they execute.
type repairJournal struct {
	Version   int                    `json:"version"`
	BackupDir string                 `json:"backup_dir"`
	StartedAt time.Time              `json:"started_at"`
	Scope     []string               `json:"scope"` // collections the backup covers
	Cols      map[string]*journalCol `json:"collections"`
}

type journalCol struct {
	// Renames maps unnumbered segment names to the numbered names adopting
	// them. Persisted before the first rename so a rerun finishes the job.
	Renames map[string]string `json:"renames,omitempty"`
	// Salvage, once SalvageWritten, names the new segment and the originals to
	// quarantine; the originals' contents then live only in the new segment (and
	// the backup), so the plan must not be recomputed.
	Salvage        *SalvageReport `json:"salvage,omitempty"`
	SalvageWritten bool           `json:"salvage_written,omitempty"`
	Done           bool           `json:"done,omitempty"`
}

func (j *repairJournal) col(name string) *journalCol {
	if j.Cols == nil {
		j.Cols = map[string]*journalCol{}
	}
	c := j.Cols[name]
	if c == nil {
		c = &journalCol{}
		j.Cols[name] = c
	}
	return c
}

func journalPath(dir string) string { return filepath.Join(dir, repairJournalFilename) }

func (j *repairJournal) save(dir string) error {
	b, err := json.MarshalIndent(j, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(journalPath(dir), b, 0o644)
}

// Repair repairs a data directory offline. It is built on VerifyDir: only
// collections whose verification shows more than informational findings are
// touched.
//
// Guarantees:
//   - It takes the exclusive directory lock and returns ErrDatabaseLocked if
//     the directory is open anywhere (this process or another).
//   - Before the first mutation it copies the affected collections to a
//     timestamped backup and verifies every file byte-for-byte (SHA-256). A
//     backup that does not verify aborts the run with nothing changed.
//   - Segments are the source of truth and are never edited. The primary
//     index, secondary indexes and meta.json are rebuilt atomically from a
//     tolerant scan of the segments, with v2 coverage that open accepts as-is;
//     tombstones stay in the segments and so stay deleted. An interrupted
//     compaction swap is rolled forward exactly as open would. The only
//     segment-level changes are: an unacknowledged torn tail on the newest
//     segment is trimmed (as open does), unnumbered seg_*.ndjson files are
//     renamed to the next free number when their ids overlap no other
//     segment, and (opt-in) Salvage.
//   - Conflicting history is never resolved, only reported or (ConflictAbort)
//     refused.
//   - It is restartable and idempotent: a journal in the data directory pins
//     the backup and multi-step plans, so rerunning after an interruption
//     continues; rerunning on a repaired directory does nothing.
//
// It has no open-time policy and no CLI; callers decide when to invoke it.
func Repair(ctx context.Context, dataDir string, opts RepairOptions) (*RepairReport, error) {
	if opts.OnConflict == "" {
		opts.OnConflict = ConflictReport
	}
	if opts.OnConflict != ConflictReport && opts.OnConflict != ConflictAbort {
		return nil, fmt.Errorf("repair: unknown conflict policy %q", opts.OnConflict)
	}
	if opts.Now.IsZero() {
		opts.Now = time.Now()
	}
	if st, err := os.Stat(dataDir); err != nil || !st.IsDir() {
		return nil, fmt.Errorf("repair: %q is not a directory", dataDir)
	}
	absData, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, err
	}
	dataDir = absData // index.json stores segment paths relative to the directory; keep them absolute while building
	if opts.BackupDir == "" {
		opts.BackupDir = filepath.Dir(absData)
	}
	if err := checkBackupOutside(absData, opts.BackupDir); err != nil {
		return nil, err
	}

	lock, err := lockDir(dataDir)
	if err != nil {
		return nil, err
	}
	defer func() { _ = lock.release() }()

	rep := &RepairReport{Dir: dataDir}
	jr, err := loadJournal(dataDir)
	if err != nil {
		return nil, err
	}
	if jr != nil {
		rep.Resumed = true
		rep.BackupDir = jr.BackupDir
	}

	// 1. Observe: verify the directory as it stands.
	vopts := VerifyOptions{Mode: VerifyFull, Collections: opts.Collections, Now: opts.Now}
	pre, err := VerifyDir(ctx, dataDir, vopts)
	if err != nil {
		return nil, err
	}
	var work []*repairWork
	for i := range pre.Collections {
		cr := pre.Collections[i]
		w := &repairWork{name: cr.Name, dir: filepath.Join(dataDir, cr.Name), before: &cr}
		for _, f := range cr.Findings {
			if f.Severity == SeverityConflict {
				w.conflicts = append(w.conflicts, f)
			}
		}
		jc := (*journalCol)(nil)
		if jr != nil {
			jc = jr.Cols[cr.Name]
		}
		w.needs = collectionNeedsRepair(w.dir, &cr) || (jc != nil && !jc.Done)
		work = append(work, w)
	}
	if opts.OnConflict == ConflictAbort {
		aborted := false
		for _, w := range work {
			if len(w.conflicts) > 0 {
				aborted = true
			}
		}
		if aborted {
			for _, w := range work {
				rep.Collections = append(rep.Collections, CollectionRepair{
					Name: w.name, Status: RepairBlocked, Before: w.before, Conflicts: w.conflicts,
					Reason: "aborted: conflicting record history in the directory (policy abort)",
				})
			}
			return rep, ErrRepairConflict
		}
	}

	var todo []*repairWork
	for _, w := range work {
		if w.needs {
			todo = append(todo, w)
		}
	}

	// 2. Plan (read-only): decide per collection whether it can be repaired
	// without guessing, before anything is backed up or mutated.
	for _, w := range todo {
		if err := ctx.Err(); err != nil {
			return rep, err
		}
		var jc *journalCol
		if jr != nil {
			jc = jr.Cols[w.name]
		}
		if err := planCollection(ctx, w, jc, opts); err != nil {
			return rep, err
		}
	}

	// 3. Backup once, then journal.
	var runnable []*repairWork
	for _, w := range todo {
		if w.blocked == "" {
			runnable = append(runnable, w)
		}
	}
	if len(runnable) > 0 && jr == nil {
		names := make([]string, len(runnable))
		for i, w := range runnable {
			names[i] = w.name
		}
		bk, err := makeVerifiedBackup(ctx, dataDir, opts.BackupDir, names, opts.Now)
		if err != nil {
			return rep, err
		}
		jr = &repairJournal{Version: 1, BackupDir: bk, StartedAt: opts.Now.UTC(), Scope: names}
		if err := jr.save(dataDir); err != nil {
			return rep, err
		}
		rep.BackupDir = bk
		if err := repairPoint("backup-done"); err != nil {
			return rep, err
		}
	}
	if jr != nil && len(runnable) > 0 {
		// A resumed run must still be covered by its backup. A collection that
		// became newly eligible (not in scope) cannot be repaired without a
		// fresh pre-mutation copy of it, which would be safe because it has no
		// journal entry and so was never touched.
		var extra []string
		inScope := map[string]bool{}
		for _, n := range jr.Scope {
			inScope[n] = true
		}
		for _, w := range runnable {
			if !inScope[w.name] {
				extra = append(extra, w.name)
			}
		}
		if len(extra) > 0 {
			if err := backupMore(ctx, dataDir, jr.BackupDir, extra); err != nil {
				return rep, err
			}
			jr.Scope = append(jr.Scope, extra...)
			if err := jr.save(dataDir); err != nil {
				return rep, err
			}
		}
	}

	// 4. Execute.
	for _, w := range runnable {
		if err := ctx.Err(); err != nil {
			return rep, err
		}
		if err := executeCollection(ctx, dataDir, w, jr, opts); err != nil {
			return rep, fmt.Errorf("repair collection %q: %w", w.name, err)
		}
	}

	// 5. Re-verify what was repaired and assemble the report.
	for _, w := range work {
		cr := CollectionRepair{Name: w.name, Before: w.before, Conflicts: w.conflicts, Actions: w.actions, Salvage: w.salvageRep}
		switch {
		case w.blocked != "":
			cr.Status, cr.Reason = RepairBlocked, w.blocked
		case !w.needs:
			cr.Status = RepairUnchanged
		default:
			cr.Status = RepairRepaired
			after, err := verifyOffline(ctx, w.dir, w.name, vopts.normalized())
			if err != nil {
				return rep, err
			}
			cr.After = after
		}
		rep.Collections = append(rep.Collections, cr)
	}

	// 6. Retire the journal (the backup stays) and leave a report beside it.
	if jr != nil {
		if allDone(jr, runnable) {
			if b, err := json.MarshalIndent(rep, "", "  "); err == nil {
				_ = writeFileAtomic(filepath.Join(jr.BackupDir, "repair-report.json"), b, 0o644)
			}
			if err := os.Remove(journalPath(dataDir)); err != nil && !errors.Is(err, os.ErrNotExist) {
				return rep, err
			}
			_ = fsyncDir(dataDir)
		}
	}
	if len(rep.Blocked()) > 0 {
		return rep, ErrRepairIncomplete
	}
	return rep, nil
}

// allDone reports whether every collection the journal started, and every one
// this run executed, finished. Blocked collections were never started.
func allDone(jr *repairJournal, ws []*repairWork) bool {
	for _, c := range jr.Cols {
		if !c.Done {
			return false
		}
	}
	for _, w := range ws {
		if c := jr.Cols[w.name]; c == nil || !c.Done {
			return false
		}
	}
	return true
}

func loadJournal(dataDir string) (*repairJournal, error) {
	b, err := os.ReadFile(journalPath(dataDir))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("repair: read journal: %w", err)
	}
	var j repairJournal
	if err := json.Unmarshal(b, &j); err != nil {
		return nil, fmt.Errorf("repair: journal %q is unreadable (%w); the backup it names is intact, remove the journal only after checking the directory against it", journalPath(dataDir), err)
	}
	if st, err := os.Stat(j.BackupDir); err != nil || !st.IsDir() {
		return nil, fmt.Errorf("repair: journal names backup %q, which is missing", j.BackupDir)
	}
	return &j, nil
}

// repairWork carries one collection through plan and execute.
type repairWork struct {
	name, dir string
	before    *CollectionReport
	conflicts []Finding
	needs     bool
	blocked   string
	actions   []RepairAction

	renames    map[string]string
	salvageRep *SalvageReport
	metaReset  bool
	sidxUnique map[string]bool
}

// collectionNeedsRepair is true when verification shows anything above
// informational severity, or an unnumbered segment file that adoption would fix.
func collectionNeedsRepair(dir string, cr *CollectionReport) bool {
	for _, f := range cr.Findings {
		if f.Severity.rank() > SeverityInfo.rank() {
			return true
		}
	}
	return len(unnumberedSegments(dir)) > 0
}

func unnumberedSegments(dir string) []string {
	paths, _ := filepath.Glob(filepath.Join(dir, "seg_*.ndjson"))
	var out []string
	for _, p := range paths {
		if _, ok := segmentNum(p); !ok {
			out = append(out, filepath.Base(p))
		}
	}
	sort.Strings(out)
	return out
}

// compactManifestReadable is true when the manifest is absent or parses. Any
// other read failure counts as unreadable: the plan must not guess past it.
func compactManifestReadable(dir string) bool {
	b, err := os.ReadFile(compactManifestPath(dir))
	if errors.Is(err, os.ErrNotExist) {
		return true
	}
	if err != nil {
		return false
	}
	var m compactManifest
	return json.Unmarshal(b, &m) == nil
}

func readSidxFile(path string) (sidxFile, bool) {
	var f sidxFile
	b, err := os.ReadFile(path)
	if err != nil {
		return f, false
	}
	return f, json.Unmarshal(b, &f) == nil
}

func block(w *repairWork, format string, args ...any) { w.blocked = fmt.Sprintf(format, args...) }

// planCollection decides, read-only, whether the collection can be rebuilt
// without guessing and records what execute must do. A blocked collection is
// never touched.
func planCollection(ctx context.Context, w *repairWork, jc *journalCol, opts RepairOptions) error {
	// A collection mid-way through salvage is already decided: its inputs have
	// changed since the plan was made, and execute finishes the recorded plan.
	if jc != nil && (jc.SalvageWritten || len(jc.Renames) > 0) {
		w.renames = jc.Renames
		w.salvageRep = jc.Salvage
		return nil
	}

	if !compactManifestReadable(w.dir) {
		block(w, "compact.manifest is unreadable; refusing to guess which side of the compaction swap the segments are on")
		return nil
	}
	for _, f := range w.before.Findings {
		if f.Code == CodeSegmentUnreadable {
			block(w, "a segment file or the collection directory cannot be read")
			return nil
		}
	}

	// meta.json: its non-derivable settings must survive.
	if _, err := loadMeta(metaPath(w.dir)); err != nil && !errors.Is(err, os.ErrNotExist) {
		if !opts.AllowMetaReset {
			block(w, "meta.json is unreadable and holds settings (default TTL, encryption) that cannot be derived; restore it from a backup or set AllowMetaReset")
			return nil
		}
		w.metaReset = true
	}

	// Secondary indexes: the unique flag is not derivable from segments.
	w.sidxUnique = map[string]bool{}
	paths, _ := filepath.Glob(filepath.Join(w.dir, "sidx_*.json"))
	for _, p := range paths {
		base := filepath.Base(p)
		field := base[len("sidx_") : len(base)-len(".json")]
		f, ok := readSidxFile(p)
		if !ok {
			block(w, "%s is unreadable, so whether index %q is unique cannot be determined without guessing", base, field)
			return nil
		}
		w.sidxUnique[field] = f.Unique
	}

	// Scan the segments as open's rolled-forward layout will see them.
	segs, err := planSegmentLayout(w.dir)
	if err != nil {
		block(w, "%v", err)
		return nil
	}
	reps := make([]SegmentReport, len(segs))
	var dmg []int // indexes of segments with damage other than a newest-segment torn tail
	for i, s := range segs {
		if err := ctx.Err(); err != nil {
			return err
		}
		r, err := scanSegmentTolerantLimit(s.path, -1)
		if err != nil {
			block(w, "segment %s cannot be scanned: %v", s.name, err)
			return nil
		}
		reps[i] = r
		for _, br := range r.BadRegions {
			if br.Reason == BadRegionTornTailLine && i == len(segs)-1 && br.Offset+br.Length == r.Size {
				continue
			}
			dmg = append(dmg, i)
			break
		}
	}

	// Adoption of unnumbered segments needs a disjoint id set.
	var un []int
	idsOf := func(r SegmentReport) map[uint64]bool {
		m := map[uint64]bool{}
		for _, e := range r.Entries {
			m[e.Entry.ID] = true
		}
		return m
	}
	for i, s := range segs {
		if _, ok := segmentNum(s.name); !ok {
			un = append(un, i)
		}
	}
	if len(un) > 0 {
		var maxNum uint64
		for _, s := range segs {
			if n, ok := segmentNum(s.name); ok && n > maxNum {
				maxNum = n
			}
		}
		for _, i := range un {
			mine := idsOf(reps[i])
			for j := range segs {
				if j == i {
					continue
				}
				for _, e := range reps[j].Entries {
					if mine[e.Entry.ID] {
						block(w, "unnumbered segment %s shares id %d with %s; adopting it would pick a replay order for ambiguous history", segs[i].name, e.Entry.ID, segs[j].name)
						return nil
					}
				}
			}
		}
		w.renames = map[string]string{}
		for _, i := range un {
			maxNum++
			w.renames[segs[i].name] = fmt.Sprintf("seg_%06d.ndjson", maxNum)
		}
	}

	if len(dmg) > 0 {
		if !opts.Salvage {
			block(w, "segment %s has damaged bytes; rebuilding the indexes would not make it readable (set Salvage to move its valid records into a new segment)", segs[dmg[0]].name)
			return nil
		}
		if len(w.conflicts) > 0 {
			block(w, "salvage refused: the collection has conflicting record history (%d findings)", len(w.conflicts))
			return nil
		}
		if len(un) > 0 {
			block(w, "salvage refused: unnumbered segments must be adopted in a separate run first")
			return nil
		}
		damaged := map[int]bool{}
		for _, i := range dmg {
			damaged[i] = true
		}
		// Moving a damaged segment's records to the end of the log changes
		// replay order. That is safe only if no kept segment that originally
		// came after it touches the same ids.
		sr := &SalvageReport{}
		for _, i := range dmg {
			sr.Quarantined = append(sr.Quarantined, segs[i].name)
			sr.BadRegions += len(reps[i].BadRegions)
			mine := idsOf(reps[i])
			for j := i + 1; j < len(segs); j++ {
				if damaged[j] {
					continue
				}
				for _, e := range reps[j].Entries {
					if mine[e.Entry.ID] {
						block(w, "salvage is ambiguous: id %d is in damaged %s and also in later %s", e.Entry.ID, segs[i].name, segs[j].name)
						return nil
					}
				}
			}
			for _, e := range reps[i].Entries {
				sr.Entries++
				if e.RecoveredFromGlued {
					sr.GluedEmbeds++
				}
			}
		}
		var maxNum uint64
		for _, s := range segs {
			if n, ok := segmentNum(s.name); ok && n > maxNum {
				maxNum = n
			}
		}
		sr.NewSegment = fmt.Sprintf("seg_%06d.ndjson", maxNum+1)
		w.salvageRep = sr
	}
	return nil
}

// planSegmentLayout lists the segments open would read after rolling forward
// any pending compaction swap, ordered like open (unnumbered first).
func planSegmentLayout(dir string) ([]vseg, error) {
	in := &verifyInput{name: filepath.Base(dir), dir: dir}
	sink := newFindingSink(in.name, 1<<30)
	listDirFindings(dir, in, sink, false)
	for _, f := range sink.out {
		if f.Code == CodeManifestCorrupt {
			return nil, errors.New("compact.manifest is unreadable")
		}
		if f.Code == CodeSegmentUnreadable {
			return nil, errors.New(f.Message)
		}
	}
	return in.segs, nil
}

// executeCollection performs the mutations for one planned collection.
func executeCollection(ctx context.Context, dataDir string, w *repairWork, jr *repairJournal, opts RepairOptions) error {
	jc := jr.col(w.name)
	if jc.Done {
		return nil
	}
	act := func(kind, target, detail string) {
		w.actions = append(w.actions, RepairAction{Kind: kind, Target: target, Detail: detail})
	}

	// Roll an interrupted compaction swap forward exactly as open does.
	if _, err := os.Stat(compactManifestPath(w.dir)); err == nil {
		if _, err := recoverCompaction(w.dir); err != nil {
			return err
		}
		act("compaction-roll-forward", "compact.manifest", "completed the interrupted compaction swap")
	} else if err := discardCompactTemps(w.dir); err != nil {
		return err
	}

	if err := repairPoint("compaction-done:" + w.name); err != nil {
		return err
	}

	// Journal the multi-step plans before executing them.
	if len(w.renames) > 0 && jc.Renames == nil {
		jc.Renames = w.renames
		if err := jr.save(dataDir); err != nil {
			return err
		}
	}
	if w.salvageRep != nil && jc.Salvage == nil {
		jc.Salvage = w.salvageRep
		if err := jr.save(dataDir); err != nil {
			return err
		}
	}
	w.renames, w.salvageRep = jc.Renames, jc.Salvage

	// Adopt unnumbered segments (rename only; contents untouched).
	for _, from := range sortedStringKeys(jc.Renames) {
		src, dst := filepath.Join(w.dir, from), filepath.Join(w.dir, jc.Renames[from])
		if _, err := os.Stat(src); err == nil {
			if _, err := os.Stat(dst); err == nil {
				return fmt.Errorf("adopt %s: %s already exists", from, jc.Renames[from])
			}
			if err := os.Rename(src, dst); err != nil {
				return err
			}
			act("adopt-segment", from, "renamed to "+jc.Renames[from])
		}
	}
	if len(jc.Renames) > 0 {
		if err := fsyncDir(w.dir); err != nil {
			return err
		}
		if err := repairPoint("renamed:" + w.name); err != nil {
			return err
		}
	}

	// Salvage into a new segment, then remove the damaged originals.
	if sr := jc.Salvage; sr != nil {
		if !jc.SalvageWritten {
			if err := writeSalvageSegment(ctx, w.dir, sr); err != nil {
				return err
			}
			jc.SalvageWritten = true
			if err := jr.save(dataDir); err != nil {
				return err
			}
			act("salvage-segment", sr.NewSegment, fmt.Sprintf("%d valid records (%d recovered after corrupt bytes)", sr.Entries, sr.GluedEmbeds))
		}
		if err := repairPoint("salvage-written:" + w.name); err != nil {
			return err
		}
		if err := quarantineSegments(w.dir, filepath.Base(jr.BackupDir), sr, opts.Now); err != nil {
			return err
		}
		for _, q := range sr.Quarantined {
			act("quarantine-segment", q, "moved byte-for-byte to "+filepath.ToSlash(filepath.Join(sr.QuarantineDir, q))+"; no longer replayed by open, verify or rebuild; also preserved in the backup")
		}
		if err := repairPoint("quarantined:" + w.name); err != nil {
			return err
		}
	}

	if err := repairPoint("pre-rebuild:" + w.name); err != nil {
		return err
	}
	if err := rebuildDerived(ctx, w, opts); err != nil {
		return err
	}
	jc.Done = true
	return jr.save(dataDir)
}

func sortedStringKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// writeSalvageSegment writes the exact on-disk bytes of every valid record of
// the damaged segments, in their original order, to the new segment via a
// temp file. It is a no-op when the segment already exists with the expected
// content, which keeps a rerun idempotent.
func writeSalvageSegment(ctx context.Context, dir string, sr *SalvageReport) error {
	var buf bytes.Buffer
	for _, name := range sr.Quarantined { // sorted by segment order
		p := filepath.Join(dir, name)
		rep, err := scanSegmentTolerantLimit(p, -1)
		if err != nil {
			return err
		}
		f, err := os.Open(p)
		if err != nil {
			return err
		}
		for _, e := range rep.Entries {
			if err := ctx.Err(); err != nil {
				_ = f.Close()
				return err
			}
			rec := make([]byte, e.Length)
			if _, err := f.ReadAt(rec, e.Offset); err != nil {
				_ = f.Close()
				return err
			}
			buf.Write(rec)
		}
		_ = f.Close()
	}
	dst := filepath.Join(dir, sr.NewSegment)
	if existing, err := os.ReadFile(dst); err == nil {
		if bytes.Equal(existing, buf.Bytes()) {
			return nil
		}
		return fmt.Errorf("salvage: %s already exists with different content", sr.NewSegment)
	}
	return writeFileAtomic(dst, buf.Bytes(), 0o644)
}

// rebuildDerived rewrites the secondary indexes, the primary index and
// meta.json from the segments as they now stand, atomically and with v2
// coverage. It also trims an unacknowledged torn tail on the newest segment,
// as open does, so persisted coverage is exact.
func rebuildDerived(ctx context.Context, w *repairWork, opts RepairOptions) error {
	segs, err := planSegmentLayout(w.dir)
	if err != nil {
		return err
	}
	if len(segs) > 0 {
		last := segs[len(segs)-1]
		rep, err := scanSegmentTolerantLimit(last.path, -1)
		if err != nil {
			return err
		}
		for _, br := range rep.BadRegions {
			if br.Reason == BadRegionTornTailLine && br.Offset+br.Length == rep.Size {
				if err := os.Truncate(last.path, br.Offset); err != nil {
					return err
				}
				if f, err := os.OpenFile(last.path, os.O_RDWR, 0); err == nil {
					_ = f.Sync()
					_ = f.Close()
				}
				w.actions = append(w.actions, RepairAction{Kind: "trim-torn-tail", Target: last.name,
					Detail: fmt.Sprintf("removed %d unacknowledged bytes", br.Length)})
			}
		}
		// Re-list sizes after the trim.
		if segs, err = planSegmentLayout(w.dir); err != nil {
			return err
		}
	}

	// Existing secondary index definitions (field + unique flag).
	fields := map[string]bool{}
	paths, _ := filepath.Glob(filepath.Join(w.dir, "sidx_*.json"))
	for _, p := range paths {
		base := filepath.Base(p)
		field := base[len("sidx_") : len(base)-len(".json")]
		if w.sidxUnique != nil {
			fields[field] = w.sidxUnique[field]
			continue
		}
		var f sidxFile
		b, rerr := os.ReadFile(p)
		if rerr != nil || json.Unmarshal(b, &f) != nil {
			return fmt.Errorf("%s: unique flag unreadable", base)
		}
		fields[field] = f.Unique
	}

	entries := map[uint64]IndexEntry{}
	latest := map[uint64]map[string]any{} // id -> field -> value (live records only)
	var maxID uint64
	cov := make([]SegmentCoverage, 0, len(segs))
	segObjs := make([]*Segment, 0, len(segs))
	for _, s := range segs {
		if err := ctx.Err(); err != nil {
			return err
		}
		rep, err := scanSegmentTolerantLimit(s.path, -1)
		if err != nil {
			return err
		}
		for _, br := range rep.BadRegions {
			return fmt.Errorf("segment %s still has damaged bytes at offset %d (%s)", s.name, br.Offset, br.Reason)
		}
		for _, se := range rep.Entries {
			e := se.Entry
			if e.ID > maxID {
				maxID = e.ID
			}
			switch e.Op {
			case store.OpInsert, store.OpUpdate:
				rev := entries[e.ID].Rev + 1
				if e.Rev > rev {
					rev = e.Rev
				}
				entries[e.ID] = IndexEntry{SegmentPath: s.path, Offset: se.Offset, Rev: rev, ExpiresAt: e.ExpiresAt, Epoch: e.Epoch}
				vals := map[string]any{}
				for f := range fields {
					if v, ok := e.Data[f]; ok {
						vals[f] = v
					}
				}
				latest[e.ID] = vals
			case store.OpDelete:
				delete(entries, e.ID)
				delete(latest, e.ID)
			}
		}
		seg := openSealedSegment(s.path, rep.Size)
		segObjs = append(segObjs, seg)
	}
	for _, seg := range segObjs {
		cv, err := captureCoverage(seg)
		if err != nil {
			return err
		}
		cov = append(cov, cv)
	}
	sort.Slice(cov, func(i, j int) bool { return cov[i].Segment < cov[j].Segment })

	for _, field := range sortedStringKeys(fields) {
		sidx := newSecondaryIndex(field, fields[field])
		buckets := map[string]map[uint64]struct{}{}
		rev := map[uint64]string{}
		kind := indexEmpty
		for id, vals := range latest {
			if val, ok := vals[field]; ok {
				key := toIndexKey(val)
				if buckets[key] == nil {
					buckets[key] = map[uint64]struct{}{}
				}
				buckets[key][id] = struct{}{}
				rev[id] = key
				vk, _ := valKind(val)
				kind = mergeKind(kind, vk)
			}
		}
		sidx.buckets, sidx.reverse, sidx.kind, sidx.sorted = buckets, rev, kind, buildSorted(buckets, kind)
		if err := sidx.PersistWithCoverage(sidxFilePath(w.dir, field), cov); err != nil {
			return err
		}
	}
	if err := repairPoint("sidx-written:" + w.name); err != nil {
		return err
	}
	if err := (&IndexSnapshot{entries: entries, coverage: cov}).Persist(filepath.Join(w.dir, "index.json")); err != nil {
		return err
	}
	if err := repairPoint("index-written:" + w.name); err != nil {
		return err
	}

	// meta.json: keep every non-derivable setting, never lower the counter.
	meta, merr := loadMeta(metaPath(w.dir))
	if merr != nil {
		meta = collectionMeta{}
		if w.metaReset || errors.Is(merr, os.ErrNotExist) {
			meta.CreatedAt = time.Now().UTC()
		}
	}
	if maxID > meta.IDCounter {
		meta.IDCounter = maxID
	}
	if meta.CreatedAt.IsZero() {
		meta.CreatedAt = time.Now().UTC()
	}
	if err := persistMeta(metaPath(w.dir), meta); err != nil {
		return err
	}
	w.actions = append(w.actions, RepairAction{Kind: "rebuild-derived", Target: "index.json",
		Detail: fmt.Sprintf("%d live records, %d segments, %d secondary indexes, id counter %d", len(entries), len(segs), len(fields), meta.IDCounter)})
	return cleanupAtomicTemps(w.dir)
}

func cleanupAtomicTemps(dir string) error {
	tmps, _ := filepath.Glob(filepath.Join(dir, "*.tmp"))
	for _, p := range tmps {
		if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

// ---- backup ---------------------------------------------------------------

// makeVerifiedBackup copies the named collections into
// <parent>/repair-backup-<UTC>/ and verifies every file by size and SHA-256.
// On any failure the partial backup is removed and nothing has been mutated.
func makeVerifiedBackup(ctx context.Context, dataDir, parent string, cols []string, now time.Time) (string, error) {
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return "", fmt.Errorf("repair: backup parent: %w", err)
	}
	base := "repair-backup-" + now.UTC().Format("20060102T150405Z")
	var dst string
	for i := 0; ; i++ {
		name := base
		if i > 0 {
			name = fmt.Sprintf("%s-%d", base, i)
		}
		dst = filepath.Join(parent, name)
		if err := os.Mkdir(dst, 0o755); err == nil {
			break
		} else if !errors.Is(err, os.ErrExist) {
			return "", fmt.Errorf("repair: create backup dir: %w", err)
		}
	}
	if err := backupMore(ctx, dataDir, dst, cols); err != nil {
		_ = os.RemoveAll(dst)
		return "", err
	}
	return dst, nil
}

// backupMore copies and verifies collections into an existing backup dir.
func backupMore(ctx context.Context, dataDir, dst string, cols []string) error {
	for _, c := range cols {
		if err := copyTreeVerified(ctx, filepath.Join(dataDir, c), filepath.Join(dst, c)); err != nil {
			return fmt.Errorf("repair: backup of %q failed verification, nothing was changed: %w", c, err)
		}
	}
	return fsyncDir(dst)
}

func copyTreeVerified(ctx context.Context, src, dst string) error {
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return err
	}
	ents, err := os.ReadDir(src)
	if err != nil {
		return err
	}
	for _, e := range ents {
		if err := ctx.Err(); err != nil {
			return err
		}
		sp, dp := filepath.Join(src, e.Name()), filepath.Join(dst, e.Name())
		if e.IsDir() {
			if err := copyTreeVerified(ctx, sp, dp); err != nil {
				return err
			}
			continue
		}
		if !e.Type().IsRegular() {
			continue
		}
		if err := copyFileVerified(sp, dp); err != nil {
			return err
		}
	}
	return fsyncDir(dst)
}

func copyFileVerified(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(out, h), in)
	if err == nil {
		err = out.Sync()
	}
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	// Read the copy back from disk and compare against the hash of the source
	// bytes that were streamed, so a short or corrupted write cannot pass.
	back, err := os.Open(dst)
	if err != nil {
		return err
	}
	defer func() { _ = back.Close() }()
	h2 := sha256.New()
	m, err := io.Copy(h2, back)
	if err != nil {
		return err
	}
	if m != n || hex.EncodeToString(h.Sum(nil)) != hex.EncodeToString(h2.Sum(nil)) {
		return fmt.Errorf("copy of %s does not match the source", filepath.Base(src))
	}
	return nil
}

// ---- backup containment ---------------------------------------------------

// realPathLenient resolves every existing symlink along path, including
// symlinked components of a not-yet-existing target: components that do not
// exist are appended lexically to the real path of the nearest existing
// parent. ".." is applied to the already-resolved prefix, never lexically to
// an unresolved symlink. A dangling symlink or a loop is an error, because
// where it would point cannot be established.
func realPathLenient(path string) (string, error) {
	if !filepath.IsAbs(path) {
		wd, err := os.Getwd()
		if err != nil {
			return "", err
		}
		path = filepath.Join(wd, path)
	}
	vol := filepath.VolumeName(path)
	cur := vol + string(filepath.Separator)
	exists := true
	for _, comp := range strings.Split(path[len(vol):], string(filepath.Separator)) {
		switch comp {
		case "", ".":
			continue
		case "..":
			cur = filepath.Dir(cur)
			continue
		}
		next := filepath.Join(cur, comp)
		if exists {
			if _, err := os.Lstat(next); err == nil {
				real, err := filepath.EvalSymlinks(next)
				if err != nil {
					return "", fmt.Errorf("resolve %q: %w", next, err)
				}
				cur = real
				continue
			} else if !errors.Is(err, os.ErrNotExist) {
				return "", err
			}
			exists = false
		}
		cur = next
	}
	return cur, nil
}

// checkBackupOutside rejects a backup location that resolves to or inside the
// data directory once symlinks (including those on the path to a directory
// that does not exist yet) are followed.
func checkBackupOutside(dataDir, backupDir string) error {
	realData, err := realPathLenient(dataDir)
	if err != nil {
		return fmt.Errorf("repair: %w", err)
	}
	realBk, err := realPathLenient(backupDir)
	if err != nil {
		return fmt.Errorf("repair: backup dir %q cannot be resolved: %w", backupDir, err)
	}
	if rel, err := filepath.Rel(realData, realBk); err == nil && (rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))) {
		return fmt.Errorf("repair: backup dir %q must be outside the data directory (it resolves to %q)", backupDir, realBk)
	}
	return nil
}

// ---- quarantine -----------------------------------------------------------

const (
	quarantineDirname      = "quarantine"
	quarantineManifestName = "MANIFEST.json"
)

type quarantineFile struct {
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

// quarantineManifest records what was set aside and why, so the originals can
// be audited or restored by hand. It is written before the first move.
type quarantineManifest struct {
	Version    int              `json:"version"`
	CreatedAt  time.Time        `json:"created_at"`
	Reason     string           `json:"reason"`
	NewSegment string           `json:"new_segment"`
	Files      []quarantineFile `json:"files"`
}

func hashFile(path string) (int64, string, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, "", err
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	n, err := io.Copy(h, f)
	return n, hex.EncodeToString(h.Sum(nil)), err
}

// quarantineSegments moves the damaged originals out of the collection's
// replay set without ever deleting them. They land in
// quarantine/<run>/ — a subdirectory, which open (it globs seg_*.ndjson in the
// collection directory), verify and the index rebuild never descend into.
// Each step is a same-filesystem rename, so a crash leaves every original in
// exactly one of the two places; rerunning finishes the moves. The manifest
// (sizes and hashes of the originals) is written first and, if it already
// exists from an interrupted run, is reused and checked rather than
// recomputed.
func quarantineSegments(colDir, run string, sr *SalvageReport, now time.Time) error {
	rel := filepath.Join(quarantineDirname, run)
	qdir := filepath.Join(colDir, rel)
	if err := os.MkdirAll(qdir, 0o755); err != nil {
		return fmt.Errorf("quarantine: %w", err)
	}
	sr.QuarantineDir = filepath.ToSlash(rel)

	mpath := filepath.Join(qdir, quarantineManifestName)
	var man quarantineManifest
	if b, err := os.ReadFile(mpath); err == nil {
		if err := json.Unmarshal(b, &man); err != nil {
			return fmt.Errorf("quarantine: manifest %q is unreadable: %w", mpath, err)
		}
	} else if errors.Is(err, os.ErrNotExist) {
		man = quarantineManifest{Version: 1, CreatedAt: now.UTC(), NewSegment: sr.NewSegment,
			Reason: "damaged segment bytes; valid records were copied to " + sr.NewSegment}
		for _, name := range sr.Quarantined {
			n, sum, err := hashFile(filepath.Join(colDir, name))
			if err != nil {
				return fmt.Errorf("quarantine: hash %s: %w", name, err)
			}
			man.Files = append(man.Files, quarantineFile{Name: name, Size: n, SHA256: sum})
		}
		b, _ := json.MarshalIndent(man, "", "  ")
		if err := writeFileAtomic(mpath, b, 0o644); err != nil {
			return err
		}
	} else {
		return err
	}
	if err := fsyncDir(qdir); err != nil {
		return err
	}
	if err := repairPoint("quarantine-manifest"); err != nil {
		return err
	}

	want := map[string]quarantineFile{}
	for _, f := range man.Files {
		want[f.Name] = f
	}
	for _, name := range sr.Quarantined {
		wf, ok := want[name]
		if !ok {
			return fmt.Errorf("quarantine: %s is not in the manifest", name)
		}
		src, dst := filepath.Join(colDir, name), filepath.Join(qdir, name)
		_, srcErr := os.Lstat(src)
		_, dstErr := os.Lstat(dst)
		switch {
		case srcErr == nil && dstErr == nil:
			return fmt.Errorf("quarantine: %s exists both in the collection and in %s; refusing to overwrite either", name, rel)
		case srcErr == nil:
			if n, sum, err := hashFile(src); err != nil || n != wf.Size || sum != wf.SHA256 {
				return fmt.Errorf("quarantine: %s no longer matches the manifest; refusing to move it", name)
			}
			if err := os.Rename(src, dst); err != nil {
				return fmt.Errorf("quarantine: %w", err)
			}
		case dstErr == nil:
			if n, sum, err := hashFile(dst); err != nil || n != wf.Size || sum != wf.SHA256 {
				return fmt.Errorf("quarantine: %s in %s does not match the manifest", name, rel)
			}
		default:
			return fmt.Errorf("quarantine: %s is in neither the collection nor %s", name, rel)
		}
		if err := repairPoint("quarantine-moved:" + name); err != nil {
			return err
		}
	}
	if err := fsyncDir(qdir); err != nil {
		return err
	}
	return fsyncDir(colDir)
}
