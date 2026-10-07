package engine

import (
	"context"
	"encoding/json"
	"errors"
	"hash/fnv"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/srjn45/scriva/store"
)

// vseg is one segment as seen by a verification pass. size is the extent the
// check is allowed to read (the snapshot size online, the file size offline).
type vseg struct {
	seg  *Segment
	path string
	name string
	num  uint64
	size int64
}

// sidxView is a secondary index as seen by a verification pass: a snapshot of
// the in-memory index online, or the loaded persisted file offline.
type sidxView struct {
	field    string
	unique   bool
	buckets  map[string][]uint64
	cov      []SegmentCoverage
	covKnown bool
}

// verifyInput is everything a verification pass compares against the segments.
type verifyInput struct {
	name, dir string
	opts      VerifyOptions
	// persisted is true for an offline check, where index/sidx come from the
	// files on disk and their coverage metadata is meaningful.
	persisted bool

	segs []vseg

	hasIndex      bool
	index         map[uint64]IndexEntry
	indexCov      []SegmentCoverage
	indexCovKnown bool

	sidx []sidxView

	idCounter      uint64
	idCounterKnown bool
	idCounterLive  bool // online: the in-memory counter, not meta.json

	stats VerifyStats
}

func (in *verifyInput) newest() *vseg {
	if len(in.segs) == 0 {
		return nil
	}
	return &in.segs[len(in.segs)-1]
}

// listDirFindings inspects the collection directory's file set: unreachable
// segment files, leftover temp files and the compaction manifest. Offline it
// also enumerates the segments into in.segs.
func listDirFindings(dir string, in *verifyInput, sink *findingSink, online bool) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		sink.add(SeverityDataCorruption, CodeSegmentUnreadable, Location{}, "cannot list collection directory: %v", err)
		return
	}
	var manifest *compactManifest
	if b, err := os.ReadFile(compactManifestPath(dir)); err == nil {
		var m compactManifest
		if jerr := json.Unmarshal(b, &m); jerr != nil {
			sink.add(SeverityDataCorruption, CodeManifestCorrupt, Location{}, "compaction manifest is unreadable: %v", jerr)
		} else {
			manifest = &m
			sink.add(SeverityRepairableIndex, CodeManifestPending, Location{},
				"an interrupted compaction swap is pending (%d renames, %d removals); open rolls it forward and rebuilds the indexes",
				len(m.Renames), len(m.Removals))
		}
	}
	pending := map[string]bool{}
	if manifest != nil {
		for src := range manifest.Renames {
			pending[filepath.Base(src)] = true
		}
	}

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		switch {
		case strings.HasPrefix(name, "seg_") && strings.HasSuffix(name, ".ndjson"):
			n, ok := segmentNum(name)
			if !ok {
				sink.add(SeverityInfo, CodeOrphanSegmentFile, Location{Segment: name},
					"file looks like a segment but its name carries no segment number; it is never read")
				continue
			}
			if online {
				continue // segments come from the open handle
			}
			info, err := e.Info()
			if err != nil {
				sink.add(SeverityDataCorruption, CodeSegmentUnreadable, Location{Segment: name}, "stat: %v", err)
				continue
			}
			p := filepath.Join(dir, name)
			in.segs = append(in.segs, vseg{seg: openSealedSegment(p, info.Size()), path: p, name: name, num: n, size: info.Size()})
		case strings.HasPrefix(name, "seg_"):
			sink.add(SeverityInfo, CodeOrphanSegmentFile, Location{Segment: name},
				"file is not reachable by the segment naming scheme and is never read")
		case strings.HasPrefix(name, ".compact_"):
			if !pending[name] {
				sink.add(SeverityInfo, CodeLeftoverTempFile, Location{Segment: name},
					"leftover compaction temp file; open discards it")
			}
		case strings.HasSuffix(name, ".tmp") && !online:
			// Online, a concurrent persist may legitimately hold a .tmp.
			sink.add(SeverityInfo, CodeLeftoverTempFile, Location{Segment: name},
				"leftover atomic-write temp file")
		}
	}
	sort.SliceStable(in.segs, func(i, j int) bool {
		if in.segs[i].num != in.segs[j].num {
			return in.segs[i].num < in.segs[j].num
		}
		return in.segs[i].name < in.segs[j].name
	})
}

// loadPersisted reads index.json, sidx_*.json and meta.json as they sit on disk.
func loadPersisted(in *verifyInput, sink *findingSink) {
	var total int64
	for _, s := range in.segs {
		total += s.size
	}

	idx := newIndex()
	switch err := idx.Load(filepath.Join(in.dir, "index.json")); {
	case err == nil:
		in.hasIndex = true
		in.index = idx.entries
		in.indexCov, in.indexCovKnown = idx.coverage, idx.coverageKnown
	case errors.Is(err, os.ErrNotExist):
		if total > 0 {
			sink.add(SeverityRepairableIndex, CodeIndexMissing, Location{}, "index.json is missing but the segments hold data; open rebuilds it")
		}
	default:
		sink.add(SeverityRepairableIndex, CodeIndexUnreadable, Location{}, "index.json cannot be trusted (%v); open rebuilds it", err)
	}

	paths, _ := filepath.Glob(filepath.Join(in.dir, "sidx_*.json"))
	sort.Strings(paths)
	for _, p := range paths {
		base := filepath.Base(p)
		field := base[len("sidx_") : len(base)-len(".json")]
		s := newSecondaryIndex(field, false)
		if err := s.Load(p); err != nil {
			sink.add(SeverityRepairableIndex, CodeSidxUnreadable, Location{Field: field}, "%s cannot be trusted (%v); open rebuilds it", base, err)
			continue
		}
		snap := s.snapshot()
		in.sidx = append(in.sidx, sidxView{field: field, unique: snap.unique, buckets: snap.buckets, cov: s.coverage, covKnown: s.coverageKnown})
	}

	switch m, err := loadMeta(metaPath(in.dir)); {
	case err == nil:
		in.idCounter, in.idCounterKnown = m.IDCounter, true
	case errors.Is(err, os.ErrNotExist):
		sink.add(SeverityInfo, CodeMetaMissing, Location{}, "meta.json is missing; open recomputes the id counter")
	default:
		sink.add(SeverityRepairableIndex, CodeMetaUnreadable, Location{}, "meta.json is unreadable: %v", err)
	}
}

type recLoc struct {
	seg string
	off int64
}

// truthRec is the replayed state of one id: the ground truth the indexes are
// compared against.
type truthRec struct {
	seg      string
	off      int64
	rev      uint64 // replay revision (what Rebuild would assign)
	lastRev  uint64 // revision the last line carried
	exp      int64
	epoch    uint64
	hash     uint64
	live     bool
	deleted  bool
	keys     map[string]string // sidx field -> bucket key
	hasField map[string]bool
}

func dataHash(d map[string]any) uint64 {
	h := fnv.New64a()
	b, _ := json.Marshal(d) // map keys are sorted: canonical
	_, _ = h.Write(b)
	return h.Sum64()
}

func runChecks(ctx context.Context, in *verifyInput, sink *findingSink) error {
	full := in.opts.Mode == VerifyFull
	in.stats.Segments = len(in.segs)
	for _, s := range in.segs {
		in.stats.SegmentBytes += s.size
	}
	in.stats.IndexEntries = len(in.index)
	in.stats.SecondaryIndexes = len(in.sidx)

	if in.persisted {
		if in.hasIndex {
			checkCoverage(in, sink, "index", Location{}, in.indexCov, in.indexCovKnown, len(in.index))
		}
		for _, v := range in.sidx {
			checkCoverage(in, sink, "sidx", Location{Field: v.field}, v.cov, v.covKnown, len(v.buckets))
		}
	}

	// Scan: every segment in full mode; only the newest offline in quick mode
	// (enough to see a torn tail and the active id high-water mark).
	reports := make(map[string]*SegmentReport, len(in.segs))
	scan := func(s vseg) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		rep, err := scanSegmentTolerantLimit(s.path, s.size)
		if err != nil {
			sink.add(SeverityDataCorruption, CodeSegmentUnreadable, Location{Segment: s.name}, "%v", err)
			return nil
		}
		reports[s.name] = &rep
		in.stats.BadRegions += len(rep.BadRegions)
		for _, br := range rep.BadRegions {
			reportBadRegion(sink, s, in.newest(), br)
		}
		return nil
	}
	if full {
		for _, s := range in.segs {
			if err := scan(s); err != nil {
				return err
			}
		}
	} else if in.persisted {
		if n := in.newest(); n != nil {
			if err := scan(*n); err != nil {
				return err
			}
		}
	}

	var activeMax uint64
	if n := in.newest(); n != nil {
		if rep := reports[n.name]; rep != nil {
			for _, e := range rep.Entries {
				if e.Entry.ID > activeMax {
					activeMax = e.Entry.ID
				}
			}
		}
	}

	var maxObserved uint64
	if full {
		truth, at, maxID, err := buildTruth(ctx, in, reports, sink)
		if err != nil {
			return err
		}
		maxObserved = maxID
		compareIndex(in, sink, truth, at)
		compareSidx(in, sink, truth)
	} else {
		for id := range in.index {
			if id > maxObserved {
				maxObserved = id
			}
		}
		spotCheckIndex(in, sink)
		for _, v := range in.sidx {
			uniqueFromBuckets(sink, v)
		}
	}

	if in.idCounterKnown {
		eff := in.idCounter
		if !in.idCounterLive && activeMax > eff {
			eff = activeMax // open reconciles meta against the active segment
		}
		if eff < maxObserved {
			sink.add(SeverityRepairableIndex, CodeIDCounterBehind, Location{},
				"id counter %d is behind the highest id %d present; new inserts could reuse live ids", eff, maxObserved)
		}
	}
	return nil
}

func reportBadRegion(sink *findingSink, s vseg, newest *vseg, br BadRegion) {
	loc := Location{Segment: s.name, Offset: br.Offset}
	switch br.Reason {
	case BadRegionTornTailLine:
		if newest != nil && s.name == newest.name && br.Offset+br.Length == s.size {
			sink.add(SeverityInfo, CodeSegmentTornTail, loc,
				"%d-byte partial line at the end of the newest segment; open trims it (it was never acknowledged)", br.Length)
			return
		}
		sink.add(SeverityDataCorruption, CodeSegmentTornTail, loc, "%d-byte partial line in a non-final position", br.Length)
	case BadRegionGluedLines:
		sink.add(SeverityDataCorruption, CodeSegmentGluedLine, loc,
			"%d bytes of a partial record are glued before a valid record on the same line", br.Length)
	default:
		sink.add(SeverityDataCorruption, CodeSegmentBadRegion, loc, "%d-byte region is not a valid record (%s: %s)", br.Length, br.Reason, br.Error)
	}
}

// buildTruth replays the salvaged entries of every segment in (segment number,
// offset) order, applying exactly the semantics of Rebuild, and records
// history anomalies as conflicts. Nothing is resolved: conflicting history is
// only reported; the replayed state is "last line wins", as at open.
func buildTruth(ctx context.Context, in *verifyInput, reports map[string]*SegmentReport, sink *findingSink) (map[uint64]*truthRec, map[recLoc]uint64, uint64, error) {
	truth := make(map[uint64]*truthRec)
	at := make(map[recLoc]uint64)
	var maxID uint64
	fields := make([]string, 0, len(in.sidx))
	for _, v := range in.sidx {
		fields = append(fields, v.field)
	}
	n := 0
	for _, s := range in.segs {
		rep := reports[s.name]
		if rep == nil {
			continue
		}
		for _, se := range rep.Entries {
			if n++; n%4096 == 0 {
				if err := ctx.Err(); err != nil {
					return nil, nil, 0, err
				}
			}
			e := se.Entry
			in.stats.Entries++
			at[recLoc{s.name, se.Offset}] = e.ID
			if e.ID > maxID {
				maxID = e.ID
			}
			loc := Location{Segment: s.name, Offset: se.Offset, ID: e.ID}
			rec := truth[e.ID]
			switch e.Op {
			case store.OpInsert, store.OpUpdate:
				h := dataHash(e.Data)
				if rec != nil {
					checkHistory(sink, loc, rec, e, h)
				}
				if rec == nil {
					rec = &truthRec{}
					truth[e.ID] = rec
				}
				rev := rec.rev + 1
				if rec.deleted || !rec.live {
					rev = 1
				}
				if e.Rev > rev {
					rev = e.Rev
				}
				*rec = truthRec{seg: s.name, off: se.Offset, rev: rev, lastRev: e.Rev, exp: e.ExpiresAt, epoch: e.Epoch, hash: h, live: true}
				for _, f := range fields {
					if val, ok := e.Data[f]; ok {
						if rec.keys == nil {
							rec.keys = map[string]string{}
						}
						rec.keys[f] = toIndexKey(val)
					}
				}
			case store.OpDelete:
				if rec == nil {
					rec = &truthRec{}
					truth[e.ID] = rec
				}
				*rec = truthRec{deleted: true}
			}
		}
	}
	for _, r := range truth {
		if r.live {
			in.stats.LiveRecords++
		}
	}
	return truth, at, maxID, nil
}

// checkHistory flags a write whose history cannot be explained by a single
// writer appending in order.
func checkHistory(sink *findingSink, loc Location, prev *truthRec, e store.Entry, h uint64) {
	switch {
	case prev.deleted && e.Op == store.OpInsert:
		sink.add(SeverityConflict, CodeConflictIDReuse, loc, "id %d is inserted again after it was deleted", e.ID)
	case prev.deleted:
		sink.add(SeverityConflict, CodeConflictWriteAfter, loc, "id %d is updated after it was deleted", e.ID)
	case e.Op == store.OpInsert:
		if e.Rev == prev.lastRev && h == prev.hash {
			sink.add(SeverityInfo, CodeDuplicateIdentical, loc, "id %d is inserted twice with identical content", e.ID)
		} else {
			sink.add(SeverityConflict, CodeConflictDuplicateID, loc,
				"id %d is inserted again while live with different content or revision (rev %d, previous rev %d)", e.ID, e.Rev, prev.lastRev)
		}
	case e.Rev > 0 && prev.lastRev > 0 && e.Rev < prev.lastRev:
		sink.add(SeverityConflict, CodeConflictRevision, loc, "id %d update has revision %d after revision %d", e.ID, e.Rev, prev.lastRev)
	case e.Rev > 0 && e.Rev == prev.lastRev:
		if h == prev.hash {
			sink.add(SeverityInfo, CodeDuplicateIdentical, loc, "id %d update repeats revision %d with identical content", e.ID, e.Rev)
		} else {
			sink.add(SeverityConflict, CodeConflictRevision, loc, "id %d has two different updates at revision %d", e.ID, e.Rev)
		}
	}
}

func sortedMapIDs[V any](m map[uint64]V) []uint64 {
	ids := make([]uint64, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// compareIndex compares the primary index with the replayed ground truth.
func compareIndex(in *verifyInput, sink *findingSink, truth map[uint64]*truthRec, at map[recLoc]uint64) {
	nowNano := in.opts.Now.UnixNano()
	if in.hasIndex {
		for _, id := range sortedMapIDs(in.index) {
			ie := in.index[id]
			base := filepath.Base(ie.SegmentPath)
			loc := Location{Segment: base, Offset: ie.Offset, ID: id}
			t := truth[id]
			switch {
			case t == nil:
				if got, ok := at[recLoc{base, ie.Offset}]; ok {
					sink.add(SeverityRepairableIndex, CodeIndexWrongRecord, loc, "index offset holds a record for id %d, not %d", got, id)
				} else {
					sink.add(SeverityRepairableIndex, CodeIndexDanglingEntry, loc, "index lists id %d but no segment holds a record for it", id)
				}
			case t.deleted:
				sink.add(SeverityRepairableIndex, CodeIndexResurrected, loc, "index lists id %d, whose latest record is a delete", id)
			case t.seg != base || t.off != ie.Offset:
				if got, ok := at[recLoc{base, ie.Offset}]; ok && got != id {
					sink.add(SeverityRepairableIndex, CodeIndexWrongRecord, loc, "index offset holds a record for id %d, not %d", got, id)
				} else if !ok {
					sink.add(SeverityRepairableIndex, CodeIndexDanglingOffset, loc, "index offset is not the start of any record")
				} else {
					sink.add(SeverityRepairableIndex, CodeIndexStaleRecord, loc,
						"index points at an older version of id %d; latest is %s@%d", id, t.seg, t.off)
				}
			case ie.Rev != t.rev || ie.ExpiresAt != t.exp || ie.Epoch != t.epoch:
				sink.add(SeverityRepairableIndex, CodeIndexStaleRecord, loc,
					"index metadata for id %d differs from its record (rev %d/%d, expires %d/%d, epoch %d/%d)",
					id, ie.Rev, t.rev, ie.ExpiresAt, t.exp, ie.Epoch, t.epoch)
			}
		}
		for _, id := range sortedMapIDs(truth) {
			t := truth[id]
			if t.live {
				if _, ok := in.index[id]; !ok {
					sink.add(SeverityRepairableIndex, CodeIndexMissingRecord, Location{Segment: t.seg, Offset: t.off, ID: id},
						"live record %d is absent from the index", id)
				}
			}
		}
	}
	for _, id := range sortedMapIDs(truth) {
		if t := truth[id]; t.live && t.exp > 0 && t.exp <= nowNano {
			sink.add(SeverityInfo, CodeRecordExpiredLive, Location{Segment: t.seg, Offset: t.off, ID: id},
				"record %d is past its TTL but not yet reclaimed", id)
		}
	}
}

// compareSidx compares each secondary index with the replayed ground truth and
// reports unique-constraint violations present in the data itself.
func compareSidx(in *verifyInput, sink *findingSink, truth map[uint64]*truthRec) {
	for _, v := range in.sidx {
		member := map[uint64][]string{}
		for key, ids := range v.buckets {
			for _, id := range ids {
				member[id] = append(member[id], key)
			}
		}
		for _, ks := range member {
			sort.Strings(ks)
		}
		all := map[uint64]struct{}{}
		for id := range member {
			all[id] = struct{}{}
		}
		for id, t := range truth {
			if t.live {
				all[id] = struct{}{}
			}
		}
		for _, id := range sortedMapIDs(all) {
			loc := Location{ID: id, Field: v.field}
			t := truth[id]
			tk, tok := "", false
			if t != nil && t.live {
				tk, tok = t.keys[v.field]
			}
			sk := member[id]
			switch {
			case tok && len(sk) == 0:
				sink.add(SeverityRepairableIndex, CodeSidxMissingEntry, loc, "record %d has %s=%q but is absent from the secondary index", id, v.field, tk)
			case !tok && len(sk) > 0:
				why := "has no such field"
				switch {
				case t == nil:
					why = "no longer exists in the segments"
				case t.deleted:
					why = "was deleted"
				}
				sink.add(SeverityRepairableIndex, CodeSidxExtraEntry, loc, "secondary index lists id %d under %q, but the record %s", id, sk[0], why)
			case tok && (len(sk) != 1 || sk[0] != tk):
				sink.add(SeverityRepairableIndex, CodeSidxWrongBucket, loc, "id %d is indexed under %v but its value is %q", id, sk, tk)
			}
		}
		if v.unique {
			byKey := map[string][]uint64{}
			for _, id := range sortedMapIDs(truth) {
				if t := truth[id]; t.live {
					if k, ok := t.keys[v.field]; ok {
						byKey[k] = append(byKey[k], id)
					}
				}
			}
			reportUnique(sink, v.field, byKey)
		}
	}
}

// uniqueFromBuckets reports unique violations visible in the index itself
// (quick mode, which has no ground truth).
func uniqueFromBuckets(sink *findingSink, v sidxView) {
	if !v.unique {
		return
	}
	byKey := make(map[string][]uint64, len(v.buckets))
	for k, ids := range v.buckets {
		byKey[k] = append([]uint64(nil), ids...)
	}
	reportUnique(sink, v.field, byKey)
}

func reportUnique(sink *findingSink, field string, byKey map[string][]uint64) {
	keys := make([]string, 0, len(byKey))
	for k, ids := range byKey {
		if len(ids) > 1 {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for _, k := range keys {
		ids := byKey[k]
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		sink.add(SeverityConflict, CodeSidxUniqueViolation, Location{Field: field, ID: ids[0]},
			"unique index on %q has %d live records sharing value %q: ids %v", field, len(ids), k, ids)
	}
}

// spotCheckIndex verifies a deterministic bounded sample of index entries —
// the newest by id plus evenly spaced others — each starts a record whose
// decoded id matches.
func spotCheckIndex(in *verifyInput, sink *findingSink) {
	if !in.hasIndex || len(in.index) == 0 {
		return
	}
	bySeg := make(map[string]*Segment, len(in.segs))
	for _, s := range in.segs {
		bySeg[s.name] = s.seg
	}
	ids := sortedMapIDs(in.index)
	pick := map[uint64]struct{}{}
	for i := len(ids) - 1; i >= 0 && len(ids)-i <= spotCheckNewest; i-- {
		pick[ids[i]] = struct{}{}
	}
	rest := len(ids) - spotCheckNewest
	if rest > 0 {
		step := rest / spotCheckRandom
		if step < 1 {
			step = 1
		}
		for i := 0; i < rest; i += step {
			pick[ids[i]] = struct{}{}
		}
	}
	for _, id := range sortedMapIDs(pick) {
		ie := in.index[id]
		base := filepath.Base(ie.SegmentPath)
		seg := bySeg[base]
		if seg == nil || !recordStartsWithID(seg, ie.Offset, id) {
			sink.add(SeverityRepairableIndex, CodeIndexSpotCheckFailed, Location{Segment: base, Offset: ie.Offset, ID: id},
				"index entry for id %d does not point at a record for that id", id)
		}
	}
}

// checkCoverage validates a persisted file's per-segment coverage against the
// segments, mirroring the open-time rules: covered segments must exist, be at
// least their covered length and hash to the recorded checksum; only the newest
// covered segment may have grown; unlisted segments newer than all covered ones
// are a replayable tail; any other unlisted segment is a layout mismatch.
func checkCoverage(in *verifyInput, sink *findingSink, kind string, base Location, cov []SegmentCoverage, known bool, nEntries int) {
	code := func(suffix string) FindingCode { return FindingCode(kind + "-" + suffix) }
	if !known {
		sink.add(SeverityRepairableIndex, code("coverage-unknown"), base,
			"%s file predates coverage metadata (v1); open rebuilds it once", kind)
		return
	}
	if len(cov) == 0 && nEntries > 0 {
		sink.add(SeverityRepairableIndex, code("coverage-mismatch"), base, "%s holds %d entries but covers no segment", kind, nEntries)
		return
	}
	byName := make(map[string]vseg, len(in.segs))
	for _, s := range in.segs {
		byName[s.name] = s
	}
	covered := map[string]bool{}
	var maxCovered uint64
	var lastName string
	var lastSize int64
	bad := false
	for _, cv := range cov {
		loc := base
		loc.Segment = cv.Segment
		s, ok := byName[cv.Segment]
		if !ok {
			sink.add(SeverityRepairableIndex, code("coverage-segment-missing"), loc, "%s covers segment %s, which is not on disk", kind, cv.Segment)
			bad = true
			continue
		}
		if covered[cv.Segment] {
			sink.add(SeverityRepairableIndex, code("coverage-mismatch"), loc, "%s lists segment %s twice", kind, cv.Segment)
			bad = true
			continue
		}
		covered[cv.Segment] = true
		switch {
		case s.size < cv.Size:
			sink.add(SeverityRepairableIndex, code("coverage-mismatch"), loc, "segment is %d bytes but %s covers %d", s.size, kind, cv.Size)
			bad = true
		case !coverageMatches(s.seg, cv):
			sink.add(SeverityRepairableIndex, code("coverage-mismatch"), loc, "first %d bytes of the segment no longer match the %s fingerprint", cv.Size, kind)
			bad = true
		}
		if s.num >= maxCovered {
			maxCovered, lastName, lastSize = s.num, cv.Segment, cv.Size
		}
	}
	for _, cv := range cov {
		s, ok := byName[cv.Segment]
		if !ok || s.name == lastName {
			continue
		}
		if s.size > cv.Size {
			loc := base
			loc.Segment = cv.Segment
			sink.add(SeverityRepairableIndex, code("coverage-mismatch"), loc, "a non-newest covered segment grew by %d bytes", s.size-cv.Size)
			bad = true
		}
	}
	if bad {
		return
	}
	if s, ok := byName[lastName]; ok && s.size > lastSize {
		loc := base
		loc.Segment = lastName
		loc.Offset = lastSize
		sink.add(SeverityRepairableIndex, code("stale-tail"), loc,
			"%d bytes were appended to %s after the %s was persisted (unclean stop); open replays them", s.size-lastSize, lastName, kind)
	}
	for _, s := range in.segs {
		if covered[s.name] {
			continue
		}
		loc := base
		loc.Segment = s.name
		if s.num > maxCovered {
			if s.size > 0 {
				sink.add(SeverityRepairableIndex, code("stale-tail"), loc,
					"segment %s (%d bytes) is newer than everything the %s covers; open replays it", s.name, s.size, kind)
			}
			continue
		}
		sink.add(SeverityRepairableIndex, code("segment-unlisted"), loc,
			"segment %s is older than covered segments but absent from the %s; open rebuilds from all segments", s.name, kind)
	}
}
