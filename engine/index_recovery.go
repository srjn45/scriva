package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Index recovery kinds reported through CollectionConfig.OnIndexRecovery.
const (
	// IndexRecoveryReplay: a valid persisted index was brought current by
	// replaying only the segment bytes appended after its coverage.
	IndexRecoveryReplay = "replay"
	// IndexRecoveryRebuild: the persisted index could not be trusted and the
	// index was rebuilt from every segment.
	IndexRecoveryRebuild = "rebuild"
	// IndexRecoverySpotCheckFail: an identity/boundary spot check found an
	// entry that does not point at a record with the expected id. Always
	// followed by a rebuild.
	IndexRecoverySpotCheckFail = "spotcheck_fail"
)

const (
	spotCheckRandom = 64 // random entries verified on load
	spotCheckNewest = 16 // highest-id (newest) entries verified on load
)

// IndexRecoveryStats are test-visible counters for load-time index recovery.
type IndexRecoveryStats struct {
	Replays           int64 // loads brought current by tail replay
	ReplayedBytes     int64 // segment bytes replayed across those loads
	FullRebuilds      int64 // full rebuilds from segments
	SpotCheckFailures int64 // spot checks that forced a rebuild

	SecondaryReplays  int64 // secondary indexes brought current by tail replay
	SecondaryRebuilds int64 // secondary indexes rebuilt from segments at load
}

// IndexRecoveryStats returns the load-time recovery counters.
func (c *Collection) IndexRecoveryStats() IndexRecoveryStats {
	return IndexRecoveryStats{
		Replays:           c.indexReplays.Load(),
		ReplayedBytes:     c.indexReplayBytes.Load(),
		FullRebuilds:      c.indexRebuilds.Load(),
		SpotCheckFailures: c.spotCheckFailures.Load(),
		SecondaryReplays:  c.sidxReplays.Load(),
		SecondaryRebuilds: c.sidxRebuilds.Load(),
	}
}

func (c *Collection) reportRecovery(kind string, bytes int64, start time.Time) {
	if h := c.cfg.OnIndexRecovery; h != nil {
		h(c.name, kind, bytes, time.Since(start))
	}
}

// recoverIndex loads the persisted index and reconciles it with the segments
// in all (sorted by numeric id; the last is the active tail). It returns
// whether the in-memory index differs from the persisted one (replayed or
// rebuilt), in which case the caller must persist it and revalidate secondary
// indexes. A persisted index is trusted only as far as its v2 coverage proves:
//
//   - every covered segment exists, is at least its covered length, and its
//     first Size bytes hash to the recorded checksum;
//   - only the newest covered segment may have grown (tail replay); any other
//     growth, any unlisted segment older than the newest covered one, a v1
//     file (no coverage) or a failed spot check forces a full rebuild;
//   - unlisted segments newer than every covered one (rotation after the last
//     persist) are replayed in full.
func (c *Collection) recoverIndex(all []*Segment, indexPath string, forceRebuild bool) (changed bool, err error) {
	start := time.Now()
	rebuild := func() (bool, error) {
		c.indexRebuilds.Add(1)
		if err := c.index.Rebuild(all); err != nil {
			return false, fmt.Errorf("collection: rebuild index: %w", err)
		}
		c.reportRecovery(IndexRecoveryRebuild, totalSize(all), start)
		return true, nil
	}
	if forceRebuild {
		return rebuild()
	}

	if err := c.index.Load(indexPath); err != nil {
		// Missing, stale, corrupt or truncated: never trusted, never fatal.
		return rebuild()
	}

	plan, ok := c.planReplay(all)
	if !ok {
		return rebuild()
	}

	var replayed int64
	if len(plan) > 0 {
		c.index.mu.Lock()
		for _, st := range plan {
			if err := applyEntries(c.index.entries, st.seg, st.from); err != nil {
				c.index.mu.Unlock()
				return rebuild()
			}
			replayed += st.seg.Size() - st.from
		}
		c.index.mu.Unlock()
	}

	// Entries must land inside real segments, and a sample must point at the
	// record they claim to. Covers a checksum-valid but wrong index.
	sizes := make(map[string]int64, len(all))
	for _, seg := range all {
		sizes[seg.Path()] = seg.Size()
	}
	if !c.index.segmentsValid(sizes) {
		return rebuild()
	}
	if !c.spotCheck(all) {
		c.spotCheckFailures.Add(1)
		c.reportRecovery(IndexRecoverySpotCheckFail, 0, start)
		return rebuild()
	}

	if len(plan) == 0 {
		return false, nil
	}
	c.indexReplays.Add(1)
	c.indexReplayBytes.Add(replayed)
	c.reportRecovery(IndexRecoveryReplay, replayed, start)
	return true, nil
}

type replayStep struct {
	seg  *Segment
	from int64
}

// planReplay validates the loaded index's coverage against all and returns the
// segments (with start offsets) that must be replayed in order. ok=false means
// the index cannot be trusted and a full rebuild is required.
func (c *Collection) planReplay(all []*Segment) (plan []replayStep, ok bool) {
	c.index.mu.RLock()
	known := c.index.coverageKnown
	cov := append([]SegmentCoverage(nil), c.index.coverage...)
	nEntries := len(c.index.entries)
	c.index.mu.RUnlock()
	return planReplayFor(all, known, cov, nEntries)
}

// planReplayFor is planReplay over explicit coverage, shared by the primary and
// secondary indexes: known=false (v1) means coverage is unknown, and nEntries>0
// with empty coverage means entries nothing proves.
func planReplayFor(all []*Segment, known bool, cov []SegmentCoverage, nEntries int) (plan []replayStep, ok bool) {
	if !known {
		return nil, false // v1: coverage unknown -> stale
	}
	if len(cov) == 0 && nEntries > 0 {
		return nil, false // entries with nothing proving them
	}

	byName := make(map[string]*Segment, len(all))
	for _, s := range all {
		byName[filepath.Base(s.Path())] = s
	}
	covered := make(map[string]bool, len(cov))
	var maxCovered uint64
	var lastCovered *Segment
	var lastCoveredSize int64
	for _, cv := range cov {
		seg := byName[cv.Segment]
		if seg == nil || covered[cv.Segment] {
			return nil, false
		}
		covered[cv.Segment] = true
		n, nok := segmentNum(cv.Segment)
		if !nok {
			return nil, false
		}
		if seg.Size() < cv.Size {
			return nil, false
		}
		if n >= maxCovered {
			maxCovered, lastCovered, lastCoveredSize = n, seg, cv.Size
		}
	}
	for _, cv := range cov {
		seg := byName[cv.Segment]
		if seg.Size() > cv.Size && seg != lastCovered {
			return nil, false // a non-newest segment grew
		}
		if !coverageMatches(seg, cv) {
			return nil, false
		}
	}
	if lastCovered != nil && lastCovered.Size() > lastCoveredSize {
		plan = append(plan, replayStep{lastCovered, lastCoveredSize})
	}
	for _, s := range all { // numeric order
		name := filepath.Base(s.Path())
		if covered[name] {
			continue
		}
		n, nok := segmentNum(name)
		if !nok || n <= maxCovered {
			return nil, false // unlisted segment older than covered ones
		}
		if s.Size() > 0 {
			plan = append(plan, replayStep{s, 0})
		}
	}
	return plan, true
}

// coverageMatches reports whether the first cv.Size bytes of seg match the
// recorded coverage: the tail fingerprint when present, and the full SHA-256
// when one was recorded. Coverage with neither never matches.
func coverageMatches(seg *Segment, cv SegmentCoverage) bool {
	if cv.Checksum == "" && cv.Tail == "" {
		return false
	}
	f, err := os.Open(seg.Path())
	if err != nil {
		return false
	}
	defer func() { _ = f.Close() }()
	if cv.Tail != "" {
		t, err := tailFingerprint(f, cv.Size)
		if err != nil || t != cv.Tail {
			return false
		}
	}
	if cv.Checksum != "" {
		h := sha256.New()
		if n, err := io.CopyN(h, f, cv.Size); err != nil || n != cv.Size {
			return false
		}
		return hex.EncodeToString(h.Sum(nil)) == cv.Checksum
	}
	return true
}

// spotCheck verifies a bounded sample of index entries — the newest by id plus
// random others — each starts a record boundary whose decoded id matches.
func (c *Collection) spotCheck(all []*Segment) bool {
	bySeg := make(map[string]*Segment, len(all))
	for _, s := range all {
		bySeg[s.Path()] = s
	}
	c.index.mu.RLock()
	ids := make([]uint64, 0, len(c.index.entries))
	for id := range c.index.entries {
		ids = append(ids, id)
	}
	sample := make(map[uint64]IndexEntry, spotCheckRandom+spotCheckNewest)
	sort.Slice(ids, func(i, j int) bool { return ids[i] > ids[j] })
	for i := 0; i < len(ids) && i < spotCheckNewest; i++ {
		sample[ids[i]] = c.index.entries[ids[i]]
	}
	if rest := ids[min(spotCheckNewest, len(ids)):]; len(rest) > 0 {
		for i := 0; i < spotCheckRandom; i++ {
			id := rest[rand.Intn(len(rest))]
			sample[id] = c.index.entries[id]
		}
	}
	c.index.mu.RUnlock()

	for id, e := range sample {
		seg := bySeg[e.SegmentPath]
		if seg == nil || !recordStartsWithID(seg, e.Offset, id) {
			return false
		}
	}
	return true
}

// recordStartsWithID checks off is a line boundary and the record there has id.
func recordStartsWithID(seg *Segment, off int64, id uint64) bool {
	if off < 0 || off >= seg.Size() {
		return false
	}
	if off > 0 {
		f, err := os.Open(seg.Path())
		if err != nil {
			return false
		}
		var b [1]byte
		_, rerr := f.ReadAt(b[:], off-1)
		_ = f.Close()
		if rerr != nil || b[0] != '\n' {
			return false
		}
	}
	e, err := seg.ReadAt(off)
	return err == nil && e.ID == id
}

func totalSize(segs []*Segment) (n int64) {
	for _, s := range segs {
		n += s.Size()
	}
	return n
}

// recoverSecondary reconciles a loaded secondary index with the segments: a v2
// file is trusted as far as its coverage proves (same rules as the primary
// index) and brought current by replaying the tail; anything else — v1, corrupt,
// uncovered, mismatching — is rebuilt. It reports whether sidx changed and so
// must be re-persisted with fresh coverage.
func (c *Collection) recoverSecondary(sidx *SecondaryIndex, p string, all []*Segment, force bool) (changed bool, err error) {
	rebuild := func() (bool, error) {
		c.sidxRebuilds.Add(1)
		if err := sidx.rebuild(all); err != nil {
			return false, err
		}
		return true, nil
	}
	if force {
		return rebuild()
	}
	if err := sidx.Load(p); err != nil {
		return rebuild()
	}
	sidx.mu.RLock()
	known, cov, n := sidx.coverageKnown, append([]SegmentCoverage(nil), sidx.coverage...), len(sidx.buckets)
	sidx.mu.RUnlock()
	plan, ok := planReplayFor(all, known, cov, n)
	if !ok {
		return rebuild()
	}
	for _, st := range plan {
		if err := sidx.replay(st.seg, st.from); err != nil {
			return rebuild()
		}
	}
	if len(plan) > 0 {
		c.sidxReplays.Add(1)
	}
	return len(plan) > 0, nil
}

// persistSecondary writes sidx with coverage of segs. Callers must hold the
// lock that excludes writes to segs.
func persistSecondary(sidx *SecondaryIndex, p string, segs []*Segment) error {
	cov := make([]SegmentCoverage, 0, len(segs))
	for i, seg := range segs {
		cv, err := captureCoverage(seg, i == len(segs)-1)
		if err != nil {
			return err
		}
		cov = append(cov, cv)
	}
	return sidx.PersistWithCoverage(p, cov)
}
