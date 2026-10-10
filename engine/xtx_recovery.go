package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/srjn45/scriva/store"
)

// runEvidence records the evidence recovery found for one participant of one txid.
type runEvidence struct {
	complete    bool
	n           uint32
	digest      string
	segPath     string
	startOffset int64
	endOffset   int64
	activeTail  bool
	entries     []stampedEntry
}

// txEvidence groups participant evidence across collections for one txid.
type txEvidence struct {
	runs map[string]*runEvidence
}

type colDiscovery struct {
	name   string
	dir    string
	sealed []*Segment
	active *Segment
	all    []*Segment
}

func (c *colDiscovery) close() {
	if c.active != nil {
		_ = c.active.Close()
	}
}

// recoverCoordinator implements §7 of docs/design-cross-collection-transactions.md:
// Phase 0..7 coordinator recovery before collections become writable.
func (db *DB) recoverCoordinator(cfg CollectionConfig) error {
	// Phase 0 & 1: Version gate and journal open
	f, err := readXTxFormat(db.dataDir)
	if err != nil {
		return err
	}
	hasFormat := (f != nil)

	xj, err := gateXTxRoot(db.dataDir, xtxOptions{wrapFile: cfg.wrapFile, renameFn: cfg.renameFn})
	if err != nil {
		return err
	}
	db.xtxJournal = xj

	// Phase 2: Discover collections and segment files
	entries, err := os.ReadDir(db.dataDir)
	if err != nil {
		_ = xj.close()
		return fmt.Errorf("db: read dir: %w", err)
	}

	existingCols := make(map[string]bool)
	var colNames []string
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		if isReservedXTxName(e.Name()) {
			_ = xj.close()
			return xtxErr("", ErrReservedName, fmt.Errorf("root directory contains reserved directory %q", e.Name()))
		}
		existingCols[e.Name()] = true
		colNames = append(colNames, e.Name())
	}
	sort.Strings(colNames)

	discoveries := make(map[string]*colDiscovery, len(colNames))
	defer func() {
		for _, cd := range discoveries {
			cd.close()
		}
	}()

	for _, name := range colNames {
		colDir := filepath.Join(db.dataDir, name)
		if _, err := recoverCompaction(colDir); err != nil {
			_ = xj.close()
			return fmt.Errorf("db: recover compaction %q: %w", name, err)
		}
		pattern := filepath.Join(colDir, "seg_*.ndjson")
		paths, err := filepath.Glob(pattern)
		if err != nil {
			_ = xj.close()
			return fmt.Errorf("db: glob segments %q: %w", name, err)
		}
		sort.SliceStable(paths, func(i, j int) bool {
			ni, _ := segmentNum(paths[i])
			nj, _ := segmentNum(paths[j])
			if ni != nj {
				return ni < nj
			}
			return paths[i] < paths[j]
		})

		var sealed []*Segment
		var activePath string
		if len(paths) == 0 {
			activePath = filepath.Join(colDir, "seg_000001.ndjson")
		} else {
			activePath = paths[len(paths)-1]
			for _, p := range paths[:len(paths)-1] {
				info, statErr := os.Stat(p)
				if statErr != nil {
					_ = xj.close()
					return fmt.Errorf("db: stat %q: %w", p, statErr)
				}
				sealed = append(sealed, openSealedSegment(p, info.Size()))
			}
		}
		active, err := openActiveSegmentWith(activePath, cfg.wrapFile)
		if err != nil {
			_ = xj.close()
			return fmt.Errorf("db: open active segment %q: %w", activePath, err)
		}
		all := make([]*Segment, 0, len(sealed)+1)
		all = append(all, sealed...)
		all = append(all, active)
		discoveries[name] = &colDiscovery{name: name, dir: colDir, sealed: sealed, active: active, all: all}
	}

	// Phase 3: Scan segments for stamped entries
	evidenceByTx := make(map[string]*txEvidence)
	hasAnyStamped := false

	type stampedItem struct {
		se       stampedEntry
		offset   int64
		endOff   int64
		segPath  string
		isActive bool
	}

	for _, name := range colNames {
		cd := discoveries[name]
		runsInCol := make(map[string][]stampedItem)

		for _, seg := range cd.all {
			isActive := (seg == cd.active)
			_ = seg.ScanStampedFromOffset(0, func(offset int64, e store.Entry, tx *TxStamp) error {
				if tx != nil && tx.T != "" {
					hasAnyStamped = true
					se := stampedEntry{
						ID:        e.ID,
						Op:        e.Op,
						Rev:       e.Rev,
						Epoch:     e.Epoch,
						ExpiresAt: e.ExpiresAt,
						Data:      e.Data,
						Tx:        *tx,
					}
					if e.CRC != nil {
						se.CRC = *e.CRC
					}
					lineBytes, _ := encodeStamped(se)
					endOffset := offset + int64(len(lineBytes))
					runsInCol[tx.T] = append(runsInCol[tx.T], stampedItem{
						se:       se,
						offset:   offset,
						endOff:   endOffset,
						segPath:  seg.Path(),
						isActive: isActive,
					})
				}
				return nil
			})
		}

		for txid, items := range runsInCol {
			if len(items) == 0 {
				continue
			}
			n := items[0].se.Tx.N
			complete := (uint32(len(items)) == n)
			opRefs := make([]xtxOpRef, len(items))
			entries := make([]stampedEntry, len(items))
			for i, it := range items {
				if it.se.Tx.I != uint32(i) || it.se.Tx.N != n {
					complete = false
				}
				opRefs[i] = xtxOpRef{ID: it.se.ID, Op: string(it.se.Op), Rev: it.se.Rev}
				entries[i] = it.se
			}
			d := partDigest(opRefs)
			first := items[0]
			last := items[len(items)-1]

			isActiveTail := false
			if first.isActive && last.isActive && last.endOff >= cd.active.Size() {
				isActiveTail = true
			}

			ev := &runEvidence{
				complete:    complete,
				n:           uint32(len(items)),
				digest:      d,
				segPath:     first.segPath,
				startOffset: first.offset,
				endOffset:   last.endOff,
				activeTail:  isActiveTail,
				entries:     entries,
			}
			tev := evidenceByTx[txid]
			if tev == nil {
				tev = &txEvidence{runs: make(map[string]*runEvidence)}
				evidenceByTx[txid] = tev
			}
			tev.runs[name] = ev
		}
	}

	// Check Row 12: xtx.format says enabled, journal is missing, but stamped entries exist.
	// A present-but-empty journal is legitimate (crash during the first prepare):
	// undecided runs are then resolved as aborted (presumed abort).
	if hasFormat && xj == nil && hasAnyStamped {
		return xtxErr("", ErrXTxJournalMissing, errors.New("stamped entries exist but xtx.journal is missing"))
	}

	// Seq restore (§5.1)
	if xj != nil {
		for txid := range evidenceByTx {
			if _, seq, parseErr := parseTxID(txid); parseErr == nil {
				xj.observeSeq(seq)
			}
		}
	}

	// Collect all txids to resolve
	allTxIDs := make(map[string]bool)
	if xj != nil {
		for _, d := range xj.decisions() {
			allTxIDs[d.Tx] = true
		}
	}
	for txid := range evidenceByTx {
		allTxIDs[txid] = true
	}

	sortedTxIDs := make([]string, 0, len(allTxIDs))
	for txid := range allTxIDs {
		sortedTxIDs = append(sortedTxIDs, txid)
	}
	sort.Strings(sortedTxIDs)

	// Phase 4 & 5: Truth table resolution and dead-run truncation
	for _, txid := range sortedTxIDs {
		var d *xtxDecision
		if xj != nil {
			if dec, ok := xj.decision(txid); ok {
				d = &dec
			}
		}
		tev := evidenceByTx[txid]

		if d == nil {
			// Undecided (Row 1 or Row 2 / Row 13)
			if tev == nil || len(tev.runs) == 0 {
				continue
			}
			// Row 2 / 13: Presumed ABORT
			for colName, rev := range tev.runs {
				if rev.activeTail {
					if cd, ok := discoveries[colName]; ok {
						_ = cd.active.rollback(rev.startOffset)
						_ = cd.active.Sync()
					}
				}
			}
			if xj != nil {
				_ = xj.abort(txid, "", "recovery")
			}
			continue
		}

		switch d.Outcome {
		case xtxKindCommit:
			if d.Retired {
				// Row 9: RETIRE(commit) -> COMMITTED
				continue
			}
			// Row 6: Check missing collection directories
			for _, p := range d.Parts {
				if !existingCols[p.C] {
					_ = xj.close()
					return xtxErr(txid, ErrXTxIncomplete, fmt.Errorf("participant collection %q missing", p.C))
				}
			}
			// Row 7: Check foreign runs
			partSet := make(map[string]bool, len(d.Parts))
			for _, p := range d.Parts {
				partSet[p.C] = true
			}
			if tev != nil {
				for colName := range tev.runs {
					if !partSet[colName] {
						_ = xj.close()
						return xtxErr(txid, ErrXTxIncomplete, fmt.Errorf("stamped entries for tx %s found in foreign collection %q", txid, colName))
					}
				}
			}
			// Row 5: Check every participant run
			for _, p := range d.Parts {
				var rev *runEvidence
				if tev != nil {
					rev = tev.runs[p.C]
				}
				if rev == nil || !rev.complete || rev.n != p.N {
					var gotN uint32
					if rev != nil {
						gotN = rev.n
					}
					_ = xj.close()
					return xtxErr(txid, ErrXTxIncomplete, fmt.Errorf("participant %q run missing or incomplete (got %d of %d ops)", p.C, gotN, p.N))
				}
				if rev.digest != p.D {
					_ = xj.close()
					return xtxErr(txid, ErrXTxIncomplete, fmt.Errorf("participant %q digest mismatch: want %s, got %s", p.C, p.D, rev.digest))
				}
			}
			// Row 3: COMMITTED

		case xtxKindAbort:
			if d.Retired {
				// Row 10: RETIRE(abort) -> ABORTED
				continue
			}
			// Row 8: ABORT -> ABORTED
			if tev != nil {
				for colName, rev := range tev.runs {
					if rev.activeTail {
						if cd, ok := discoveries[colName]; ok {
							_ = cd.active.rollback(rev.startOffset)
							_ = cd.active.Sync()
						}
					}
				}
			}
		}
	}

	// Close temporary segment handles
	for _, cd := range discoveries {
		cd.close()
	}

	// Phase 6: Open collections with decisions map
	var decisions map[string]string
	if xj != nil {
		decisions = xj.decisionsMap()
	} else if hasFormat {
		decisions = make(map[string]string)
	}
	cfg.decisions = decisions
	db.defaultCfg.decisions = decisions

	for _, name := range colNames {
		col, err := OpenCollection(name, db.dataDir, cfg)
		if err != nil {
			for _, c := range db.collections {
				_ = c.Close()
			}
			_ = xj.close()
			return fmt.Errorf("db: open collection %q: %w", name, err)
		}
		col.broker = db.broker
		db.collections[name] = col
	}

	// Coordinator evidence remains live until a later atomic checkpoint can
	// prove every participant's committed effects were safely materialized.
	// Indexes and compaction are derived from the coordinator state, so open
	// must not retire decisions merely because recovery succeeded.

	return nil
}
