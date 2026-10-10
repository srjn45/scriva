package engine

// Read-only XTx graph validation for VerifyDir and offline repair.  This is
// intentionally separate from recoverCoordinator: verification must never
// trim a prepared tail, append an ABORT, or remove a stale checkpoint tmp.

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

type verifyXTxRun struct{ entries []SalvagedEntry }

func readXTxDecisions(dataDir string) (map[string]string, error) {
	b, err := os.ReadFile(filepath.Join(dataDir, xtxJournalFile))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	p, err := parseXTxJournal(b)
	if err != nil {
		return nil, err
	}
	m := make(map[string]string, len(p.Table.byTx))
	for tx, d := range p.Table.byTx {
		if d.Retired {
			m[tx] = "retire-" + d.Outcome
		} else {
			m[tx] = d.Outcome
		}
	}
	return m, nil
}

// verifyXTxGraph validates the coordinator against stamped participant runs
// without modifying the directory.  It returns false when a finding makes it
// unsafe for an offline writer to proceed.
func verifyXTxGraph(dataDir string, rep *IntegrityReport, _ int) bool {
	add := func(code FindingCode, tx, msg string) {
		rep.Findings = append(rep.Findings, Finding{Severity: SeverityConflict, Code: code, Location: Location{Tx: tx}, Message: msg})
	}
	format, err := readXTxFormat(dataDir)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			add(CodeXTxJournalCorrupt, "", err.Error())
			return false
		}
		format = nil
	}
	b, err := os.ReadFile(filepath.Join(dataDir, xtxJournalFile))
	journalPresent := err == nil
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		add(CodeXTxJournalCorrupt, "", err.Error())
		return false
	}
	var parsed *xtxParsed
	if journalPresent {
		parsed, err = parseXTxJournal(b)
		if err != nil {
			add(CodeXTxJournalCorrupt, "", err.Error())
			return false
		}
	} else if format != nil {
		// An enabled empty root can legitimately lack a journal until its first
		// transaction; stamped evidence below makes that state irreconcilable.
		parsed = &xtxParsed{Table: newXTxTable()}
	}

	runs := map[string]map[string]*verifyXTxRun{} // tx -> collection -> run
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		add(CodeXTxJournalCorrupt, "", err.Error())
		return false
	}
	for _, e := range entries {
		if !e.IsDir() || isReservedXTxName(e.Name()) {
			continue
		}
		paths, _ := filepath.Glob(filepath.Join(dataDir, e.Name(), "seg_*.ndjson"))
		sort.Strings(paths)
		for _, path := range paths {
			sr, scanErr := ScanSegmentTolerant(path)
			if scanErr != nil {
				add(CodeXTxJournalCorrupt, "", scanErr.Error())
				return false
			}
			for _, se := range sr.Entries {
				if se.Tx == nil {
					continue
				}
				byCol := runs[se.Tx.T]
				if byCol == nil {
					byCol = map[string]*verifyXTxRun{}
					runs[se.Tx.T] = byCol
				}
				r := byCol[e.Name()]
				if r == nil {
					r = &verifyXTxRun{}
					byCol[e.Name()] = r
				}
				r.entries = append(r.entries, se)
			}
		}
	}
	if len(runs) > 0 && !journalPresent {
		add(CodeXTxJournalMissing, "", "stamped participant evidence exists but xtx.journal is missing")
		return false
	}
	if parsed == nil {
		return true
	} // legacy root
	ok := true
	for tx, byCol := range runs {
		d, decided := parsed.Table.byTx[tx]
		if !decided {
			add(CodeXTxUndecidedRun, tx, "stamped participant run has no coordinator decision")
			ok = false
			continue
		}
		if d.Outcome != xtxKindCommit || d.Retired {
			continue
		}
		parts := map[string]xtxPart{}
		for _, p := range d.Parts {
			parts[p.C] = p
		}
		for col := range byCol {
			if _, found := parts[col]; !found {
				add(CodeXTxForeignRun, tx, fmt.Sprintf("participant evidence found in foreign collection %q", col))
				ok = false
			}
		}
		for _, p := range d.Parts {
			r := byCol[p.C]
			if r == nil {
				add(CodeXTxParticipantMissing, tx, fmt.Sprintf("committed participant %q has no run", p.C))
				ok = false
				continue
			}
			refs := make([]xtxOpRef, len(r.entries))
			complete := len(r.entries) == int(p.N)
			for i, se := range r.entries {
				complete = complete && se.Tx.I == uint32(i) && se.Tx.N == p.N
				refs[i] = xtxOpRef{ID: se.Entry.ID, Op: string(se.Entry.Op), Rev: se.Entry.Rev}
			}
			if !complete || partDigest(refs) != p.D {
				add(CodeXTxDigestMismatch, tx, fmt.Sprintf("participant %q does not match committed digest", p.C))
				ok = false
			}
		}
	}
	for tx, d := range parsed.Table.byTx {
		if d.Outcome != xtxKindCommit || d.Retired {
			continue
		}
		for _, p := range d.Parts {
			if runs[tx] == nil || runs[tx][p.C] == nil {
				add(CodeXTxParticipantMissing, tx, fmt.Sprintf("committed participant %q has no run", p.C))
				ok = false
			}
		}
	}
	return ok
}
