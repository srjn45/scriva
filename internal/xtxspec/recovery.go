package xtxspec

// Outcome is the recovery result for one txid.
type Outcome string

const (
	OutNothing   Outcome = "nothing"
	OutAborted   Outcome = "aborted"
	OutCommitted Outcome = "committed"
	OutRefuse    Outcome = "refuse" // integrity failure: Open must not proceed
)

// Finding codes (design §10).
const (
	CodeNone               = ""
	CodeParticipantMissing = "xtx-participant-missing"
	CodeDigestMismatch     = "xtx-digest-mismatch"
	CodeMissingCollection  = "xtx-missing-collection"
	CodeForeignRun         = "xtx-foreign-run"
	CodeDecisionConflict   = "xtx-decision-conflict"
	CodeJournalMissing     = "xtx-journal-missing"
	CodeJournalCorrupt     = "xtx-journal-corrupt"
	CodeUndecidedRun       = "xtx-undecided-run"
)

// Run is the evidence recovery found for one participant of one txid.
type Run struct {
	Complete  bool   // all N stamped entries present and valid
	N         uint32 // run length found
	Digest    string // ParticipantDigest over what was found
	Destamped bool   // ops already rewritten as plain entries after RETIRE-verified compaction (row 4)
}

// Input is everything Resolve may look at: a pure function, no I/O.
type Input struct {
	Decision Decision
	Parts    []Part // from the COMMIT record (empty when Decision != DecCommit)
	// Evidence is keyed by collection name; only collections with stamped
	// entries of this txid appear.
	Evidence map[string]Run
	// Existing lists collections whose directory exists.
	Existing map[string]bool
	// JournalMissing is true when xtx.format says XTx is enabled but
	// xtx.journal is absent (row 12).
	JournalMissing bool
	// Conflict is true when the journal holds both COMMIT and ABORT (row 11).
	Conflict bool
	// TornJournalTail is true when the journal had a torn tail that was
	// truncated at open (row 13); outcome is identical to row 2.
	TornJournalTail bool
}

// Result is the truth-table verdict. Row is the §7.2 row number.
type Result struct {
	Row     int
	Outcome Outcome
	Code    string
	Action  string
}

// Resolve implements the §7.2 truth table exactly.
func Resolve(in Input) Result {
	switch {
	case in.Conflict:
		return Result{11, OutRefuse, CodeDecisionConflict, "refuse open"}
	case in.JournalMissing && len(in.Evidence) > 0:
		return Result{12, OutRefuse, CodeJournalMissing, "refuse open; operator repair --xtx-presume-abort"}
	}
	switch in.Decision {
	case DecNone:
		if len(in.Evidence) == 0 {
			return Result{1, OutNothing, CodeNone, ""}
		}
		row := 2
		if in.TornJournalTail {
			row = 13
		}
		return Result{row, OutAborted, CodeUndecidedRun, "truncate tail runs; append ABORT{why:recovery}; fsync"}
	case DecAbort:
		return Result{8, OutAborted, CodeNone, "skip entries; truncate if still tail"}
	case DecRetireAbort:
		return Result{10, OutAborted, CodeNone, "skip; drop at next compaction"}
	case DecRetireCommit:
		return Result{9, OutCommitted, CodeNone, "entries visible; no digest check"}
	}
	// DecCommit
	inParts := map[string]bool{}
	for _, p := range in.Parts {
		inParts[p.C] = true
	}
	for c := range in.Evidence {
		if !inParts[c] {
			return Result{7, OutRefuse, CodeForeignRun, "ErrXTxIncomplete; refuse open"}
		}
	}
	for _, p := range in.Parts {
		if !in.Existing[p.C] {
			return Result{6, OutRefuse, CodeMissingCollection, "refuse open"}
		}
	}
	row := 3
	for _, p := range in.Parts {
		ev, ok := in.Evidence[p.C]
		switch {
		case ok && ev.Destamped:
			row = 4
		case !ok || !ev.Complete || ev.N != p.N:
			return Result{5, OutRefuse, CodeParticipantMissing, "ErrXTxIncomplete; never commit a subset"}
		case ev.Digest != p.D:
			return Result{5, OutRefuse, CodeDigestMismatch, "ErrXTxIncomplete; never commit a subset"}
		}
	}
	return Result{row, OutCommitted, CodeNone, "entries visible; indexes tail-replayed"}
}

// EntryVisible is the single visibility predicate of design §3.3: a stamped
// entry is part of the database iff its txid is committed (or retired-commit).
func EntryVisible(txid string, decisions map[string]Decision) bool {
	d := decisions[txid]
	return d == DecCommit || d == DecRetireCommit
}
