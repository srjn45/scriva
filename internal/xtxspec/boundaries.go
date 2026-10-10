package xtxspec

// Kind is the class of durable-state transition a boundary represents.
type Kind string

const (
	KindWrite  Kind = "write"
	KindFsync  Kind = "fsync"
	KindRename Kind = "rename"
	KindCreate Kind = "create" // file creation (needs a directory fsync to be durable)
)

// CrashState is one deterministic post-crash disk state at a boundary, with the
// exact outcome the recovery truth table must produce.
type CrashState struct {
	Label string
	In    Input
	// Want is the required truth-table verdict (Row and Outcome are compared).
	Want Result
	// MayHaveAck is true when a client could already have been told "committed"
	// in this state; then Want.Outcome must be OutCommitted (invariant I3).
	MayHaveAck bool
}

// Boundary is one write/fsync/rename step of the planned protocol.
type Boundary struct {
	ID     string
	Design string // design section
	Step   string // protocol step (S0..S7, F*, J*, G*, R*)
	Kind   Kind
	Target string
	What   string
	States []CrashState
}

// Canonical two-participant scenario: orders (n=2) and stock (n=1).
var (
	runA = []RunOp{{ID: 1, Op: "insert", Rev: 1}, {ID: 2, Op: "update", Rev: 2}}
	runB = []RunOp{{ID: 7, Op: "delete", Rev: 4}}

	// ScenarioParts are the canonical commit parts of the fixture transaction.
	ScenarioParts = []Part{
		{C: "orders", N: 2, D: ParticipantDigest(runA)},
		{C: "stock", N: 1, D: ParticipantDigest(runB)},
	}
	existing = map[string]bool{"orders": true, "stock": true}
	fullA    = Run{Complete: true, N: 2, Digest: ParticipantDigest(runA)}
	fullB    = Run{Complete: true, N: 1, Digest: ParticipantDigest(runB)}
	tornA    = Run{Complete: false, N: 1, Digest: "partial"}
)

func undecided(ev map[string]Run) Input {
	return Input{Decision: DecNone, Evidence: ev, Existing: existing}
}

func committed(ev map[string]Run) Input {
	return Input{Decision: DecCommit, Parts: ScenarioParts, Evidence: ev, Existing: existing}
}

func st(label string, in Input, row int, out Outcome, ack bool) CrashState {
	code := CodeNone
	if row == 2 || row == 13 {
		code = CodeUndecidedRun
	}
	return CrashState{Label: label, In: in, Want: Result{Row: row, Outcome: out, Code: code}, MayHaveAck: ack}
}

// Boundaries is the fault-boundary checklist for every write/fsync/rename of
// the protocol in docs/design-cross-collection-transactions.md. The prose
// version is docs/design-xtx-fault-boundaries.md; TestBoundaryChecklistDocInSync
// keeps the two in step.
func Boundaries() []Boundary {
	none := map[string]Run{}
	return []Boundary{
		// ---- first-use files (§11.3 R1 order: xtx.format → xtx.journal → runs)
		{"F1", "§6.3.5", "F1", KindCreate, "xtx.format", "create + write xtx.format", []CrashState{
			st("absent or torn: gate not yet durable, no stamped bytes exist", undecided(none), 1, OutNothing, false)}},
		{"F2", "§6.3.5", "F2", KindFsync, "xtx.format", "fsync xtx.format file", []CrashState{
			st("file may be empty/torn; still no stamped bytes", undecided(none), 1, OutNothing, false)}},
		{"F3", "§6.3.4", "F3", KindFsync, "<root>", "fsync root dir after xtx.format create", []CrashState{
			st("format entry may vanish; no stamped bytes exist yet", undecided(none), 1, OutNothing, false)}},
		{"J1", "§4.1", "J1", KindCreate, "xtx.journal", "create + write journal header", []CrashState{
			st("header torn: rewritten at open; no runs yet", undecided(none), 1, OutNothing, false)}},
		{"J2", "§6.3.4", "J2", KindFsync, "<root>", "fsync file + root dir after journal create", []CrashState{
			st("journal may vanish before any run was appended", undecided(none), 1, OutNothing, false)}},

		// ---- S1: appending runs
		{"S1a", "§6.2 S1", "S1", KindWrite, "participant[k] segment", "append of one stamped line torn mid-line", []CrashState{
			st("first participant's run torn", undecided(map[string]Run{"orders": tornA}), 2, OutAborted, false),
			st("a complete, b torn", undecided(map[string]Run{"orders": fullA, "stock": {Complete: false, N: 0}}), 2, OutAborted, false)}},
		{"S1b", "§6.2 S1", "S1", KindWrite, "participant[k] segment", "run k complete, run k+1 not started", []CrashState{
			st("only orders has a complete run", undecided(map[string]Run{"orders": fullA}), 2, OutAborted, false)}},
		{"S1c", "§6.2 S1", "S1", KindWrite, "all segments", "all runs appended, none fsynced (page cache lost or kept)", []CrashState{
			st("every run survived but no decision", undecided(map[string]Run{"orders": fullA, "stock": fullB}), 2, OutAborted, false),
			st("nothing survived", undecided(none), 1, OutNothing, false)}},

		// ---- S2: fsync runs
		{"S2a", "§6.2 S2", "S2", KindFsync, "participant[k] segment", "crash inside the k-th fsync (some runs durable)", []CrashState{
			st("orders durable, stock lost", undecided(map[string]Run{"orders": fullA}), 2, OutAborted, false)}},
		{"S2b", "§6.3.3", "S2", KindFsync, "participant[k] segment", "fsync returns error (process survives)", []CrashState{
			st("segment poisoned, XTx aborted; restart sees undecided runs", undecided(map[string]Run{"orders": fullA, "stock": fullB}), 2, OutAborted, false)}},
		{"S2c", "§6.2 S2", "S2", KindFsync, "participant dir", "directory fsync for a segment created in this XTx", []CrashState{
			st("new segment name may be lost, run with it", undecided(map[string]Run{"orders": fullA}), 2, OutAborted, false)}},

		// ---- S3/S4: the commit point
		{"S3a", "§4.3", "S3", KindWrite, "xtx.journal", "COMMIT line torn (no newline / zero fill)", []CrashState{
			st("journal tail torn ⇒ truncated, runs complete, no decision", Input{Decision: DecNone, TornJournalTail: true, Evidence: map[string]Run{"orders": fullA, "stock": fullB}, Existing: existing}, 13, OutAborted, false)}},
		{"S3b", "§6.2 S3", "S3", KindWrite, "xtx.journal", "COMMIT line whole in page cache, not fsynced", []CrashState{
			st("record lost", undecided(map[string]Run{"orders": fullA, "stock": fullB}), 2, OutAborted, false),
			// Atomic either way: the kernel may have written it back. No ack was sent.
			st("record survived", committed(map[string]Run{"orders": fullA, "stock": fullB}), 3, OutCommitted, false)}},
		{"S4a", "§6.3.3", "S4", KindFsync, "xtx.journal", "journal fsync returns error", []CrashState{
			st("outcome unknown to the process: record lost", undecided(map[string]Run{"orders": fullA, "stock": fullB}), 2, OutAborted, false),
			st("outcome unknown to the process: record durable", committed(map[string]Run{"orders": fullA, "stock": fullB}), 3, OutCommitted, false)}},
		{"S4b", "§6.2 S4", "S4", KindFsync, "xtx.journal", "COMMIT durable, indexes not yet updated", []CrashState{
			st("committed, tail-replayed at open", committed(map[string]Run{"orders": fullA, "stock": fullB}), 3, OutCommitted, true)}},

		// ---- S5/S7: apply and ack
		{"S5", "§6.2 S5", "S5", KindWrite, "in-memory indexes", "crash while applying runs to indexes", []CrashState{
			st("committed; indexes rebuilt from segments + journal", committed(map[string]Run{"orders": fullA, "stock": fullB}), 3, OutCommitted, true)}},
		{"S7", "§6.2 S7", "S7", KindWrite, "client connection", "crash after COMMIT fsync, before/after ack bytes", []CrashState{
			st("client may or may not have the ack; tx is committed", committed(map[string]Run{"orders": fullA, "stock": fullB}), 3, OutCommitted, true)}},

		// ---- ABORT, retirement, de-stamping, GC
		{"A1", "§4.2", "A1", KindWrite, "xtx.journal", "advisory ABORT append (lost or kept)", []CrashState{
			st("ABORT lost ⇒ presumed abort", undecided(map[string]Run{"orders": fullA}), 2, OutAborted, false),
			st("ABORT durable", Input{Decision: DecAbort, Evidence: map[string]Run{"orders": fullA}, Existing: existing}, 8, OutAborted, false)}},
		{"R1", "§4.4.2", "R1", KindWrite, "xtx.journal", "RETIRE append torn", []CrashState{
			st("torn ⇒ truncated; COMMIT still authoritative", committed(map[string]Run{"orders": fullA, "stock": fullB}), 3, OutCommitted, true)}},
		{"R2", "§4.4.2", "R2", KindFsync, "xtx.journal", "RETIRE fsync", []CrashState{
			st("retired; no evidence needed", Input{Decision: DecRetireCommit, Evidence: none, Existing: existing}, 9, OutCommitted, true),
			st("not retired; evidence still required", committed(map[string]Run{"orders": fullA, "stock": fullB}), 3, OutCommitted, true)}},
		{"R3", "§4.4.3", "R3", KindRename, "participant segments", "compaction swap that de-stamps retired entries", []CrashState{
			st("old or new segments whole (existing compact.manifest recovery); de-stamped part ok", Input{Decision: DecCommit, Parts: ScenarioParts, Evidence: map[string]Run{"orders": {Destamped: true}, "stock": fullB}, Existing: existing}, 4, OutCommitted, true)}},
		{"G1", "§4.5", "G1", KindWrite, "xtx.journal.tmp", "write rewritten journal", []CrashState{
			st("tmp deleted at open; old journal intact", committed(map[string]Run{"orders": fullA, "stock": fullB}), 3, OutCommitted, true)}},
		{"G2", "§4.5", "G2", KindFsync, "xtx.journal.tmp", "fsync tmp journal", []CrashState{
			st("tmp deleted at open; old journal intact", committed(map[string]Run{"orders": fullA, "stock": fullB}), 3, OutCommitted, true)}},
		{"G3", "§4.5", "G3", KindRename, "xtx.journal", "rename tmp over journal", []CrashState{
			st("old or new journal whole, never mixed", committed(map[string]Run{"orders": fullA, "stock": fullB}), 3, OutCommitted, true)}},
		{"G4", "§4.5", "G4", KindFsync, "<root>", "fsync root dir after rename", []CrashState{
			st("rename may revert to the old whole journal", committed(map[string]Run{"orders": fullA, "stock": fullB}), 3, OutCommitted, true)}},

		// ---- journal loss / corruption (not crash-reachable; hazards)
		{"H1", "§7.2 r12", "H1", KindWrite, "xtx.journal", "journal file lost while stamped runs exist", []CrashState{
			{Label: "lost journal is never presumed abort", In: Input{Decision: DecNone, Evidence: map[string]Run{"orders": fullA}, Existing: existing, JournalMissing: true},
				Want: Result{Row: 12, Outcome: OutRefuse, Code: CodeJournalMissing}}}},
		{"H2", "§7.2 r5", "H2", KindWrite, "participant segment", "COMMIT durable but one run lost", []CrashState{
			{Label: "never commit a subset, never abort a decided tx", In: committed(map[string]Run{"orders": fullA}),
				Want: Result{Row: 5, Outcome: OutRefuse, Code: CodeParticipantMissing}}}},
	}
}
