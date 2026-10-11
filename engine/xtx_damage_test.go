package engine

// Journal-damage regression contract.
//
// This file deliberately contains no recovery implementation.  It preserves a
// byte-for-byte crash image made at the durable COMMIT boundary, damages only
// the coordinator journal, and describes the only safe observable outcomes:
//
//   - exact PRE: no participant exposes the transaction;
//   - exact POST: every participant exposes it; or
//   - a typed, fail-closed refusal to open.
//
// A mixture of participant visibility is never an acceptable recovery result.
// TestXTxJournalZeroedDamageContract is a deterministic regression harness for
// the case Warden PR #917 calls TestJournalDamage.  It does not prescribe PRE
// versus POST: recovery policy owns that choice, but it must never expose a
// mixture.

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestXTxJournalZeroedDamageContract(t *testing.T) {
	root := t.TempDir()
	_, img := lcPopulate(t, filepath.Join(root, "live"), root)

	// lcPopulate copies this image while t-decided has a durable COMMIT and
	// before recovery has applied that decision.  This is the state exercised
	// by Warden PR #917, without timing or fault-injection races.
	journal := filepath.Join(img.crash, xtxJournalFile)
	if err := os.Truncate(journal, 0); err != nil {
		t.Fatalf("zero %s: %v", journal, err)
	}

	db, err := Open(img.crash, lcCfg())
	if err != nil {
		if !isJournalDamageRefusal(err) {
			t.Fatalf("zeroed journal: untyped open error: %v", err)
		}
		return // Typed refusal is one of the permitted contract outcomes.
	}
	defer func() { _ = db.Close() }()

	for _, tx := range img.crashModel.txs {
		if len(tx.parts) < 2 {
			continue
		}
		applied := 0
		for _, part := range tx.parts {
			if lcApplied(t, db, tx, part) {
				applied++
			}
		}
		if applied != 0 && applied != len(tx.parts) {
			t.Fatalf("zeroed %s: mixed visible state for %s: %d/%d participants", xtxJournalFile, tx.key, applied, len(tx.parts))
		}
	}
}

func isJournalDamageRefusal(err error) bool {
	return errors.Is(err, ErrXTxJournalCorrupt) ||
		errors.Is(err, ErrXTxJournalMissing) ||
		errors.Is(err, ErrXTxRecoveryRequired) ||
		errors.Is(err, ErrXTxUnsupported)
}
