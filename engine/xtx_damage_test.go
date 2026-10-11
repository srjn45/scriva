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
	"bytes"
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

func TestXTxJournalDamageAtDecisionBoundary(t *testing.T) {
	root := t.TempDir()
	_, img := lcPopulate(t, filepath.Join(root, "live"), root)

	cases := []struct {
		name    string
		damage  func(t *testing.T, dir string)
		wantErr error
	}{
		{
			name: "valid",
			damage: func(t *testing.T, dir string) {
				t.Helper()
			},
		},
		{
			name: "empty",
			damage: func(t *testing.T, dir string) {
				t.Helper()
				if err := os.Truncate(filepath.Join(dir, xtxJournalFile), 0); err != nil {
					t.Fatalf("truncate journal: %v", err)
				}
			},
			wantErr: ErrXTxRecoveryRequired,
		},
		{
			name: "zeroed",
			damage: func(t *testing.T, dir string) {
				t.Helper()
				journal := filepath.Join(dir, xtxJournalFile)
				info, err := os.Stat(journal)
				if err != nil {
					t.Fatalf("stat journal: %v", err)
				}
				if err := os.WriteFile(journal, make([]byte, info.Size()), 0o644); err != nil {
					t.Fatalf("zero journal: %v", err)
				}
			},
			wantErr: ErrXTxRecoveryRequired,
		},
		{
			name: "missing",
			damage: func(t *testing.T, dir string) {
				t.Helper()
				if err := os.Remove(filepath.Join(dir, xtxJournalFile)); err != nil {
					t.Fatalf("remove journal: %v", err)
				}
			},
			wantErr: ErrXTxJournalMissing,
		},
		{
			name: "corrupt-record",
			damage: func(t *testing.T, dir string) {
				t.Helper()
				journal := filepath.Join(dir, xtxJournalFile)
				b, err := os.ReadFile(journal)
				if err != nil {
					t.Fatalf("read journal: %v", err)
				}
				headerEnd := bytes.IndexByte(b, '\n') + 1
				if headerEnd <= 0 || headerEnd >= len(b) {
					t.Fatalf("journal lacks a complete record to corrupt")
				}
				b[headerEnd] ^= 1
				if err := os.WriteFile(journal, b, 0o644); err != nil {
					t.Fatalf("write corrupt journal: %v", err)
				}
			},
			wantErr: ErrXTxJournalCorrupt,
		},
		{
			name: "torn-final-tail",
			damage: func(t *testing.T, dir string) {
				t.Helper()
				f, err := os.OpenFile(filepath.Join(dir, xtxJournalFile), os.O_WRONLY|os.O_APPEND, 0)
				if err != nil {
					t.Fatalf("open journal append: %v", err)
				}
				if _, err := f.WriteString(`{"k":"commit","tx":"torn`); err != nil {
					_ = f.Close()
					t.Fatalf("append torn tail: %v", err)
				}
				if err := f.Close(); err != nil {
					t.Fatalf("close journal: %v", err)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := filepath.Join(root, "case-"+tc.name)
			lcCopyTree(t, img.crash, dir)
			tc.damage(t, dir)

			sig, err := lcOpenCheck(t, dir, lcCfg(), img.crashModel, tc.name)
			if tc.wantErr != nil {
				if err == nil {
					t.Fatalf("Open accepted damaged journal with signature %s", sig)
				}
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("Open error = %v, want %v", err, tc.wantErr)
				}
				if !isJournalDamageRefusal(err) {
					t.Fatalf("Open error is not a typed journal-damage refusal: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Open rejected recoverable journal: %v", err)
			}
			if again, err := lcOpenCheck(t, dir, lcCfg(), img.crashModel, tc.name+" repeat"); err != nil {
				t.Fatalf("second Open: %v", err)
			} else if again != sig {
				t.Fatalf("visibility changed between opens:\n first  %s\n second %s", sig, again)
			}
		})
	}
}

func isJournalDamageRefusal(err error) bool {
	return errors.Is(err, ErrXTxJournalCorrupt) ||
		errors.Is(err, ErrXTxJournalMissing) ||
		errors.Is(err, ErrXTxRecoveryRequired) ||
		errors.Is(err, ErrXTxUnsupported)
}
