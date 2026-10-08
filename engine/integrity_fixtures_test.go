package engine

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

// Table tests over the synthetic incident fixtures in testdata/integrity (see
// integrity_fixtures_gen_test.go). Every case pins, end to end: what Open does
// by default and under PolicyReport, the exact Verify codes, what Repair does
// (status, actions, counts), that no deleted record is resurrected, that the
// originals survive in the backup, that a second Repair changes nothing, and
// that the repaired directory verifies clean (or, for conflicts, exactly as
// before — Repair never resolves them).

type integrityCase struct {
	name string

	verify   []FindingCode // exact full-mode Verify codes on the pristine fixture
	openFail bool          // default policy refuses to open (ErrIntegrity)
	live     map[uint64]string
	deleted  []uint64

	repair       RepairOptions
	repairErr    error
	status       RepairStatus
	actions      []string // Repair action kinds, in order
	conflicts    int
	salvaged     int // SalvageReport.Entries (0 = no salvage)
	liveAfter    int // LiveRecords in the post-repair report (when repaired)
	after        []FindingCode
	opensAfter   bool // default-policy open works after repair
	segsPreserve bool // segment files byte-identical after repair
}

var integrityCases = []integrityCase{
	{
		name:   "stale-crash-index",
		verify: []FindingCode{CodeIndexMissingRecord, CodeIndexResurrected, CodeIndexStaleRecord, CodeIndexStaleTail},
		live:   map[uint64]string{1: "updated", 2: "v2", 3: "v3", 5: "late"}, deleted: []uint64{4},
		status: RepairRepaired, actions: []string{"rebuild-derived"}, liveAfter: 4,
		opensAfter: true, segsPreserve: true,
	},
	{
		name:   "orphan-segment",
		verify: []FindingCode{CodeIndexMissingRecord, CodeIndexSegmentUnlisted},
		live:   map[uint64]string{1: "v1", 3: "v3", 4: "v4"}, deleted: []uint64{2},
		status: RepairRepaired, actions: []string{"rebuild-derived"}, liveAfter: 3,
		opensAfter: true, segsPreserve: true,
	},
	{
		name:   "wrong-offset",
		verify: []FindingCode{CodeIndexDanglingOffset, CodeIndexWrongRecord},
		live:   map[uint64]string{1: "v1", 2: "v2", 3: "v3", 4: "v4"},
		status: RepairRepaired, actions: []string{"rebuild-derived"}, liveAfter: 4,
		opensAfter: true, segsPreserve: true,
	},
	{
		// Without Salvage the damaged segment is left exactly as it was.
		name:     "glued-corrupt",
		verify:   []FindingCode{CodeIndexMissing, CodeSegmentGluedLine},
		openFail: true,
		live:     map[uint64]string{1: "v1", 2: "v2", 3: "v3", 4: "v4", 5: "glued", 6: "v6"},
		status:   RepairBlocked, repairErr: ErrRepairIncomplete,
		after:        []FindingCode{CodeIndexMissing, CodeSegmentGluedLine},
		segsPreserve: true,
	},
	{
		name:     "glued-corrupt+salvage",
		verify:   []FindingCode{CodeIndexMissing, CodeSegmentGluedLine},
		openFail: true,
		live:     map[uint64]string{1: "v1", 2: "v2", 3: "v3", 4: "v4", 5: "glued", 6: "v6"},
		repair:   RepairOptions{Salvage: true},
		status:   RepairRepaired, actions: []string{"salvage-segment", "quarantine-segment", "rebuild-derived"},
		salvaged: 6, liveAfter: 6, opensAfter: true,
	},
	{
		name:   "conflicting-dupes",
		verify: []FindingCode{CodeConflictDuplicateID, CodeConflictIDReuse, CodeIndexMissing},
		// Last write wins, exactly as it always did: id 4's delete stays a delete.
		openFail: true,
		live:     map[uint64]string{1: "v1", 2: "v2-conflict", 3: "v3-reused"}, deleted: []uint64{4},
		status: RepairRepaired, actions: []string{"rebuild-derived"}, conflicts: 2, liveAfter: 3,
		after:        []FindingCode{CodeConflictDuplicateID, CodeConflictIDReuse},
		segsPreserve: true,
	},
	{
		name:   "conflicting-dupes+abort",
		verify: []FindingCode{CodeConflictDuplicateID, CodeConflictIDReuse, CodeIndexMissing},
		repair: RepairOptions{OnConflict: ConflictAbort}, repairErr: ErrRepairConflict,
		openFail: true,
		live:     map[uint64]string{1: "v1", 2: "v2-conflict", 3: "v3-reused"}, deleted: []uint64{4},
		status: RepairBlocked, conflicts: 2,
		after:        []FindingCode{CodeConflictDuplicateID, CodeConflictIDReuse, CodeIndexMissing},
		segsPreserve: true,
	},
	{
		name:   "relocated-v1",
		verify: []FindingCode{CodeIndexCoverageUnknown},
		live:   map[uint64]string{1: "v1", 2: "v2", 3: "v3", 4: "v4"},
		status: RepairRepaired, actions: []string{"rebuild-derived"}, liveAfter: 4,
		opensAfter: true, segsPreserve: true,
	},
}

func fixtureCopy(t *testing.T, name string) string {
	t.Helper()
	return copyDataDir(t, filepath.Join(integrityFixtureRoot, strings.SplitN(name, "+", 2)[0]))
}

func sortedCodes(c []FindingCode) []FindingCode {
	out := append([]FindingCode(nil), c...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func wantCodeSet(t *testing.T, what string, got, want []FindingCode) {
	t.Helper()
	if !reflect.DeepEqual(sortedCodes(got), sortedCodes(want)) {
		t.Fatalf("%s codes = %v, want %v", what, sortedCodes(got), sortedCodes(want))
	}
}

func checkLive(t *testing.T, dir string, pol IntegrityPolicy, live map[uint64]string, deleted []uint64) {
	t.Helper()
	db, err := Open(dir, CollectionConfig{CompactInterval: time.Hour, IntegrityPolicy: pol})
	if err != nil {
		t.Fatalf("open (%s): %v", pol, err)
	}
	defer db.Close()
	col, err := db.Collection("c")
	if err != nil {
		t.Fatal(err)
	}
	for id, v := range live {
		r, err := col.Get(id)
		if err != nil || r.Data["v"] != v {
			t.Fatalf("id %d: got %+v err %v, want v=%q", id, r, err, v)
		}
	}
	for _, id := range deleted {
		if _, err := col.Get(id); err == nil {
			t.Fatalf("deleted id %d resurrected", id)
		}
	}
}

func TestIntegrityFixtures(t *testing.T) {
	for _, tc := range integrityCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pristine := fixtureCopy(t, tc.name)
			orig := snapshotDir(t, pristine)

			// Verify: exact codes, and read-only.
			rep, err := VerifyDir(context.Background(), pristine, VerifyOptions{Mode: VerifyFull})
			if err != nil {
				t.Fatal(err)
			}
			wantCodeSet(t, "verify", rep.Codes(), tc.verify)
			if snapshotDir(t, pristine) != orig {
				t.Fatal("verify modified the fixture")
			}

			// Open policy: default fails closed on corruption/conflicts; report
			// opts in to salvage-and-continue with last-write-wins; neither
			// resurrects a delete.
			_, err = Open(fixtureCopy(t, tc.name), CollectionConfig{CompactInterval: time.Hour})
			var oie *OpenIntegrityError
			switch {
			case tc.openFail:
				if !errors.Is(err, ErrIntegrity) || !errors.As(err, &oie) {
					t.Fatalf("default open: want ErrIntegrity, got %v", err)
				}
				checkLive(t, fixtureCopy(t, tc.name), PolicyReport, tc.live, tc.deleted)
			case err != nil:
				t.Fatalf("default open: %v", err)
			default:
				checkLive(t, fixtureCopy(t, tc.name), PolicyFail, tc.live, tc.deleted)
			}

			// Repair.
			dir := fixtureCopy(t, tc.name)
			colDir := filepath.Join(dir, "c")
			segsBefore := segHashes(t, colDir)
			opts := tc.repair
			opts.BackupDir = t.TempDir()
			rr, err := Repair(context.Background(), dir, opts)
			if !errors.Is(err, tc.repairErr) || (tc.repairErr == nil && err != nil) {
				t.Fatalf("repair err = %v, want %v", err, tc.repairErr)
			}
			c := colRepair(t, rr, "c")
			if c.Status != tc.status {
				t.Fatalf("status %s (%s), want %s", c.Status, c.Reason, tc.status)
			}
			var kinds []string
			for _, a := range c.Actions {
				kinds = append(kinds, a.Kind)
			}
			if !reflect.DeepEqual(kinds, tc.actions) {
				t.Fatalf("actions %v, want %v", kinds, tc.actions)
			}
			if len(c.Conflicts) != tc.conflicts {
				t.Fatalf("conflicts %d, want %d", len(c.Conflicts), tc.conflicts)
			}
			if tc.salvaged == 0 && c.Salvage != nil || tc.salvaged > 0 && (c.Salvage == nil || c.Salvage.Entries != tc.salvaged) {
				t.Fatalf("salvage %+v, want %d entries", c.Salvage, tc.salvaged)
			}
			if tc.status == RepairRepaired && c.After.Stats.LiveRecords != tc.liveAfter {
				t.Fatalf("live after = %d, want %d", c.After.Stats.LiveRecords, tc.liveAfter)
			}
			if tc.segsPreserve && segHashes(t, colDir) != segsBefore {
				t.Fatal("segments were edited")
			}
			if tc.status == RepairRepaired {
				// The originals survive, byte for byte, in the verified backup.
				if rr.BackupDir == "" || segHashes(t, filepath.Join(rr.BackupDir, "c")) != segsBefore {
					t.Fatalf("backup %q does not preserve the original segments", rr.BackupDir)
				}
			} else if rr.BackupDir != "" {
				t.Fatalf("blocked repair took a backup: %s", rr.BackupDir)
			}

			// Post-repair state.
			post, err := VerifyDir(context.Background(), dir, VerifyOptions{Mode: VerifyFull})
			if err != nil {
				t.Fatal(err)
			}
			wantCodeSet(t, "post-repair", post.Codes(), tc.after)
			if len(tc.after) == 0 && !post.Clean() {
				t.Fatalf("not clean after repair: %+v", post.AllFindings())
			}
			if tc.opensAfter {
				checkLive(t, copyDataDir(t, dir), PolicyFail, tc.live, tc.deleted)
			} else {
				checkLive(t, copyDataDir(t, dir), PolicyReport, tc.live, tc.deleted)
			}

			// Idempotence: running Repair again changes nothing on disk.
			settled := snapshotDir(t, dir)
			opts.BackupDir = t.TempDir()
			rr2, err := Repair(context.Background(), dir, opts)
			if tc.status == RepairRepaired && tc.conflicts == 0 && err != nil {
				t.Fatalf("second repair: %v", err)
			}
			if snapshotDir(t, dir) != settled {
				t.Fatal("second repair modified the directory")
			}
			if rr2 != nil && rr2.BackupDir != "" && tc.conflicts == 0 {
				t.Fatalf("second repair took a backup: %s", rr2.BackupDir)
			}
		})
	}
}
