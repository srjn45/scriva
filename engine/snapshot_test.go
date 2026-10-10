//nolint:errcheck
package engine

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/srjn45/scriva/store"
)

// extractTarGz unpacks a gzip-compressed tar archive into dir.
func extractTarGz(t *testing.T, data []byte, dir string) {
	t.Helper()
	gz, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("gzip reader: %v", err)
	}
	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("tar next: %v", err)
		}
		target := filepath.Join(dir, hdr.Name)
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		f, err := os.Create(target)
		if err != nil {
			t.Fatalf("create %q: %v", target, err)
		}
		if _, err := io.Copy(f, tr); err != nil { //nolint:gosec // trusted test archive
			t.Fatalf("copy: %v", err)
		}
		f.Close()
	}
}

func TestSnapshot_RoundTrip(t *testing.T) {
	src := t.TempDir()
	db, err := Open(src, CollectionConfig{
		SegmentMaxSize:  256, // tiny, so multiple sealed segments form
		CompactInterval: 24 * time.Hour,
		CompactDirtyPct: 0.30,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	users, _ := db.CreateCollection("users")
	orders, _ := db.CreateCollection("orders")

	// Populate enough to roll several segments, plus updates and a delete so the
	// snapshot exercises multi-segment + stale-entry state.
	ids := make([]uint64, 0, 20)
	for i := 0; i < 20; i++ {
		id, _, _ := users.Insert(map[string]any{"n": i, "name": "user"})
		ids = append(ids, id)
	}
	users.Update(ids[0], map[string]any{"n": 999, "name": "updated"})
	users.Delete(ids[1])
	orders.Insert(map[string]any{"item": "widget"})

	// An index should survive too.
	users.EnsureIndex("name")

	// Take the snapshot into a buffer.
	var buf bytes.Buffer
	if err := db.SnapshotTo(&buf); err != nil {
		t.Fatalf("SnapshotTo: %v", err)
	}
	db.Close()

	// Restore into a fresh data dir and reopen.
	dst := t.TempDir()
	extractTarGz(t, buf.Bytes(), dst)

	restored, err := Open(dst, CollectionConfig{
		SegmentMaxSize:  256,
		CompactInterval: 24 * time.Hour,
		CompactDirtyPct: 0.30,
	})
	if err != nil {
		t.Fatalf("Open restored: %v", err)
	}
	defer restored.Close()

	// Both collections present.
	got := restored.ListCollections()
	if len(got) != 2 {
		t.Fatalf("restored collections: got %v, want users+orders", got)
	}

	ru, err := restored.Collection("users")
	if err != nil {
		t.Fatalf("restored users: %v", err)
	}

	// The updated record reflects the latest value.
	rec, _, err := ru.FindByID(ids[0])
	if err != nil {
		t.Fatalf("FindByID updated: %v", err)
	}
	if rec["n"] != float64(999) {
		t.Errorf("updated record n = %v, want 999", rec["n"])
	}

	// The deleted record stays gone.
	if _, _, err := ru.FindByID(ids[1]); err == nil {
		t.Error("deleted record visible after restore")
	}

	// Live count matches: 20 inserted, 1 deleted = 19.
	res, err := ru.Scan(nil)
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(res) != 19 {
		t.Errorf("restored users live count = %d, want 19", len(res))
	}

	// The secondary index was captured and still answers lookups.
	if idxs := ru.ListIndexes(); len(idxs) == 0 {
		t.Error("expected the 'name' secondary index to survive the snapshot")
	}
}

func TestSnapshot_EmptyDB(t *testing.T) {
	db, err := Open(t.TempDir(), CollectionConfig{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	var buf bytes.Buffer
	if err := db.SnapshotTo(&buf); err != nil {
		t.Fatalf("SnapshotTo empty: %v", err)
	}

	// A valid, empty gzip tar must still extract cleanly.
	dst := t.TempDir()
	extractTarGz(t, buf.Bytes(), dst)
}

func TestSnapshot_XTxRoundTripIncludesCoordinator(t *testing.T) {
	src := t.TempDir()
	db, err := Open(src, CollectionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	orders, err := db.CreateCollection("orders")
	if err != nil {
		t.Fatal(err)
	}
	stock, err := db.CreateCollection("stock")
	if err != nil {
		t.Fatal(err)
	}

	orderID := orders.ReserveID()
	stockID := stock.ReserveID()
	result, err := db.CommitXTx("snapshot-xtx", []XTxOp{
		{Collection: "orders", Op: "insert", ID: orderID, Data: map[string]any{"sku": "widget"}},
		{Collection: "stock", Op: "insert", ID: stockID, Data: map[string]any{"sku": "widget", "qty": 3}},
	})
	if err != nil {
		t.Fatalf("CommitXTx: %v", err)
	}

	var archive bytes.Buffer
	if err := db.SnapshotTo(&archive); err != nil {
		t.Fatalf("SnapshotTo: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close source: %v", err)
	}

	dst := t.TempDir()
	extractTarGz(t, archive.Bytes(), dst)
	for _, base := range []string{xtxFormatFile, xtxJournalFile} {
		if _, err := os.Stat(filepath.Join(dst, base)); err != nil {
			t.Fatalf("restored coordinator file %s: %v", base, err)
		}
	}

	restored, err := Open(dst, CollectionConfig{})
	if err != nil {
		t.Fatalf("Open restored: %v", err)
	}
	defer func() { _ = restored.Close() }()
	if status, txid := restored.XTxStatus("snapshot-xtx"); status != XTxCommitted || txid != result.TxID {
		t.Fatalf("restored coordinator status = %s, %s; want committed, %s", status, txid, result.TxID)
	}
	for _, want := range []struct {
		collection string
		id         uint64
	}{{"orders", orderID}, {"stock", stockID}} {
		col, err := restored.Collection(want.collection)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := col.Get(want.id); err != nil {
			t.Fatalf("restored %s/%d: %v", want.collection, want.id, err)
		}
	}
}

func TestSnapshot_XTxBarrierWaitsForInFlightCommit(t *testing.T) {
	db, err := Open(t.TempDir(), CollectionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	// Take the shared side as an in-flight CommitXTx does. SnapshotTo must not
	// start its archive until that complete transaction window has ended.
	db.xtxSnapshotMu.RLock()
	done := make(chan error, 1)
	go func() {
		var archive bytes.Buffer
		done <- db.SnapshotTo(&archive)
	}()
	select {
	case err := <-done:
		t.Fatalf("SnapshotTo passed XTx barrier early: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	db.xtxSnapshotMu.RUnlock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("SnapshotTo after barrier: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("SnapshotTo did not resume after XTx barrier")
	}
}

func TestSnapshot_XTxRestoreRecoveryPhases(t *testing.T) {
	for _, tc := range []struct {
		name        string
		decision    string
		checkpoint  bool
		incomplete  bool
		wantStatus  XTxStatus
		wantVisible bool
	}{
		{name: "prepared_presumed_abort", wantStatus: XTxAborted},
		{name: "prepared_incomplete_presumed_abort", incomplete: true, wantStatus: XTxAborted},
		{name: "aborted", decision: xtxKindAbort, wantStatus: XTxAborted},
		{name: "committed_before_materialization", decision: xtxKindCommit, wantStatus: XTxCommitted, wantVisible: true},
		{name: "post_checkpoint", decision: xtxKindCommit, checkpoint: true, wantStatus: XTxCommitted, wantVisible: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := t.TempDir()
			db, err := Open(src, CollectionConfig{})
			if err != nil {
				t.Fatal(err)
			}
			orders, err := db.CreateCollection("orders")
			if err != nil {
				t.Fatal(err)
			}
			stock, err := db.CreateCollection("stock")
			if err != nil {
				t.Fatal(err)
			}
			j, err := db.ensureXTxJournal()
			if err != nil {
				t.Fatal(err)
			}
			txid, err := j.allocateTxID("snapshot-phase-" + tc.name)
			if err != nil {
				t.Fatal(err)
			}
			orderID, stockID := orders.ReserveID(), stock.ReserveID()
			parts := []xtxPart{
				{C: "orders", N: 1, D: partDigest([]xtxOpRef{{ID: orderID, Op: string(store.OpInsert), Rev: 1}})},
				{C: "stock", N: 1, D: partDigest([]xtxOpRef{{ID: stockID, Op: string(store.OpInsert), Rev: 1}})},
			}
			runs := []struct {
				col *Collection
				id  uint64
			}{
				{orders, orderID}, {stock, stockID},
			}
			if tc.incomplete {
				runs = runs[:1]
			}
			for _, run := range runs {
				line := stampedEntry{ID: run.id, Op: store.OpInsert, Rev: 1, Data: map[string]any{"phase": tc.name}, Tx: TxStamp{T: txid, I: 0, N: 1}}
				if _, err := run.col.active.AppendStamped(line); err != nil {
					t.Fatal(err)
				}
				if err := run.col.active.Sync(); err != nil {
					t.Fatal(err)
				}
			}
			switch tc.decision {
			case xtxKindCommit:
				if err := j.commitPrepared(txid, "snapshot-phase-"+tc.name, parts); err != nil {
					t.Fatal(err)
				}
				if tc.checkpoint {
					if err := j.retire(txid); err != nil {
						t.Fatal(err)
					}
					if _, err := j.checkpoint(func(d xtxDecision) bool { return d.Tx == txid }); err != nil {
						t.Fatal(err)
					}
				}
			case xtxKindAbort:
				if err := j.abort(txid, "snapshot-phase-"+tc.name, "client"); err != nil {
					t.Fatal(err)
				}
			}

			var archive bytes.Buffer
			if err := db.SnapshotTo(&archive); err != nil {
				t.Fatalf("SnapshotTo: %v", err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			dst := t.TempDir()
			extractTarGz(t, archive.Bytes(), dst)
			restored, err := Open(dst, CollectionConfig{})
			if err != nil {
				t.Fatalf("Open restored: %v", err)
			}
			defer func() { _ = restored.Close() }()
			if status, _ := restored.XTxStatus(txid); status != tc.wantStatus {
				t.Fatalf("restored status = %s, want %s", status, tc.wantStatus)
			}
			for _, want := range []struct {
				collection string
				id         uint64
			}{{"orders", orderID}, {"stock", stockID}} {
				col, err := restored.Collection(want.collection)
				if err != nil {
					t.Fatal(err)
				}
				_, err = col.Get(want.id)
				if tc.wantVisible && err != nil {
					t.Fatalf("%s/%d should be visible: %v", want.collection, want.id, err)
				}
				if !tc.wantVisible && err == nil {
					t.Fatalf("%s/%d should be absent", want.collection, want.id)
				}
			}
		})
	}
}
