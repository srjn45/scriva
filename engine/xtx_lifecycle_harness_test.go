package engine

// Shared harness of the directory-lifecycle tests (xtx_lifecycle_test.go,
// xtx_damage_test.go). It builds a data directory that holds cross-collection
// transactions in every durable state, keeps a model of what each transaction
// must look like, and checks the one property all of those tests care about:
// a transaction is visible in every participant or in none.
//
// Everything is real disk I/O under t.TempDir(). Nothing here sleeps to create
// a state: in-flight states are captured as crash images, i.e. a byte copy of
// the live directory taken from inside the commit path's own test hook.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/srjn45/scriva/store"
)

// lcCols are the collections every lifecycle directory has.
var lcCols = []string{"accounts", "audit", "ledger", "orders"}

// lcOutcome is what the model requires of a transaction after a successful open.
type lcOutcome int

const (
	// lcCommitted: the commit was acknowledged (or its COMMIT record is
	// durable). Every participant must show it.
	lcCommitted lcOutcome = iota
	// lcAborted: no COMMIT record exists. No participant may show it.
	lcAborted
)

func (o lcOutcome) String() string {
	return [...]string{"committed", "aborted"}[o]
}

// lcPart is one participant's single operation in a model transaction. Each
// operation kind leaves a different, independently observable trace, so a
// partially applied transaction cannot hide behind one code path.
type lcPart struct {
	col string
	op  store.Op
	id  uint64
}

type lcTx struct {
	key   string
	txid  string
	parts []lcPart
	want  lcOutcome
}

// lcModel is the expected content of one data directory.
type lcModel struct {
	plain map[string]map[uint64]float64 // collection → id → "n" of a non-transactional doc
	txs   []*lcTx
}

func (m *lcModel) clone() *lcModel {
	c := &lcModel{plain: map[string]map[uint64]float64{}}
	for col, docs := range m.plain {
		c.plain[col] = map[uint64]float64{}
		for id, n := range docs {
			c.plain[col][id] = n
		}
	}
	for _, tx := range m.txs {
		cp := *tx
		cp.parts = append([]lcPart(nil), tx.parts...)
		c.txs = append(c.txs, &cp)
	}
	return c
}

func (m *lcModel) tx(key string) *lcTx {
	for _, tx := range m.txs {
		if tx.key == key {
			return tx
		}
	}
	return nil
}

// setWant overrides the required outcome of the named transactions.
func (m *lcModel) setWant(o lcOutcome, keys ...string) *lcModel {
	for _, k := range keys {
		if tx := m.tx(k); tx != nil {
			tx.want = o
		}
	}
	return m
}

func lcCfg() CollectionConfig {
	// Background compaction is disabled (no timer tick, and a dirty threshold
	// no segment can reach when rotation signals the compactor) so the byte
	// layout a damage shape targets is the one the builder produced. Forced
	// compaction is exercised explicitly by lcUseAgain.
	return CollectionConfig{CompactInterval: time.Hour, CompactDirtyPct: 2}
}

// lcBuilder drives the writes that populate a lifecycle directory.
type lcBuilder struct {
	t  *testing.T
	db *DB
	m  *lcModel
	n  float64
}

func (b *lcBuilder) col(name string) *Collection {
	b.t.Helper()
	c, err := b.db.Collection(name)
	if err != nil {
		b.t.Fatalf("collection %q: %v", name, err)
	}
	return c
}

// plain inserts one non-transactional document.
func (b *lcBuilder) plain(col string) uint64 {
	b.t.Helper()
	b.n++
	id, _, err := b.col(col).Insert(map[string]any{"kind": "plain", "n": b.n})
	if err != nil {
		b.t.Fatalf("plain insert %s: %v", col, err)
	}
	if b.m.plain[col] == nil {
		b.m.plain[col] = map[uint64]float64{}
	}
	b.m.plain[col][id] = b.n
	return id
}

// stage prepares a transaction over cols: the first participant inserts, the
// second updates a document seeded for it, the third deletes one. The seeds are
// ordinary writes made before the transaction and are not part of plain.
func (b *lcBuilder) stage(key string, cols ...string) (*lcTx, []XTxOp) {
	b.t.Helper()
	tx := &lcTx{key: key}
	var ops []XTxOp
	kinds := []store.Op{store.OpInsert, store.OpUpdate, store.OpDelete}
	for i, name := range cols {
		c := b.col(name)
		kind := kinds[i%len(kinds)]
		switch kind {
		case store.OpInsert:
			id := c.ReserveID()
			ops = append(ops, XTxOp{Collection: name, Op: kind, ID: id, Data: map[string]any{"kind": "tx", "tx": key}})
			tx.parts = append(tx.parts, lcPart{name, kind, id})
		default:
			id, _, err := c.Insert(map[string]any{"kind": "slot", "slot": key, "state": "before"})
			if err != nil {
				b.t.Fatalf("seed %s: %v", name, err)
			}
			op := XTxOp{Collection: name, Op: kind, ID: id}
			if kind == store.OpUpdate {
				op.Data = map[string]any{"kind": "slot", "slot": key, "state": "after"}
			}
			ops = append(ops, op)
			tx.parts = append(tx.parts, lcPart{name, kind, id})
		}
	}
	b.m.txs = append(b.m.txs, tx)
	return tx, ops
}

// commit stages and commits a transaction through the public one-shot path.
func (b *lcBuilder) commit(key string, cols ...string) *lcTx {
	b.t.Helper()
	tx, ops := b.stage(key, cols...)
	res, err := b.db.CommitXTx(key, ops)
	if err != nil {
		b.t.Fatalf("CommitXTx %s: %v", key, err)
	}
	tx.txid, tx.want = res.TxID, lcCommitted
	return tx
}

// lcImages is what lcPopulate returns besides the source model.
type lcImages struct {
	// crash is a byte copy of the live directory taken while "t-prepared" had
	// its runs fsynced but no COMMIT, and "t-decided" had a durable COMMIT that
	// was not yet applied to any index.
	crash      string
	crashModel *lcModel
}

// lcPopulate opens dir (fresh or already holding collections), writes plain
// documents and transactions in every durable state, and closes it cleanly.
// The returned model describes dir; images.crash/crashModel describe the copy
// taken mid-commit. scratch must be a directory outside dir.
func lcPopulate(t *testing.T, dir, scratch string) (*lcModel, lcImages) {
	t.Helper()
	cfg := lcCfg()

	var (
		imgDir   = filepath.Join(scratch, "crash-image")
		prepared = make(chan struct{})
		release  = make(chan struct{})
		copyOnce sync.Once
	)
	SetTestXTxHook(&cfg, func(p XTxHookPoint, key, _ string) {
		switch {
		case p == XTxHookPrepared && key == "t-prepared":
			close(prepared)
			<-release
		case p == XTxHookDecided && key == "t-decided":
			copyOnce.Do(func() { lcCopyTree(t, dir, imgDir) })
		}
	})

	db, err := Open(dir, cfg)
	if err != nil {
		t.Fatalf("open %s: %v", dir, err)
	}
	b := &lcBuilder{t: t, db: db, m: &lcModel{plain: map[string]map[uint64]float64{}}}
	for _, name := range lcCols {
		if _, err := db.Collection(name); err != nil {
			if _, err := db.CreateCollection(name); err != nil {
				t.Fatalf("create %s: %v", name, err)
			}
		}
	}
	for _, name := range lcCols {
		b.plain(name)
		b.plain(name)
	}

	b.commit("c1", "accounts", "audit", "ledger")
	b.commit("c2", "audit", "ledger", "orders")
	// Seal the segments holding c1/c2 so their runs sit mid-history in sealed
	// files, then keep writing.
	for _, name := range []string{"accounts", "audit", "ledger"} {
		if err := b.col(name).rotateSegment(); err != nil {
			t.Fatalf("rotate %s: %v", name, err)
		}
		b.plain(name)
	}
	b.commit("c3", "orders", "accounts")
	b.plain("accounts") // c3's run in accounts is no longer the tail
	b.plain("orders")

	// A transaction rejected by validation leaves nothing on disk.
	rej, ops := b.stage("c-rejected", "accounts", "ledger")
	ops[1].ExpectedRev = 99
	if _, err := db.CommitXTx(rej.key, ops); !errors.Is(err, ErrXTxConflict) {
		t.Fatalf("rejected tx: want ErrXTxConflict, got %v", err)
	}
	rej.want = lcAborted

	b.commit("c4", "ledger", "orders")

	// Crash image. t-prepared blocks inside the commit path with its runs
	// fsynced on accounts+audit; t-decided then commits on ledger+orders and the
	// directory is copied from its "COMMIT durable, not applied" hook.
	tp, tpOps := b.stage("t-prepared", "accounts", "audit")
	td, tdOps := b.stage("t-decided", "ledger", "orders")
	// A handle with staged but uncommitted writes: it has no durable existence.
	h, err := db.BeginXTx(context.Background(), []string{"accounts", "orders"}, XTxOptions{Key: "t-staged"})
	if err != nil {
		t.Fatalf("BeginXTx: %v", err)
	}
	stagedID, err := h.Insert("accounts", map[string]any{"kind": "tx", "tx": "t-staged"})
	if err != nil {
		t.Fatalf("stage insert: %v", err)
	}
	b.m.txs = append(b.m.txs, &lcTx{key: "t-staged", want: lcAborted,
		parts: []lcPart{{"accounts", store.OpInsert, stagedID}}})

	done := make(chan error, 1)
	go func() {
		res, err := db.CommitXTx(tp.key, tpOps)
		if err == nil {
			tp.txid = res.TxID
		}
		done <- err
	}()
	<-prepared
	res, err := db.CommitXTx(td.key, tdOps)
	if err != nil {
		t.Fatalf("CommitXTx t-decided: %v", err)
	}
	td.txid, td.want = res.TxID, lcCommitted
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("CommitXTx t-prepared: %v", err)
	}
	tp.want = lcCommitted
	if err := h.Rollback(); err != nil {
		t.Fatalf("rollback staged handle: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	img := lcImages{crash: imgDir, crashModel: b.m.clone()}
	// In the image t-prepared has no decision: presumed abort is the defined
	// outcome (design §7.2 row 2), not an unknown one.
	img.crashModel.setWant(lcAborted, "t-prepared")
	return b.m, img
}

// lcApplied reports whether one participant shows the transaction's effect.
func lcApplied(t *testing.T, db *DB, tx *lcTx, p lcPart) bool {
	t.Helper()
	col, err := db.Collection(p.col)
	if err != nil {
		t.Fatalf("tx %s: collection %s: %v", tx.key, p.col, err)
	}
	rec, err := col.Get(p.id)
	switch p.op {
	case store.OpInsert:
		// An id reserved by a transaction that never committed is free again
		// after a restart, so another document may legitimately live there.
		return err == nil && rec.Data["tx"] == tx.key
	case store.OpUpdate:
		if err != nil {
			t.Fatalf("tx %s: update slot %s/%d vanished: %v", tx.key, p.col, p.id, err)
		}
		switch rec.Data["state"] {
		case "after":
			return true
		case "before":
			return false
		}
		t.Fatalf("tx %s: update slot %s/%d has state %v", tx.key, p.col, p.id, rec.Data["state"])
	case store.OpDelete:
		return err != nil
	}
	return false
}

// lcCheck asserts the model against an open database and returns the
// visibility signature ("key=1" / "key=0" per transaction) so callers can
// compare repeated opens. where names the directory condition for messages.
func lcCheck(t *testing.T, db *DB, m *lcModel, where string) string {
	t.Helper()
	var sig []string
	for _, tx := range m.txs {
		applied := 0
		var detail []string
		for _, p := range tx.parts {
			ok := lcApplied(t, db, tx, p)
			if ok {
				applied++
			}
			detail = append(detail, fmt.Sprintf("%s:%s/%d=%v", p.op, p.col, p.id, ok))
		}
		if applied != 0 && applied != len(tx.parts) {
			t.Fatalf("%s: PARTIAL VISIBILITY of tx %s (%s): %v", where, tx.key, tx.txid, detail)
		}
		visible := applied > 0
		switch tx.want {
		case lcCommitted:
			if !visible {
				t.Fatalf("%s: committed tx %s (%s) is not visible: %v", where, tx.key, tx.txid, detail)
			}
		case lcAborted:
			if visible {
				t.Fatalf("%s: aborted tx %s (%s) is visible: %v", where, tx.key, tx.txid, detail)
			}
		}
		// The coordinator's answer must agree with what the data shows.
		if tx.txid != "" {
			st, _ := db.XTxStatus(tx.txid)
			if visible && st != XTxCommitted {
				t.Fatalf("%s: tx %s is visible but its status is %s", where, tx.key, st)
			}
			if !visible && st == XTxCommitted {
				t.Fatalf("%s: tx %s is invisible but its status is %s", where, tx.key, st)
			}
		}
		sig = append(sig, fmt.Sprintf("%s=%v", tx.key, visible))
	}
	for col, docs := range m.plain {
		c, err := db.Collection(col)
		if err != nil {
			t.Fatalf("%s: collection %s: %v", where, col, err)
		}
		for id, n := range docs {
			rec, err := c.Get(id)
			if err != nil || rec.Data["n"] != n {
				t.Fatalf("%s: plain doc %s/%d: got %v, %v; want n=%v", where, col, id, rec.Data, err, n)
			}
		}
		// A scan must agree with point reads: no document is reachable one way
		// and not the other.
		res, err := c.Scan(nil)
		if err != nil {
			t.Fatalf("%s: scan %s: %v", where, col, err)
		}
		for _, r := range res {
			if _, err := c.Get(r.ID); err != nil {
				t.Fatalf("%s: %s/%d is scanned but not readable: %v", where, col, r.ID, err)
			}
		}
	}
	sort.Strings(sig)
	return strings.Join(sig, " ")
}

// lcOpenCheck opens dir, checks the model, runs an online Verify, and closes.
func lcOpenCheck(t *testing.T, dir string, cfg CollectionConfig, m *lcModel, where string) (string, error) {
	t.Helper()
	db, err := Open(dir, cfg)
	if err != nil {
		return "", err
	}
	sig := lcCheck(t, db, m, where)
	rep, err := db.Verify(context.Background(), VerifyOptions{})
	if err != nil {
		t.Fatalf("%s: online verify: %v", where, err)
	}
	for _, f := range rep.AllFindings() {
		if strings.HasPrefix(string(f.Code), "xtx-") {
			t.Fatalf("%s: online verify reports %s on a directory open accepted: %s", where, f.Code, f.Message)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatalf("%s: close: %v", where, err)
	}
	return sig, nil
}

// lcMustOpen opens dir twice (recovery must be repeatable) and requires the
// same visibility both times.
func lcMustOpen(t *testing.T, dir string, m *lcModel, where string) string {
	t.Helper()
	first, err := lcOpenCheck(t, dir, lcCfg(), m, where)
	if err != nil {
		t.Fatalf("%s: open: %v", where, err)
	}
	second, err := lcOpenCheck(t, dir, lcCfg(), m, where+" (second open)")
	if err != nil {
		t.Fatalf("%s: second open: %v", where, err)
	}
	if first != second {
		t.Fatalf("%s: visibility changed between opens:\n first  %s\n second %s", where, first, second)
	}
	lcMustVerifyClean(t, dir, where)
	return first
}

// lcMustVerifyClean requires an offline VerifyDir with no transaction finding
// and nothing above informational severity, and that it changed no byte.
func lcMustVerifyClean(t *testing.T, dir, where string) {
	t.Helper()
	before := lcTreeHash(t, dir)
	rep, err := VerifyDir(context.Background(), dir, VerifyOptions{})
	if err != nil {
		t.Fatalf("%s: VerifyDir: %v", where, err)
	}
	if !rep.Clean() {
		t.Fatalf("%s: VerifyDir not clean: %v", where, lcFindings(rep))
	}
	if after := lcTreeHash(t, dir); after != before {
		t.Fatalf("%s: VerifyDir modified the directory", where)
	}
}

func lcFindings(rep *IntegrityReport) []string {
	var out []string
	for _, f := range rep.AllFindings() {
		if f.Severity == SeverityInfo {
			continue
		}
		out = append(out, fmt.Sprintf("%s[%s %s] %s", f.Code, f.Collection, f.Location.Tx, f.Message))
	}
	return out
}

// lcCopyTree copies src to dst byte for byte, including LOCK files (an flock
// does not travel with a copy).
func lcCopyTree(t *testing.T, src, dst string) {
	t.Helper()
	err := filepath.WalkDir(src, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, p)
		if d.IsDir() {
			return os.MkdirAll(filepath.Join(dst, rel), 0o755)
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(dst, rel), b, 0o644)
	})
	if err != nil {
		t.Fatalf("copy %s -> %s: %v", src, dst, err)
	}
}

// lcTreeHash fingerprints every file under dir by relative path and content.
// LOCK is excluded: taking and releasing the directory lock is not a mutation.
func lcTreeHash(t *testing.T, dir string) string {
	t.Helper()
	h := sha256.New()
	if real, err := filepath.EvalSymlinks(dir); err == nil {
		dir = real
	}
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(dir, p)
		if d.IsDir() {
			fmt.Fprintf(h, "D %s\n", rel)
			return nil
		}
		if d.Name() == "LOCK" {
			return nil
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		fmt.Fprintf(h, "F %s %d %x\n", rel, len(b), sha256.Sum256(b))
		return nil
	})
	if err != nil {
		t.Fatalf("hash %s: %v", dir, err)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// lcSegments returns a collection's segment paths, oldest first.
func lcSegments(t *testing.T, dir, col string) []string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(dir, col, "seg_*.ndjson"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("segments of %s: %v (%d found)", col, err, len(paths))
	}
	sort.Strings(paths)
	return paths
}

// lcLine is one physical line of a segment or journal file.
type lcLine struct {
	off  int64
	text []byte // without the newline
}

func lcLines(t *testing.T, path string) []lcLine {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var out []lcLine
	var off int64
	for len(data) > 0 {
		i := bytes.IndexByte(data, '\n')
		if i < 0 {
			out = append(out, lcLine{off, data})
			break
		}
		out = append(out, lcLine{off, data[:i]})
		off += int64(i) + 1
		data = data[i+1:]
	}
	return out
}

func lcRel(dir, path string) string {
	rel, err := filepath.Rel(dir, path)
	if err != nil {
		return path
	}
	return rel
}
