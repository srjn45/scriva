package server_test

import (
	"context"
	"errors"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/srjn45/scriva/engine"
	"github.com/srjn45/scriva/internal/metrics"
	"github.com/srjn45/scriva/server"
	"github.com/srjn45/scriva/store"
)

func engineCfgWithXTxMetrics(m *metrics.Metrics) engine.CollectionConfig {
	cfg := server.DefaultConfig().EngineConfig()
	cfg.CompactInterval = time.Hour
	cfg.OnIndexRecovery = m.ObserveRecovery
	cfg.OnIntegrity = server.IntegrityMetricsHook(m)
	cfg.OnAppend = server.AppendMetricsHook(m)
	cfg.OnSegmentPoisoned = func(c, _ string, _ error) { m.ObserveSegmentPoisoned(c) }
	cfg.OnLock = func(_, r string) { m.ObserveDirLock(r) }
	cfg.OnXTx = m.ObserveXTx
	cfg.OnXTxConflict = m.ObserveXTxConflict
	cfg.OnXTxRecovery = m.ObserveXTxRecovery
	return cfg
}

func TestXTxMetrics_CommitAndRollback(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	dir := t.TempDir()

	db, err := engine.Open(dir, engineCfgWithXTxMetrics(m))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	_, _ = db.CreateCollection("c1")
	_, _ = db.CreateCollection("c2")

	// 1. Commit
	x1, err := db.BeginXTx(context.Background(), []string{"c1", "c2"}, engine.XTxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := x1.Insert("c1", map[string]any{"v": 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := x1.Insert("c2", map[string]any{"v": 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := x1.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}

	// 2. Rollback
	x2, err := db.BeginXTx(context.Background(), []string{"c1", "c2"}, engine.XTxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := x2.Insert("c1", map[string]any{"v": 3}); err != nil {
		t.Fatal(err)
	}
	if err := x2.Rollback(); err != nil {
		t.Fatal(err)
	}

	out := scrape(t, reg)
	for _, want := range []string{
		`scriva_xtx_total{outcome="commit"} 1`,
		`scriva_xtx_total{outcome="rollback"} 1`,
		`scriva_xtx_duration_seconds_count{outcome="commit"} 1`,
		`scriva_xtx_duration_seconds_count{outcome="rollback"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in scrape:\n%s", want, grep(out, "scriva_xtx"))
		}
	}
}

func TestXTxMetrics_Conflicts(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	dir := t.TempDir()

	db, err := engine.Open(dir, engineCfgWithXTxMetrics(m))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	col1, _ := db.CreateCollection("c1")
	_, _ = db.CreateCollection("c2")
	_ = col1.EnsureUniqueIndex("email")

	id1, _, _ := col1.Insert(map[string]any{"email": "a@example.com", "balance": 100})

	// Scenario A: Read conflict
	xRead, _ := db.BeginXTx(context.Background(), []string{"c1"}, engine.XTxOptions{})
	_, _ = xRead.Get("c1", id1)
	// Mutate id1 concurrently
	_, _ = col1.Update(id1, map[string]any{"email": "a@example.com", "balance": 150})
	// Commit xRead (read-only commit) -> conflict
	_, err = xRead.Commit(context.Background())
	if !errors.Is(err, engine.ErrXTxConflict) {
		t.Fatalf("want conflict, got %v", err)
	}

	// Scenario B: Write conflict
	xWrite, _ := db.BeginXTx(context.Background(), []string{"c1"}, engine.XTxOptions{})
	_ = xWrite.Update("c1", id1, map[string]any{"email": "a@example.com", "balance": 200})
	// Mutate id1 concurrently again
	_, _ = col1.Update(id1, map[string]any{"email": "a@example.com", "balance": 175})
	_, err = xWrite.Commit(context.Background())
	if !errors.Is(err, engine.ErrXTxConflict) {
		t.Fatalf("want conflict, got %v", err)
	}

	// Scenario C: Unique constraint conflict
	xConst, _ := db.BeginXTx(context.Background(), []string{"c1", "c2"}, engine.XTxOptions{})
	_, _ = xConst.Insert("c1", map[string]any{"email": "a@example.com"})
	_, err = xConst.Commit(context.Background())
	if !errors.Is(err, engine.ErrDuplicateKey) && !errors.Is(err, engine.ErrXTxConflict) {
		t.Fatalf("want duplicate key/conflict, got %v", err)
	}

	out := scrape(t, reg)
	for _, want := range []string{
		`scriva_xtx_total{outcome="conflict"} 3`,
		`scriva_xtx_conflicts_total{kind="read"} 1`,
		`scriva_xtx_conflicts_total{kind="write"} 1`,
		`scriva_xtx_conflicts_total{kind="constraint"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in scrape:\n%s", want, grep(out, "scriva_xtx"))
		}
	}
}

func TestXTxMetrics_CanceledAndExpiry(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	dir := t.TempDir()

	db, err := engine.Open(dir, engineCfgWithXTxMetrics(m))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	_, _ = db.CreateCollection("c1")

	// 1. Canceled before prepare
	xCancel, _ := db.BeginXTx(context.Background(), []string{"c1"}, engine.XTxOptions{})
	_, _ = xCancel.Insert("c1", map[string]any{"v": 10})
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = xCancel.Commit(canceledCtx)
	if !errors.Is(err, engine.ErrXTxCanceled) {
		t.Fatalf("want ErrXTxCanceled, got %v", err)
	}

	// 2. Expired handle
	xExp, _ := db.BeginXTx(context.Background(), []string{"c1"}, engine.XTxOptions{
		IdleTimeout: 10 * time.Millisecond,
	})
	time.Sleep(25 * time.Millisecond)
	_, err = xExp.Insert("c1", map[string]any{"v": 20})
	if !errors.Is(err, engine.ErrXTxExpired) {
		t.Fatalf("want ErrXTxExpired, got %v", err)
	}

	out := scrape(t, reg)
	for _, want := range []string{
		`scriva_xtx_total{outcome="canceled"} 1`,
		`scriva_xtx_total{outcome="expired"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in scrape:\n%s", want, grep(out, "scriva_xtx"))
		}
	}
}

func TestXTxMetrics_Recovery(t *testing.T) {
	dir := t.TempDir()
	db1, err := engine.Open(dir, server.DefaultConfig().EngineConfig())
	if err != nil {
		t.Fatal(err)
	}
	_, _ = db1.CreateCollection("c1")
	_, _ = db1.CreateCollection("c2")

	res, err := db1.CommitXTx("tx-rec-commit", []engine.XTxOp{
		{Collection: "c1", Op: store.OpInsert, ID: 100, Data: map[string]any{"k": "v1"}},
		{Collection: "c2", Op: store.OpInsert, ID: 200, Data: map[string]any{"k": "v2"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.TxID == "" {
		t.Fatal("expected non-empty txid")
	}

	if err := db1.Close(); err != nil {
		t.Fatal(err)
	}

	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	db2, err := engine.Open(dir, engineCfgWithXTxMetrics(m))
	if err != nil {
		t.Fatal(err)
	}
	_ = db2.Close()

	out := scrape(t, reg)
	want := `scriva_xtx_recovery_total{outcome="recovered_committed"} 1`
	if !strings.Contains(out, want) {
		t.Errorf("missing %q in scrape:\n%s", want, grep(out, "scriva_xtx"))
	}
}

func TestXTxMetrics_OutcomeUnknownMetric(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	m.ObserveXTx("unknown", 50*time.Millisecond)

	out := scrape(t, reg)
	for _, want := range []string{
		`scriva_xtx_total{outcome="unknown"} 1`,
		`scriva_xtx_duration_seconds_count{outcome="unknown"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in scrape:\n%s", want, grep(out, "scriva_xtx"))
		}
	}
}

func TestEnginePackageDoesNotImportMetrics(t *testing.T) {
	cmd := exec.Command("go", "list", "-deps", "github.com/srjn45/scriva/engine")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go list failed: %v\n%s", err, string(out))
	}
	for _, forbidden := range []string{
		"github.com/srjn45/scriva/internal/metrics",
		"github.com/prometheus/client_golang",
	} {
		if strings.Contains(string(out), forbidden) {
			t.Errorf("engine package must not import %s", forbidden)
		}
	}
}
