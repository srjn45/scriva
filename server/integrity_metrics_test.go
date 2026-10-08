package server_test

import (
	"errors"
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/srjn45/scriva/engine"
	"github.com/srjn45/scriva/internal/metrics"
	"github.com/srjn45/scriva/server"
	"github.com/srjn45/scriva/store"
)

func scrape(t *testing.T, reg *prometheus.Registry) string {
	t.Helper()
	rec := httptest.NewRecorder()
	metrics.Handler(reg).ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	b, _ := io.ReadAll(rec.Body)
	return string(b)
}

// engineCfgWithMetrics wires the same hooks cmd/scriva/main.go does.
func engineCfgWithMetrics(m *metrics.Metrics) engine.CollectionConfig {
	cfg := server.DefaultConfig().EngineConfig()
	cfg.CompactInterval = time.Hour
	cfg.OnIndexRecovery = m.ObserveRecovery
	cfg.OnIntegrity = server.IntegrityMetricsHook(m)
	cfg.OnAppend = server.AppendMetricsHook(m)
	cfg.OnSegmentPoisoned = func(c, _ string, _ error) { m.ObserveSegmentPoisoned(c) }
	cfg.OnLock = func(_, r string) { m.ObserveDirLock(r) }
	return cfg
}

// crashedCopy returns a data dir that looks like a SIGKILLed server: the index
// was persisted mid-run and more acknowledged records were appended afterwards.
//
// Do not copy a live directory here. Index persistence writes index.json.tmp and
// atomically renames it, so a directory walk can race a disappearing temporary
// file. Instead, close after the first persisted point, then append valid tail
// entries directly. This is the precise durable on-disk state a crash leaves:
// an older index plus a newer segment tail.
func crashedCopy(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	cfg := server.DefaultConfig().EngineConfig()
	cfg.CompactInterval = time.Hour
	db, err := engine.Open(dir, cfg)
	if err != nil {
		t.Fatal(err)
	}
	col, err := db.CreateCollection("c")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, _, err := col.Insert(map[string]any{"i": i}); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	seg := filepath.Join(dir, "c", "seg_000001.ndjson")
	f, err := os.OpenFile(seg, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := 3; i < 6; i++ {
		b, err := store.Encode(store.NewInsert(uint64(i+1), map[string]any{"i": i}))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write(b); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestRecoveryMetricsAfterCrash(t *testing.T) {
	dir := crashedCopy(t)
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	db, err := engine.Open(dir, engineCfgWithMetrics(m))
	if err != nil {
		t.Fatal(err)
	}
	col, _ := db.Collection("c")
	if _, _, err := col.Insert(map[string]any{"after": true}); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	out := scrape(t, reg)
	for _, want := range []string{
		`scriva_recovery_total{collection="c",kind="replay"} 1`,
		`scriva_dir_lock_total{result="acquired"} 1`,
		`scriva_append_total{collection="c"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in scrape:\n%s", want, grep(out, "scriva_"))
		}
	}
	if !strings.Contains(out, `scriva_recovery_bytes_total{collection="c",kind="replay"}`) ||
		!strings.Contains(out, `scriva_recovery_duration_seconds_count{collection="c",kind="replay"} 1`) {
		t.Errorf("replay bytes/duration missing:\n%s", grep(out, "scriva_recovery"))
	}
}

func TestIntegrityMetricsFailClosedOpen(t *testing.T) {
	dir := crashedCopy(t)
	_ = os.Remove(filepath.Join(dir, "c", "index.json"))
	p := filepath.Join(dir, "c", "seg_000001.ndjson")
	b, _ := os.ReadFile(p)
	b[20] ^= 1
	_ = os.WriteFile(p, b, 0o644)

	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	_, err := engine.Open(dir, engineCfgWithMetrics(m))
	if !errors.Is(err, engine.ErrIntegrity) {
		t.Fatalf("Open err = %v, want ErrIntegrity", err)
	}
	out := scrape(t, reg)
	for _, want := range []string{
		`scriva_integrity_open_total{collection="c",outcome="failed",policy="fail"} 1`,
		`scriva_integrity_findings_total{code="segment-bad-region",collection="c",severity="data-corruption"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q:\n%s", want, grep(out, "scriva_integrity"))
		}
	}
}

func grep(s, sub string) string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if strings.Contains(l, sub) && !strings.HasPrefix(l, "#") {
			out = append(out, l)
		}
	}
	return strings.Join(out, "\n")
}
