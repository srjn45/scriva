package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/srjn45/scriva/engine"
)

// makeDB builds a closed data directory with one collection of 5 records.
func makeDB(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "data")
	db, err := engine.Open(dir, engine.CollectionConfig{})
	if err != nil {
		t.Fatal(err)
	}
	col, err := db.CreateCollection("things")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if _, _, err := col.Insert(map[string]any{"n": i}); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return dir
}

func run(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	var out, errb bytes.Buffer
	code := execute(args, &out, &errb)
	return code, out.String(), errb.String()
}

func snapshotTree(t *testing.T, dir string) map[string]string {
	t.Helper()
	m := map[string]string{}
	_ = filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			b, _ := os.ReadFile(p)
			m[p] = string(b)
		}
		return nil
	})
	return m
}

func equalTree(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func firstSegment(t *testing.T, dir string) string {
	t.Helper()
	m, _ := filepath.Glob(filepath.Join(dir, "things", "seg_*.ndjson"))
	if len(m) == 0 {
		t.Fatal("no segment")
	}
	return m[0]
}

// damageIndex makes derived state stale: repairable, no data loss.
func damageIndex(t *testing.T, dir string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "things", "index.json"), []byte("{garbage"), 0o644); err != nil {
		t.Fatal(err)
	}
}

// damageSegment corrupts bytes in the middle of the first record.
func damageSegment(t *testing.T, dir string) {
	t.Helper()
	p := firstSegment(t, dir)
	b, _ := os.ReadFile(p)
	lines := bytes.SplitN(b, []byte("\n"), 3)
	for i := 2; i < len(lines[0])-2; i++ {
		lines[0][i] = '#'
	}
	if err := os.WriteFile(p, bytes.Join(lines, []byte("\n")), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestVerifyCleanIsReadOnly(t *testing.T) {
	dir := makeDB(t)
	before := snapshotTree(t, dir)
	for _, mode := range []string{"quick", "full"} {
		code, out, _ := run(t, "verify", "--data", dir, "--mode", mode)
		if code != 0 || !strings.Contains(out, "result: clean") {
			t.Fatalf("mode %s: code=%d out=%s", mode, code, out)
		}
	}
	code, out, _ := run(t, "verify", "--data", dir, "--json", "--collection", "things")
	if code != 0 {
		t.Fatalf("json code=%d", code)
	}
	var rep struct {
		Status      string `json:"status"`
		ExitCode    int    `json:"exit_code"`
		Collections []struct {
			Name string `json:"name"`
		} `json:"collections"`
	}
	if err := json.Unmarshal([]byte(out), &rep); err != nil {
		t.Fatalf("bad json: %v\n%s", err, out)
	}
	if rep.Status != "clean" || len(rep.Collections) != 1 || rep.Collections[0].Name != "things" {
		t.Fatalf("report: %+v", rep)
	}
	if !equalTree(before, snapshotTree(t, dir)) {
		t.Fatal("verify modified the directory")
	}
}

func TestVerifyDamagedExitCodes(t *testing.T) {
	dir := makeDB(t)
	damageIndex(t, dir)
	before := snapshotTree(t, dir)
	code, out, _ := run(t, "verify", "--data", dir)
	if code != 1 || !strings.Contains(out, "repairable") || !strings.Contains(out, "next: scriva repair") {
		t.Fatalf("code=%d out=%s", code, out)
	}
	if !equalTree(before, snapshotTree(t, dir)) {
		t.Fatal("verify modified the directory")
	}

	dir2 := makeDB(t)
	damageSegment(t, dir2)
	code, out, _ = run(t, "verify", "--data", dir2)
	if code != 2 || !strings.Contains(out, "result: corrupt") {
		t.Fatalf("code=%d out=%s", code, out)
	}
	code, out, _ = run(t, "verify", "--data", dir2, "--json")
	var rep struct {
		ExitCode int `json:"exit_code"`
	}
	if code != 2 || json.Unmarshal([]byte(out), &rep) != nil || rep.ExitCode != 2 {
		t.Fatalf("json code=%d out=%s", code, out)
	}
}

func TestUsageErrorsExit3(t *testing.T) {
	dir := makeDB(t)
	cases := [][]string{
		{"verify"},
		{"verify", "--data", dir, "--mode", "bogus"},
		{"verify", "--data", filepath.Join(dir, "missing")},
		{"verify", "--data", dir, "--nope"},
		{"repair"},
		{"repair", "--data", dir, "--on-conflict", "bogus"},
		{"repair", "--data", filepath.Join(dir, "missing")},
		{"repair", "--data", dir, "--nope"},
	}
	for _, args := range cases {
		if code, _, _ := run(t, args...); code != 3 {
			t.Errorf("%v: code=%d, want 3", args, code)
		}
	}
}

func TestRepairDryRunPrintsPlanWithoutChanges(t *testing.T) {
	dir := makeDB(t)
	damageIndex(t, dir)
	before := snapshotTree(t, dir)
	code, out, _ := run(t, "repair", "--data", dir, "--dry-run")
	if code != 1 || !strings.Contains(out, "rebuild derived structures") || !strings.Contains(out, "nothing will be modified") {
		t.Fatalf("code=%d out=%s", code, out)
	}
	if !equalTree(before, snapshotTree(t, dir)) {
		t.Fatal("dry-run modified the directory")
	}
	code, out, _ = run(t, "repair", "--data", dir, "--dry-run", "--json")
	var rep struct {
		DryRun bool `json:"dry_run"`
		Plan   []struct{ Collection, Plan string }
	}
	if code != 1 || json.Unmarshal([]byte(out), &rep) != nil || !rep.DryRun || len(rep.Plan) != 1 {
		t.Fatalf("json code=%d out=%s", code, out)
	}
}

func TestRepairFixesRepairableWithBackup(t *testing.T) {
	dir := makeDB(t)
	damageIndex(t, dir)
	bk := t.TempDir()
	code, out, _ := run(t, "repair", "--data", dir, "--backup-dir", bk)
	if code != 0 || !strings.Contains(out, "backup: ") || !strings.Contains(out, "next: scriva verify") {
		t.Fatalf("code=%d out=%s", code, out)
	}
	if m, _ := filepath.Glob(filepath.Join(bk, "repair-backup-*")); len(m) != 1 {
		t.Fatalf("backup not created in %s", bk)
	}
	if code, out, _ := run(t, "verify", "--data", dir); code != 0 {
		t.Fatalf("still damaged after repair: %d %s", code, out)
	}
	// Idempotent on a repaired directory.
	if code, _, _ := run(t, "repair", "--data", dir, "--backup-dir", bk); code != 0 {
		t.Fatalf("second repair code=%d", code)
	}
}

func TestRepairDamagedNeedsSalvage(t *testing.T) {
	dir := makeDB(t)
	damageSegment(t, dir)
	bk := t.TempDir()
	before := snapshotTree(t, dir)

	code, out, _ := run(t, "repair", "--data", dir, "--dry-run")
	if code != 2 || !strings.Contains(out, "--salvage") {
		t.Fatalf("dry-run code=%d out=%s", code, out)
	}
	code, out, _ = run(t, "repair", "--data", dir, "--backup-dir", bk)
	if code != 2 || !strings.Contains(out, "blocked") {
		t.Fatalf("no-salvage code=%d out=%s", code, out)
	}
	if !equalTree(before, snapshotTree(t, dir)) {
		t.Fatal("blocked repair modified the directory")
	}

	code, out, _ = run(t, "repair", "--data", dir, "--salvage", "--backup-dir", bk)
	if code != 0 || !strings.Contains(out, "salvage:") || !strings.Contains(out, "backup: ") {
		t.Fatalf("salvage code=%d out=%s", code, out)
	}
	if code, out, _ := run(t, "verify", "--data", dir); code != 0 {
		t.Fatalf("verify after salvage: %d %s", code, out)
	}
}

func TestRepairLockedRefused(t *testing.T) {
	dir := makeDB(t)
	damageIndex(t, dir)
	db, err := engine.Open(dir, engine.CollectionConfig{})
	if err != nil {
		t.Skipf("cannot hold directory open: %v", err)
	}
	defer db.Close()
	bk := t.TempDir()
	code, _, errOut := run(t, "repair", "--data", dir, "--backup-dir", bk)
	if code != 3 || !strings.Contains(errOut, "stop the server") {
		t.Fatalf("code=%d stderr=%s", code, errOut)
	}
	if m, _ := filepath.Glob(filepath.Join(bk, "*")); len(m) != 0 {
		t.Fatalf("backup taken despite lock: %v", m)
	}
}
