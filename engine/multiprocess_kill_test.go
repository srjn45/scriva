//go:build !windows

// Real-process crash tests: a re-exec'd copy of the test binary acts as a
// writer, prints every acknowledged operation to stdout and is SIGKILLed by the
// parent at seeded points. The parent then reopens the directory and asserts
// the recovered state equals exactly the acknowledged operations, plus at most
// the single in-flight operation (fully applied or fully absent, never torn).
//
// Skipped on windows (build tag): SIGKILL and flock semantics differ there.
//
// Knobs: SCRIVA_KILL_SEED=<n> reproduces one seed; SCRIVA_KILL_SOAK=1 raises
// the iteration count. The seed is always logged.
package engine

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/srjn45/scriva/store"
)

const (
	envKillChild = "SCRIVA_KILL_CHILD"
	envKillDir   = "SCRIVA_KILL_DIR"
	envKillMode  = "SCRIVA_KILL_MODE" // "writer" | "compact" | "rotate" | "hold"
	envKillSeed  = "SCRIVA_KILL_SEED"
	envKillSoak  = "SCRIVA_KILL_SOAK"
)

// killChildCfg is the collection config shared by parent and child.
func killChildCfg(mode string) CollectionConfig {
	cfg := defaultConfig()
	cfg.SyncMode = SyncModeAlways
	switch mode {
	case "compact", "xtx_compact":
		cfg.SegmentMaxSize = 2 << 10
		cfg.CompactInterval = 5 * time.Millisecond
		cfg.CompactDirtyPct = 0.01
	case "rotate":
		cfg.SegmentMaxSize = 512
		cfg.CompactInterval = 24 * time.Hour
	case "xtx_repl":
		cfg.ReplicationRingSize = 100
		cfg.CompactInterval = 24 * time.Hour
	default:
		cfg.CompactInterval = 24 * time.Hour
	}
	return cfg
}

// TestMain-free re-exec: each test that spawns a child calls maybeRunKillChild
// first, so the child never runs the parent's assertions.
func maybeRunKillChild() {
	if os.Getenv(envKillChild) != "1" {
		return
	}
	dir, mode := os.Getenv(envKillDir), os.Getenv(envKillMode)
	seed, _ := strconv.ParseInt(os.Getenv(envKillSeed), 10, 64)
	if mode == "batch" {
		runBatchKillChild(dir, seed)
	}
	db, err := Open(dir, killChildCfg(mode))
	if err != nil {
		if errors.Is(err, ErrDatabaseLocked) {
			os.Exit(42)
		}
		fmt.Fprintln(os.Stderr, "child open:", err)
		os.Exit(1)
	}
	if mode == "hold" {
		os.Stdout.WriteString("READY\n")
		time.Sleep(time.Hour)
		db.Close()
		os.Exit(0)
	}

	if strings.HasPrefix(mode, "xtx") {
		colA, err := db.Collection("kill_a")
		if err != nil {
			colA, err = db.CreateCollection("kill_a")
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "child collection kill_a:", err)
			os.Exit(1)
		}
		colB, err := db.Collection("kill_b")
		if err != nil {
			colB, err = db.CreateCollection("kill_b")
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "child collection kill_b:", err)
			os.Exit(1)
		}
		_ = colA.EnsureIndex("v")
		_ = colB.EnsureIndex("v")
		os.Stdout.WriteString("READY\n")

		rng := rand.New(rand.NewSource(seed))
		var liveA, liveB []uint64
		for n := 1; ; n++ {
			if rng.Intn(10) < 4 {
				// Single write on colA or colB
				useA := rng.Intn(2) == 0
				targetCol := colA
				colName := "kill_a"
				live := &liveA
				if !useA {
					targetCol = colB
					colName = "kill_b"
					live = &liveB
				}
				switch r := rng.Intn(10); {
				case r < 5 || len(*live) == 0:
					fmt.Printf("S single %s insert %d\n", colName, n)
					id, _, err := targetCol.Insert(map[string]any{"v": n})
					if err != nil {
						fmt.Fprintln(os.Stderr, "insert:", err)
						os.Exit(1)
					}
					*live = append(*live, id)
					fmt.Printf("A single %s insert %d %d\n", colName, id, n)
				case r < 8:
					id := (*live)[rng.Intn(len(*live))]
					fmt.Printf("S single %s update %d %d\n", colName, id, n)
					if _, err := targetCol.Update(id, map[string]any{"v": n}); err != nil {
						fmt.Fprintln(os.Stderr, "update:", err)
						os.Exit(1)
					}
					fmt.Printf("A single %s update %d %d\n", colName, id, n)
				default:
					i := rng.Intn(len(*live))
					id := (*live)[i]
					*live = append((*live)[:i], (*live)[i+1:]...)
					fmt.Printf("S single %s delete %d\n", colName, id)
					if err := targetCol.Delete(id); err != nil {
						fmt.Fprintln(os.Stderr, "delete:", err)
						os.Exit(1)
					}
					fmt.Printf("A single %s delete %d\n", colName, id)
				}
			} else {
				// Cross-collection transaction
				idA := colA.ReserveID()
				idB := colB.ReserveID()
				var ops []XTxOp
				var logSpecs []string
				if len(liveA) > 0 && rng.Intn(3) == 0 {
					pickA := liveA[rng.Intn(len(liveA))]
					ops = append(ops, XTxOp{Collection: "kill_a", Op: store.OpUpdate, ID: pickA, Data: map[string]any{"v": n}})
					logSpecs = append(logSpecs, fmt.Sprintf("kill_a:update:%d:%d", pickA, n))
				} else {
					ops = append(ops, XTxOp{Collection: "kill_a", Op: store.OpInsert, ID: idA, Data: map[string]any{"v": n}})
					logSpecs = append(logSpecs, fmt.Sprintf("kill_a:insert:%d:%d", idA, n))
				}
				if len(liveB) > 0 && rng.Intn(3) == 0 {
					pickB := liveB[rng.Intn(len(liveB))]
					ops = append(ops, XTxOp{Collection: "kill_b", Op: store.OpUpdate, ID: pickB, Data: map[string]any{"v": n}})
					logSpecs = append(logSpecs, fmt.Sprintf("kill_b:update:%d:%d", pickB, n))
				} else {
					ops = append(ops, XTxOp{Collection: "kill_b", Op: store.OpInsert, ID: idB, Data: map[string]any{"v": n}})
					logSpecs = append(logSpecs, fmt.Sprintf("kill_b:insert:%d:%d", idB, n))
				}

				key := fmt.Sprintf("k-%d-%d", seed, n)
				fmt.Printf("S xtx %s %s\n", key, strings.Join(logSpecs, " "))
				res, err := db.CommitXTx(key, ops)
				if err != nil {
					fmt.Fprintln(os.Stderr, "commitXTx:", err)
					os.Exit(1)
				}
				for _, spec := range logSpecs {
					parts := strings.Split(spec, ":")
					col, kind := parts[0], parts[1]
					docID, _ := strconv.ParseUint(parts[2], 10, 64)
					if kind == "insert" {
						if col == "kill_a" {
							liveA = append(liveA, docID)
						} else {
							liveB = append(liveB, docID)
						}
					}
				}
				fmt.Printf("A xtx %s %s %s\n", key, res.TxID, strings.Join(logSpecs, " "))
			}
		}
	}

	col, err := db.Collection("kill")
	if err != nil {
		col, err = db.CreateCollection("kill")
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "child collection:", err)
		os.Exit(1)
	}
	os.Stdout.WriteString("READY\n")

	rng := rand.New(rand.NewSource(seed))
	var live []uint64
	for n := 1; ; n++ {
		// S line marks the op in flight; A line is printed only after the
		// engine acknowledged it.
		switch r := rng.Intn(10); {
		case r < 5 || len(live) == 0:
			fmt.Printf("S insert %d\n", n)
			id, _, err := col.Insert(map[string]any{"v": n})
			if err != nil {
				fmt.Fprintln(os.Stderr, "insert:", err)
				os.Exit(1)
			}
			live = append(live, id)
			fmt.Printf("A insert %d %d\n", id, n)
		case r < 8:
			id := live[rng.Intn(len(live))]
			fmt.Printf("S update %d %d\n", id, n)
			if _, err := col.Update(id, map[string]any{"v": n}); err != nil {
				fmt.Fprintln(os.Stderr, "update:", err)
				os.Exit(1)
			}
			fmt.Printf("A update %d %d\n", id, n)
		default:
			i := rng.Intn(len(live))
			id := live[i]
			live = append(live[:i], live[i+1:]...)
			fmt.Printf("S delete %d\n", id)
			if err := col.Delete(id); err != nil {
				fmt.Fprintln(os.Stderr, "delete:", err)
				os.Exit(1)
			}
			fmt.Printf("A delete %d\n", id)
		}
	}
}

// killModel is the acknowledged state plus the in-flight op, as observed by
// the parent from the child's stdout.
type killModel struct {
	vals     map[uint64]int            // id -> v of live records (single-collection mode)
	colVals  map[string]map[uint64]int // col -> id -> v (multi-collection mode)
	inflight []string                  // fields of the last S line without a matching A
}

func (m *killModel) feed(line string) {
	f := strings.Fields(line)
	if len(f) == 0 {
		return
	}
	if m.colVals == nil {
		m.colVals = make(map[string]map[uint64]int)
	}
	switch f[0] {
	case "S":
		m.inflight = f[1:]
	case "A":
		m.inflight = nil
		if len(f) > 1 && f[1] == "single" {
			col := f[2]
			if m.colVals[col] == nil {
				m.colVals[col] = make(map[uint64]int)
			}
			id, _ := strconv.ParseUint(f[4], 10, 64)
			switch f[3] {
			case "insert", "update":
				v, _ := strconv.Atoi(f[5])
				m.colVals[col][id] = v
			case "delete":
				delete(m.colVals[col], id)
			}
		} else if len(f) > 1 && f[1] == "xtx" {
			for _, spec := range f[4:] {
				parts := strings.Split(spec, ":")
				if len(parts) != 4 {
					continue
				}
				col, kind := parts[0], parts[1]
				id, _ := strconv.ParseUint(parts[2], 10, 64)
				v, _ := strconv.Atoi(parts[3])
				if m.colVals[col] == nil {
					m.colVals[col] = make(map[uint64]int)
				}
				if kind == "delete" {
					delete(m.colVals[col], id)
				} else {
					m.colVals[col][id] = v
				}
			}
		} else if len(f) >= 3 {
			id, _ := strconv.ParseUint(f[2], 10, 64)
			switch f[1] {
			case "insert", "update":
				if len(f) > 3 {
					v, _ := strconv.Atoi(f[3])
					m.vals[id] = v
				}
			case "delete":
				delete(m.vals, id)
			}
		}
	}
}

// startKillChild launches the writer and returns the process and its stdout
// line channel.
func startKillChild(t *testing.T, dir, mode string, seed int64) (*exec.Cmd, <-chan string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$")
	cmd.Env = append(os.Environ(),
		envKillChild+"=1", envKillDir+"="+dir, envKillMode+"="+mode,
		envKillSeed+"="+strconv.FormatInt(seed, 10))
	cmd.Stderr = os.Stderr
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	lines := make(chan string, 1024)
	go func() {
		defer close(lines)
		sc := bufio.NewScanner(out)
		for sc.Scan() {
			lines <- sc.Text()
		}
		_ = sc.Err()
	}()
	return cmd, lines
}

func sigkill(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatalf("SIGKILL: %v", err)
	}
	_ = cmd.Wait()
}

// runKillIteration runs one writer, kills it after a seeded number of acked
// ops (plus seeded jitter), and returns the model including any ops the child
// flushed before dying.
func runKillIteration(t *testing.T, dir, mode string, seed int64, model *killModel) {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	target := 1 + rng.Intn(60)
	jitter := time.Duration(rng.Intn(2000)) * time.Microsecond

	cmd, lines := startKillChild(t, dir, mode, seed)
	acked := 0
	deadline := time.After(30 * time.Second)
	for acked < target {
		select {
		case l, ok := <-lines:
			if !ok {
				t.Fatalf("seed %d: child exited early", seed)
			}
			if l == "READY" {
				continue
			}
			model.feed(l)
			if strings.HasPrefix(l, "A ") {
				acked++
			}
		case <-deadline:
			cmd.Process.Kill()
			t.Fatalf("seed %d: timed out waiting for child", seed)
		}
	}
	time.Sleep(jitter)
	if err := cmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatalf("SIGKILL: %v", err)
	}
	// Drain whatever the child managed to print before dying (the channel
	// closes at pipe EOF) before Wait closes the pipe.
	for l := range lines {
		model.feed(l)
	}
	_ = cmd.Wait()
}

// checkRecovered reopens dir and asserts it equals the acked model modulo the
// single in-flight op.
func checkRecovered(t *testing.T, dir, mode string, seed int64, m *killModel) {
	t.Helper()
	if strings.HasPrefix(mode, "xtx") {
		checkRecoveredXTx(t, dir, mode, seed, m)
		return
	}
	db, err := Open(dir, killChildCfg(mode))
	if err != nil {
		t.Fatalf("seed %d: reopen after SIGKILL: %v", seed, err)
	}
	defer db.Close()
	col, err := db.Collection("kill")
	if err != nil {
		// Killed before the first collection write was acknowledged.
		if len(m.vals) == 0 {
			return
		}
		t.Fatalf("seed %d: collection: %v", seed, err)
	}

	res, err := col.Scan(nil)
	if err != nil {
		t.Fatalf("seed %d: scan: %v", seed, err)
	}
	got := make(map[uint64]int, len(res))
	for _, r := range res {
		v, ok := r.Data["v"].(float64)
		if !ok {
			t.Fatalf("seed %d: id %d torn/odd data %v", seed, r.ID, r.Data)
		}
		got[r.ID] = int(v)
	}

	// Lookups must agree with the scan.
	for id, v := range got {
		rec, err := col.Get(id)
		if err != nil || int(rec.Data["v"].(float64)) != v {
			t.Fatalf("seed %d: Get(%d)=%v,%v disagrees with scan v=%d", seed, id, rec.Data, err, v)
		}
	}

	// Compute the allowed outcomes for the in-flight op.
	var infID uint64
	var infKind string
	var infV int
	if len(m.inflight) > 0 {
		infKind = m.inflight[0]
		switch infKind {
		case "insert":
			infV, _ = strconv.Atoi(m.inflight[1])
		case "update":
			infID, _ = strconv.ParseUint(m.inflight[1], 10, 64)
			infV, _ = strconv.Atoi(m.inflight[2])
		case "delete":
			infID, _ = strconv.ParseUint(m.inflight[1], 10, 64)
		}
	}

	extra := 0
	for id, v := range got {
		want, acked := m.vals[id]
		switch {
		case acked && want == v:
		case acked && infKind == "update" && id == infID && v == infV:
		case !acked && infKind == "insert" && v == infV && extra == 0:
			extra++
		default:
			t.Fatalf("seed %d: id %d v=%d unexpected (acked=%v want=%d inflight=%v)",
				seed, id, v, acked, want, m.inflight)
		}
	}
	for id, want := range m.vals {
		if _, ok := got[id]; ok {
			continue
		}
		if infKind == "delete" && id == infID {
			continue
		}
		t.Fatalf("seed %d: acknowledged id %d (v=%d) missing after recovery; inflight=%v",
			seed, id, want, m.inflight)
	}
}

func checkRecoveredXTx(t *testing.T, dir, mode string, seed int64, m *killModel) {
	t.Helper()
	db, err := Open(dir, killChildCfg(mode))
	if err != nil {
		t.Fatalf("seed %d: reopen after SIGKILL: %v", seed, err)
	}
	defer db.Close()

	colNames := []string{"kill_a", "kill_b"}
	cols := make(map[string]*Collection, len(colNames))
	scans := make(map[string]map[uint64]int, len(colNames))

	for _, name := range colNames {
		col, err := db.Collection(name)
		if err != nil {
			if len(m.colVals[name]) == 0 {
				continue
			}
			t.Fatalf("seed %d: collection %s: %v", seed, name, err)
		}
		cols[name] = col
		res, err := col.Scan(nil)
		if err != nil {
			t.Fatalf("seed %d: scan %s: %v", seed, name, err)
		}
		scan := make(map[uint64]int, len(res))
		for _, r := range res {
			v, ok := r.Data["v"].(float64)
			if !ok {
				t.Fatalf("seed %d: %s id %d torn/odd data %v", seed, name, r.ID, r.Data)
			}
			scan[r.ID] = int(v)
		}
		scans[name] = scan

		for id, v := range scan {
			rec, err := col.Get(id)
			if err != nil || int(rec.Data["v"].(float64)) != v {
				t.Fatalf("seed %d: %s Get(%d)=%v,%v disagrees with scan v=%d", seed, name, id, rec.Data, err, v)
			}
		}
	}

	type inFlightOp struct {
		col  string
		kind string
		id   uint64
		v    int
	}
	var infOps []inFlightOp

	if len(m.inflight) > 0 {
		switch m.inflight[0] {
		case "single":
			if len(m.inflight) >= 4 {
				col := m.inflight[1]
				kind := m.inflight[2]
				switch kind {
				case "insert":
					v, _ := strconv.Atoi(m.inflight[3])
					infOps = append(infOps, inFlightOp{col: col, kind: kind, v: v})
				case "update":
					if len(m.inflight) >= 5 {
						id, _ := strconv.ParseUint(m.inflight[3], 10, 64)
						v, _ := strconv.Atoi(m.inflight[4])
						infOps = append(infOps, inFlightOp{col: col, kind: kind, id: id, v: v})
					}
				case "delete":
					id, _ := strconv.ParseUint(m.inflight[3], 10, 64)
					infOps = append(infOps, inFlightOp{col: col, kind: kind, id: id})
				}
			}
		case "xtx":
			for _, spec := range m.inflight[2:] {
				parts := strings.Split(spec, ":")
				if len(parts) == 4 {
					col, kind := parts[0], parts[1]
					id, _ := strconv.ParseUint(parts[2], 10, 64)
					v, _ := strconv.Atoi(parts[3])
					infOps = append(infOps, inFlightOp{col: col, kind: kind, id: id, v: v})
				}
			}
		}
	}

	if len(m.inflight) > 0 && m.inflight[0] == "xtx" && len(infOps) > 0 {
		seenCount := 0
		for _, op := range infOps {
			scan := scans[op.col]
			switch op.kind {
			case "insert", "update":
				if scan != nil && scan[op.id] == op.v {
					seenCount++
				}
			case "delete":
				if scan == nil || scan[op.id] == 0 {
					seenCount++
				}
			}
		}
		if seenCount > 0 && seenCount < len(infOps) {
			t.Fatalf("seed %d: PARTIAL VISIBILITY: in-flight XTx had %d of %d ops visible",
				seed, seenCount, len(infOps))
		}
		if seenCount == len(infOps) {
			for _, op := range infOps {
				if m.colVals[op.col] == nil {
					m.colVals[op.col] = make(map[uint64]int)
				}
				if op.kind == "delete" {
					delete(m.colVals[op.col], op.id)
				} else {
					m.colVals[op.col][op.id] = op.v
				}
			}
		}
	}

	for _, name := range colNames {
		scan := scans[name]
		live := m.colVals[name]

		for id, v := range scan {
			want, acked := live[id]
			if acked && want == v {
				continue
			}
			if len(m.inflight) > 0 && m.inflight[0] == "single" && len(infOps) == 1 {
				single := infOps[0]
				if single.col == name {
					if single.kind == "insert" && single.v == v {
						continue
					}
					if single.kind == "update" && single.id == id && single.v == v {
						continue
					}
				}
			}
			t.Fatalf("seed %d: %s id %d v=%d unexpected (acked=%v want=%d inflight=%v)",
				seed, name, id, v, acked, want, m.inflight)
		}

		for id, want := range live {
			if _, ok := scan[id]; ok {
				continue
			}
			if len(m.inflight) > 0 && m.inflight[0] == "single" && len(infOps) == 1 {
				single := infOps[0]
				if single.col == name && single.kind == "delete" && single.id == id {
					continue
				}
			}
			t.Fatalf("seed %d: %s acknowledged id %d (v=%d) missing after recovery; inflight=%v",
				seed, name, id, want, m.inflight)
		}
	}

	if mode == "xtx_repl" {
		if db.CurrentLSN() == 0 && len(m.colVals["kill_a"])+len(m.colVals["kill_b"]) > 0 {
			t.Fatalf("seed %d: replication LSN is 0 after acknowledged ops", seed)
		}
	}
}

func killIterations() (n int, seeds []int64) {
	if s := os.Getenv(envKillSeed); s != "" {
		v, _ := strconv.ParseInt(s, 10, 64)
		return 1, []int64{v}
	}
	n = 6
	if os.Getenv(envKillSoak) != "" {
		n = 300
	}
	for i := 0; i < n; i++ {
		seeds = append(seeds, int64(1000+i))
	}
	return n, seeds
}

func testKillRecovery(t *testing.T, mode string) {
	maybeRunKillChild()
	_, seeds := killIterations()
	// One directory across iterations: every iteration also exercises recovery
	// from the previous crash's state, and the model carries over. The child's
	// ids continue from the recovered counter, so seeds only drive op choice.
	dir := t.TempDir()
	model := &killModel{vals: map[uint64]int{}, colVals: map[string]map[uint64]int{}}
	for _, seed := range seeds {
		t.Logf("seed %d (reproduce: %s=%d)", seed, envKillSeed, seed)
		runKillIteration(t, dir, mode, seed, model)
		checkRecovered(t, dir, mode, seed, model)
		// Fold the post-recovery truth back so the next child's view of
		// "live" ids (which it restarts empty) cannot conflict: acked state
		// is now exactly what the engine recovered.
		model = recoveredModel(t, dir, mode)
	}
}

func recoveredModel(t *testing.T, dir, mode string) *killModel {
	t.Helper()
	m := &killModel{vals: map[uint64]int{}, colVals: map[string]map[uint64]int{}}
	db, err := Open(dir, killChildCfg(mode))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if strings.HasPrefix(mode, "xtx") {
		for _, name := range []string{"kill_a", "kill_b"} {
			m.colVals[name] = make(map[uint64]int)
			col, err := db.Collection(name)
			if err != nil {
				continue
			}
			res, err := col.Scan(nil)
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range res {
				m.colVals[name][r.ID] = int(r.Data["v"].(float64))
			}
		}
		return m
	}
	col, err := db.Collection("kill")
	if err != nil {
		return m
	}
	res, err := col.Scan(nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range res {
		m.vals[r.ID] = int(r.Data["v"].(float64))
	}
	return m
}

func TestKill9_WriterRecovery(t *testing.T)               { testKillRecovery(t, "writer") }
func TestKill9_DuringRotation(t *testing.T)               { testKillRecovery(t, "rotate") }
func TestKill9_DuringCompaction(t *testing.T)             { testKillRecovery(t, "compact") }
func TestKill9_CrossCollectionTransactions(t *testing.T) { testKillRecovery(t, "xtx") }
func TestKill9_XTxWithCompaction(t *testing.T)           { testKillRecovery(t, "xtx_compact") }
func TestKill9_XTxWithReplication(t *testing.T)          { testKillRecovery(t, "xtx_repl") }

// TestKill9_LockReleasedOnKill: a second process is rejected while the holder
// lives and the lock is free once the holder is SIGKILLed.
func TestKill9_LockReleasedOnKill(t *testing.T) {
	maybeRunKillChild()
	dir := t.TempDir()

	holder, lines := startKillChild(t, dir, "hold", 1)
	select {
	case l := <-lines:
		if l != "READY" {
			t.Fatalf("holder said %q", l)
		}
	case <-time.After(15 * time.Second):
		holder.Process.Kill()
		t.Fatal("holder never became ready")
	}

	// In-process attempt is rejected by the OS lock held by the child.
	if _, err := Open(dir, killChildCfg("hold")); !errors.Is(err, ErrDatabaseLocked) {
		sigkill(t, holder)
		t.Fatalf("open while holder alive: want ErrDatabaseLocked, got %v", err)
	}
	// A second child process is rejected too (exit code 42).
	second, _ := startKillChild(t, dir, "hold", 2)
	err := second.Wait()
	var ee *exec.ExitError
	if !errors.As(err, &ee) || ee.ExitCode() != 42 {
		sigkill(t, holder)
		t.Fatalf("second process: want exit 42, got %v", err)
	}

	sigkill(t, holder)
	db, err := Open(dir, killChildCfg("hold"))
	if err != nil {
		t.Fatalf("open after holder SIGKILL: %v", err)
	}
	db.Close()
}

// TestKill9_RecoveredDirVerifies asserts that a directory recovered from a
// SIGKILL verifies clean, quick and full: online on the reopened handle, and
// offline (VerifyDir) once that handle is closed.
func TestKill9_RecoveredDirVerifies(t *testing.T) {
	maybeRunKillChild()
	_, seeds := killIterations()
	if len(seeds) > 20 {
		seeds = seeds[:20] // soak: the recovery tests above carry the volume
	}
	for _, mode := range []string{"writer", "rotate", "compact", "xtx", "xtx_compact", "xtx_repl"} {
		t.Run(mode, func(t *testing.T) {
			// The parent only verifies: keep its own compactor out of the way.
			// Recovery of a swap the child was killed in still runs at Open.
			cfg := killChildCfg(mode)
			cfg.CompactInterval = 24 * time.Hour

			dir := t.TempDir()
			for _, seed := range seeds {
				t.Logf("seed %d (reproduce: %s=%d)", seed, envKillSeed, seed)
				runKillIteration(t, dir, mode, seed, &killModel{vals: map[uint64]int{}})

				db, err := Open(dir, cfg)
				if err != nil {
					t.Fatalf("seed %d: reopen after SIGKILL: %v", seed, err)
				}
				for _, vm := range []VerifyMode{VerifyQuick, VerifyFull} {
					rep, err := db.Verify(context.Background(), VerifyOptions{Mode: vm})
					if err != nil {
						db.Close()
						t.Fatalf("seed %d: online Verify(%s): %v", seed, vm, err)
					}
					if !rep.Clean() {
						db.Close()
						t.Fatalf("seed %d: online Verify(%s) not clean: %+v", seed, vm, rep.AllFindings())
					}
				}
				if err := db.Close(); err != nil {
					t.Fatalf("seed %d: close: %v", seed, err)
				}
				for _, vm := range []VerifyMode{VerifyQuick, VerifyFull} {
					rep, err := VerifyDir(context.Background(), dir, VerifyOptions{Mode: vm})
					if err != nil {
						t.Fatalf("seed %d: VerifyDir(%s): %v", seed, vm, err)
					}
					if !rep.Clean() {
						t.Fatalf("seed %d: VerifyDir(%s) not clean: %+v", seed, vm, rep.AllFindings())
					}
				}
			}
		})
	}
}
