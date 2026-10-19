//go:build !windows

package engine

// Real-process SIGKILL gate for crash-atomic single-collection batches. A
// re-exec'd child commits InsertMany and CommitTx batches as fast as it can,
// printing "I <batch> <n>" / "B <batch> <gen>" only after the engine
// acknowledged them. The parent kills it at seeded points, reopens the same
// directory (repeatedly across iterations) and requires, per batch, a uniform
// record count (0 or n, never a prefix), a uniform generation, and every
// acknowledged batch fully present at its acknowledged generation.
//
// SCRIVA_KILL_SEED=<n> reproduces one seed; SCRIVA_KILL_SOAK=1 raises the
// iteration count.

import (
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"
)

// runBatchKillChild never returns.
func runBatchKillChild(dir string, seed int64) {
	atomicBatchWriterReady = true
	cfg := killChildCfg("batch")
	db, err := Open(dir, cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "child open:", err)
		os.Exit(1)
	}
	c, err := db.Collection("kill")
	if err != nil {
		c, err = db.CreateCollection("kill")
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "child collection:", err)
		os.Exit(1)
	}
	if _, err := db.ensureXTxJournal(); err != nil {
		fmt.Fprintln(os.Stderr, "child journal:", err)
		os.Exit(1)
	}
	_ = c.EnsureIndex("b")

	// Learn what already exists so batch numbers keep increasing.
	known := map[int][]uint64{} // batch -> ids
	gen := map[int]int{}
	next := 1
	res, _ := c.Scan(nil)
	for _, r := range res {
		b := int(r.Data["b"].(float64))
		known[b] = append(known[b], r.ID)
		gen[b] = int(r.Data["g"].(float64))
		if b >= next {
			next = b + 1
		}
	}
	os.Stdout.WriteString("READY\n")

	rng := rand.New(rand.NewSource(seed))
	for {
		if len(known) > 0 && rng.Intn(3) == 0 {
			// Bump every record of one batch to the next generation, in one CommitTx.
			var b int
			for b = range known {
				break
			}
			g := gen[b] + 1
			now := time.Now().UTC()
			n := len(known[b])
			var ops []txOp
			for i, id := range known[b] {
				ops = append(ops, txOp{kind: txOpUpdate, id: id, ts: now,
					data: map[string]any{"b": float64(b), "n": float64(n), "i": float64(i), "g": float64(g)}})
			}
			if len(ops) < 2 {
				continue
			}
			if err := c.CommitTx(ops); err != nil {
				fmt.Fprintln(os.Stderr, "child bump:", err)
				os.Exit(1)
			}
			gen[b] = g
			fmt.Printf("B %d %d\n", b, g)
			continue
		}
		n := 2 + rng.Intn(6)
		b := next
		next++
		recs := make([]map[string]any, n)
		for i := range recs {
			recs[i] = map[string]any{"b": float64(b), "n": float64(n), "i": float64(i), "g": float64(1)}
		}
		ids, _, err := c.InsertMany(recs, time.Time{})
		if err != nil {
			fmt.Fprintln(os.Stderr, "child insert:", err)
			os.Exit(1)
		}
		known[b], gen[b] = ids, 1
		fmt.Printf("I %d %d\n", b, n)
	}
}

type batchKillModel struct {
	n   map[int]int // acknowledged batch -> size
	gen map[int]int // acknowledged batch -> acknowledged generation
}

func (m *batchKillModel) feed(l string) {
	var b, v int
	switch {
	case strings.HasPrefix(l, "I "):
		fmt.Sscanf(l, "I %d %d", &b, &v)
		m.n[b], m.gen[b] = v, 1
	case strings.HasPrefix(l, "B "):
		fmt.Sscanf(l, "B %d %d", &b, &v)
		m.gen[b] = v
	}
}

func checkBatchRecovered(t *testing.T, dir string, seed int64, m *batchKillModel) *batchKillModel {
	t.Helper()
	cfg := killChildCfg("batch")
	var firstCounts map[int]int
	for pass := 0; pass < 3; pass++ {
		db, err := Open(dir, cfg)
		if err != nil {
			t.Fatalf("seed %d pass %d: reopen refused: %v", seed, pass, err)
		}
		c, err := db.Collection("kill")
		if err != nil {
			if len(m.n) == 0 {
				_ = db.Close()
				return m
			}
			t.Fatalf("seed %d: %v", seed, err)
		}
		res, err := c.Scan(nil)
		if err != nil {
			t.Fatalf("seed %d: scan: %v", seed, err)
		}
		count := map[int]int{}
		gens := map[int]map[int]bool{}
		size := map[int]int{}
		for _, r := range res {
			b := int(r.Data["b"].(float64))
			count[b]++
			size[b] = int(r.Data["n"].(float64))
			if gens[b] == nil {
				gens[b] = map[int]bool{}
			}
			gens[b][int(r.Data["g"].(float64))] = true
		}
		for b, k := range count {
			if k != size[b] {
				t.Fatalf("seed %d pass %d: batch %d recovered as a prefix: %d of %d records (replay: %s=%d)", seed, pass, b, k, size[b], envKillSeed, seed)
			}
			if len(gens[b]) != 1 {
				t.Fatalf("seed %d pass %d: batch %d has mixed generations %v (replay: %s=%d)", seed, pass, b, gens[b], envKillSeed, seed)
			}
		}
		for b, n := range m.n {
			if count[b] != n {
				t.Fatalf("seed %d pass %d: acknowledged batch %d has %d of %d records", seed, pass, b, count[b], n)
			}
			var g int
			for g = range gens[b] {
			}
			// The single in-flight bump may or may not have landed.
			if g != m.gen[b] && g != m.gen[b]+1 {
				t.Fatalf("seed %d pass %d: batch %d generation %d, acknowledged %d", seed, pass, b, g, m.gen[b])
			}
		}
		if pass == 0 {
			firstCounts = count
			// Fold truth back into the model for the next iteration.
			nm := &batchKillModel{n: map[int]int{}, gen: map[int]int{}}
			for b, k := range count {
				nm.n[b] = k
				for g := range gens[b] {
					nm.gen[b] = g
				}
			}
			m = nm
		} else if len(count) != len(firstCounts) {
			t.Fatalf("seed %d pass %d: recovered batch set changed across reopen", seed, pass)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
	return m
}

func TestKill9_AtomicBatches(t *testing.T) {
	maybeRunKillChild()
	SetAtomicBatchWriterReady(t, true)
	_, seeds := killIterations()
	dir := t.TempDir()
	model := &batchKillModel{n: map[int]int{}, gen: map[int]int{}}
	for _, seed := range seeds {
		t.Logf("seed %d (reproduce: %s=%d)", seed, envKillSeed, seed)
		runBatchKillIteration(t, dir, seed, model)
		model = checkBatchRecovered(t, dir, seed, model)
	}
}

func runBatchKillIteration(t *testing.T, dir string, seed int64, model *batchKillModel) {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	target := 1 + rng.Intn(40)
	jitter := time.Duration(rng.Intn(2000)) * time.Microsecond
	cmd, lines := startKillChild(t, dir, "batch", seed)
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
			acked++
		case <-deadline:
			cmd.Process.Kill()
			t.Fatalf("seed %d: timed out waiting for child", seed)
		}
	}
	time.Sleep(jitter)
	sigkill(t, cmd)
	// Lines flushed before death are still acknowledgments.
	for l := range lines {
		model.feed(l)
	}
}
