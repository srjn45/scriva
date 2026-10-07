//nolint:errcheck
package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// BenchmarkOpenCrashTail measures reopening a directory whose persisted index
// lags the data by a bounded tail (simulated crash: the directory is copied
// while the collection is still open, before Close rewrites the index).
func BenchmarkOpenCrashTail(b *testing.B) {
	for _, tail := range []int{1000, 10000} {
		b.Run("tail="+itoa(tail), func(b *testing.B) {
			base := b.TempDir()
			cfg := benchCfg(SyncModeNone)
			cfg.IndexPersistInterval = -1
			benchSeed(b, base, cfg, benchN())
			col, err := OpenCollection("c", base, cfg)
			if err != nil {
				b.Fatal(err)
			}
			for i := 0; i < tail; i++ {
				if _, _, err := col.Insert(sampleRecord(i)); err != nil {
					b.Fatal(err)
				}
			}
			crashed := filepath.Join(b.TempDir(), "snap")
			copyDirTree(b, filepath.Join(base, "c"), filepath.Join(crashed, "c"))
			col.Close()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				work := filepath.Join(b.TempDir(), "w")
				copyDirTree(b, filepath.Join(crashed, "c"), filepath.Join(work, "c"))
				b.StartTimer()
				c, err := OpenCollection("c", work, cfg)
				if err != nil {
					b.Fatal(err)
				}
				b.StopTimer()
				c.Close()
				os.RemoveAll(work)
				b.StartTimer()
			}
		})
	}
}

// BenchmarkOpenV1Upgrade measures the one-time rebuild when the persisted
// index is the legacy v1 format (no coverage).
func BenchmarkOpenV1Upgrade(b *testing.B) {
	base := b.TempDir()
	cfg := benchCfg(SyncModeNone)
	benchSeed(b, base, cfg, benchN())
	ip := filepath.Join(base, "c", "index.json")
	idx := newIndex()
	if err := idx.Load(ip); err != nil {
		b.Fatal(err)
	}
	raw, _ := json.Marshal(idx.entries)
	sum := sha256.Sum256(raw)
	v1, _ := json.Marshal(indexFile{Entries: idx.entries, Checksum: hex.EncodeToString(sum[:])})
	if err := os.WriteFile(ip, v1, 0o644); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		work := filepath.Join(b.TempDir(), "w")
		copyDirTree(b, filepath.Join(base, "c"), filepath.Join(work, "c"))
		b.StartTimer()
		c, err := OpenCollection("c", work, cfg)
		if err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		c.Close()
		os.RemoveAll(work)
		b.StartTimer()
	}
}

// BenchmarkPersistIndexes measures one background persist pass over a
// benchN()-record collection with a small active segment and reports the
// writer pause: the max latency of concurrent inserts while persists run.
func BenchmarkPersistIndexes(b *testing.B) {
	cfg := benchCfg(SyncModeNone)
	cfg.IndexPersistInterval = -1
	col, err := OpenCollection("c", b.TempDir(), cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer col.Close()
	for i := 0; i < benchN(); i++ {
		if _, _, err := col.Insert(sampleRecord(i)); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := col.persistIndexes(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkInsertDuringPersist reports the worst insert latency observed while
// persists run back-to-back (the writer pause).
func BenchmarkInsertDuringPersist(b *testing.B) {
	cfg := benchCfg(SyncModeNone)
	cfg.IndexPersistInterval = -1
	col, err := OpenCollection("c", b.TempDir(), cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer col.Close()
	for i := 0; i < benchN(); i++ {
		if _, _, err := col.Insert(sampleRecord(i)); err != nil {
			b.Fatal(err)
		}
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				_ = col.persistIndexes()
			}
		}
	}()
	var worst time.Duration
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		t := time.Now()
		if _, _, err := col.Insert(sampleRecord(i)); err != nil {
			b.Fatal(err)
		}
		if d := time.Since(t); d > worst {
			worst = d
		}
	}
	b.StopTimer()
	close(stop)
	<-done
	b.ReportMetric(float64(worst.Microseconds()), "max-insert-µs")
}

func itoa(n int) string { return strconv.Itoa(n) }
