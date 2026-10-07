//nolint:errcheck
package engine

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// benchN is the record count for the large-directory open benchmarks. Override
// with SCRIVA_BENCH_N (the recorded results use 1000000).
func benchN() int {
	if v, err := strconv.Atoi(os.Getenv("SCRIVA_BENCH_N")); err == nil && v > 0 {
		return v
	}
	return 100000
}

func benchSeed(b *testing.B, dir string, cfg CollectionConfig, n int) []uint64 {
	b.Helper()
	col, err := OpenCollection("c", dir, cfg)
	if err != nil {
		b.Fatal(err)
	}
	ids := make([]uint64, n)
	for i := 0; i < n; i++ {
		if ids[i], _, err = col.Insert(sampleRecord(i)); err != nil {
			b.Fatal(err)
		}
	}
	if err := col.Close(); err != nil {
		b.Fatal(err)
	}
	return ids
}

// BenchmarkUpdate measures single-record update throughput per SyncMode.
func BenchmarkUpdate(b *testing.B) {
	for _, mode := range []SyncMode{SyncModeNone, SyncModeInterval, SyncModeAlways} {
		b.Run(string(mode), func(b *testing.B) {
			col, err := OpenCollection("bench", b.TempDir(), benchCfg(mode))
			if err != nil {
				b.Fatal(err)
			}
			defer col.Close()
			const n = 1000
			ids := make([]uint64, n)
			for i := range ids {
				if ids[i], _, err = col.Insert(sampleRecord(i)); err != nil {
					b.Fatal(err)
				}
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := col.Update(ids[i%n], sampleRecord(i)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkDelete measures delete throughput per SyncMode (records are
// pre-inserted outside the timer).
func BenchmarkDelete(b *testing.B) {
	for _, mode := range []SyncMode{SyncModeNone, SyncModeInterval, SyncModeAlways} {
		b.Run(string(mode), func(b *testing.B) {
			col, err := OpenCollection("bench", b.TempDir(), benchCfg(mode))
			if err != nil {
				b.Fatal(err)
			}
			defer col.Close()
			ids := make([]uint64, b.N)
			for i := range ids {
				if ids[i], _, err = col.Insert(sampleRecord(i)); err != nil {
					b.Fatal(err)
				}
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := col.Delete(ids[i]); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkOpenClean measures reopening a cleanly closed directory (valid
// persisted index) of benchN() records.
func BenchmarkOpenClean(b *testing.B) {
	dir := b.TempDir()
	cfg := benchCfg(SyncModeNone)
	benchSeed(b, dir, cfg, benchN())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		col, err := OpenCollection("c", dir, cfg)
		if err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		col.Close()
		b.StartTimer()
	}
}

func copyDirTree(b *testing.B, src, dst string) {
	b.Helper()
	ents, err := os.ReadDir(src)
	if err != nil {
		b.Fatal(err)
	}
	if err := os.MkdirAll(dst, 0o755); err != nil {
		b.Fatal(err)
	}
	for _, e := range ents {
		data, err := os.ReadFile(filepath.Join(src, e.Name()))
		if err != nil {
			b.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dst, e.Name()), data, 0o644); err != nil {
			b.Fatal(err)
		}
	}
}
