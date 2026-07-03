//nolint:errcheck
package engine

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/srjn45/filedbv2/query"
)

func benchCfg(mode SyncMode) CollectionConfig {
	cfg := defaultConfig()
	cfg.CompactInterval = 24 * time.Hour // keep compaction out of the measurement
	cfg.SyncMode = mode
	return cfg
}

func sampleRecord(i int) map[string]any {
	return map[string]any{
		"name":   fmt.Sprintf("user-%d", i),
		"email":  fmt.Sprintf("user-%d@example.com", i),
		"age":    float64(i % 100),
		"active": i%2 == 0,
	}
}

// BenchmarkInsert measures single-record insert throughput under each
// durability mode. Run: go test ./internal/engine -bench BenchmarkInsert -benchmem
func BenchmarkInsert(b *testing.B) {
	for _, mode := range []SyncMode{SyncModeNone, SyncModeInterval, SyncModeAlways} {
		b.Run(string(mode), func(b *testing.B) {
			col, err := OpenCollection("bench", b.TempDir(), benchCfg(mode))
			if err != nil {
				b.Fatal(err)
			}
			defer col.Close()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, _, err := col.Insert(sampleRecord(i)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkFindByID measures point-lookup latency via the in-memory primary
// index plus a single disk seek.
func BenchmarkFindByID(b *testing.B) {
	col, err := OpenCollection("bench", b.TempDir(), benchCfg(SyncModeNone))
	if err != nil {
		b.Fatal(err)
	}
	defer col.Close()

	const n = 10000
	ids := make([]uint64, n)
	for i := 0; i < n; i++ {
		id, _, err := col.Insert(sampleRecord(i))
		if err != nil {
			b.Fatal(err)
		}
		ids[i] = id
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := col.FindByID(ids[i%n]); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkScanFull measures a full-collection scan with a predicate that has
// no secondary index (the O(n) slow path).
func BenchmarkScanFull(b *testing.B) {
	col, err := OpenCollection("bench", b.TempDir(), benchCfg(SyncModeNone))
	if err != nil {
		b.Fatal(err)
	}
	defer col.Close()

	const n = 10000
	for i := 0; i < n; i++ {
		if _, _, err := col.Insert(sampleRecord(i)); err != nil {
			b.Fatal(err)
		}
	}
	f := &query.FieldFilter{Field: "name", Op: query.OpEq, Value: `"user-9999"`}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := col.Scan(f); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkFindLimitVsFull contrasts an unordered `limit 10` streaming query
// against draining the whole collection. The limited query stops after the
// first ten matches, so its cost is bounded by the limit rather than the
// collection size — this is the Q1 push-down guarantee. Run:
//
//	go test ./internal/engine -bench BenchmarkFindLimitVsFull -benchmem
func BenchmarkFindLimitVsFull(b *testing.B) {
	col, err := OpenCollection("bench", b.TempDir(), benchCfg(SyncModeNone))
	if err != nil {
		b.Fatal(err)
	}
	defer col.Close()

	const n = 50000
	for i := 0; i < n; i++ {
		if _, _, err := col.Insert(sampleRecord(i)); err != nil {
			b.Fatal(err)
		}
	}

	drain := func(opts ScanOptions) {
		count := 0
		if _, err := col.ScanStream(context.Background(), opts, func(ScanResult) error {
			count++
			return nil
		}); err != nil {
			b.Fatal(err)
		}
	}

	b.Run("limit10", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			drain(ScanOptions{Limit: 10})
		}
	})
	b.Run("full", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			drain(ScanOptions{})
		}
	})
}

// BenchmarkRangeIndexedVsFull contrasts a narrow range query (gte, matching a
// handful of rows) served by a secondary index against the same query with no
// index (full scan). With the ordered index the cost tracks the number of
// matches, not the collection size — the Q3 push-down guarantee. Run:
//
//	go test ./engine -bench BenchmarkRangeIndexedVsFull -benchmem
func BenchmarkRangeIndexedVsFull(b *testing.B) {
	const n = 50000
	load := func(col *Collection) {
		for i := 0; i < n; i++ {
			if _, _, err := col.Insert(map[string]any{"age": float64(i)}); err != nil {
				b.Fatal(err)
			}
		}
	}
	// gte n-5 → only the top 5 rows match.
	f := &query.FieldFilter{Field: "age", Op: query.OpGte, Value: fmt.Sprintf("%d", n-5)}

	b.Run("indexed", func(b *testing.B) {
		col, err := OpenCollection("bench", b.TempDir(), benchCfg(SyncModeNone))
		if err != nil {
			b.Fatal(err)
		}
		defer col.Close()
		if err := col.EnsureIndex("age"); err != nil {
			b.Fatal(err)
		}
		load(col)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := col.Scan(f); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("full", func(b *testing.B) {
		col, err := OpenCollection("bench", b.TempDir(), benchCfg(SyncModeNone))
		if err != nil {
			b.Fatal(err)
		}
		defer col.Close()
		load(col)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := col.Scan(f); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkScanIndexed measures the same single-eq query accelerated by a
// secondary index (the O(1) fast path).
func BenchmarkScanIndexed(b *testing.B) {
	col, err := OpenCollection("bench", b.TempDir(), benchCfg(SyncModeNone))
	if err != nil {
		b.Fatal(err)
	}
	defer col.Close()

	if err := col.EnsureIndex("name"); err != nil {
		b.Fatal(err)
	}
	const n = 10000
	for i := 0; i < n; i++ {
		if _, _, err := col.Insert(sampleRecord(i)); err != nil {
			b.Fatal(err)
		}
	}
	f := &query.FieldFilter{Field: "name", Op: query.OpEq, Value: `"user-9999"`}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := col.Scan(f); err != nil {
			b.Fatal(err)
		}
	}
}
