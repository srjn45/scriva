package engine

import (
	"fmt"
	"testing"
	"time"
)

// BenchmarkBatchWrite compares the legacy (prefix-visible) InsertMany path with
// the crash-atomic one-participant XTx path. The atomic path forces a data
// fsync and a decision fsync regardless of SyncMode (hardening §3.1 item 2), so
// the relaxed modes pay the largest relative cost. Numbers are quoted in
// docs/operations.md; regenerate with:
//
//	go test ./engine -run xxx -bench BatchWrite -benchtime=300x
func BenchmarkBatchWrite(b *testing.B) {
	for _, mode := range []SyncMode{SyncModeAlways, SyncModeNone} {
		for _, size := range []int{2, 10, 100} {
			for _, atomic := range []bool{false, true} {
				name := fmt.Sprintf("sync=%s/size=%d/%s", mode, size, map[bool]string{false: "legacy", true: "atomic"}[atomic])
				b.Run(name, func(b *testing.B) {
					SetAtomicBatchWriterReady(b, atomic)
					cfg := defaultConfig()
					cfg.SyncMode = mode
					cfg.CompactInterval = 24 * time.Hour
					db, err := Open(b.TempDir(), cfg)
					if err != nil {
						b.Fatal(err)
					}
					defer db.Close()
					c, err := db.CreateCollection("bench")
					if err != nil {
						b.Fatal(err)
					}
					recs := make([]map[string]any, size)
					for i := range recs {
						recs[i] = map[string]any{"i": float64(i), "payload": "0123456789abcdef0123456789abcdef"}
					}
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if _, _, err := c.InsertMany(recs, time.Time{}); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
	}
}
