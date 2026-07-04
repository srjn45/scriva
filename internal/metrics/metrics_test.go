package metrics

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestObserveQuotaReject(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)

	m.ObserveQuotaReject("users")
	m.ObserveQuotaReject("users")
	m.ObserveQuotaReject("events")

	want := `
# HELP filedb_quota_rejected_total Total number of writes refused because they would exceed a collection's quota.
# TYPE filedb_quota_rejected_total counter
filedb_quota_rejected_total{collection="events"} 1
filedb_quota_rejected_total{collection="users"} 2
`
	if err := testutil.CollectAndCompare(m.QuotaRejectedTotal, strings.NewReader(want)); err != nil {
		t.Fatalf("unexpected quota-reject metric: %v", err)
	}
}

func TestDBCollectorEmitsBytes(t *testing.T) {
	reg := prometheus.NewRegistry()
	NewDBCollector(reg, func() []CollectionStats {
		return []CollectionStats{{Name: "c", RecordCount: 3, SegmentCount: 1, SizeBytes: 4096}}
	})

	if got := testutil.CollectAndCount(reg, "filedb_collection_bytes"); got != 1 {
		t.Fatalf("filedb_collection_bytes series count = %d, want 1", got)
	}
	want := `
# HELP filedb_collection_bytes Current on-disk size of the collection in bytes (summed segment files).
# TYPE filedb_collection_bytes gauge
filedb_collection_bytes{collection="c"} 4096
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "filedb_collection_bytes"); err != nil {
		t.Fatalf("unexpected bytes gauge: %v", err)
	}
}
