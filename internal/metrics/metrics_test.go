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
# HELP scriva_quota_rejected_total Total number of writes refused because they would exceed a collection's quota.
# TYPE scriva_quota_rejected_total counter
scriva_quota_rejected_total{collection="events"} 1
scriva_quota_rejected_total{collection="users"} 2
`
	if err := testutil.CollectAndCompare(m.QuotaRejectedTotal, strings.NewReader(want)); err != nil {
		t.Fatalf("unexpected quota-reject metric: %v", err)
	}
}

func TestObserveWriteOutcomeUnknown(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)

	m.ObserveWriteOutcomeUnknown("users", "insert")
	m.ObserveWriteOutcomeUnknown("events", "syncLoop")

	want := `
# HELP scriva_write_outcome_unknown_total Writes whose outcome could not be proven (fsync, truncate or seal-sync failure); each fences its collection until reopen.
# TYPE scriva_write_outcome_unknown_total counter
scriva_write_outcome_unknown_total{collection="events",op="syncLoop"} 1
scriva_write_outcome_unknown_total{collection="users",op="insert"} 1
`
	if err := testutil.CollectAndCompare(m.WriteOutcomeUnknownTotal, strings.NewReader(want)); err != nil {
		t.Fatalf("unexpected write-outcome-unknown metric: %v", err)
	}
}

func TestDBCollectorEmitsBytes(t *testing.T) {
	reg := prometheus.NewRegistry()
	NewDBCollector(reg, func() []CollectionStats {
		return []CollectionStats{{Name: "c", RecordCount: 3, SegmentCount: 1, SizeBytes: 4096}}
	})

	if got := testutil.CollectAndCount(reg, "scriva_collection_bytes"); got != 1 {
		t.Fatalf("scriva_collection_bytes series count = %d, want 1", got)
	}
	want := `
# HELP scriva_collection_bytes Current on-disk size of the collection in bytes (summed segment files).
# TYPE scriva_collection_bytes gauge
scriva_collection_bytes{collection="c"} 4096
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "scriva_collection_bytes"); err != nil {
		t.Fatalf("unexpected bytes gauge: %v", err)
	}
}
