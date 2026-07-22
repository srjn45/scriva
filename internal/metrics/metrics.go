// Package metrics provides Prometheus instrumentation for ScrivaDB.
package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// CollectionStats is the minimal stats snapshot used by DBCollector.
type CollectionStats struct {
	Name         string
	RecordCount  uint64
	SegmentCount uint64
	SizeBytes    uint64
}

// Metrics holds all Prometheus instruments for ScrivaDB.
type Metrics struct {
	reg                prometheus.Registerer
	CompactionTotal    *prometheus.CounterVec
	CompactionDuration *prometheus.HistogramVec
	GRPCDuration       *prometheus.HistogramVec
	ScanRowsScanned    *prometheus.HistogramVec
	QuotaRejectedTotal *prometheus.CounterVec
}

// New creates a Metrics and registers all instruments with reg.
// Pass prometheus.DefaultRegisterer for production use.
func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{reg: reg}

	m.CompactionTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "scriva_compaction_runs_total",
		Help: "Total number of compaction runs per collection.",
	}, []string{"collection"})

	m.CompactionDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "scriva_compaction_duration_seconds",
		Help:    "Duration of compaction runs in seconds.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
	}, []string{"collection"})

	m.GRPCDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "scriva_grpc_request_duration_seconds",
		Help:    "Duration of gRPC unary requests in seconds.",
		Buckets: []float64{0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5},
	}, []string{"method", "code"})

	// Rows examined per Find/Scan query, bucketed on an exponential scale so a
	// pathological full scan (many rows scanned) is visible against cheap indexed
	// lookups. An operator pairs this with the slow-query log to find unindexed
	// hot queries.
	m.ScanRowsScanned = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "scriva_scan_rows_scanned",
		Help:    "Number of live records examined per Find/Scan query.",
		Buckets: prometheus.ExponentialBuckets(1, 4, 10), // 1, 4, 16, ... ~262144
	}, []string{"collection"})

	// Writes refused because they would breach a collection's configured quota
	// (S4). Paired with the scriva_collection_records_total / segments gauges (and
	// the SizeBytes the DBCollector reads), an operator sees both consumption and
	// the rejections it triggers.
	m.QuotaRejectedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "scriva_quota_rejected_total",
		Help: "Total number of writes refused because they would exceed a collection's quota.",
	}, []string{"collection"})

	reg.MustRegister(m.CompactionTotal, m.CompactionDuration, m.GRPCDuration, m.ScanRowsScanned, m.QuotaRejectedTotal)
	return m
}

// ObserveCompaction records one completed compaction run.
func (m *Metrics) ObserveCompaction(collection string, dur time.Duration) {
	m.CompactionTotal.WithLabelValues(collection).Inc()
	m.CompactionDuration.WithLabelValues(collection).Observe(dur.Seconds())
}

// ObserveGRPC records one completed gRPC unary request.
func (m *Metrics) ObserveGRPC(method, code string, dur time.Duration) {
	m.GRPCDuration.WithLabelValues(method, code).Observe(dur.Seconds())
}

// ObserveScan records the number of rows examined by one completed Find/Scan
// query against the named collection.
func (m *Metrics) ObserveScan(collection string, rowsScanned int) {
	m.ScanRowsScanned.WithLabelValues(collection).Observe(float64(rowsScanned))
}

// ObserveQuotaReject records one write refused by the named collection's quota.
func (m *Metrics) ObserveQuotaReject(collection string) {
	m.QuotaRejectedTotal.WithLabelValues(collection).Inc()
}

// DBCollector is a prometheus.Collector that emits per-collection record and
// segment gauges by calling statsFunc at every scrape.
type DBCollector struct {
	statsFunc    func() []CollectionStats
	recordsDesc  *prometheus.Desc
	segmentsDesc *prometheus.Desc
	bytesDesc    *prometheus.Desc
}

// NewDBCollector returns a DBCollector backed by statsFunc and registers it
// with reg.
func NewDBCollector(reg prometheus.Registerer, statsFunc func() []CollectionStats) *DBCollector {
	c := &DBCollector{
		statsFunc: statsFunc,
		recordsDesc: prometheus.NewDesc(
			"scriva_collection_records_total",
			"Current number of live records in the collection.",
			[]string{"collection"}, nil,
		),
		segmentsDesc: prometheus.NewDesc(
			"scriva_collection_segments_total",
			"Current number of segment files in the collection.",
			[]string{"collection"}, nil,
		),
		bytesDesc: prometheus.NewDesc(
			"scriva_collection_bytes",
			"Current on-disk size of the collection in bytes (summed segment files).",
			[]string{"collection"}, nil,
		),
	}
	reg.MustRegister(c)
	return c
}

// Describe implements prometheus.Collector.
func (c *DBCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.recordsDesc
	ch <- c.segmentsDesc
	ch <- c.bytesDesc
}

// Collect implements prometheus.Collector.
func (c *DBCollector) Collect(ch chan<- prometheus.Metric) {
	for _, s := range c.statsFunc() {
		ch <- prometheus.MustNewConstMetric(
			c.recordsDesc, prometheus.GaugeValue,
			float64(s.RecordCount), s.Name,
		)
		ch <- prometheus.MustNewConstMetric(
			c.segmentsDesc, prometheus.GaugeValue,
			float64(s.SegmentCount), s.Name,
		)
		ch <- prometheus.MustNewConstMetric(
			c.bytesDesc, prometheus.GaugeValue,
			float64(s.SizeBytes), s.Name,
		)
	}
}

// Handler returns an http.Handler that serves the Prometheus metrics page.
// Pass prometheus.DefaultGatherer for the default registry.
func Handler(gatherer prometheus.Gatherer) http.Handler {
	return promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{})
}
