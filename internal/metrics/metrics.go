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

	RecoveryTotal        *prometheus.CounterVec
	RecoveryDuration     *prometheus.HistogramVec
	RecoveryBytesTotal   *prometheus.CounterVec
	IntegrityOpenTotal   *prometheus.CounterVec
	IntegrityFindings    *prometheus.CounterVec
	AppendTotal          *prometheus.CounterVec
	AppendBytesTotal     *prometheus.CounterVec
	AppendErrorsTotal    *prometheus.CounterVec
	SegmentPoisonedTotal *prometheus.CounterVec
	DirLockTotal         *prometheus.CounterVec

	XTxTotal          *prometheus.CounterVec
	XTxDuration       *prometheus.HistogramVec
	XTxConflictsTotal *prometheus.CounterVec
	XTxRecoveryTotal  *prometheus.CounterVec
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

	// Crash-recovery / integrity observability (open-time).
	m.RecoveryTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "scriva_recovery_total",
		Help: "Index recovery actions taken at open, by kind (replay|rebuild|spotcheck_fail).",
	}, []string{"collection", "kind"})
	m.RecoveryDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "scriva_recovery_duration_seconds",
		Help:    "Duration of index recovery actions at open, by kind.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 15, 60, 300},
	}, []string{"collection", "kind"})
	m.RecoveryBytesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "scriva_recovery_bytes_total",
		Help: "Segment bytes replayed or scanned by index recovery at open, by kind.",
	}, []string{"collection", "kind"})
	m.IntegrityOpenTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "scriva_integrity_open_total",
		Help: "Open-time integrity scans by policy and outcome (clean|reported|failed).",
	}, []string{"collection", "policy", "outcome"})
	m.IntegrityFindings = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "scriva_integrity_findings_total",
		Help: "Integrity findings seen by open-time scans, by severity and code.",
	}, []string{"collection", "severity", "code"})
	m.AppendTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "scriva_append_total",
		Help: "Successful segment appends.",
	}, []string{"collection"})
	m.AppendBytesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "scriva_append_bytes_total",
		Help: "Bytes appended to segments.",
	}, []string{"collection"})
	m.AppendErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "scriva_append_errors_total",
		Help: "Failed segment appends by reason (poisoned|too_large|io).",
	}, []string{"collection", "reason"})
	m.SegmentPoisonedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "scriva_segment_poisoned_total",
		Help: "Active segments poisoned by a write that could not be rolled back.",
	}, []string{"collection"})
	m.DirLockTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "scriva_dir_lock_total",
		Help: "Data-directory lock acquisitions by result (acquired|contended|failed).",
	}, []string{"result"})

	m.XTxTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "scriva_xtx_total",
		Help: "Total cross-collection transactions by outcome (commit|conflict|abort|canceled|unknown|rollback|expired).",
	}, []string{"outcome"})

	m.XTxDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "scriva_xtx_duration_seconds",
		Help:    "Duration of cross-collection transactions from begin to outcome in seconds.",
		Buckets: []float64{0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
	}, []string{"outcome"})

	m.XTxConflictsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "scriva_xtx_conflicts_total",
		Help: "Cross-collection transaction conflicts by kind (read|write|constraint).",
	}, []string{"kind"})

	m.XTxRecoveryTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "scriva_xtx_recovery_total",
		Help: "Cross-collection transactions resolved during startup recovery, by outcome (recovered_committed|presumed_abort).",
	}, []string{"outcome"})

	reg.MustRegister(m.CompactionTotal, m.CompactionDuration, m.GRPCDuration, m.ScanRowsScanned, m.QuotaRejectedTotal,
		m.RecoveryTotal, m.RecoveryDuration, m.RecoveryBytesTotal, m.IntegrityOpenTotal, m.IntegrityFindings,
		m.AppendTotal, m.AppendBytesTotal, m.AppendErrorsTotal, m.SegmentPoisonedTotal, m.DirLockTotal,
		m.XTxTotal, m.XTxDuration, m.XTxConflictsTotal, m.XTxRecoveryTotal)
	return m
}

// ObserveXTx records one cross-collection transaction lifecycle outcome and
// duration. The begin event is ignored (it only marks the start).
func (m *Metrics) ObserveXTx(event string, dur time.Duration) {
	if event == "begin" || event == "" {
		return
	}
	m.XTxTotal.WithLabelValues(event).Inc()
	m.XTxDuration.WithLabelValues(event).Observe(dur.Seconds())
}

// ObserveXTxConflict records one transaction rejected due to contention,
// classified by kind (read, write, or constraint).
func (m *Metrics) ObserveXTxConflict(kind string) {
	if kind == "" {
		kind = "constraint"
	}
	m.XTxConflictsTotal.WithLabelValues(kind).Inc()
}

// ObserveXTxRecovery records transactions resolved during startup recovery.
func (m *Metrics) ObserveXTxRecovery(outcome string, count int) {
	if outcome == "" || count <= 0 {
		return
	}
	m.XTxRecoveryTotal.WithLabelValues(outcome).Add(float64(count))
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

// ObserveRecovery records one index-recovery action performed at open. It
// matches engine.CollectionConfig.OnIndexRecovery.
func (m *Metrics) ObserveRecovery(collection, kind string, bytes int64, dur time.Duration) {
	m.RecoveryTotal.WithLabelValues(collection, kind).Inc()
	m.RecoveryDuration.WithLabelValues(collection, kind).Observe(dur.Seconds())
	if bytes > 0 {
		m.RecoveryBytesTotal.WithLabelValues(collection, kind).Add(float64(bytes))
	}
}

// ObserveIntegrity records one open-time integrity scan: its outcome and each
// finding at warning level or above (informational findings are not counted).
func (m *Metrics) ObserveIntegrity(collection, policy, outcome string, findings []IntegrityFinding) {
	m.IntegrityOpenTotal.WithLabelValues(collection, policy, outcome).Inc()
	for _, f := range findings {
		m.IntegrityFindings.WithLabelValues(collection, f.Severity, f.Code).Inc()
	}
}

// IntegrityFinding is the metrics-layer view of an engine finding, keeping this
// package free of an engine dependency.
type IntegrityFinding struct{ Severity, Code string }

// ObserveAppend records one segment append attempt. reason classifies a failure
// ("" for success).
func (m *Metrics) ObserveAppend(collection string, bytes int, reason string) {
	if reason != "" {
		m.AppendErrorsTotal.WithLabelValues(collection, reason).Inc()
		return
	}
	m.AppendTotal.WithLabelValues(collection).Inc()
	m.AppendBytesTotal.WithLabelValues(collection).Add(float64(bytes))
}

// ObserveSegmentPoisoned records a segment poisoning event.
func (m *Metrics) ObserveSegmentPoisoned(collection string) {
	m.SegmentPoisonedTotal.WithLabelValues(collection).Inc()
}

// ObserveDirLock records a data-directory lock acquisition attempt.
func (m *Metrics) ObserveDirLock(result string) {
	m.DirLockTotal.WithLabelValues(result).Inc()
}
