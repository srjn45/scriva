package server

import (
	"errors"

	"github.com/srjn45/scriva/engine"
	"github.com/srjn45/scriva/internal/metrics"
)

// IntegrityMetricsHook adapts metrics to engine.CollectionConfig.OnIntegrity,
// counting each blocking or warning-level finding by severity and code.
func IntegrityMetricsHook(m *metrics.Metrics) func(collection string, policy engine.IntegrityPolicy, outcome string, rep *engine.CollectionReport) {
	return func(collection string, policy engine.IntegrityPolicy, outcome string, rep *engine.CollectionReport) {
		var fs []metrics.IntegrityFinding
		if rep != nil {
			for _, f := range rep.Findings {
				if f.Severity != engine.SeverityInfo {
					fs = append(fs, metrics.IntegrityFinding{Severity: string(f.Severity), Code: string(f.Code)})
				}
			}
		}
		pol := string(policy)
		if pol == "" {
			pol = string(engine.PolicyFail)
		}
		m.ObserveIntegrity(collection, pol, outcome, fs)
	}
}

// AppendMetricsHook adapts metrics to engine.CollectionConfig.OnAppend,
// classifying failures as poisoned, too_large or io.
func AppendMetricsHook(m *metrics.Metrics) func(collection string, bytes int, err error) {
	return func(collection string, bytes int, err error) {
		reason := ""
		switch {
		case err == nil:
		case errors.Is(err, engine.ErrSegmentPoisoned):
			reason = "poisoned"
		case errors.Is(err, engine.ErrRecordTooLarge):
			reason = "too_large"
		default:
			reason = "io"
		}
		m.ObserveAppend(collection, bytes, reason)
	}
}
