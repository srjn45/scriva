package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
)

// ErrIntegrity is matched (errors.Is) by every open-time failure caused by
// damaged segment bytes or ambiguous record history. The concrete error is an
// *OpenIntegrityError carrying the report that justified the refusal.
var ErrIntegrity = errors.New("engine: integrity violation")

// IntegrityPolicy decides what open does when it finds corruption or
// conflicting record history in the segments. The zero value is PolicyFail:
// open is fail-closed.
//
// Whatever the policy, open still repairs what is provably safe on its own:
// trimming a torn, never-acknowledged tail on the newest segment, replaying the
// bytes appended after a persisted index's coverage, and rebuilding an
// untrustworthy index (primary or secondary) from segments that read back
// intact. Nothing else is automatic.
type IntegrityPolicy string

const (
	// PolicyFail (default): corruption or conflicts found while open scans
	// the segments fail the open with an *OpenIntegrityError (errors.Is ErrIntegrity).
	PolicyFail IntegrityPolicy = "fail"
	// PolicyReport opts in to opening anyway: the damaged regions are skipped
	// (salvaged records are indexed, conflicting history resolves last-write-wins
	// as it always did), the findings are recorded on the collection
	// (Collection.OpenIntegrityReport) and surfaced through OnIntegrity, logs and
	// metrics. Nothing on disk is rewritten.
	PolicyReport IntegrityPolicy = "report"
	// PolicyRebuildIndexOnly refuses data corruption and conflicts exactly like
	// PolicyFail and states, explicitly, that rebuilding derived indexes from
	// intact segments is the only repair open may perform. It is accepted as a
	// distinct value so an operator can pin that contract; any future automatic
	// repair is gated on PolicyFail/RebuildIndexOnly never including it.
	PolicyRebuildIndexOnly IntegrityPolicy = "rebuild-index-only"
)

// Outcomes reported through CollectionConfig.OnIntegrity.
const (
	IntegrityOutcomeClean    = "clean"    // segments scanned, nothing blocking found
	IntegrityOutcomeReported = "reported" // blocking findings; opened anyway (PolicyReport)
	IntegrityOutcomeFailed   = "failed"   // blocking findings; open refused
)

// ParseIntegrityPolicy parses a policy name; the empty string is PolicyFail.
func ParseIntegrityPolicy(s string) (IntegrityPolicy, error) {
	switch p := IntegrityPolicy(strings.ToLower(strings.TrimSpace(s))); p {
	case "", PolicyFail:
		return PolicyFail, nil
	case PolicyReport, PolicyRebuildIndexOnly:
		return p, nil
	}
	return "", fmt.Errorf("invalid integrity policy %q (want fail|report|rebuild-index-only)", s)
}

func (p IntegrityPolicy) normalized() IntegrityPolicy {
	if p == "" {
		return PolicyFail
	}
	return p
}

// OpenIntegrityError is the typed open failure. It matches ErrIntegrity.
type OpenIntegrityError struct {
	Collection string
	Policy     IntegrityPolicy
	Report     *CollectionReport
}

func (e *OpenIntegrityError) Error() string {
	n, worst := 0, SeverityInfo
	var first *Finding
	if e.Report != nil {
		for i := range e.Report.Findings {
			f := &e.Report.Findings[i]
			if f.Severity.rank() >= SeverityDataCorruption.rank() {
				n++
				if first == nil {
					first = f
				}
				if f.Severity.rank() > worst.rank() {
					worst = f.Severity
				}
			}
		}
	}
	msg := fmt.Sprintf("engine: collection %q refused to open: %d %s finding(s) under policy %q", e.Collection, n, worst, e.Policy)
	if first != nil {
		msg += fmt.Sprintf("; first: %s %s@%d: %s", first.Code, first.Location.Segment, first.Location.Offset, first.Message)
	}
	return msg + " (inspect with Verify; open with IntegrityPolicy=report to salvage)"
}

// Is makes errors.Is(err, ErrIntegrity) true.
func (e *OpenIntegrityError) Is(target error) bool { return target == ErrIntegrity }

func blockingFindings(rep *CollectionReport) bool {
	for _, f := range rep.Findings {
		if f.Severity.rank() >= SeverityDataCorruption.rank() {
			return true
		}
	}
	return false
}

// OpenIntegrityReport returns the report produced when open had to scan the
// segments (a full index rebuild), or nil if open only trusted/replayed
// persisted state and so never inspected the full history.
func (c *Collection) OpenIntegrityReport() *CollectionReport {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.openReport
}

// integrityGate scans every segment tolerantly (once per open) and applies the
// policy. It returns tolerant=true when blocking findings exist and the policy
// opted in to opening anyway, so rebuilds must skip damaged regions.
func (c *Collection) integrityGate(all []*Segment) (tolerant bool, err error) {
	if c.gateRan {
		if c.gateFailed != nil {
			return false, c.gateFailed
		}
		return c.tolerantOpen, nil
	}
	c.gateRan = true

	segs := make([]vseg, 0, len(all))
	for _, s := range all {
		n, _ := segmentNum(s.Path())
		segs = append(segs, vseg{seg: s, path: s.Path(), name: filepath.Base(s.Path()), num: n, size: s.Size()})
	}
	opts := VerifyOptions{Mode: VerifyFull}.normalized()
	in := &verifyInput{name: c.name, dir: c.dir, opts: opts, segs: segs}
	sink := newFindingSink(c.name, opts.MaxFindingsPerCode)
	if err := runChecks(context.Background(), in, sink); err != nil {
		return false, err
	}
	rep := in.report(sink)
	c.openReport = rep

	policy := c.cfg.IntegrityPolicy.normalized()
	outcome := IntegrityOutcomeClean
	if blockingFindings(rep) {
		outcome = IntegrityOutcomeFailed
		if policy == PolicyReport {
			outcome = IntegrityOutcomeReported
			c.tolerantOpen = true
		}
	}
	c.reportIntegrity(policy, outcome, rep)
	if outcome == IntegrityOutcomeFailed {
		c.gateFailed = &OpenIntegrityError{Collection: c.name, Policy: policy, Report: rep}
		return false, c.gateFailed
	}
	return c.tolerantOpen, nil
}

func (c *Collection) reportIntegrity(policy IntegrityPolicy, outcome string, rep *CollectionReport) {
	if h := c.cfg.OnIntegrity; h != nil {
		h(c.name, policy, outcome, rep)
	}
	if l := c.cfg.Logger; l != nil {
		level := slog.LevelInfo
		if outcome != IntegrityOutcomeClean {
			level = slog.LevelError
		}
		attrs := []any{"collection", c.name, "policy", string(policy), "outcome", outcome, "findings", len(rep.Findings)}
		for _, f := range rep.Findings {
			if f.Severity.rank() >= SeverityDataCorruption.rank() {
				attrs = append(attrs, "first_code", string(f.Code), "first_segment", f.Location.Segment, "first_offset", f.Location.Offset)
				break
			}
		}
		l.Log(context.Background(), level, "integrity scan at open", attrs...)
	}
}

// rebuildTolerant is Rebuild over salvaged entries: damaged regions are
// skipped, intact records are indexed with the same last-write-wins semantics.
func (idx *Index) rebuildTolerant(segments []*Segment, decisions ...map[string]string) error {
	var dec map[string]string
	if len(decisions) > 0 {
		dec = decisions[0]
	}
	fresh := make(map[uint64]IndexEntry)
	for _, seg := range sortSegments(segments) {
		rep, err := scanSegmentTolerantLimit(seg.Path(), seg.Size())
		if err != nil {
			return err
		}
		for _, se := range rep.Entries {
			if !entryVisible(se.Tx, dec) {
				continue
			}
			applyOne(fresh, seg.Path(), se.Offset, se.Entry)
		}
	}
	idx.mu.Lock()
	idx.entries = fresh
	idx.mu.Unlock()
	return nil
}

// Has reports whether any finding carries code.
func (r *CollectionReport) Has(code FindingCode) bool {
	for _, f := range r.Findings {
		if f.Code == code {
			return true
		}
	}
	return false
}
