package engine

import (
	"errors"
	"fmt"

	"github.com/srjn45/filedbv2/store"
)

// ErrResourceExhausted is returned by the write path when applying a write would
// push the collection past its configured MaxRecords or MaxBytes quota (S4). The
// server maps it to gRPC codes.ResourceExhausted. Callers can match it with
// errors.Is.
//
// Quotas gate only the creation of new records: an in-place Update, CAS, or
// Delete is never refused, so a tenant sitting at its limit can still edit or
// delete existing records to recover. The check runs before the durable append,
// so a refused write appends nothing and mutates no index.
var ErrResourceExhausted = errors.New("engine: resource quota exhausted")

// quotaEnabled reports whether either quota dimension is configured for this
// collection. It lets the hot write paths skip all quota work when unlimited.
func (c *Collection) quotaEnabled() bool {
	return c.cfg.MaxRecords > 0 || c.cfg.MaxBytes > 0
}

// entryQuotaBytes returns the number of bytes e will add to a segment when
// appended, but only when a byte quota is configured — an unlimited collection
// pays no encoding cost here. A best-effort 0 on an encode error defers the real
// failure to the durable append, which re-encodes and surfaces it.
func (c *Collection) entryQuotaBytes(e store.Entry) int64 {
	if c.cfg.MaxBytes == 0 {
		return 0
	}
	b, err := store.Encode(e)
	if err != nil {
		return 0
	}
	return int64(len(b))
}

// sizeBytesLocked sums the on-disk size of every segment (sealed + active). The
// caller must hold c.mu (read or write) so the segment set is stable. It is the
// same figure Stats reports as SizeBytes, computed without re-taking the lock.
func (c *Collection) sizeBytesLocked() int64 {
	var total int64
	for _, s := range c.sealed {
		total += s.Size()
	}
	total += c.active.Size()
	return total
}

// checkQuotaLocked refuses a write that would create newRecords additional live
// records (totalling newBytes once appended) when either configured quota would
// be breached. The caller must hold c.mu (write lock) so the usage read and the
// subsequent append are atomic with respect to every other writer — this is what
// makes a batch check all-or-nothing. It returns ErrResourceExhausted on breach
// and nil when the collection is unlimited or the write fits within budget.
func (c *Collection) checkQuotaLocked(newRecords uint64, newBytes int64) error {
	if c.cfg.MaxRecords > 0 {
		if want := uint64(c.index.Len()) + newRecords; want > c.cfg.MaxRecords {
			return fmt.Errorf("%w: collection %q record limit %d reached (would hold %d)",
				ErrResourceExhausted, c.name, c.cfg.MaxRecords, want)
		}
	}
	if c.cfg.MaxBytes > 0 {
		if want := c.sizeBytesLocked() + newBytes; want > int64(c.cfg.MaxBytes) {
			return fmt.Errorf("%w: collection %q byte limit %d reached (would use %d)",
				ErrResourceExhausted, c.name, c.cfg.MaxBytes, want)
		}
	}
	return nil
}
