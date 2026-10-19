package server

import "time"

// SetBackoff overrides the follower reconnect delay for tests.
func (f *Follower) SetBackoff(d time.Duration) { f.backoff = d }
