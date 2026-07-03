package engine

// Config returns a copy of the effective configuration this collection is
// running with, after zero-valued fields have been normalized to their defaults
// at open time. It lets embedders (and tests) observe the resolved durability,
// segment-size, compaction, and watch settings a collection was opened under —
// for example to confirm a per-collection SyncModeAlways override took effect.
func (c *Collection) Config() CollectionConfig {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.cfg
}
