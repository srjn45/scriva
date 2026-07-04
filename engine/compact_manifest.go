package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// compactManifestFilename is the swap-intent record a compaction pass writes
// before mutating any segment file and removes only after the post-swap index
// persist. Its presence at open means a crash interrupted the swap: the
// on-disk segments may be part old / part new and the persisted indexes
// cannot be trusted.
const compactManifestFilename = "compact.manifest"

// compactManifest describes one compaction swap so an interrupted pass can be
// rolled forward idempotently at the next open.
type compactManifest struct {
	// Renames maps each sealed temp file to the final segment path it must
	// occupy. When the final name belongs to an old sealed segment the rename
	// replaces that file atomically.
	Renames map[string]string `json:"renames"`
	// Removals lists the old sealed segments whose names are not reused by any
	// rename; they hold only data superseded by the renamed temps.
	Removals []string `json:"removals"`
}

func compactManifestPath(dir string) string {
	return filepath.Join(dir, compactManifestFilename)
}

// writeCompactManifest durably records the swap intent before the swap begins.
func writeCompactManifest(dir string, m compactManifest) error {
	b, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("manifest: marshal: %w", err)
	}
	return writeFileAtomic(compactManifestPath(dir), b, 0o644)
}

// clearCompactManifest retires the intent record once the swap and the
// post-swap index persist have both completed.
func clearCompactManifest(dir string) error {
	if err := os.Remove(compactManifestPath(dir)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("manifest: remove: %w", err)
	}
	return fsyncDir(dir)
}

// recoverCompaction brings a collection directory back to a consistent segment
// layout after a crash, before the segment files are enumerated. Without a
// manifest, any .compact_* temp files are leftovers from a pass killed while
// still writing them — the old segments remain authoritative, so the temps are
// discarded. With a manifest, the swap had begun: it is rolled forward
// idempotently (outstanding renames applied, listed removals deleted) and the
// caller must rebuild the indexes from the resulting segments. Returns true
// when a manifest was found.
func recoverCompaction(dir string) (bool, error) {
	b, err := os.ReadFile(compactManifestPath(dir))
	if errors.Is(err, os.ErrNotExist) {
		return false, discardCompactTemps(dir)
	}
	if err != nil {
		return false, fmt.Errorf("manifest: read: %w", err)
	}

	var m compactManifest
	if err := json.Unmarshal(b, &m); err != nil {
		// The manifest is written atomically, so torn content should never
		// occur. Refuse to guess which side of the swap the segments are on.
		return true, fmt.Errorf("manifest: unmarshal %q: %w", compactManifestPath(dir), err)
	}

	for src, dst := range m.Renames {
		if _, statErr := os.Stat(src); statErr == nil {
			if err := os.Rename(src, dst); err != nil {
				return true, fmt.Errorf("manifest: roll forward %q → %q: %w", src, dst, err)
			}
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return true, fmt.Errorf("manifest: stat %q: %w", src, statErr)
		}
		// src missing means this rename was already applied before the crash.
	}
	for _, p := range m.Removals {
		if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
			return true, fmt.Errorf("manifest: remove %q: %w", p, err)
		}
	}
	// Any remaining temps were superseded during the pass (e.g. by a rebalance
	// merge) and are not part of the final layout.
	if err := discardCompactTemps(dir); err != nil {
		return true, err
	}
	if err := fsyncDir(dir); err != nil {
		return true, fmt.Errorf("manifest: %w", err)
	}
	return true, clearCompactManifest(dir)
}

// discardCompactTemps deletes stray compaction temp files (.compact_*,
// including .merge intermediates, which share the prefix).
func discardCompactTemps(dir string) error {
	stray, err := filepath.Glob(filepath.Join(dir, ".compact_*"))
	if err != nil {
		return fmt.Errorf("manifest: glob temps: %w", err)
	}
	for _, p := range stray {
		if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("manifest: discard temp %q: %w", p, err)
		}
	}
	return nil
}
