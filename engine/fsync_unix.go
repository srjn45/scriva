//go:build !windows

package engine

import (
	"fmt"
	"os"
)

// fsyncDir flushes a directory's entries to stable storage so that file
// creations, renames, and deletions within it survive a crash.
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("fsync dir: open %q: %w", dir, err)
	}
	syncErr := d.Sync()
	closeErr := d.Close()
	if syncErr != nil {
		return fmt.Errorf("fsync dir: sync %q: %w", dir, syncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("fsync dir: close %q: %w", dir, closeErr)
	}
	return nil
}
