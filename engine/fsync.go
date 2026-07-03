package engine

import (
	"fmt"
	"os"
	"path/filepath"
)

// writeFileAtomic writes data to path durably: it writes to a temp file, fsyncs
// the temp file, renames it into place (atomic, so readers never observe a
// partial file), and fsyncs the parent directory so the rename itself survives
// a crash. It replaces the plain write-temp+rename pattern used for the index,
// secondary index, and meta files.
func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	tmp := path + ".tmp"

	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, perm)
	if err != nil {
		return fmt.Errorf("atomic write: create temp: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("atomic write: write temp: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("atomic write: sync temp: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("atomic write: close temp: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("atomic write: rename: %w", err)
	}
	if err := fsyncDir(dir); err != nil {
		return fmt.Errorf("atomic write: %w", err)
	}
	return nil
}
