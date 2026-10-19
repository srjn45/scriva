package engine

import (
	"fmt"
	"os"
	"path/filepath"
)

// afterAtomicTempCreate is a test seam (nil in production) invoked with the
// temp path right after it is opened, before any data is written.
var afterAtomicTempCreate func(tmp string)

// atomicRename is the rename step of writeFileAtomicUnique (os.Rename in
// production; tests swap it to inject a rename failure).
var atomicRename = os.Rename

// writeFileAtomicUnique is writeFileAtomic with a private, uniquely named temp
// file (os.CreateTemp in path's directory, "<base>.*"), so concurrent writers of
// the same path — in this process or another — never share a temp inode. It is
// the shared lower layer of Collection.writeMeta and the offline repair rebuild.
// On any failure the temp file is removed and the destination is untouched.
func writeFileAtomicUnique(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, filepath.Base(path)+".*")
	if err != nil {
		return fmt.Errorf("atomic write: create temp: %w", err)
	}
	tmp := f.Name()
	fail := func(what string, err error) error {
		_ = f.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("atomic write: %s: %w", what, err)
	}
	if h := afterAtomicTempCreate; h != nil {
		h(tmp)
	}
	if err := f.Chmod(perm); err != nil {
		return fail("chmod temp", err)
	}
	if _, err := f.Write(data); err != nil {
		return fail("write temp", err)
	}
	if err := f.Sync(); err != nil {
		return fail("sync temp", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("atomic write: close temp: %w", err)
	}
	if err := atomicRename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("atomic write: rename: %w", err)
	}
	if err := fsyncDir(dir); err != nil {
		return fmt.Errorf("atomic write: %w", err)
	}
	return nil
}

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
	if h := afterAtomicTempCreate; h != nil {
		h(tmp)
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
