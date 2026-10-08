//go:build windows

package engine

import (
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/windows"
)

func lockDirOS(dir string) (*dirLock, error) {
	lockFile := filepath.Join(dir, "LOCK")
	f, err := os.OpenFile(lockFile, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open lock file: %w", err)
	}

	// LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY
	flags := uint32(windows.LOCKFILE_EXCLUSIVE_LOCK | windows.LOCKFILE_FAIL_IMMEDIATELY)
	var overlapped windows.Overlapped
	err = windows.LockFileEx(windows.Handle(f.Fd()), flags, 0, 1, 0, &overlapped)
	if err != nil {
		_ = f.Close()
		// If error is ERROR_LOCK_VIOLATION or ERROR_IO_PENDING, it's already locked
		if err == windows.ERROR_LOCK_VIOLATION {
			return nil, ErrDatabaseLocked
		}
		return nil, fmt.Errorf("acquire directory lock: %w", err)
	}

	return &dirLock{f: f}, nil
}

func (l *dirLock) unlockOS() error {
	f, ok := l.f.(*os.File)
	if !ok {
		return nil
	}
	defer func() {
		_ = f.Close()
	}()
	var overlapped windows.Overlapped
	return windows.UnlockFileEx(windows.Handle(f.Fd()), 0, 1, 0, &overlapped)
}

// probeDirLockOS reports whether another holder has the directory LOCK,
// without creating the file or taking ownership.
func probeDirLockOS(dir string) (bool, error) {
	f, err := os.Open(filepath.Join(dir, "LOCK"))
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer func() { _ = f.Close() }()
	var overlapped windows.Overlapped
	// Shared, fail-immediately: conflicts only with an exclusive holder.
	err = windows.LockFileEx(windows.Handle(f.Fd()), windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, &overlapped)
	if err == windows.ERROR_LOCK_VIOLATION {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	_ = windows.UnlockFileEx(windows.Handle(f.Fd()), 0, 1, 0, &overlapped)
	return false, nil
}
