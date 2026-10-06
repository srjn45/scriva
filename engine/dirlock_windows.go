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
		f.Close()
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
	defer f.Close()
	var overlapped windows.Overlapped
	return windows.UnlockFileEx(windows.Handle(f.Fd()), 0, 1, 0, &overlapped)
}
