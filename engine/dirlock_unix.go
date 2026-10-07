//go:build !windows

package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

func lockDirOS(dir string) (*dirLock, error) {
	lockFile := filepath.Join(dir, "LOCK")
	f, err := os.OpenFile(lockFile, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open lock file: %w", err)
	}

	err = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if err != nil {
		_ = f.Close()
		if errors.Is(err, unix.EWOULDBLOCK) {
			return nil, ErrDatabaseLocked
		}
		// On some NFS setups, flock is not supported (ENOSYS, EOPNOTSUPP, etc).
		// We make failure to acquire due to unsupported FS an explicit error.
		return nil, fmt.Errorf("acquire directory lock: unsupported file system: %w", err)
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
	return unix.Flock(int(f.Fd()), unix.LOCK_UN)
}

// probeDirLockOS reports whether another holder has the directory LOCK,
// without creating the file or taking ownership: a missing LOCK is "not held",
// and a shared non-blocking flock conflicts only with an exclusive holder.
func probeDirLockOS(dir string) (bool, error) {
	f, err := os.Open(filepath.Join(dir, "LOCK"))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer func() { _ = f.Close() }()
	if err := unix.Flock(int(f.Fd()), unix.LOCK_SH|unix.LOCK_NB); err != nil {
		if errors.Is(err, unix.EWOULDBLOCK) {
			return true, nil
		}
		return false, err
	}
	_ = unix.Flock(int(f.Fd()), unix.LOCK_UN)
	return false, nil
}
