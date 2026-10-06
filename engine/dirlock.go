package engine

import (
	"errors"
	"path/filepath"
	"sync"
)

// ErrDatabaseLocked is returned when a database is already open in another
// process, or in another DB instance within the same process.
var ErrDatabaseLocked = errors.New("database already open")

var (
	registryMu sync.Mutex
	openDirs   = make(map[string]struct{})
)

type dirLock struct {
	path string
	f    interface{} // file handle, holds the OS lock
}

func lockDir(dir string) (*dirLock, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	realPath, err := filepath.EvalSymlinks(abs)
	if err != nil {
		// If the directory doesn't exist yet, we just use the absolute path.
		realPath = abs
	}

	registryMu.Lock()
	if _, ok := openDirs[realPath]; ok {
		registryMu.Unlock()
		return nil, ErrDatabaseLocked
	}
	
	dl, err := lockDirOS(dir)
	if err != nil {
		registryMu.Unlock()
		return nil, err
	}

	openDirs[realPath] = struct{}{}
	registryMu.Unlock()

	dl.path = realPath
	return dl, nil
}

func (l *dirLock) release() error {
	registryMu.Lock()
	delete(openDirs, l.path)
	registryMu.Unlock()
	return l.unlockOS()
}
