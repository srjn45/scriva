package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// Collection-name policy and path containment
// (docs/reviews/ratification/security-contracts.md D1 and D2).
//
// A collection name is used verbatim as a directory name under the database
// root, so it must never be able to name anything but one real directory
// directly below that root. ValidateCollectionName is the single check for new
// names; names that already exist on disk but fail it ("legacy-named") stay
// operable but can never be created again.

var (
	// ErrInvalidName is returned for a collection name that violates the
	// naming policy. Reserved names additionally match ErrReservedName.
	ErrInvalidName = errors.New("engine: invalid collection name")

	// ErrUnsafePath is returned when a collection directory is not a real
	// directory directly below the database root (for example a symlink or a
	// reparse point), so it must not be opened, created over or dropped.
	ErrUnsafePath = errors.New("engine: unsafe collection path")

	// ErrCollectionExists is returned when creating a collection whose name is
	// already taken, exactly or up to ASCII case folding.
	ErrCollectionExists = errors.New("engine: collection already exists")
)

// maxCollectionNameLen is the longest accepted new collection name, in bytes.
const maxCollectionNameLen = 64

// reservedExactNames are case-insensitive names no new collection may take:
// REST route segments and the engine's own dotless root entries.
var reservedExactNames = map[string]struct{}{
	"xtx": {}, "collections": {}, "replication": {}, "lock": {},
	"con": {}, "prn": {}, "aux": {}, "nul": {},
	"com1": {}, "com2": {}, "com3": {}, "com4": {}, "com5": {}, "com6": {}, "com7": {}, "com8": {}, "com9": {},
	"lpt1": {}, "lpt2": {}, "lpt3": {}, "lpt4": {}, "lpt5": {}, "lpt6": {}, "lpt7": {}, "lpt8": {}, "lpt9": {},
}

func invalidName(name, why string) error {
	return fmt.Errorf("%w: %q: %s", ErrInvalidName, name, why)
}

// ValidateCollectionName reports whether name is acceptable for a NEW
// collection: 1-64 ASCII bytes matching ^[A-Za-z0-9][A-Za-z0-9_-]*$ and not
// reserved. There are no dots, separators, whitespace or non-ASCII bytes, and
// names are never normalized. A reserved name's error also matches
// ErrReservedName.
func ValidateCollectionName(name string) error {
	if name == "" {
		return invalidName(name, "empty")
	}
	if isReservedXTxName(name) || strings.HasPrefix(strings.ToLower(name), xtxReservedPrefix) {
		return fmt.Errorf("%w: %q: %w", ErrInvalidName, name, ErrReservedName)
	}
	if len(name) > maxCollectionNameLen {
		return invalidName(name, fmt.Sprintf("longer than %d bytes", maxCollectionNameLen))
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		alnum := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
		if alnum || (i > 0 && (c == '_' || c == '-')) {
			continue
		}
		return invalidName(name, "only ASCII letters, digits, '_' and '-' are allowed, starting with a letter or digit")
	}
	if _, ok := reservedExactNames[strings.ToLower(name)]; ok {
		return fmt.Errorf("%w: %q: %w", ErrInvalidName, name, ErrReservedName)
	}
	return nil
}

// validateLegacyName is the minimum an already-existing directory name must
// satisfy to be operated on: one plain path component. It deliberately allows
// spaces, dots and non-ASCII so existing data stays reachable.
func validateLegacyName(name string) error {
	switch {
	case name == "", name == ".", name == "..":
		return invalidName(name, "not a plain path component")
	case isReservedXTxName(name):
		return fmt.Errorf("%w: %q: %w", ErrInvalidName, name, ErrReservedName)
	case len(name) > 255:
		return invalidName(name, "longer than 255 bytes")
	case strings.ContainsAny(name, "/\\:\x00"):
		return invalidName(name, "contains a path separator, drive separator or NUL")
	}
	return nil
}

// realDirExists classifies path with Lstat (never following a symlink).
// exists is false when nothing is there; ErrUnsafePath is returned when the
// entry is anything other than a real directory.
func realDirExists(path string) (exists bool, err error) {
	fi, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("collection: lstat %q: %w", path, err)
	}
	if fi.Mode()&os.ModeSymlink != 0 || !fi.IsDir() {
		return true, fmt.Errorf("%w: %q is not a real directory", ErrUnsafePath, path)
	}
	return true, nil
}

// openContainedDir returns dataDir/name as a real directory, creating it (with
// Mkdir, never MkdirAll) when absent. The directory is Lstat-checked before and
// again after it is opened and compared with the open handle, so a symlink
// swapped in between the check and the use is detected.
func openContainedDir(dataDir, name string) (string, string, *os.File, error) {
	dir := filepath.Join(dataDir, name)
	if filepath.Dir(dir) != filepath.Clean(dataDir) {
		return "", "", nil, fmt.Errorf("%w: %q escapes the data directory", ErrUnsafePath, name)
	}
	exists, err := realDirExists(dir)
	if err != nil {
		return "", "", nil, err
	}
	if !exists {
		if err := os.Mkdir(dir, 0o755); err != nil && !errors.Is(err, os.ErrExist) {
			return "", "", nil, fmt.Errorf("collection: mkdir %q: %w", dir, err)
		}
	}
	h, err := os.Open(dir)
	if err != nil {
		return "", "", nil, fmt.Errorf("collection: open dir %q: %w", dir, err)
	}
	hi, err := h.Stat()
	if err != nil {
		_ = h.Close()
		return "", "", nil, fmt.Errorf("collection: stat dir %q: %w", dir, err)
	}
	li, err := os.Lstat(dir)
	if err != nil {
		_ = h.Close()
		return "", "", nil, fmt.Errorf("collection: lstat %q: %w", dir, err)
	}
	if li.Mode()&os.ModeSymlink != 0 || !li.IsDir() || !os.SameFile(hi, li) {
		_ = h.Close()
		return "", "", nil, fmt.Errorf("%w: %q changed while being opened", ErrUnsafePath, dir)
	}
	// On Linux, /proc/self/fd/N resolves through the open directory descriptor,
	// not through the collection's mutable name. Keep h open for the collection
	// lifetime: every existing file operation below this path is consequently
	// rooted at the directory that passed the containment check, even if an
	// attacker renames it and replaces its old pathname with a symlink.
	if runtime.GOOS == "linux" {
		return fmt.Sprintf("/proc/self/fd/%d", h.Fd()), dir, h, nil
	}
	return dir, dir, h, nil
}

// resolveRoot makes dataDir absolute and resolves symlinks in it, so every
// later join starts from a real path. A symlinked root is allowed.
func resolveRoot(dataDir string) (string, error) {
	abs, err := filepath.Abs(dataDir)
	if err != nil {
		return "", fmt.Errorf("db: resolve %q: %w", dataDir, err)
	}
	real, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return "", fmt.Errorf("db: resolve %q: %w", dataDir, err)
	}
	return real, nil
}

// checkExistingNameLocked validates a name used to reach a collection that must
// already exist: it passes if the name meets the strict grammar or is a
// registered (legacy-named) collection. Caller must hold db.mu (read or write).
func (db *DB) checkExistingNameLocked(name string) error {
	if _, ok := db.collections[name]; ok {
		return nil
	}
	return ValidateCollectionName(name)
}

// checkParticipantName is checkExistingNameLocked for callers that do not hold
// db.mu.
func (db *DB) checkParticipantName(name string) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.checkExistingNameLocked(name)
}

// foldCollisionLocked returns an open collection whose name equals name up to
// case folding but differs exactly. Caller must hold db.mu.
func (db *DB) foldCollisionLocked(name string) (string, bool) {
	for other := range db.collections {
		if other != name && strings.EqualFold(other, name) {
			return other, true
		}
	}
	return "", false
}

// reportRootNames logs and counts what root discovery found that is not a
// plain, grammar-conforming collection directory (D1 items 6-7). colNames is
// sorted.
func (db *DB) reportRootNames(cfg CollectionConfig, colNames, legacy, skipped []string) {
	if l := cfg.Logger; l != nil {
		for _, n := range legacy {
			l.Warn("legacy collection name: operable but cannot be created again", "collection", n)
		}
		for _, n := range skipped {
			l.Error("root entry skipped: symlink or not a plain directory name", "entry", n)
		}
		for i, a := range colNames {
			for _, b := range colNames[i+1:] {
				if strings.EqualFold(a, b) {
					l.Warn("collection names differ only by case", "a", a, "b", b)
				}
			}
		}
	}
	if h := cfg.OnRootNames; h != nil {
		h(len(legacy), len(skipped))
	}
}
