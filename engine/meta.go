package engine

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"
)

const metaFilename = "meta.json"

// metaPath returns the meta.json path for a collection directory.
func metaPath(dir string) string { return filepath.Join(dir, metaFilename) }

// collectionMeta holds the small amount of state that is expensive to
// reconstruct by scanning segments on every startup.
type collectionMeta struct {
	IDCounter uint64    `json:"id_counter"`
	CreatedAt time.Time `json:"created_at"`
	// DefaultTTLSeconds, when > 0, is a per-collection default record TTL set
	// explicitly at CreateCollection time. It is omitted (0) for collections
	// that simply inherit the server-wide default, so changing the global
	// default still applies to them.
	DefaultTTLSeconds int64 `json:"default_ttl_seconds,omitempty"`
}

// loadMeta reads and parses the meta.json file at path.
// Returns os.ErrNotExist if the file does not exist.
func loadMeta(path string) (collectionMeta, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return collectionMeta{}, err
	}
	var m collectionMeta
	if err := json.Unmarshal(data, &m); err != nil {
		return collectionMeta{}, err
	}
	return m, nil
}

// persistMeta writes m to path as JSON, atomically and durably (temp file →
// fsync → rename → fsync dir). A corrupt or partial write still degrades
// gracefully: the next startup falls back to the full segment scan and rewrites
// a fresh meta.json.
func persistMeta(path string, m collectionMeta) error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return writeFileAtomic(path, data, 0o644)
}
