package engine

import (
	"encoding/json"
	"os"
	"time"
)

const metaFilename = "meta.json"

// collectionMeta holds the small amount of state that is expensive to
// reconstruct by scanning segments on every startup.
type collectionMeta struct {
	IDCounter uint64    `json:"id_counter"`
	CreatedAt time.Time `json:"created_at"`
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
