package engine

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"github.com/srjn45/scriva/crypto"
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
	// Encryption, when non-nil, records that this collection encrypts data at
	// rest: the policy applied to writes, the key-derivation parameters (for the
	// passphrase path), a key-check for fast wrong-key detection on open, and the
	// id of the key new writes use. It carries no secret material. It is omitted
	// for collections that are not encrypted.
	Encryption *encryptionMeta `json:"encryption,omitempty"`
}

// encryptionMeta is the persisted "encryption" block of a collection's meta.json.
// Everything in it is non-secret and safe to store in cleartext — it reveals
// nothing without the key.
type encryptionMeta struct {
	// Policy is the current write policy: what new writes seal. A zero Mode ("")
	// means writes are not sealed (encryption disabled), while ReadMode still
	// governs how any residual on-disk ciphertext is reconstructed on read.
	Policy EncryptionPolicy `json:"policy"`
	// ReadMode is the reconstruction mode fixed when encryption was first enabled
	// (fields or record). It never changes for the life of the collection — reads
	// always reconstruct under it, so a policy that is later disabled or has its
	// field list adjusted still decodes older records correctly. Loaded meta from
	// before this field existed defaults it to Policy.Mode.
	ReadMode EncryptionMode `json:"read_mode,omitempty"`
	// Epoch is the current policy epoch, bumped on each policy change (field
	// add/remove, key rotation, enable/disable). New writes stamp it onto their
	// entries; migration is complete when every live entry has reached it. Omitted
	// when zero (a collection encrypted before epochs existed reads as epoch 0).
	Epoch        uint64         `json:"epoch,omitempty"`
	KDF          *EncryptionKDF `json:"kdf,omitempty"`
	KeyCheck     string         `json:"key_check"`
	CurrentKeyID string         `json:"current_key_id"`
}

// readMode returns the reconstruction mode for the collection, defaulting to the
// write policy's mode for meta written before ReadMode was persisted (a stage-2
// collection whose write policy and read mode were necessarily identical).
func (m *encryptionMeta) readMode() EncryptionMode {
	if m.ReadMode != "" {
		return m.ReadMode
	}
	return m.Policy.Mode
}

// EncryptionKDF records the key-derivation parameters for a passphrase-derived
// key so the same key can be re-derived on reopen. The salt and parameters are
// non-secret. It is nil when the key is supplied directly (raw-key provider).
type EncryptionKDF struct {
	Algo   string           `json:"algo"`   // e.g. "argon2id"
	Salt   string           `json:"salt"`   // base64-encoded
	Params crypto.KDFParams `json:"params"` // cost parameters
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

// persistMeta writes m to path as JSON, atomically and durably (unique temp file →
// fsync → rename → fsync dir). Live collections go through Collection.writeMeta,
// which serializes callers; offline code (repair) calls this directly. A corrupt or partial write still degrades
// gracefully: the next startup falls back to the full segment scan and rewrites
// a fresh meta.json.
func persistMeta(path string, m collectionMeta) error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return writeFileAtomicUnique(path, data, 0o644)
}

// writeMeta is the single persist path for a live collection's meta.json. Under
// the leaf metaMu it takes the snapshot, applies mut to it, and writes it
// durably, so concurrent persists can neither interleave nor persist a stale
// snapshot over a newer one. mut may be nil.
func (c *Collection) writeMeta(mut func(*collectionMeta)) error {
	return c.writeMetaThen("meta", mut, nil)
}

// writeMetaThen is writeMeta plus a commit step: commit runs still under metaMu,
// only after the write succeeded, and installs the in-memory counterpart of what
// mut put into the snapshot — so memory changes only once the state is durable,
// and no other persist can snapshot between the write and the install. src
// labels the caller for the postMetaSnapshotHook test seam.
//
// Lock order: encAdminMu → c.mu → metaMu (leaf). Nothing else is acquired while
// metaMu is held; the snapshot reads only atomics and immutables.
func (c *Collection) writeMetaThen(src string, mut func(*collectionMeta), commit func()) error {
	c.metaMu.Lock()
	defer c.metaMu.Unlock()
	snap := c.metaSnapshot()
	if mut != nil {
		mut(&snap)
	}
	if h := c.cfg.postMetaSnapshotHook; h != nil {
		h(src)
	}
	if err := persistMeta(metaPath(c.dir), snap); err != nil {
		return err
	}
	if commit != nil {
		commit()
	}
	return nil
}
