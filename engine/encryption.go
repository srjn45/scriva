package engine

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/srjn45/scriva/crypto"
	"github.com/srjn45/scriva/query"
	"github.com/srjn45/scriva/store"
)

// EncryptionMode selects the granularity of transparent at-rest encryption for a
// collection.
type EncryptionMode string

const (
	// EncryptModeFields encrypts a deny-list of named top-level fields, leaving
	// every other field plaintext and queryable.
	EncryptModeFields EncryptionMode = "fields"
	// EncryptModeRecord encrypts the whole record into a single blob, keeping only
	// an allow-list of index fields (and the reserved _key) plaintext.
	EncryptModeRecord EncryptionMode = "record"
)

// secureDataField is the reserved top-level field that holds the sealed blob in
// record-level mode. It is reserved on write when record-level encryption is
// active, so it can never collide with a caller field.
const secureDataField = "secure_data"

// EncryptionPolicy configures transparent field- or record-level encryption for a
// single collection. The zero value (empty Mode) means no encryption. It is
// persisted in the collection's meta.json so it survives restart and is applied
// consistently by every writer; reads are policy-independent (driven by the
// per-value marker), so a mixed/half-migrated collection still reads correctly.
type EncryptionPolicy struct {
	Mode EncryptionMode `json:"mode"`
	// Fields is the deny-list of top-level field names to encrypt (mode "fields").
	Fields []string `json:"fields,omitempty"`
	// IndexFields is the allow-list of top-level field names left plaintext and
	// queryable (mode "record"); every other field is sealed into secure_data.
	IndexFields []string `json:"index_fields,omitempty"`
}

// validate rejects a policy that cannot be applied safely: an unknown mode, an
// empty field-level deny-list, or a reserved field (_key, secure_data) named as
// encryptable. The reserved _key must stay plaintext because keyed CRUD depends
// on it (see keys.go). The empty ("") mode is not valid here — disabling
// encryption is expressed by SetEncryptionPolicy, not by an EncryptionPolicy value.
func (p EncryptionPolicy) validate() error {
	switch p.Mode {
	case EncryptModeFields:
		if len(p.Fields) == 0 {
			return fmt.Errorf("engine: encryption policy: field-level mode needs at least one field")
		}
		for _, f := range p.Fields {
			if f == KeyField {
				return fmt.Errorf("engine: encryption policy: reserved field %q cannot be encrypted", KeyField)
			}
			if f == secureDataField {
				return fmt.Errorf("engine: encryption policy: reserved field %q cannot be encrypted", secureDataField)
			}
		}
		return nil
	case EncryptModeRecord:
		for _, f := range p.IndexFields {
			if f == secureDataField {
				return fmt.Errorf("engine: encryption policy: reserved field %q cannot be a plaintext index field", secureDataField)
			}
		}
		return nil
	default:
		return fmt.Errorf("engine: encryption policy: unknown mode %q", p.Mode)
	}
}

// encryptor applies a collection's encryption policy at the read/write boundary.
// It is the only engine component that touches key material; everything
// downstream (segments, compaction, index, replication) stays key-oblivious
// because the stored data map holds ciphertext.
//
// An encryptor is immutable once constructed: a policy change builds a fresh one
// and the collection atomically swaps it in, so lock-free readers always see a
// consistent snapshot. It deliberately separates two concerns:
//
//   - readMode is fixed for the life of the collection (set when encryption is
//     first enabled) and drives how stored records are reconstructed. Because it
//     never changes, older records written under an earlier write policy — or
//     residual ciphertext left after encryption is disabled — always decode
//     correctly.
//   - writePolicy governs only what new writes seal. A zero Mode means writes are
//     not sealed (encryption disabled) while reads still reconstruct any residual
//     ciphertext under readMode.
type encryptor struct {
	collection  string
	readMode    EncryptionMode // fixed reconstruction mode
	writePolicy EncryptionPolicy
	keys        crypto.KeyProvider
	fieldSet    map[string]struct{} // fields write-mode: fields to encrypt
	indexSet    map[string]struct{} // record write-mode: plaintext allow-list
	epoch       uint64              // policy epoch stamped onto writes
}

// newEncryptor builds an immutable encryptor. readMode fixes reconstruction;
// writePolicy (whose Mode may be "" to disable sealing) fixes what new writes
// seal; epoch is stamped onto every sealed entry.
func newEncryptor(collection string, readMode EncryptionMode, writePolicy EncryptionPolicy, keys crypto.KeyProvider, epoch uint64) *encryptor {
	e := &encryptor{
		collection:  collection,
		readMode:    readMode,
		writePolicy: writePolicy,
		keys:        keys,
		epoch:       epoch,
	}
	switch writePolicy.Mode {
	case EncryptModeFields:
		e.fieldSet = toSet(writePolicy.Fields)
	case EncryptModeRecord:
		e.indexSet = toSet(writePolicy.IndexFields)
	}
	return e
}

// enabled reports whether new writes are sealed under this encryptor. When false
// (encryption disabled), writes pass through as plaintext but reads still
// reconstruct any residual ciphertext under readMode.
func (e *encryptor) enabled() bool { return e.writePolicy.Mode != "" }

func toSet(fields []string) map[string]struct{} {
	s := make(map[string]struct{}, len(fields))
	for _, f := range fields {
		s[f] = struct{}{}
	}
	return s
}

// aad returns the AEAD associated data binding a blob to its context — the
// collection name and the field it lives under — so a ciphertext cannot be
// relocated to another field or collection and still decrypt.
func (e *encryptor) aad(field string) []byte {
	// The unit separator cannot appear in either component in practice and keeps
	// the two parts unambiguous.
	return []byte(e.collection + "\x1f" + field)
}

// encrypt transforms a plaintext record into its stored (ciphertext) form under
// the write policy. It first rejects any caller value that collides with the
// reserved marker so the marker stays an infallible read discriminator, then —
// when encryption is disabled — returns the data unchanged. In record mode it
// reserves the secure_data field. The input map is never mutated.
func (e *encryptor) encrypt(data map[string]any) (map[string]any, error) {
	for _, v := range data {
		if s, ok := v.(string); ok && crypto.HasReservedPrefix(s) {
			return nil, fmt.Errorf("%w: a value begins with the reserved encryption marker", crypto.ErrReservedPrefix)
		}
	}

	if !e.enabled() {
		// Disabled: writes are plaintext. A defensive copy keeps the no-mutation
		// contract (callers may reuse the input map).
		out := make(map[string]any, len(data))
		for f, v := range data {
			out[f] = v
		}
		return out, nil
	}

	keyID, key, err := e.keys.Current(context.Background())
	if err != nil {
		return nil, fmt.Errorf("engine: encrypt %q: %w", e.collection, err)
	}

	switch e.writePolicy.Mode {
	case EncryptModeFields:
		out := make(map[string]any, len(data))
		for f, v := range data {
			out[f] = v
		}
		for f := range e.fieldSet {
			v, ok := data[f]
			if !ok {
				continue
			}
			env, err := e.seal(key, keyID, f, v)
			if err != nil {
				return nil, err
			}
			out[f] = env
		}
		return out, nil

	case EncryptModeRecord:
		if _, ok := data[secureDataField]; ok {
			return nil, fmt.Errorf("%w: %q is reserved for record-level encryption", ErrReservedField, secureDataField)
		}
		out := make(map[string]any, len(e.indexSet)+2)
		rest := make(map[string]any, len(data))
		for f, v := range data {
			if f == KeyField {
				out[f] = v
				continue
			}
			if _, ok := e.indexSet[f]; ok {
				out[f] = v
				continue
			}
			rest[f] = v
		}
		env, err := e.seal(key, keyID, secureDataField, rest)
		if err != nil {
			return nil, err
		}
		out[secureDataField] = env
		return out, nil

	default:
		return data, nil
	}
}

// seal JSON-encodes v and returns its envelope under the given key and field aad.
func (e *encryptor) seal(key []byte, keyID, field string, v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", fmt.Errorf("engine: encrypt %s.%s: %w", e.collection, field, err)
	}
	env, err := crypto.Encrypt(key, keyID, b, e.aad(field))
	if err != nil {
		return "", fmt.Errorf("engine: encrypt %s.%s: %w", e.collection, field, err)
	}
	return env, nil
}

// reencrypt brings a stored record to the current policy: it decrypts the record
// in full (under the fixed readMode, so any earlier-policy or old-key ciphertext
// is understood) and re-seals it under the current write policy. When encryption
// is disabled the re-seal is a plaintext pass-through, so a de-encrypting
// migration rewrites blobs back to plaintext. It is fail-closed: a decrypt error
// (e.g. a retired key) is propagated so the caller can abort rather than lose
// data. Used by the re-encrypting compaction pass.
func (e *encryptor) reencrypt(ctx context.Context, stored map[string]any) (map[string]any, error) {
	pt, err := e.materialize(ctx, stored, nil)
	if err != nil {
		return nil, err
	}
	return e.encrypt(pt)
}

// materialize returns the plaintext record limited to fields (empty = the full
// record), decrypting only the blobs required to produce those fields. It is
// policy-independent: reconstruction is driven by the fixed readMode and the
// per-value marker, never by the current write policy, so records written under
// any earlier policy still decode. It is fail-closed: a decrypt error
// (crypto.ErrKeyUnavailable / crypto.ErrDecryptFailed) is propagated with
// collection/field context, never swallowed or partially returned. The stored map
// is never mutated.
func (e *encryptor) materialize(ctx context.Context, stored map[string]any, fields []string) (map[string]any, error) {
	if e.readMode == EncryptModeRecord {
		return e.materializeRecord(ctx, stored, fields)
	}
	return e.materializeFields(ctx, stored, fields)
}

func (e *encryptor) materializeFields(ctx context.Context, stored map[string]any, fields []string) (map[string]any, error) {
	// Full record: decrypt every marker-bearing top-level value.
	if len(fields) == 0 {
		out := make(map[string]any, len(stored))
		for f, v := range stored {
			dv, err := e.maybeDecryptValue(ctx, f, v)
			if err != nil {
				return nil, err
			}
			out[f] = dv
		}
		return out, nil
	}
	// Projection: only the selected fields are materialised, so a projection that
	// excludes every encrypted field needs no key and cannot fail (lazy decrypt).
	// The reserved _key is always retained, mirroring ProjectData.
	out := make(map[string]any, len(fields)+1)
	for _, f := range fields {
		v, ok := stored[f]
		if !ok {
			continue
		}
		dv, err := e.maybeDecryptValue(ctx, f, v)
		if err != nil {
			return nil, err
		}
		out[f] = dv
	}
	if v, ok := stored[KeyField]; ok {
		out[KeyField] = v // _key is never encrypted
	}
	return out, nil
}

func (e *encryptor) materializeRecord(ctx context.Context, stored map[string]any, fields []string) (map[string]any, error) {
	// Full record: copy the plaintext columns, then decrypt and merge the blob.
	if len(fields) == 0 {
		out := make(map[string]any, len(stored))
		for f, v := range stored {
			if f == secureDataField {
				continue
			}
			out[f] = v
		}
		merged, err := e.decryptBlob(ctx, stored[secureDataField])
		if err != nil {
			return nil, err
		}
		for k, v := range merged {
			out[k] = v
		}
		return out, nil
	}
	// Projection: take a requested field from the stored plaintext column when it
	// is present there (an index column — old or new — or _key), and only touch the
	// blob for fields not found as columns. This keeps the lazy-decrypt property
	// (a projection over columns alone needs no key) while staying correct across a
	// half-migrated index-field change, where the same field lives in the blob for
	// old records and as a plaintext column for new ones.
	out := make(map[string]any, len(fields)+1)
	needBlob := false
	for _, f := range fields {
		if f == KeyField {
			continue // retained below
		}
		if v, ok := stored[f]; ok && f != secureDataField {
			out[f] = v
			continue
		}
		needBlob = true
	}
	if v, ok := stored[KeyField]; ok {
		out[KeyField] = v
	}
	if needBlob {
		merged, err := e.decryptBlob(ctx, stored[secureDataField])
		if err != nil {
			return nil, err
		}
		for _, f := range fields {
			if f == KeyField {
				continue
			}
			if _, done := out[f]; done {
				continue
			}
			if v, ok := merged[f]; ok {
				out[f] = v
			}
		}
	}
	return out, nil
}

// maybeDecryptValue decrypts a single field value when it is a marker-bearing
// string, otherwise returns it unchanged (the policy-independent read invariant:
// a value is decrypted iff it carries the marker).
func (e *encryptor) maybeDecryptValue(ctx context.Context, field string, v any) (any, error) {
	s, ok := v.(string)
	if !ok || !crypto.IsEncrypted(s) {
		return v, nil
	}
	pt, err := crypto.DecryptWith(ctx, e.keys, s, e.aad(field))
	if err != nil {
		return nil, e.wrap(field, err)
	}
	var val any
	if err := json.Unmarshal(pt, &val); err != nil {
		return nil, e.wrap(field, fmt.Errorf("%w: %w", crypto.ErrDecryptFailed, err))
	}
	return val, nil
}

// decryptBlob decrypts the record-level secure_data blob into its field map. A
// blob that is absent or not a marker string (a legacy plaintext record that
// predates record mode, or a de-encrypted record) yields an empty map, so such
// records read through unchanged.
func (e *encryptor) decryptBlob(ctx context.Context, blob any) (map[string]any, error) {
	s, ok := blob.(string)
	if !ok || !crypto.IsEncrypted(s) {
		return nil, nil
	}
	pt, err := crypto.DecryptWith(ctx, e.keys, s, e.aad(secureDataField))
	if err != nil {
		return nil, e.wrap(secureDataField, err)
	}
	var m map[string]any
	if err := json.Unmarshal(pt, &m); err != nil {
		return nil, e.wrap(secureDataField, fmt.Errorf("%w: %w", crypto.ErrDecryptFailed, err))
	}
	return m, nil
}

func (e *encryptor) wrap(field string, err error) error {
	return fmt.Errorf("engine: decrypt %s.%s: %w", e.collection, field, err)
}

// isEncryptedField reports whether field is opaque on disk under the current
// write policy, and therefore cannot be indexed, filtered, sorted, or aggregated.
// It follows the write policy (not readMode): once encryption is disabled nothing
// is opaque to new queries — though residual ciphertext may remain on disk until a
// migration pass completes, which is why queries newly enabled on a de-encrypted
// field are gated on security completion (see MigrationStatus).
func (e *encryptor) isEncryptedField(field string) bool {
	switch e.writePolicy.Mode {
	case EncryptModeFields:
		_, ok := e.fieldSet[field]
		return ok
	case EncryptModeRecord:
		if field == KeyField {
			return false
		}
		_, ok := e.indexSet[field]
		return !ok
	default:
		return false
	}
}

// checkField rejects a sort/group/aggregate reference to an encrypted field. An
// empty field name (no such clause) is always allowed.
func (e *encryptor) checkField(field string) error {
	if field != "" && e.isEncryptedField(field) {
		return fmt.Errorf("%w: cannot query on encrypted field %q", crypto.ErrFieldEncrypted, field)
	}
	return nil
}

// checkFilterFields walks a filter tree and rejects any predicate on an encrypted
// field, so an opaque field is a hard error at scan planning rather than a filter
// that silently never matches.
func (e *encryptor) checkFilterFields(f query.Filter) error {
	if f == nil {
		return nil
	}
	if bad, ok := e.encryptedFilterField(f); ok {
		return fmt.Errorf("%w: cannot filter on encrypted field %q", crypto.ErrFieldEncrypted, bad)
	}
	return nil
}

func (e *encryptor) encryptedFilterField(f query.Filter) (string, bool) {
	switch ff := f.(type) {
	case *query.FieldFilter:
		if e.isEncryptedField(ff.Field) {
			return ff.Field, true
		}
	case *query.AndFilter:
		for _, sub := range ff.Filters {
			if bad, ok := e.encryptedFilterField(sub); ok {
				return bad, true
			}
		}
	case *query.OrFilter:
		for _, sub := range ff.Filters {
			if bad, ok := e.encryptedFilterField(sub); ok {
				return bad, true
			}
		}
	}
	return "", false
}

// ---- Collection integration ------------------------------------------------

// initEncryption wires up the collection's encryptor after load(). For an
// already-encrypted collection it verifies the supplied key against the
// persisted key-check (fail-fast wrong-key detection) and loads the policy, read
// mode, and epoch from meta.json. For a collection that config asks to encrypt
// for the first time, it validates and persists the policy plus a fresh key-check
// at epoch 1. A collection that is neither encrypted on disk nor asked to be stays
// plaintext, but retains the key provider (if any) so encryption can be enabled
// later via SetEncryptionPolicy.
func (c *Collection) initEncryption(cfg CollectionConfig) error {
	c.keyProvider = cfg.KeyProvider

	m, err := loadMeta(metaPath(c.dir))
	if err != nil {
		// A missing/corrupt meta.json means nothing is persisted yet; treat it as
		// no encryption block. load() has already reconstructed the rest.
		m = collectionMeta{}
	}

	switch {
	case m.Encryption != nil:
		// The collection holds encrypted data on disk — a key is mandatory.
		if cfg.KeyProvider == nil {
			return fmt.Errorf("engine: collection %q is encrypted and requires a key to open: %w", c.name, crypto.ErrKeyUnavailable)
		}
		_, key, err := cfg.KeyProvider.Current(context.Background())
		if err != nil {
			return fmt.Errorf("engine: open encrypted collection %q: %w", c.name, err)
		}
		if err := crypto.VerifyKeyCheck(key, m.Encryption.KeyCheck); err != nil {
			return fmt.Errorf("engine: open collection %q: %w", c.name, err)
		}
		c.encMeta.Store(m.Encryption)
		c.enc.Store(newEncryptor(c.name, m.Encryption.readMode(), m.Encryption.Policy, cfg.KeyProvider, m.Encryption.Epoch))
		// Self-heal the sentinel (E2) now that a valid block and key are in hand.
		// A failure only costs the extra evidence — meta.json is intact — so it
		// is logged rather than failing a healthy open (e.g. a read-only mount).
		if _, err := ensureSentinel(c.dir); err != nil {
			if l := c.cfg.Logger; l != nil {
				l.Warn("could not restore encryption sentinel", "collection", c.name, "file", encryptionSentinelFile, "err", err)
			}
		}
		return nil

	case cfg.Encryption != nil:
		// Enabling encryption on a new (or legacy-plaintext) collection.
		if err := cfg.Encryption.validate(); err != nil {
			return err
		}
		if cfg.KeyProvider == nil {
			return fmt.Errorf("engine: collection %q: encryption policy set without a key provider", c.name)
		}
		keyID, key, err := cfg.KeyProvider.Current(context.Background())
		if err != nil {
			return fmt.Errorf("engine: enable encryption on %q: %w", c.name, err)
		}
		kc, err := crypto.MakeKeyCheck(key, keyID)
		if err != nil {
			return fmt.Errorf("engine: enable encryption on %q: %w", c.name, err)
		}
		c.encMeta.Store(&encryptionMeta{
			Policy:       *cfg.Encryption,
			ReadMode:     cfg.Encryption.Mode,
			Epoch:        1,
			KDF:          cfg.EncryptionKDF,
			KeyCheck:     kc,
			CurrentKeyID: keyID,
		})
		c.enc.Store(newEncryptor(c.name, cfg.Encryption.Mode, *cfg.Encryption, cfg.KeyProvider, 1))
		// Persist the encryption block immediately so a crash right after enabling
		// still records the policy and key-check.
		if err := c.writeMeta(nil); err != nil {
			return fmt.Errorf("engine: persist encryption meta for %q: %w", c.name, err)
		}
		// The sentinel follows the block (never precedes it: a crash between
		// them must not leave a sentinel next to a plaintext meta) and still
		// lands before open returns, i.e. before any encrypted write exists.
		if _, err := ensureSentinel(c.dir); err != nil {
			return fmt.Errorf("engine: enable encryption on %q: %w", c.name, err)
		}
		return nil

	default:
		return nil // no encryption yet (may be enabled later via SetEncryptionPolicy)
	}
}

// sealEntryData replaces e.Data with its stored (ciphertext) form under the
// collection's current write policy and stamps the current policy epoch onto the
// entry, validating that no caller value collides with the reserved marker. It is
// a no-op when the collection has never been encryption-configured. Callers invoke
// it before appending so nothing is written if sealing fails, and so the stamped
// epoch and the sealed bytes are always consistent.
func (c *Collection) sealEntryData(e *store.Entry) error {
	enc := c.enc.Load()
	if enc == nil {
		return nil
	}
	stored, err := enc.encrypt(e.Data)
	if err != nil {
		return err
	}
	e.Data = stored
	e.Epoch = enc.epoch
	return nil
}
