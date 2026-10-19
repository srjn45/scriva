package engine

import (
	"context"
	"fmt"

	"github.com/srjn45/scriva/crypto"
)

// EncryptionStatus reports a collection's encryption configuration and how far a
// policy migration has progressed. It is cheap to obtain: the migration figures
// come from an in-memory index walk with no segment reads.
type EncryptionStatus struct {
	// Configured reports whether the collection has ever had encryption enabled
	// (and therefore may hold ciphertext, even if writes are currently disabled).
	Configured bool
	// Enabled reports whether new writes are currently sealed.
	Enabled bool
	// Mode is the fixed reconstruction mode (fields or record); empty when the
	// collection has never been encrypted.
	Mode EncryptionMode
	// Epoch is the current policy epoch. New writes stamp it; migration is complete
	// when every live record has reached it.
	Epoch uint64
	// CurrentKeyID is the id of the key new writes seal under.
	CurrentKeyID string
	// LiveRecords is the number of live records tracked by the primary index.
	LiveRecords int
	// LiveAtEpoch is how many live records are already at Epoch.
	LiveAtEpoch int
	// FunctionalComplete reports that every live record is at the current epoch, so
	// reads behave exactly per the current policy. It does NOT imply old-form bytes
	// are gone from disk — stale (superseded) rows may still hold them until a
	// compaction pass reclaims them. Operations that require the old bytes to be
	// physically gone (retiring an old key, indexing a de-encrypted field) are
	// gated on security completion: a completed CompactNow.
	FunctionalComplete bool
}

// EncryptionStatus returns the collection's current encryption state and
// migration progress. A collection that was never encrypted reports the zero
// value with FunctionalComplete true (nothing to migrate).
func (c *Collection) EncryptionStatus() EncryptionStatus {
	enc := c.enc.Load()
	if enc == nil {
		return EncryptionStatus{FunctionalComplete: true}
	}
	total, at := c.index.countAtEpoch(enc.epoch)
	st := EncryptionStatus{
		Configured:         true,
		Enabled:            enc.enabled(),
		Mode:               enc.readMode,
		Epoch:              enc.epoch,
		LiveRecords:        total,
		LiveAtEpoch:        at,
		FunctionalComplete: total == at,
	}
	if m := c.encMeta.Load(); m != nil {
		st.CurrentKeyID = m.CurrentKeyID
	}
	return st
}

// SetEncryptionPolicy changes the collection's encryption write policy at runtime:
// enabling encryption for the first time, adjusting which fields are encrypted, or
// disabling it (a nil policy). It bumps the policy epoch, persists the new meta,
// and atomically swaps in a new encryptor, so new writes conform immediately;
// existing records migrate lazily as they are rewritten, or in bulk via CompactNow.
//
// The reconstruction mode is fixed when encryption is first enabled and cannot be
// switched between fields and record while the collection may still hold data
// under the old mode — disable, run CompactNow to security completion, then
// re-enable under the new mode. First-time enable requires a KeyProvider to have
// been supplied when the collection was opened.
func (c *Collection) SetEncryptionPolicy(ctx context.Context, policy *EncryptionPolicy) error {
	if err := c.checkFenced(); err != nil {
		return err
	}
	c.encAdminMu.Lock()
	defer c.encAdminMu.Unlock()

	cur := c.enc.Load()

	// Disable: keep the reconstruction mode (so residual ciphertext still decodes)
	// but stop sealing new writes.
	if policy == nil {
		if cur == nil || !cur.enabled() {
			return nil // never enabled, or already disabled — nothing to do
		}
		return c.applyEncryptionChange(cur.readMode, EncryptionPolicy{}, cur.keys, cur.epoch+1, nil)
	}

	if err := policy.validate(); err != nil {
		return err
	}

	// Adjust or re-enable an already-configured collection.
	if cur != nil {
		if policy.Mode != cur.readMode {
			return fmt.Errorf("engine: collection %q: cannot switch encryption mode from %q to %q while data exists; disable, run CompactNow, then re-enable under the new mode",
				c.name, cur.readMode, policy.Mode)
		}
		return c.applyEncryptionChange(cur.readMode, *policy, cur.keys, cur.epoch+1, nil)
	}

	// First-time enable on a collection opened without encryption. A key provider
	// must have been supplied at open, and a fresh key-check is minted so a later
	// reopen detects a wrong key.
	if c.keyProvider == nil {
		return fmt.Errorf("engine: collection %q: cannot enable encryption without a key provider supplied at open", c.name)
	}
	keyID, key, err := c.keyProvider.Current(ctx)
	if err != nil {
		return fmt.Errorf("engine: enable encryption on %q: %w", c.name, err)
	}
	kc, err := crypto.MakeKeyCheck(key, keyID)
	if err != nil {
		return fmt.Errorf("engine: enable encryption on %q: %w", c.name, err)
	}
	m := &encryptionMeta{KeyCheck: kc, CurrentKeyID: keyID}
	return c.applyEncryptionChange(policy.Mode, *policy, c.keyProvider, 1, m)
}

// RotateKey advances the collection to seal new writes under the key provider's
// current key. The caller is expected to have already added and promoted the new
// key on the shared provider (KeyProvider.Current now returns it); RotateKey bumps
// the epoch, refreshes the persisted key-check and current-key id, and swaps in a
// new encryptor. Older blobs remain readable as long as their key stays resolvable
// via the provider; a CompactNow re-encrypts them under the new key, after which
// the old key can be retired.
func (c *Collection) RotateKey(ctx context.Context) error {
	if err := c.checkFenced(); err != nil {
		return err
	}
	c.encAdminMu.Lock()
	defer c.encAdminMu.Unlock()

	cur := c.enc.Load()
	if cur == nil {
		return fmt.Errorf("engine: collection %q: RotateKey on a collection that is not encrypted", c.name)
	}
	keyID, key, err := cur.keys.Current(ctx)
	if err != nil {
		return fmt.Errorf("engine: rotate key on %q: %w", c.name, err)
	}
	kc, err := crypto.MakeKeyCheck(key, keyID)
	if err != nil {
		return fmt.Errorf("engine: rotate key on %q: %w", c.name, err)
	}
	m := &encryptionMeta{KeyCheck: kc, CurrentKeyID: keyID}
	return c.applyEncryptionChange(cur.readMode, cur.writePolicy, cur.keys, cur.epoch+1, m)
}

// applyEncryptionChange atomically installs a new write policy at newEpoch: it
// builds the fresh (immutable) encryptor, updates the persisted meta block, writes
// meta.json durably, and only then swaps the encryptor pointer — so a crash before
// the swap leaves the persisted policy and the live one consistent. keyOverride,
// when non-nil, supplies a refreshed KeyCheck/CurrentKeyID (first-enable or key
// rotation); otherwise the existing key-check and key id are preserved.
//
// It holds c.mu for the meta update + persist so the meta snapshot cannot race a
// concurrent write path's persistMeta. The caller holds encAdminMu, serializing
// the read-modify-write of the epoch across admin calls.
func (c *Collection) applyEncryptionChange(readMode EncryptionMode, writePolicy EncryptionPolicy, keys crypto.KeyProvider, newEpoch uint64, keyOverride *encryptionMeta) error {
	newEnc := newEncryptor(c.name, readMode, writePolicy, keys, newEpoch)

	c.mu.Lock()
	prev := c.encMeta.Load()
	m := &encryptionMeta{
		Policy:   writePolicy,
		ReadMode: readMode,
		Epoch:    newEpoch,
	}
	if prev != nil {
		m.KDF = prev.KDF
		m.KeyCheck = prev.KeyCheck
		m.CurrentKeyID = prev.CurrentKeyID
	}
	if keyOverride != nil {
		m.KeyCheck = keyOverride.KeyCheck
		m.CurrentKeyID = keyOverride.CurrentKeyID
	}
	// The candidate block is installed in the snapshot by the mutator and in
	// memory (encMeta) only after the write succeeded, both under metaMu, so no
	// concurrent persist can write an uncommitted block or miss a committed one.
	err := c.writeMetaThen("enc",
		func(s *collectionMeta) { s.Encryption = m },
		func() { c.encMeta.Store(m) })
	if err != nil {
		c.mu.Unlock()
		return fmt.Errorf("engine: persist encryption meta for %q: %w", c.name, err)
	}
	c.mu.Unlock()

	c.enc.Store(newEnc)
	return nil
}
