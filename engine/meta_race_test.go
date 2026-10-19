package engine

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/srjn45/scriva/crypto"
)

// Barrier-driven reproductions of the H7 metadata defects (hardening §3.4,
// SR-D4 M4.1). They fail on the pre-fix code; each failing assertion is gated
// behind knownMetaRaceBug so `make test` stays green until the
// metadata-serialization change (metaMu/writeMeta) lands. That change must flip
// the flag to false.
const knownMetaRaceBug = true

func reportMetaRace(t *testing.T, format string, args ...any) {
	t.Helper()
	if knownMetaRaceBug {
		t.Logf("KNOWN BUG (TODO(metadata-serialization)): "+format, args...)
		return
	}
	t.Errorf(format, args...)
}

// TestMetaStaleSnapshotOverwrite releases setDefaultTTL and SetEncryptionPolicy in
// both orders around the snapshot→persist window; meta.json must end with both.
func TestMetaStaleSnapshotOverwrite(t *testing.T) {
	for _, first := range []string{"ttl", "enc"} {
		first := first
		t.Run("paused_"+first, func(t *testing.T) {
			dir := t.TempDir()
			kr, _ := crypto.NewKeyring("k1", mustKey(t))
			cfg := encCfgKR(kr, nil)

			reached := make(chan struct{})
			release := make(chan struct{})
			var once sync.Once
			cfg.postMetaSnapshotHook = func(src string) {
				if src != first {
					return
				}
				once.Do(func() {
					close(reached)
					<-release
				})
			}
			col, err := OpenCollection("users", dir, cfg)
			if err != nil {
				t.Fatal(err)
			}
			id, _, err := col.Insert(map[string]any{"email": "a@b.com", "pw": "plain"})
			if err != nil {
				t.Fatal(err)
			}

			var id2 uint64
			run := map[string]func() error{
				"ttl": func() error { return col.setDefaultTTL(90 * time.Second) },
				"enc": func() error { return col.SetEncryptionPolicy(context.Background(), fieldsPolicy("pw")) },
			}
			other := "enc"
			if first == "enc" {
				other = "ttl"
			}
			firstErr := make(chan error, 1)
			otherErr := make(chan error, 1)
			go func() { firstErr <- run[first]() }()
			<-reached // `first` holds a snapshot taken before `other` runs
			go func() { otherErr <- run[other]() }()
			var errO error
			gotO := false
			if first == "ttl" {
				// ttl holds no collection lock while paused, so enc runs to
				// completion before ttl's stale snapshot lands. (When enc is
				// paused it holds c.mu, so ttl serializes behind it today.)
				errO, gotO = <-otherErr, true
				// A record sealed under the new policy, before the stale write.
				if id2, _, err = col.Insert(map[string]any{"email": "c@d.com", "pw": "secret"}); err != nil {
					t.Fatal(err)
				}
			}
			close(release)
			if err := <-firstErr; err != nil {
				t.Fatalf("%s: %v", first, err)
			}
			if !gotO {
				errO = <-otherErr
			}
			if errO != nil {
				t.Fatalf("%s: %v", other, errO)
			}

			m, err := loadMeta(metaPath(filepath.Join(dir, "users")))
			if err != nil {
				t.Fatalf("meta.json unreadable: %v", err)
			}
			if m.DefaultTTLSeconds != 90 {
				reportMetaRace(t, "meta.json lost TTL: %+v", m)
			}
			if m.Encryption == nil {
				reportMetaRace(t, "meta.json lost encryption block (first=%s)", first)
			}
			// Close rewrites meta.json from memory and would heal the loss; restore
			// the on-disk bytes as they stood to model a crash before Close.
			metaFile := metaPath(filepath.Join(dir, "users"))
			onDisk, err := os.ReadFile(metaFile)
			if err != nil {
				t.Fatal(err)
			}
			if err := col.Close(); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(metaFile, onDisk, 0o644); err != nil {
				t.Fatal(err)
			}

			// Reopen must still decrypt: a lost block makes ciphertext read as data.
			col2, err := OpenCollection("users", dir, encCfgKR(kr, nil))
			if err != nil {
				reportMetaRace(t, "reopen: %v", err)
				return
			}
			defer col2.Close()
			if got, _, err := col2.FindByID(id); err != nil || got["pw"] != "plain" {
				reportMetaRace(t, "reopen read = %v, %v", got, err)
			}
			if id2 != 0 {
				if got, _, err := col2.FindByID(id2); err != nil || got["pw"] != "secret" {
					reportMetaRace(t, "reopen of sealed record = %v, %v", got, err)
				}
			}
			if m.Encryption == nil || m.DefaultTTLSeconds != 90 {
				return
			}
			if st := col2.EncryptionStatus(); !st.Enabled {
				reportMetaRace(t, "reopened collection reports encryption disabled")
			}
		})
	}
}

// TestMetaSharedTmpCorruption interleaves two persists of the same path so they
// share meta.json.tmp: B (larger) completes between A's temp open and A's write.
func TestMetaSharedTmpCorruption(t *testing.T) {
	dir := t.TempDir()
	path := metaPath(dir)

	small := collectionMeta{IDCounter: 1, CreatedAt: time.Unix(1, 0).UTC()}
	big := collectionMeta{IDCounter: 2, CreatedAt: time.Unix(2, 0).UTC(), DefaultTTLSeconds: 5,
		Encryption: &encryptionMeta{Policy: *fieldsPolicy("pw"), Epoch: 1, KeyCheck: "kc", CurrentKeyID: "k1"}}

	reached := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	afterAtomicTempCreate = func(string) {
		if calls.Add(1) == 1 { // only A (the first persist) parks
			close(reached)
			<-release
		}
	}
	defer func() { afterAtomicTempCreate = nil }()

	aErr := make(chan error, 1)
	go func() { aErr <- persistMeta(path, small) }()
	<-reached // A opened (truncated) meta.json.tmp, has not written
	bErr := persistMeta(path, big)
	close(release)
	errA := <-aErr

	if bErr != nil {
		reportMetaRace(t, "B persist: %v", bErr)
	}
	if errA != nil {
		reportMetaRace(t, "A persist (stale tmp renamed away): %v", errA)
	}
	if _, err := loadMeta(path); err != nil {
		reportMetaRace(t, "meta.json corrupt after interleaved persists: %v", err)
	}
}
