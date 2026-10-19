//nolint:errcheck
package engine

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/srjn45/scriva/crypto"
	"github.com/srjn45/scriva/store"
)

func ecd(root string) string { return filepath.Join(root, "users") }

const eiSecret = "hunter2-very-secret"

// eiSeed creates an encrypted collection holding one record, closes it, and
// returns the dir and key.
func eiSeed(t *testing.T) (string, []byte) {
	t.Helper()
	dir := t.TempDir()
	key := mustKey(t)
	col, err := OpenCollection("users", dir, encCfg(t, key, fieldsPolicy("password")))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := col.Insert(map[string]any{"email": "a@b.com", "password": eiSecret}); err != nil {
		t.Fatal(err)
	}
	if err := col.Close(); err != nil {
		t.Fatal(err)
	}
	return dir, key
}

func eiKeyCfg(t *testing.T, key []byte) CollectionConfig {
	kr, err := crypto.NewKeyring("k1", key)
	if err != nil {
		t.Fatal(err)
	}
	cfg := testCfg()
	cfg.KeyProvider = kr
	return cfg
}

func eiRead(t *testing.T, p string) []byte {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// eiAssertRefused opens the collection and requires ErrEncryptionMetaIntegrity
// with the given evidence, no secret in the message, and meta.json untouched.
func eiAssertRefused(t *testing.T, dir string, cfg CollectionConfig, evidence string, wantCorruptCopy bool) {
	t.Helper()
	mp := filepath.Join(ecd(dir), metaFilename)
	before, rerr := os.ReadFile(mp)
	col, err := OpenCollection("users", dir, cfg)
	if err == nil {
		col.Close()
		t.Fatal("open succeeded; want ErrEncryptionMetaIntegrity")
	}
	if !errors.Is(err, ErrEncryptionMetaIntegrity) {
		t.Fatalf("got %v, want ErrEncryptionMetaIntegrity", err)
	}
	if errors.Is(err, crypto.ErrWrongEncryptionKey) || errors.Is(err, crypto.ErrKeyUnavailable) {
		t.Fatalf("error must be distinct from key errors: %v", err)
	}
	var ie *EncryptionMetaIntegrityError
	if !errors.As(err, &ie) || ie.Evidence != evidence || ie.Collection != "users" {
		t.Fatalf("typed error = %+v, want evidence %q", ie, evidence)
	}
	for _, bad := range []string{eiSecret, "scrivaenc", "k1"} {
		if strings.Contains(err.Error(), bad) {
			t.Fatalf("error leaks %q: %v", bad, err)
		}
	}
	after, aerr := os.ReadFile(mp)
	if (rerr == nil) != (aerr == nil) || !bytes.Equal(before, after) {
		t.Fatalf("meta.json changed by a refused open (before err=%v, after err=%v)", rerr, aerr)
	}
	copies, _ := filepath.Glob(mp + ".corrupt-*")
	if wantCorruptCopy {
		if len(copies) != 1 || !bytes.Equal(eiRead(t, copies[0]), before) {
			t.Fatalf("want one exact .corrupt-* copy, got %v", copies)
		}
	} else if len(copies) != 0 {
		t.Fatalf("unexpected corrupt copies: %v", copies)
	}
}

func TestEncryptionMetaIntegrity_E1_IndexEpoch(t *testing.T) {
	cases := map[string]func(dir string){
		"missing":     func(dir string) { os.Remove(filepath.Join(ecd(dir), metaFilename)) },
		"unparseable": func(dir string) { os.WriteFile(filepath.Join(ecd(dir), metaFilename), []byte("{not json"), 0o644) },
		"no-block": func(dir string) {
			os.WriteFile(filepath.Join(ecd(dir), metaFilename), []byte(`{"id_counter":1,"created_at":"2024-01-01T00:00:00Z"}`), 0o644)
		},
		"lower-epoch": nil, // handled below
	}
	for name, damage := range cases {
		if damage == nil {
			continue
		}
		t.Run(name, func(t *testing.T) {
			dir, key := eiSeed(t)
			os.Remove(filepath.Join(ecd(dir), encryptionSentinelFile)) // force E1 (not E2)
			damage(dir)
			eiAssertRefused(t, dir, eiKeyCfg(t, key), EvidenceIndexEpoch, name == "unparseable")
		})
	}
}

func TestEncryptionMetaIntegrity_LowerMetaEpoch(t *testing.T) {
	dir, key := eiSeed(t)
	m, err := loadMeta(filepath.Join(ecd(dir), metaFilename))
	if err != nil {
		t.Fatal(err)
	}
	m.Encryption.Epoch = 0
	if err := persistMeta(filepath.Join(ecd(dir), metaFilename), m); err != nil {
		t.Fatal(err)
	}
	eiAssertRefused(t, dir, eiKeyCfg(t, key), EvidenceIndexEpoch, false)
}

func TestEncryptionMetaIntegrity_E2_Sentinel(t *testing.T) {
	for name, damage := range map[string]func(dir string){
		"missing": func(dir string) { os.Remove(filepath.Join(ecd(dir), metaFilename)) },
		"unparseable": func(dir string) {
			os.WriteFile(filepath.Join(ecd(dir), metaFilename), []byte("\x00\x01garbage"), 0o644)
		},
		"no-block": func(dir string) {
			os.WriteFile(filepath.Join(ecd(dir), metaFilename), []byte(`{"id_counter":1}`), 0o644)
		},
	} {
		t.Run(name, func(t *testing.T) {
			// A plaintext-looking collection (no entry epoch) that carries a
			// sentinel: epoch evidence is absent, only E2 can fire.
			dir := t.TempDir()
			col, err := OpenCollection("users", dir, testCfg())
			if err != nil {
				t.Fatal(err)
			}
			col.Insert(map[string]any{"email": "a@b.com"})
			col.Close()
			if _, err := ensureSentinel(ecd(dir)); err != nil {
				t.Fatal(err)
			}
			damage(dir)
			eiAssertRefused(t, dir, testCfg(), EvidenceSentinel, name == "unparseable")
		})
	}
}

// E3: legacy epoch-0 ciphertext (written before epochs existed), no sentinel:
// only the marker scan can tell, and only when meta is missing/unparseable.
func TestEncryptionMetaIntegrity_E3_MarkerScan(t *testing.T) {
	for name, damage := range map[string]func(dir string){
		"missing":     func(dir string) { os.Remove(filepath.Join(ecd(dir), metaFilename)) },
		"unparseable": func(dir string) { os.WriteFile(filepath.Join(ecd(dir), metaFilename), []byte("{"), 0o644) },
	} {
		t.Run(name, func(t *testing.T) {
			dir, key := eiSeed(t)
			// Rewrite the stored entries as epoch-0 legacy ciphertext and drop
			// the sentinel and the persisted index.
			seg := filepath.Join(ecd(dir), "seg_000001.ndjson")
			raw := eiRead(t, seg)
			if !bytes.Contains(raw, []byte(`\u0000scrivaenc:`)) {
				t.Fatalf("segment has no envelope marker: %s", raw)
			}
			e, derr := store.Decode(bytes.TrimSpace(raw))
			if derr != nil {
				t.Fatal(derr)
			}
			e.Epoch = 0
			enc, eerr := store.Encode(e)
			if eerr != nil {
				t.Fatal(eerr)
			}
			raw = enc
			if err := os.WriteFile(seg, raw, 0o644); err != nil {
				t.Fatal(err)
			}
			os.Remove(filepath.Join(ecd(dir), "index.json"))
			os.Remove(filepath.Join(ecd(dir), encryptionSentinelFile))
			damage(dir)
			eiAssertRefused(t, dir, eiKeyCfg(t, key), EvidenceSegmentMarker, name == "unparseable")
		})
	}
}

// A marker split across the scan buffer boundary is still found.
func TestFileContainsAcrossChunks(t *testing.T) {
	p := filepath.Join(t.TempDir(), "f")
	pad := bytes.Repeat([]byte("x"), 1<<20-5)
	os.WriteFile(p, append(append(pad, encryptionMarkerOnDisk...), "tail"...), 0o644)
	if !fileContains(p, encryptionMarkerOnDisk) {
		t.Fatal("marker across chunk boundary not found")
	}
	os.WriteFile(p, pad, 0o644)
	if fileContains(p, encryptionMarkerOnDisk) {
		t.Fatal("false positive")
	}
}

// Plaintext collections with the same corruption keep today's behaviour.
func TestEncryptionMetaIntegrity_PlaintextRegenerates(t *testing.T) {
	for name, damage := range map[string]func(dir string){
		"missing":     func(dir string) { os.Remove(filepath.Join(ecd(dir), metaFilename)) },
		"unparseable": func(dir string) { os.WriteFile(filepath.Join(ecd(dir), metaFilename), []byte("{nope"), 0o644) },
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			col, err := OpenCollection("users", dir, testCfg())
			if err != nil {
				t.Fatal(err)
			}
			id, _, _ := col.Insert(map[string]any{"v": "plain"})
			col.Close()
			damage(dir)
			col2, err := OpenCollection("users", dir, testCfg())
			if err != nil {
				t.Fatalf("plaintext collection must open: %v", err)
			}
			defer col2.Close()
			if got, _, err := col2.FindByID(id); err != nil || got["v"] != "plain" {
				t.Fatalf("FindByID = %v, %v", got, err)
			}
			if _, err := loadMeta(filepath.Join(ecd(dir), metaFilename)); err != nil {
				t.Fatalf("meta.json not regenerated: %v", err)
			}
			if c, _ := filepath.Glob(filepath.Join(ecd(dir), "meta.json.corrupt-*")); len(c) != 0 {
				t.Fatalf("plaintext open must not leave corrupt copies: %v", c)
			}
		})
	}
}

func TestEncryptionSentinel_WrittenAndSelfHealed(t *testing.T) {
	dir, key := eiSeed(t)
	sp := filepath.Join(ecd(dir), encryptionSentinelFile)
	if _, err := os.Stat(sp); err != nil {
		t.Fatalf("sentinel not written on enable: %v", err)
	}
	// Valid meta + missing sentinel: open succeeds, decrypts, recreates it.
	os.Remove(sp)
	col, err := OpenCollection("users", dir, eiKeyCfg(t, key))
	if err != nil {
		t.Fatalf("open with valid meta, no sentinel: %v", err)
	}
	defer col.Close()
	if _, err := os.Stat(sp); err != nil {
		t.Fatalf("sentinel not self-healed: %v", err)
	}
	if got, _, err := col.FindByID(1); err != nil || got["password"] != eiSecret {
		t.Fatalf("FindByID after self-heal = %v, %v", got, err)
	}
}

func TestEncryptionMetaIntegrity_RestartAndWrongKey(t *testing.T) {
	dir, key := eiSeed(t)
	col, err := OpenCollection("users", dir, eiKeyCfg(t, key))
	if err != nil {
		t.Fatalf("restart with correct key: %v", err)
	}
	col.Close()
	// Wrong key still fails as ErrWrongEncryptionKey, not an integrity error.
	_, err = OpenCollection("users", dir, eiKeyCfg(t, mustKey(t)))
	if !errors.Is(err, crypto.ErrWrongEncryptionKey) || errors.Is(err, ErrEncryptionMetaIntegrity) {
		t.Fatalf("got %v, want ErrWrongEncryptionKey only", err)
	}
}

// SetEncryptionPolicy on a plaintext collection writes the sentinel too.
func TestEncryptionSentinel_WrittenOnSetPolicy(t *testing.T) {
	dir := t.TempDir()
	key := mustKey(t)
	col, err := OpenCollection("users", dir, eiKeyCfg(t, key))
	if err != nil {
		t.Fatal(err)
	}
	defer col.Close()
	if sentinelExists(ecd(dir)) {
		t.Fatal("plaintext collection must not have a sentinel")
	}
	if err := col.SetEncryptionPolicy(context.Background(), fieldsPolicy("password")); err != nil {
		t.Fatal(err)
	}
	if !sentinelExists(ecd(dir)) {
		t.Fatal("sentinel missing after enabling encryption")
	}
}

func TestEncryptionMetaIntegrity_NotTolerableUnderPolicyReport(t *testing.T) {
	dir, key := eiSeed(t)
	os.Remove(filepath.Join(ecd(dir), metaFilename))
	cfg := eiKeyCfg(t, key)
	cfg.IntegrityPolicy = PolicyReport
	if _, err := OpenCollection("users", dir, cfg); !errors.Is(err, ErrEncryptionMetaIntegrity) {
		t.Fatalf("PolicyReport: got %v, want ErrEncryptionMetaIntegrity", err)
	}
}

// Verify flags the lost block; Repair refuses to regenerate it.
func TestEncryptionMetaIntegrity_VerifyRepair(t *testing.T) {
	parent := t.TempDir()
	dir := parent
	col, err := OpenCollection("users", dir, encCfg(t, mustKey(t), fieldsPolicy("password")))
	if err != nil {
		t.Fatal(err)
	}
	col.Insert(map[string]any{"password": eiSecret})
	col.Close()
	os.Remove(filepath.Join(ecd(dir), metaFilename))
	rep, err := VerifyDir(context.Background(), parent, VerifyOptions{Mode: VerifyQuick})
	if err != nil {
		t.Fatal(err)
	}
	wantCodes(t, rep.Collections[0], CodeEncryptionMetaLost)
}

func TestEncryptionMetaIntegrity_RepairRefusesAndSentinelSurvivesCompaction(t *testing.T) {
	parent := t.TempDir()
	key := mustKey(t)
	col, err := OpenCollection("users", parent, encCfg(t, key, fieldsPolicy("password")))
	if err != nil {
		t.Fatal(err)
	}
	col.Insert(map[string]any{"password": eiSecret})
	if err := col.CompactNow(); err != nil {
		t.Fatal(err)
	}
	col.Close()
	cd := filepath.Join(parent, "users")
	if !sentinelExists(cd) {
		t.Fatal("sentinel lost across compaction/close")
	}
	if !SnapshotFileAllowed(encryptionSentinelFile) {
		t.Fatal("sentinel not in snapshot allow-list")
	}
	os.Remove(filepath.Join(cd, metaFilename))
	rep, err := Repair(context.Background(), parent, RepairOptions{AllowMetaReset: true})
	if err == nil {
		t.Fatal("repair must report the blocked collection")
	}
	if len(rep.Blocked()) != 1 {
		t.Fatalf("repair must block an encrypted collection without meta.json: %+v", rep.Collections)
	}
	if _, err := os.Stat(filepath.Join(cd, metaFilename)); err == nil {
		t.Fatal("repair regenerated meta.json of an encrypted collection")
	}
}
