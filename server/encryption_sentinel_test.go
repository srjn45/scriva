package server

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/srjn45/scriva/crypto"
	"github.com/srjn45/scriva/engine"
)

func encDBConfig(t *testing.T, key []byte) engine.CollectionConfig {
	t.Helper()
	kr, err := crypto.NewKeyring("k1", key)
	if err != nil {
		t.Fatal(err)
	}
	return engine.CollectionConfig{
		SegmentMaxSize:  4 * 1024 * 1024,
		CompactInterval: 24 * time.Hour,
		CompactDirtyPct: 0.30,
		KeyProvider:     kr,
		Encryption:      &engine.EncryptionPolicy{Mode: engine.EncryptModeFields, Fields: []string{"password"}},
	}
}

// An encrypted collection's snapshot carries encryption.sentinel; extracting it
// (restore / follower bootstrap) keeps it, and the restored root opens and
// decrypts. Losing meta.json afterwards fails closed on the restored copy.
func TestSnapshotRestorePreservesEncryptionSentinel(t *testing.T) {
	key, err := crypto.NewKey()
	if err != nil {
		t.Fatal(err)
	}
	src := t.TempDir()
	db, err := engine.Open(src, encDBConfig(t, key))
	if err != nil {
		t.Fatal(err)
	}
	col, err := db.CreateCollection("users")
	if err != nil {
		t.Fatal(err)
	}
	id, _, err := col.Insert(map[string]any{"email": "a@b.com", "password": "hunter2"})
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := db.SnapshotTo(&buf); err != nil {
		t.Fatal(err)
	}
	db.Close()

	dst := filepath.Join(t.TempDir(), "data")
	if err := extractSnapshot(&buf, dst); err != nil {
		t.Fatalf("extract: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dst, "users", "encryption.sentinel")); err != nil {
		t.Fatalf("sentinel not restored: %v", err)
	}

	db2, err := engine.Open(dst, encDBConfig(t, key))
	if err != nil {
		t.Fatalf("open restored: %v", err)
	}
	c2, err := db2.Collection("users")
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := c2.FindByID(id)
	if err != nil || got["password"] != "hunter2" {
		t.Fatalf("restored read = %v, %v", got, err)
	}
	db2.Close()

	// The restored sentinel is what makes a later meta.json loss detectable.
	if err := os.Remove(filepath.Join(dst, "users", "meta.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.Open(dst, encDBConfig(t, key)); !errors.Is(err, engine.ErrEncryptionMetaIntegrity) {
		t.Fatalf("open with lost meta: got %v, want ErrEncryptionMetaIntegrity", err)
	}
}

func TestEncryptionMetaIntegrityGRPCStatus(t *testing.T) {
	err := fmt.Errorf("wrap: %w", &engine.EncryptionMetaIntegrityError{Collection: "users", Evidence: engine.EvidenceSentinel, File: "meta.json", Problem: "missing"})
	for name, mapped := range map[string]error{"lookup": notFoundOrInvalid(err), "create": createErr(err)} {
		if c := status.Code(mapped); c != codes.DataLoss {
			t.Errorf("%s: code = %v, want DataLoss", name, c)
		}
		if strings.Contains(mapped.Error(), "hunter2") {
			t.Error("leak")
		}
	}
}
