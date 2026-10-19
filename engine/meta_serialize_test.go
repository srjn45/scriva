package engine

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/srjn45/scriva/crypto"
)

// failMetaRename makes the atomic rename of any meta.json fail while the
// returned flag is set (fault injection; restored on cleanup).
func failMetaRename(t *testing.T) *atomic.Bool {
	t.Helper()
	var fail atomic.Bool
	atomicRename = func(oldpath, newpath string) error {
		if fail.Load() && filepath.Base(newpath) == metaFilename {
			return errors.New("injected rename failure")
		}
		return os.Rename(oldpath, newpath)
	}
	t.Cleanup(func() { atomicRename = os.Rename })
	return &fail
}

func noMetaTemps(t *testing.T, dir string) {
	t.Helper()
	left, _ := filepath.Glob(filepath.Join(dir, "meta.json.*"))
	if len(left) != 0 {
		t.Fatalf("leftover meta temp files: %v", left)
	}
}

// 300 free-running mixed persists: no corrupt JSON, no failed rename, and the
// final file carries every setting.
func TestMetaWriteHammer(t *testing.T) {
	root := t.TempDir()
	kr, _ := crypto.NewKeyring("k1", mustKey(t))
	cfg := encCfgKR(kr, nil)
	cfg.SegmentMaxSize = 512 // rotate (and persist) often
	col, err := OpenCollection("users", root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := col.SetEncryptionPolicy(context.Background(), fieldsPolicy("pw")); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, "users")

	var wg sync.WaitGroup
	errc := make(chan error, 1024)
	report := func(err error) {
		if err != nil {
			select {
			case errc <- err:
			default:
			}
		}
	}
	const iters = 300
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				switch g {
				case 0:
					report(col.setDefaultTTL(time.Duration(60+i%5) * time.Second))
				case 1:
					report(col.writeMeta(nil))
				case 2:
					_, _, err := col.Insert(map[string]any{"pw": "x", "n": float64(i)})
					report(err)
				case 3:
					report(col.CommitTx([]txOp{{kind: txOpInsert, id: col.ReserveID(), data: map[string]any{"pw": "y"}, ts: time.Now()}}))
				}
				if g == 1 {
					if _, err := loadMeta(metaPath(dir)); err != nil {
						report(err)
					}
				}
			}
		}(g)
	}
	wg.Wait()
	close(errc)
	for err := range errc {
		t.Fatal(err)
	}
	if err := col.Close(); err != nil {
		t.Fatal(err)
	}
	m, err := loadMeta(metaPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	if m.Encryption == nil || m.DefaultTTLSeconds < 60 || m.IDCounter == 0 {
		t.Fatalf("final meta lost settings: %+v", m)
	}
	noMetaTemps(t, dir)
}

func TestMetaEncryptionPersistFailure(t *testing.T) {
	root := t.TempDir()
	kr, _ := crypto.NewKeyring("k1", mustKey(t))
	col, err := OpenCollection("users", root, encCfgKR(kr, nil))
	if err != nil {
		t.Fatal(err)
	}
	defer col.Close()
	before := col.EncryptionStatus()

	fail := failMetaRename(t)
	fail.Store(true)
	if err := col.SetEncryptionPolicy(context.Background(), fieldsPolicy("pw")); err == nil {
		t.Fatal("SetEncryptionPolicy succeeded despite rename failure")
	}
	if after := col.EncryptionStatus(); after != before {
		t.Fatalf("EncryptionStatus changed after failed persist: %+v -> %+v", before, after)
	}
	// A concurrent persist after the failure must write the old policy.
	fail.Store(false)
	if err := col.writeMeta(nil); err != nil {
		t.Fatal(err)
	}
	m, err := loadMeta(metaPath(filepath.Join(root, "users")))
	if err != nil {
		t.Fatal(err)
	}
	if m.Encryption != nil {
		t.Fatalf("candidate block leaked into meta.json: %+v", m.Encryption)
	}
	noMetaTemps(t, filepath.Join(root, "users"))
}

func TestMetaSetDefaultTTLPersistFailure(t *testing.T) {
	root := t.TempDir()
	col, err := OpenCollection("users", root, testCfg())
	if err != nil {
		t.Fatal(err)
	}
	defer col.Close()
	fail := failMetaRename(t)
	fail.Store(true)
	if err := col.setDefaultTTL(90 * time.Second); err == nil {
		t.Fatal("setDefaultTTL succeeded despite rename failure")
	}
	if got := col.explicitDefaultTTLSecs.Load(); got != 0 {
		t.Fatalf("explicitDefaultTTLSecs = %d after failed persist", got)
	}
	if exp := col.resolveInsertExpiry(time.Time{}); exp != 0 {
		t.Fatalf("insert expiry stamped after failed setDefaultTTL: %d", exp)
	}
}

func TestMetaRotationPersistFailureIsPostCommit(t *testing.T) {
	root := t.TempDir()
	cfg := testCfg()
	cfg.SegmentMaxSize = 64
	col, err := OpenCollection("users", root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	fail := failMetaRename(t)
	fail.Store(true)
	id, _, err := col.Insert(map[string]any{"v": "a payload larger than the segment limit"})
	if !errors.Is(err, ErrPostCommit) {
		t.Fatalf("Insert err = %v, want PostCommitError", err)
	}
	fail.Store(false)
	if err := col.Close(); err != nil {
		t.Fatal(err)
	}
	col2, err := OpenCollection("users", root, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer col2.Close()
	if got, _, err := col2.FindByID(id); err != nil || got["v"] == nil {
		t.Fatalf("record not durable after reopen: %v, %v", got, err)
	}
}

func TestMetaCommitTxPersistFailureIsPostCommit(t *testing.T) {
	root := t.TempDir()
	col, err := OpenCollection("users", root, testCfg())
	if err != nil {
		t.Fatal(err)
	}
	fail := failMetaRename(t)
	fail.Store(true)
	id := col.ReserveID()
	err = col.CommitTx([]txOp{{kind: txOpInsert, id: id, data: map[string]any{"v": 1.0}, ts: time.Now()}})
	var pce *PostCommitError
	if !errors.As(err, &pce) || pce.Stage != "meta" {
		t.Fatalf("CommitTx err = %v, want PostCommitError stage meta", err)
	}
	fail.Store(false)
	if _, _, err := col.FindByID(id); err != nil {
		t.Fatalf("committed record invisible: %v", err)
	}
	if err := col.Close(); err != nil {
		t.Fatal(err)
	}
	col2, err := OpenCollection("users", root, testCfg())
	if err != nil {
		t.Fatal(err)
	}
	defer col2.Close()
	if _, _, err := col2.FindByID(id); err != nil {
		t.Fatalf("record lost after reopen: %v", err)
	}
}
