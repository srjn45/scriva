package engine_test

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/srjn45/scriva/engine"
)

func TestDBLock_SameProcess(t *testing.T) {
	dir := t.TempDir()

	db1, err := engine.Open(dir, engine.CollectionConfig{})
	if err != nil {
		t.Fatalf("first open failed: %v", err)
	}
	defer db1.Close()

	// Double open in the same process should fail
	_, err = engine.Open(dir, engine.CollectionConfig{})
	if !errors.Is(err, engine.ErrDatabaseLocked) {
		t.Fatalf("expected ErrDatabaseLocked, got %v", err)
	}
}

func TestDBLock_PathVariations(t *testing.T) {
	dir := t.TempDir()
	
	// Create a symlink
	symlinkDir := filepath.Join(t.TempDir(), "symlink")
	if err := os.Symlink(dir, symlinkDir); err != nil {
		t.Fatalf("create symlink: %v", err)
	}

	// Open with absolute path
	db1, err := engine.Open(dir, engine.CollectionConfig{})
	if err != nil {
		t.Fatalf("first open failed: %v", err)
	}
	
	// Open via relative path
	pwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	relPath, err := filepath.Rel(pwd, dir)
	if err != nil {
		t.Fatal(err)
	}
	_, err = engine.Open(relPath, engine.CollectionConfig{})
	if !errors.Is(err, engine.ErrDatabaseLocked) {
		t.Fatalf("expected ErrDatabaseLocked for rel path, got %v", err)
	}

	// Open via symlink
	_, err = engine.Open(symlinkDir, engine.CollectionConfig{})
	if !errors.Is(err, engine.ErrDatabaseLocked) {
		t.Fatalf("expected ErrDatabaseLocked for symlink, got %v", err)
	}

	// Release lock and try again
	db1.Close()

	db2, err := engine.Open(symlinkDir, engine.CollectionConfig{})
	if err != nil {
		t.Fatalf("open after close failed: %v", err)
	}
	db2.Close()
}

func TestDBLock_SeparateProcess(t *testing.T) {
	if os.Getenv("TEST_DIRLOCK_PROCESS") == "1" {
		dir := os.Getenv("TEST_DIRLOCK_DIR")
		db, err := engine.Open(dir, engine.CollectionConfig{})
		if err != nil {
			if errors.Is(err, engine.ErrDatabaseLocked) {
				os.Exit(42)
			}
			os.Exit(1)
		}
		// Hold lock, wait for kill
		time.Sleep(1 * time.Hour)
		db.Close()
		os.Exit(0)
	}

	dir := t.TempDir()
	db1, err := engine.Open(dir, engine.CollectionConfig{})
	if err != nil {
		t.Fatalf("first open failed: %v", err)
	}

	// Start second process
	cmd := exec.Command(os.Args[0], "-test.run=TestDBLock_SeparateProcess")
	cmd.Env = append(os.Environ(), "TEST_DIRLOCK_PROCESS=1", "TEST_DIRLOCK_DIR="+dir)
	err = cmd.Run()
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		if exitErr.ExitCode() != 42 {
			t.Fatalf("expected exit code 42 (ErrDatabaseLocked), got %d", exitErr.ExitCode())
		}
	} else {
		t.Fatalf("expected second process to fail with exit code, got %v", err)
	}

	// Release lock
	db1.Close()

	// Start third process (should succeed and hold lock, then we kill it)
	cmd2 := exec.Command(os.Args[0], "-test.run=TestDBLock_SeparateProcess")
	cmd2.Env = append(os.Environ(), "TEST_DIRLOCK_PROCESS=1", "TEST_DIRLOCK_DIR="+dir)
	if err := cmd2.Start(); err != nil {
		t.Fatalf("start third process failed: %v", err)
	}
	
	// Give it time to acquire lock
	time.Sleep(500 * time.Millisecond)

	// In the original test process, verify we can't open it
	_, err = engine.Open(dir, engine.CollectionConfig{})
	if !errors.Is(err, engine.ErrDatabaseLocked) {
		t.Fatalf("expected ErrDatabaseLocked while third process holds lock, got %v", err)
	}

	// Kill third process
	if err := cmd2.Process.Kill(); err != nil {
		t.Fatalf("kill third process: %v", err)
	}
	cmd2.Wait()

	// After kill, OS lock should be released, we can open it again
	db3, err := engine.Open(dir, engine.CollectionConfig{})
	if err != nil {
		t.Fatalf("open after process kill failed: %v", err)
	}
	db3.Close()
}
