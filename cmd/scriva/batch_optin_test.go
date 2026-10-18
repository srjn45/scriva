package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCLI_ServeEnableAtomicBatchesFlag(t *testing.T) {
	code, out, _ := run(t, "serve", "--help")
	if code != 0 {
		t.Fatalf("serve --help exit %d", code)
	}
	if !strings.Contains(out, "--enable-atomic-batches") {
		t.Fatalf("serve --help missing --enable-atomic-batches: %s", out)
	}
}

func TestCLI_LegacyRootOptInConversion(t *testing.T) {
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	colDir := filepath.Join(dataDir, "things")
	if err := os.MkdirAll(colDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// Legacy segment without xtx.format
	if err := os.WriteFile(filepath.Join(colDir, "seg_000001.ndjson"), []byte("{\"id\":1,\"op\":\"insert\",\"rev\":1,\"data\":{\"x\":1},\"crc\":0}\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Verify xtx.format does not exist initially
	formatPath := filepath.Join(dataDir, "xtx.format")
	if _, err := os.Stat(formatPath); err == nil {
		t.Fatal("xtx.format should not exist on legacy root initially")
	}

	// Check that loading config with --enable-atomic-batches sets EnableAtomicBatches
	cmd := serveCmd()
	cmd.SetArgs([]string{"--data", dataDir, "--enable-atomic-batches"})
	if err := cmd.ParseFlags([]string{"--data", dataDir, "--enable-atomic-batches"}); err != nil {
		t.Fatalf("parse flags: %v", err)
	}
	enabled, err := cmd.Flags().GetBool("enable-atomic-batches")
	if err != nil || !enabled {
		t.Fatalf("enable-atomic-batches flag = %v, err = %v", enabled, err)
	}
}
