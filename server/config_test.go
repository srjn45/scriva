package server

import (
	"os"
	"testing"
	"time"
)

func TestLoadConfigFile(t *testing.T) {
	t.Run("full config", func(t *testing.T) {
		f, err := os.CreateTemp(t.TempDir(), "filedb*.yaml")
		if err != nil {
			t.Fatal(err)
		}
		_, _ = f.WriteString(`
data_dir: /tmp/mydata
grpc_addr: :9000
rest_addr: :9001
unix_socket: /tmp/mydb.sock
api_key: secret
segment_max_size: 1048576
compact_interval: 10m
compact_dirty_pct: 0.5
sync_mode: interval
sync_interval: 2s
`)
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}

		cfg, err := LoadConfigFile(f.Name())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if cfg.DataDir != "/tmp/mydata" {
			t.Errorf("DataDir = %q, want /tmp/mydata", cfg.DataDir)
		}
		if cfg.GRPCAddr != ":9000" {
			t.Errorf("GRPCAddr = %q, want :9000", cfg.GRPCAddr)
		}
		if cfg.RESTAddr != ":9001" {
			t.Errorf("RESTAddr = %q, want :9001", cfg.RESTAddr)
		}
		if cfg.UnixSocket != "/tmp/mydb.sock" {
			t.Errorf("UnixSocket = %q, want /tmp/mydb.sock", cfg.UnixSocket)
		}
		if cfg.APIKey != "secret" {
			t.Errorf("APIKey = %q, want secret", cfg.APIKey)
		}
		if cfg.SegmentMaxSize != 1048576 {
			t.Errorf("SegmentMaxSize = %d, want 1048576", cfg.SegmentMaxSize)
		}
		if cfg.CompactInterval != 10*time.Minute {
			t.Errorf("CompactInterval = %v, want 10m", cfg.CompactInterval)
		}
		if cfg.CompactDirtyPct != 0.5 {
			t.Errorf("CompactDirtyPct = %v, want 0.5", cfg.CompactDirtyPct)
		}
		if cfg.SyncMode != "interval" {
			t.Errorf("SyncMode = %q, want interval", cfg.SyncMode)
		}
		if cfg.SyncInterval != 2*time.Second {
			t.Errorf("SyncInterval = %v, want 2s", cfg.SyncInterval)
		}
	})

	t.Run("partial config uses defaults", func(t *testing.T) {
		f, err := os.CreateTemp(t.TempDir(), "filedb*.yaml")
		if err != nil {
			t.Fatal(err)
		}
		_, _ = f.WriteString("data_dir: /custom\n")
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}

		cfg, err := LoadConfigFile(f.Name())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		defaults := DefaultConfig()
		if cfg.DataDir != "/custom" {
			t.Errorf("DataDir = %q, want /custom", cfg.DataDir)
		}
		if cfg.GRPCAddr != defaults.GRPCAddr {
			t.Errorf("GRPCAddr = %q, want %q", cfg.GRPCAddr, defaults.GRPCAddr)
		}
		if cfg.CompactInterval != defaults.CompactInterval {
			t.Errorf("CompactInterval = %v, want %v", cfg.CompactInterval, defaults.CompactInterval)
		}
	})

	t.Run("missing file returns error", func(t *testing.T) {
		_, err := LoadConfigFile("/nonexistent/filedb.yaml")
		if err == nil {
			t.Fatal("expected error for missing file")
		}
	})

	t.Run("invalid duration returns error", func(t *testing.T) {
		f, err := os.CreateTemp(t.TempDir(), "filedb*.yaml")
		if err != nil {
			t.Fatal(err)
		}
		_, _ = f.WriteString("compact_interval: notaduration\n")
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}

		_, err = LoadConfigFile(f.Name())
		if err == nil {
			t.Fatal("expected error for invalid duration")
		}
	})

	t.Run("unknown field returns error", func(t *testing.T) {
		f, err := os.CreateTemp(t.TempDir(), "filedb*.yaml")
		if err != nil {
			t.Fatal(err)
		}
		_, _ = f.WriteString("unknown_field: oops\n")
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}

		_, err = LoadConfigFile(f.Name())
		if err == nil {
			t.Fatal("expected error for unknown field")
		}
	})
}
