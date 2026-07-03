package server

import (
	"fmt"
	"os"
	"time"

	"github.com/srjn45/filedbv2/engine"
	"gopkg.in/yaml.v3"
)

// APIKeyConfig is one scoped API key entry in the config file's `keys:` list.
type APIKeyConfig struct {
	Key   string `yaml:"key"`   // secret presented in the x-api-key header
	Name  string `yaml:"name"`  // human-readable principal name
	Scope string `yaml:"scope"` // "read" or "read-write" (default: read)
}

// Config holds all server configuration, loaded from CLI flags → env vars →
// config file, in priority order.
type Config struct {
	// Storage
	DataDir string `yaml:"data_dir"` // default: ./data

	// Network
	GRPCAddr    string `yaml:"grpc_addr"`    // default: :5433
	RESTAddr    string `yaml:"rest_addr"`    // default: :8080
	UnixSocket  string `yaml:"unix_socket"`  // default: /tmp/filedb.sock
	MetricsAddr string `yaml:"metrics_addr"` // default: :9090

	// TLS (optional — both cert and key must be set to enable)
	TLSCert string `yaml:"tls_cert"` // path to PEM certificate file
	TLSKey  string `yaml:"tls_key"`  // path to PEM private key file

	// Auth
	APIKey string `yaml:"api_key"` // legacy single read-write key; empty = no auth
	// Keys is an optional list of scoped API keys. Combined with APIKey (which,
	// when set, acts as an additional read-write key named "default"). If both
	// Keys and APIKey are empty, authentication is disabled.
	Keys []APIKeyConfig `yaml:"keys"`

	// Engine tuning
	SegmentMaxSize  int64         `yaml:"segment_max_size"`  // default: 4 MiB
	CompactInterval time.Duration `yaml:"compact_interval"`  // default: 5m
	CompactDirtyPct float64       `yaml:"compact_dirty_pct"` // default: 0.30

	// Durability
	SyncMode     string        `yaml:"sync_mode"`     // none|always|interval (default: none)
	SyncInterval time.Duration `yaml:"sync_interval"` // flush cadence for interval mode (default: 1s)

	// Transactions
	TxTimeout time.Duration `yaml:"tx_timeout"` // idle expiry for open transactions (default: 5m, 0 = disabled)

	// TTL
	DefaultTTL time.Duration `yaml:"default_ttl"` // default expiry for inserted records (default: 0 = never expire)

	// Watch
	WatchBufferSize int `yaml:"watch_buffer_size"` // per-subscriber event buffer (default: 64)
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		DataDir:         "./data",
		GRPCAddr:        ":5433",
		RESTAddr:        ":8080",
		UnixSocket:      "/tmp/filedb.sock",
		MetricsAddr:     ":9090",
		SegmentMaxSize:  4 * 1024 * 1024,
		CompactInterval: 5 * time.Minute,
		CompactDirtyPct: 0.30,
		SyncMode:        string(engine.SyncModeNone),
		SyncInterval:    engine.DefaultSyncInterval,
		TxTimeout:       5 * time.Minute,
		WatchBufferSize: engine.DefaultWatchBufferSize,
		DefaultTTL:      0,
	}
}

// EngineConfig converts server config into an engine.CollectionConfig.
func (c Config) EngineConfig() engine.CollectionConfig {
	return engine.CollectionConfig{
		SegmentMaxSize:  c.SegmentMaxSize,
		CompactInterval: c.CompactInterval,
		CompactDirtyPct: c.CompactDirtyPct,
		SyncMode:        engine.SyncMode(c.SyncMode),
		SyncInterval:    c.SyncInterval,
		WatchBufferSize: c.WatchBufferSize,
		DefaultTTL:      c.DefaultTTL,
	}
}

// fileConfig mirrors Config but uses a string for CompactInterval so yaml.v3
// can unmarshal human-readable durations like "5m" or "1h30m".
type fileConfig struct {
	DataDir         string         `yaml:"data_dir"`
	GRPCAddr        string         `yaml:"grpc_addr"`
	RESTAddr        string         `yaml:"rest_addr"`
	UnixSocket      string         `yaml:"unix_socket"`
	MetricsAddr     string         `yaml:"metrics_addr"`
	TLSCert         string         `yaml:"tls_cert"`
	TLSKey          string         `yaml:"tls_key"`
	APIKey          string         `yaml:"api_key"`
	Keys            []APIKeyConfig `yaml:"keys"`
	SegmentMaxSize  int64          `yaml:"segment_max_size"`
	CompactInterval string         `yaml:"compact_interval"`
	CompactDirtyPct float64        `yaml:"compact_dirty_pct"`
	SyncMode        string         `yaml:"sync_mode"`
	SyncInterval    string         `yaml:"sync_interval"`
	TxTimeout       string         `yaml:"tx_timeout"`
	WatchBufferSize int            `yaml:"watch_buffer_size"`
	DefaultTTL      string         `yaml:"default_ttl"`
}

// LoadConfigFile reads a YAML config file and returns a Config populated with
// its values, falling back to DefaultConfig() for any omitted fields.
func LoadConfigFile(path string) (Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return Config{}, fmt.Errorf("open config file: %w", err)
	}
	defer func() { _ = f.Close() }()

	// Start from defaults so omitted keys keep their default value.
	defaults := DefaultConfig()
	fc := fileConfig{
		DataDir:         defaults.DataDir,
		GRPCAddr:        defaults.GRPCAddr,
		RESTAddr:        defaults.RESTAddr,
		UnixSocket:      defaults.UnixSocket,
		MetricsAddr:     defaults.MetricsAddr,
		TLSCert:         defaults.TLSCert,
		TLSKey:          defaults.TLSKey,
		APIKey:          defaults.APIKey,
		Keys:            defaults.Keys,
		SegmentMaxSize:  defaults.SegmentMaxSize,
		CompactInterval: defaults.CompactInterval.String(),
		CompactDirtyPct: defaults.CompactDirtyPct,
		SyncMode:        defaults.SyncMode,
		SyncInterval:    defaults.SyncInterval.String(),
		TxTimeout:       defaults.TxTimeout.String(),
		WatchBufferSize: defaults.WatchBufferSize,
		DefaultTTL:      defaults.DefaultTTL.String(),
	}

	dec := yaml.NewDecoder(f)
	dec.KnownFields(true)
	if err := dec.Decode(&fc); err != nil {
		return Config{}, fmt.Errorf("parse config file: %w", err)
	}

	d, err := time.ParseDuration(fc.CompactInterval)
	if err != nil {
		return Config{}, fmt.Errorf("config file compact_interval %q: %w", fc.CompactInterval, err)
	}

	syncInterval, err := time.ParseDuration(fc.SyncInterval)
	if err != nil {
		return Config{}, fmt.Errorf("config file sync_interval %q: %w", fc.SyncInterval, err)
	}

	txTimeout, err := time.ParseDuration(fc.TxTimeout)
	if err != nil {
		return Config{}, fmt.Errorf("config file tx_timeout %q: %w", fc.TxTimeout, err)
	}

	defaultTTL, err := time.ParseDuration(fc.DefaultTTL)
	if err != nil {
		return Config{}, fmt.Errorf("config file default_ttl %q: %w", fc.DefaultTTL, err)
	}

	return Config{
		DataDir:         fc.DataDir,
		GRPCAddr:        fc.GRPCAddr,
		RESTAddr:        fc.RESTAddr,
		UnixSocket:      fc.UnixSocket,
		MetricsAddr:     fc.MetricsAddr,
		TLSCert:         fc.TLSCert,
		TLSKey:          fc.TLSKey,
		APIKey:          fc.APIKey,
		Keys:            fc.Keys,
		SegmentMaxSize:  fc.SegmentMaxSize,
		CompactInterval: d,
		CompactDirtyPct: fc.CompactDirtyPct,
		SyncMode:        fc.SyncMode,
		SyncInterval:    syncInterval,
		TxTimeout:       txTimeout,
		WatchBufferSize: fc.WatchBufferSize,
		DefaultTTL:      defaultTTL,
	}, nil
}
