package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/srjn45/filedbv2/engine"
	"github.com/srjn45/filedbv2/internal/auth"
	"github.com/srjn45/filedbv2/internal/metrics"
	pb "github.com/srjn45/filedbv2/internal/pb/proto"
	"github.com/srjn45/filedbv2/server"
)

// Build information, injected at release time via -ldflags -X (see .goreleaser.yml).
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	if err := rootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}

func rootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:     "filedb",
		Short:   "FileDB — lightweight append-only file database",
		Version: version,
	}
	root.SetVersionTemplate("filedb {{.Version}}\n")
	root.AddCommand(serveCmd(), versionCmd())
	return root
}

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version, commit, and build date",
		Run: func(cmd *cobra.Command, _ []string) {
			fmt.Printf("filedb %s (commit %s, built %s)\n", version, commit, date)
		},
	}
}

func serveCmd() *cobra.Command {
	cfg := server.DefaultConfig()
	var configFile string

	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Start the FileDB server",
		RunE: func(cmd *cobra.Command, _ []string) error {
			// If a config file was given, load it (overrides defaults).
			// Then re-apply any flags that were explicitly set on the CLI
			// (CLI always wins over config file).
			if configFile != "" {
				fileCfg, err := server.LoadConfigFile(configFile)
				if err != nil {
					return err
				}
				// Overlay: start from file config, then re-apply explicit flags.
				merged := fileCfg
				cmd.Flags().Visit(func(f *pflag.Flag) {
					switch f.Name {
					case "data":
						merged.DataDir = cfg.DataDir
					case "grpc-addr":
						merged.GRPCAddr = cfg.GRPCAddr
					case "rest-addr":
						merged.RESTAddr = cfg.RESTAddr
					case "socket":
						merged.UnixSocket = cfg.UnixSocket
					case "api-key":
						merged.APIKey = cfg.APIKey
					case "segment-size":
						merged.SegmentMaxSize = cfg.SegmentMaxSize
					case "compact-interval":
						merged.CompactInterval = cfg.CompactInterval
					case "compact-dirty":
						merged.CompactDirtyPct = cfg.CompactDirtyPct
					case "sync":
						merged.SyncMode = cfg.SyncMode
					case "sync-interval":
						merged.SyncInterval = cfg.SyncInterval
					case "tx-timeout":
						merged.TxTimeout = cfg.TxTimeout
					case "default-ttl":
						merged.DefaultTTL = cfg.DefaultTTL
					case "watch-buffer":
						merged.WatchBufferSize = cfg.WatchBufferSize
					case "metrics-addr":
						merged.MetricsAddr = cfg.MetricsAddr
					case "tls-cert":
						merged.TLSCert = cfg.TLSCert
					case "tls-key":
						merged.TLSKey = cfg.TLSKey
					case "log-level":
						merged.LogLevel = cfg.LogLevel
					case "log-format":
						merged.LogFormat = cfg.LogFormat
					case "slow-query-ms":
						merged.SlowQueryMs = cfg.SlowQueryMs
					case "max-concurrent-streams":
						merged.MaxConcurrentStreams = cfg.MaxConcurrentStreams
					case "max-inflight":
						merged.MaxInflight = cfg.MaxInflight
					case "rate-limit":
						merged.RateLimit = cfg.RateLimit
					case "otlp-endpoint":
						merged.OTLPEndpoint = cfg.OTLPEndpoint
					case "otlp-sample-ratio":
						merged.OTLPSampleRatio = cfg.OTLPSampleRatio
					case "replicate-from":
						merged.ReplicateFrom = cfg.ReplicateFrom
					case "replicate-id":
						merged.FollowerID = cfg.FollowerID
					case "replication-ring-size":
						merged.ReplicationRingSize = cfg.ReplicationRingSize
					}
				})
				cfg = merged
			}
			return serve(cfg, configFile)
		},
	}

	f := cmd.Flags()
	f.StringVar(&configFile, "config", "", "Path to YAML config file (filedb.yaml)")
	f.StringVar(&cfg.DataDir, "data", cfg.DataDir, "Data directory")
	f.StringVar(&cfg.GRPCAddr, "grpc-addr", cfg.GRPCAddr, "gRPC listen address")
	f.StringVar(&cfg.RESTAddr, "rest-addr", cfg.RESTAddr, "REST listen address")
	f.StringVar(&cfg.UnixSocket, "socket", cfg.UnixSocket, "Unix socket path")
	f.StringVar(&cfg.APIKey, "api-key", os.Getenv("FILEDB_API_KEY"), "API key (env: FILEDB_API_KEY)")
	f.Int64Var(&cfg.SegmentMaxSize, "segment-size", cfg.SegmentMaxSize, "Max segment file size in bytes")
	f.DurationVar(&cfg.CompactInterval, "compact-interval", cfg.CompactInterval, "Compaction interval")
	f.Float64Var(&cfg.CompactDirtyPct, "compact-dirty", cfg.CompactDirtyPct, "Dirty ratio threshold to trigger compaction (0–1)")
	f.StringVar(&cfg.SyncMode, "sync", cfg.SyncMode, "Durability mode: none (OS flush), always (fsync per write), interval (fsync on a timer)")
	f.DurationVar(&cfg.SyncInterval, "sync-interval", cfg.SyncInterval, "Flush cadence when --sync=interval")
	f.DurationVar(&cfg.TxTimeout, "tx-timeout", cfg.TxTimeout, "Idle timeout before an open transaction is reaped (0 = disabled)")
	f.DurationVar(&cfg.DefaultTTL, "default-ttl", cfg.DefaultTTL, "Default expiry applied to inserted records (0 = never expire)")
	f.IntVar(&cfg.WatchBufferSize, "watch-buffer", cfg.WatchBufferSize, "Per-subscriber Watch event buffer; a slow subscriber gets an overflow signal once full")
	f.StringVar(&cfg.MetricsAddr, "metrics-addr", cfg.MetricsAddr, "Prometheus metrics listen address (empty = disabled)")
	f.StringVar(&cfg.TLSCert, "tls-cert", cfg.TLSCert, "Path to TLS certificate PEM file (enables TLS when set with --tls-key)")
	f.StringVar(&cfg.TLSKey, "tls-key", cfg.TLSKey, "Path to TLS private key PEM file (enables TLS when set with --tls-cert)")
	f.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "Log level: debug|info|warn|error")
	f.StringVar(&cfg.LogFormat, "log-format", cfg.LogFormat, "Log output format: json|text")
	f.IntVar(&cfg.SlowQueryMs, "slow-query-ms", cfg.SlowQueryMs, "Log any Find slower than this many milliseconds at WARN, with scan stats (0 = disabled)")
	f.Uint32Var(&cfg.MaxConcurrentStreams, "max-concurrent-streams", cfg.MaxConcurrentStreams, "Max concurrent HTTP/2 streams per gRPC connection (0 = gRPC library default)")
	f.IntVar(&cfg.MaxInflight, "max-inflight", cfg.MaxInflight, "Server-wide concurrent in-flight RPC ceiling; excess calls get RESOURCE_EXHAUSTED (0 = unlimited)")
	f.Float64Var(&cfg.RateLimit, "rate-limit", cfg.RateLimit, "Per-API-key rate limit in requests/sec; over-budget calls get RESOURCE_EXHAUSTED (0 = disabled)")
	f.StringVar(&cfg.OTLPEndpoint, "otlp-endpoint", cfg.OTLPEndpoint, "OTLP/gRPC collector address for OpenTelemetry tracing, e.g. localhost:4317 (empty = tracing disabled)")
	f.Float64Var(&cfg.OTLPSampleRatio, "otlp-sample-ratio", cfg.OTLPSampleRatio, "Fraction of traces to sample when tracing is enabled (0–1)")
	f.StringVar(&cfg.ReplicateFrom, "replicate-from", cfg.ReplicateFrom, "Run as a follower tailing the leader at this gRPC address (empty = leader mode)")
	f.StringVar(&cfg.FollowerID, "replicate-id", cfg.FollowerID, "Follower identity reported to the leader in ReplicationStatus (default: hostname)")
	f.IntVar(&cfg.ReplicationRingSize, "replication-ring-size", cfg.ReplicationRingSize, "Leader-side buffer of recent committed entries for follower resume (0 = disable replication)")

	return cmd
}

func serve(cfg server.Config, configFile string) error {
	// Validate durability mode up front so a typo fails loudly.
	switch engine.SyncMode(cfg.SyncMode) {
	case engine.SyncModeNone, engine.SyncModeAlways, engine.SyncModeInterval:
	default:
		return fmt.Errorf("invalid --sync mode %q (want none|always|interval)", cfg.SyncMode)
	}

	// Structured logger for the server layer. Built before anything else so a
	// bad --log-level/--log-format fails loudly at startup.
	logger, err := server.NewLogger(os.Stderr, cfg.LogLevel, cfg.LogFormat)
	if err != nil {
		return err
	}

	// Set up Prometheus metrics.
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	m := metrics.New(reg)

	// Optional OpenTelemetry tracing. Off unless --otlp-endpoint is set; when it
	// is, the server owns the OTel SDK and exports spans to an OTLP collector.
	var tracerProvider *sdktrace.TracerProvider
	if cfg.OTLPEndpoint != "" {
		tp, err := server.NewTracerProvider(context.Background(), cfg.OTLPEndpoint, cfg.OTLPSampleRatio)
		if err != nil {
			return fmt.Errorf("set up tracing: %w", err)
		}
		tracerProvider = tp
		logger.Info("tracing enabled", "otlp_endpoint", cfg.OTLPEndpoint, "sample_ratio", cfg.OTLPSampleRatio)
		defer func() {
			shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = tracerProvider.Shutdown(shutCtx)
		}()
	}

	// Follower mode (R1): when configured to replicate from a leader, dial it and
	// — on a fresh data directory — bootstrap from a snapshot before opening the
	// DB, so the DB opens over the restored state. An existing data directory is
	// resumed from its persisted applied-LSN instead.
	var leaderConn *grpc.ClientConn
	var followerWatermark uint64
	var followerBootstrapped bool
	if cfg.ReplicateFrom != "" {
		conn, err := grpc.NewClient(cfg.ReplicateFrom, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("dial leader %q: %w", cfg.ReplicateFrom, err)
		}
		leaderConn = conn
		defer func() { _ = leaderConn.Close() }()

		fresh, err := dirHasNoCollections(cfg.DataDir)
		if err != nil {
			return err
		}
		if fresh {
			logger.Info("follower: bootstrapping from leader snapshot", "leader", cfg.ReplicateFrom)
			bctx := server.ReplicationAuthContext(context.Background(), cfg.APIKey)
			wm, err := server.Bootstrap(bctx, pb.NewFileDBClient(conn), cfg.DataDir)
			if err != nil {
				return fmt.Errorf("follower bootstrap: %w", err)
			}
			followerWatermark = wm
			followerBootstrapped = true
			logger.Info("follower: bootstrap complete", "resume_lsn", wm)
		} else {
			logger.Info("follower: resuming from existing data", "leader", cfg.ReplicateFrom)
		}
	}

	// Open the database, attaching the compaction hook.
	engineCfg := cfg.EngineConfig()
	engineCfg.OnCompaction = m.ObserveCompaction
	if tracerProvider != nil {
		// Compose the metrics compaction hook with a tracing span, and add the
		// scan span hook. The engine stays dependency-free — it only calls these
		// hooks; the server owns the OTel SDK that turns them into spans.
		metricsCompaction := engineCfg.OnCompaction
		traceCompaction := server.CompactionTraceHook(tracerProvider)
		engineCfg.OnCompaction = func(collection string, dur time.Duration) {
			metricsCompaction(collection, dur)
			traceCompaction(collection, dur)
		}
		engineCfg.OnScan = server.ScanTraceHook(tracerProvider)
	}

	db, err := engine.Open(cfg.DataDir, engineCfg)
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	defer func() { _ = db.Close() }()
	logger.Info("database opened", "data_dir", cfg.DataDir)

	// If we just bootstrapped a fresh follower, record the leader LSN watermark
	// captured before the snapshot as the resume point for the replication tail.
	if followerBootstrapped {
		if err := db.SetAppliedLSN(followerWatermark); err != nil {
			return fmt.Errorf("record follower resume lsn: %w", err)
		}
	}

	// Register the per-collection gauge collector.
	metrics.NewDBCollector(reg, func() []metrics.CollectionStats {
		names := db.ListCollections()
		stats := make([]metrics.CollectionStats, 0, len(names))
		for _, name := range names {
			col, err := db.Collection(name)
			if err != nil {
				continue
			}
			s := col.Stats()
			stats = append(stats, metrics.CollectionStats{
				Name:         s.Name,
				RecordCount:  s.RecordCount,
				SegmentCount: s.SegmentCount,
			})
		}
		return stats
	})

	// Start the metrics HTTP server (if configured).
	if cfg.MetricsAddr != "" {
		metricsMux := http.NewServeMux()
		metricsMux.Handle("/metrics", metrics.Handler(reg))
		metricsSrv := &http.Server{Addr: cfg.MetricsAddr, Handler: metricsMux}
		go func() {
			logger.Info("metrics listening", "addr", cfg.MetricsAddr+"/metrics")
			if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				logger.Error("metrics server error", "err", err)
			}
		}()
	}

	// Build TLS credentials (optional).
	var serverCreds credentials.TransportCredentials
	var restDialCreds credentials.TransportCredentials
	if cfg.TLSCert != "" && cfg.TLSKey != "" {
		cert, err := tls.LoadX509KeyPair(cfg.TLSCert, cfg.TLSKey)
		if err != nil {
			return fmt.Errorf("load TLS key pair: %w", err)
		}
		serverCreds = credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}})
		// The REST gateway dials gRPC on loopback; skip verification for this
		// internal hop since the cert may be self-signed.
		restDialCreds = credentials.NewTLS(&tls.Config{InsecureSkipVerify: true}) //nolint:gosec
		logger.Info("TLS enabled", "cert", cfg.TLSCert)
	} else {
		serverCreds = insecure.NewCredentials()
		restDialCreds = insecure.NewCredentials()
	}

	// Build the API key set (scoped keys from config + legacy --api-key).
	keys, err := buildKeys(cfg)
	if err != nil {
		return err
	}
	authn, err := auth.New(keys)
	if err != nil {
		return err
	}
	logger.Info("auth enabled", "keys", len(keys))

	// Shared interceptors, in order: tracing outermost (so its per-RPC span wraps
	// the whole handler and its span-bearing context flows down to the engine scan
	// hook), then auth (resolves the principal onto the context), then the limiter
	// (reads that principal to rate-limit and sheds load), then structured request
	// logging (records the outcome — including a shed request's ResourceExhausted),
	// then metrics. Tracing and the limiter are chained only when configured, so
	// the default path is unchanged.
	authUnary, authStream := authn.Interceptors()
	logUnary, logStream := server.LoggingInterceptors(logger)
	limiter := server.NewLimiter(cfg.MaxInflight, cfg.RateLimit)

	var unaryInts []grpc.UnaryServerInterceptor
	var streamInts []grpc.StreamServerInterceptor
	if tracerProvider != nil {
		traceUnary, traceStream := server.TracingInterceptors(tracerProvider)
		unaryInts = append(unaryInts, traceUnary)
		streamInts = append(streamInts, traceStream)
	}
	unaryInts = append(unaryInts, authUnary)
	streamInts = append(streamInts, authStream)
	// Follower mode (R2): once authenticated, refuse write RPCs so this node
	// serves reads only and clients are told to write to the leader. The guard is
	// installed solely when replicating from a leader, so its presence *is* the
	// read-only role — see server.ReadOnlyInterceptors.
	if cfg.ReplicateFrom != "" {
		roUnary, roStream := server.ReadOnlyInterceptors()
		unaryInts = append(unaryInts, roUnary)
		streamInts = append(streamInts, roStream)
		logger.Info("read-only replica mode: writes rejected, reads served from applied state")
	}
	if limiter.Enabled() {
		limUnary, limStream := limiter.Interceptors()
		unaryInts = append(unaryInts, limUnary)
		streamInts = append(streamInts, limStream)
		logger.Info("backpressure enabled", "max_inflight", cfg.MaxInflight, "rate_limit_rps", cfg.RateLimit)
	}
	unaryInts = append(unaryInts, logUnary, metricsUnaryInterceptor(m))
	streamInts = append(streamInts, logStream)
	unaryChain := grpc.ChainUnaryInterceptor(unaryInts...)
	streamChain := grpc.ChainStreamInterceptor(streamInts...)

	// gRPC health service (grpc.health.v1.Health). Shared across both servers;
	// marked SERVING once the listeners are up and NOT_SERVING on shutdown.
	healthSvc := server.NewHealthService()

	// Optional per-connection concurrent-stream cap (0 = gRPC library default),
	// applied to both the TCP and unix-socket servers.
	var streamCapOpts []grpc.ServerOption
	if cfg.MaxConcurrentStreams > 0 {
		streamCapOpts = append(streamCapOpts, grpc.MaxConcurrentStreams(cfg.MaxConcurrentStreams))
		logger.Info("max concurrent streams set", "streams", cfg.MaxConcurrentStreams)
	}

	// Slow-query observability (O5): a scan-cost metric hook and, when a
	// threshold is set, a WARN slow-query log. Both are passed to every API
	// instance so TCP and unix-socket queries are observed identically.
	slowQueryOpts := []server.GRPCOption{
		server.WithScanObserver(m.ObserveScan),
		server.WithSlowQueryLog(logger, time.Duration(cfg.SlowQueryMs)*time.Millisecond),
	}
	if cfg.SlowQueryMs > 0 {
		logger.Info("slow-query log enabled", "threshold_ms", cfg.SlowQueryMs)
	}

	// TCP gRPC server — uses configurable TLS credentials.
	grpcSrv := grpc.NewServer(append([]grpc.ServerOption{
		unaryChain,
		streamChain,
		grpc.Creds(serverCreds),
	}, streamCapOpts...)...)
	tcpAPI := server.NewGRPCServer(db, cfg.TxTimeout, slowQueryOpts...)
	pb.RegisterFileDBServer(grpcSrv, tcpAPI)
	healthSvc.Register(grpcSrv)

	// TCP listener for gRPC.
	tcpLn, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("grpc tcp listen %q: %w", cfg.GRPCAddr, err)
	}
	logger.Info("gRPC listening", "addr", cfg.GRPCAddr)

	// Unix socket gRPC server — always insecure (local-only transport).
	unixGrpcSrv := grpc.NewServer(append([]grpc.ServerOption{
		unaryChain,
		streamChain,
		grpc.Creds(insecure.NewCredentials()),
	}, streamCapOpts...)...)
	unixAPI := server.NewGRPCServer(db, cfg.TxTimeout, slowQueryOpts...)
	pb.RegisterFileDBServer(unixGrpcSrv, unixAPI)
	healthSvc.Register(unixGrpcSrv)

	_ = os.Remove(cfg.UnixSocket)
	unixLn, err := net.Listen("unix", cfg.UnixSocket)
	if err != nil {
		logger.Warn("unix socket unavailable, skipping", "err", err)
	} else {
		logger.Info("gRPC unix socket", "path", cfg.UnixSocket)
		go func() { _ = unixGrpcSrv.Serve(unixLn) }()
	}

	// Readiness: the process is ready when the DB is open and its data directory
	// accepts writes. Liveness (/healthz) is unconditional.
	ready := func() error { return server.CheckDataDirWritable(cfg.DataDir) }

	// REST gateway dials the TCP gRPC server using the matching credentials.
	ctx, cancelGW := context.WithCancel(context.Background())
	defer cancelGW()

	restHandler, err := server.NewRESTGateway(ctx, cfg.GRPCAddr, restDialCreds, ready)
	if err != nil {
		return fmt.Errorf("rest gateway: %w", err)
	}
	restSrv := &http.Server{Addr: cfg.RESTAddr, Handler: restHandler}
	go func() {
		logger.Info("REST listening", "addr", cfg.RESTAddr)
		if err := restSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("REST server error", "err", err)
		}
	}()

	// Start gRPC (TCP) and mark the server SERVING now that listeners are up.
	go func() { _ = grpcSrv.Serve(tcpLn) }()
	healthSvc.SetServing()

	// Follower mode (R1): start tailing the leader once the local server is up.
	// The apply loop resumes from the DB's persisted applied-LSN and reconnects
	// with backoff on transient errors. stopFollower is called first on shutdown
	// so the tail drains before the DB closes.
	stopFollower := func() {}
	if cfg.ReplicateFrom != "" {
		fctx, cancelF := context.WithCancel(context.Background())
		stopFollower = cancelF
		followerID := cfg.FollowerID
		if followerID == "" {
			if hn, herr := os.Hostname(); herr == nil {
				followerID = hn
			}
		}
		fol := server.NewFollower(db, pb.NewFileDBClient(leaderConn), followerID, cfg.APIKey, logger)
		go func() {
			if err := fol.Run(fctx); err != nil && fctx.Err() == nil {
				logger.Error("follower replication stopped", "err", err)
			}
		}()
		logger.Info("follower replication started", "leader", cfg.ReplicateFrom, "follower_id", followerID)
	}

	// Hot-reload API keys on SIGHUP (rotation without a restart). Only useful
	// when keys come from a config file; the startup --api-key is preserved.
	if configFile != "" {
		hup := make(chan os.Signal, 1)
		signal.Notify(hup, syscall.SIGHUP)
		go func() {
			for range hup {
				newCfg, err := server.LoadConfigFile(configFile)
				if err != nil {
					logger.Error("key reload failed (parse)", "file", configFile, "err", err)
					continue
				}
				newCfg.APIKey = cfg.APIKey // preserve the startup/CLI legacy key
				newKeys, err := buildKeys(newCfg)
				if err != nil {
					logger.Error("key reload failed", "err", err)
					continue
				}
				if err := authn.Reload(newKeys); err != nil {
					logger.Error("key reload rejected", "err", err)
					continue
				}
				logger.Info("reloaded API keys", "keys", len(newKeys), "file", configFile)
			}
		}()
	}

	// Graceful shutdown.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	logger.Info("shutting down")

	// Stop the follower tail (if any) before closing the DB it writes into.
	stopFollower()

	// Flip health to NOT_SERVING first so load balancers stop routing new work,
	// then drain in-flight RPCs via GracefulStop.
	healthSvc.SetNotServing()
	healthSvc.Shutdown()

	grpcSrv.GracefulStop()
	unixGrpcSrv.GracefulStop()
	tcpAPI.Close()
	unixAPI.Close()

	shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = restSrv.Shutdown(shutCtx)

	if unixLn != nil {
		_ = os.Remove(cfg.UnixSocket)
	}

	logger.Info("stopped")
	return nil
}

// dirHasNoCollections reports whether dataDir contains no collection
// sub-directories yet — i.e. this is a fresh follower that must bootstrap from a
// leader snapshot. A missing directory counts as empty.
func dirHasNoCollections(dataDir string) (bool, error) {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return true, nil
		}
		return false, fmt.Errorf("read data dir %q: %w", dataDir, err)
	}
	for _, e := range entries {
		if e.IsDir() {
			return false, nil
		}
	}
	return true, nil
}

// buildKeys converts the server config's scoped key list plus the legacy
// single --api-key into the auth package's key type. When set, the legacy key
// is added as an additional read-write key named "default". An empty result
// disables authentication.
func buildKeys(cfg server.Config) ([]auth.Key, error) {
	keys := make([]auth.Key, 0, len(cfg.Keys)+1)
	for _, k := range cfg.Keys {
		if k.Key == "" {
			return nil, fmt.Errorf("config key %q: empty key value", k.Name)
		}
		scope, err := auth.ParseScope(k.Scope)
		if err != nil {
			return nil, fmt.Errorf("config key %q: %w", k.Name, err)
		}
		keys = append(keys, auth.Key{Key: k.Key, Name: k.Name, Scope: scope})
	}
	if cfg.APIKey != "" {
		keys = append(keys, auth.Key{Key: cfg.APIKey, Name: "default", Scope: auth.ScopeReadWrite})
	}
	return keys, nil
}

// metricsUnaryInterceptor records request duration and gRPC status code.
func metricsUnaryInterceptor(m *metrics.Metrics) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		code := codes.OK
		if err != nil {
			code = grpcstatus.Code(err)
		}
		m.ObserveGRPC(info.FullMethod, code.String(), time.Since(start))
		return resp, err
	}
}
