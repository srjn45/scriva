package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
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
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/srjn45/filedbv2/internal/auth"
	"github.com/srjn45/filedbv2/internal/engine"
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
					case "watch-buffer":
						merged.WatchBufferSize = cfg.WatchBufferSize
					case "metrics-addr":
						merged.MetricsAddr = cfg.MetricsAddr
					case "tls-cert":
						merged.TLSCert = cfg.TLSCert
					case "tls-key":
						merged.TLSKey = cfg.TLSKey
					}
				})
				cfg = merged
			}
			return serve(cfg)
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
	f.IntVar(&cfg.WatchBufferSize, "watch-buffer", cfg.WatchBufferSize, "Per-subscriber Watch event buffer; a slow subscriber gets an overflow signal once full")
	f.StringVar(&cfg.MetricsAddr, "metrics-addr", cfg.MetricsAddr, "Prometheus metrics listen address (empty = disabled)")
	f.StringVar(&cfg.TLSCert, "tls-cert", cfg.TLSCert, "Path to TLS certificate PEM file (enables TLS when set with --tls-key)")
	f.StringVar(&cfg.TLSKey, "tls-key", cfg.TLSKey, "Path to TLS private key PEM file (enables TLS when set with --tls-cert)")

	return cmd
}

func serve(cfg server.Config) error {
	// Validate durability mode up front so a typo fails loudly.
	switch engine.SyncMode(cfg.SyncMode) {
	case engine.SyncModeNone, engine.SyncModeAlways, engine.SyncModeInterval:
	default:
		return fmt.Errorf("invalid --sync mode %q (want none|always|interval)", cfg.SyncMode)
	}

	// Set up Prometheus metrics.
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	m := metrics.New(reg)

	// Open the database, attaching the compaction hook.
	engineCfg := cfg.EngineConfig()
	engineCfg.OnCompaction = m.ObserveCompaction

	db, err := engine.Open(cfg.DataDir, engineCfg)
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	defer func() { _ = db.Close() }()
	log.Printf("filedb: data dir=%q", cfg.DataDir)

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
			log.Printf("filedb: metrics listening on %s/metrics", cfg.MetricsAddr)
			if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Printf("filedb: metrics server error: %v", err)
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
		log.Printf("filedb: TLS enabled (cert=%s)", cfg.TLSCert)
	} else {
		serverCreds = insecure.NewCredentials()
		restDialCreds = insecure.NewCredentials()
	}

	// TCP gRPC server — uses configurable TLS credentials.
	unary, stream := auth.Interceptors(cfg.APIKey)
	grpcMetrics := grpc.UnaryInterceptor(chainUnary(unary, metricsUnaryInterceptor(m)))
	grpcSrv := grpc.NewServer(
		grpcMetrics,
		grpc.StreamInterceptor(stream),
		grpc.Creds(serverCreds),
	)
	tcpAPI := server.NewGRPCServer(db, cfg.TxTimeout)
	pb.RegisterFileDBServer(grpcSrv, tcpAPI)

	// TCP listener for gRPC.
	tcpLn, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return fmt.Errorf("grpc tcp listen %q: %w", cfg.GRPCAddr, err)
	}
	log.Printf("filedb: gRPC listening on %s", cfg.GRPCAddr)

	// Unix socket gRPC server — always insecure (local-only transport).
	unixGrpcSrv := grpc.NewServer(
		grpcMetrics,
		grpc.StreamInterceptor(stream),
		grpc.Creds(insecure.NewCredentials()),
	)
	unixAPI := server.NewGRPCServer(db, cfg.TxTimeout)
	pb.RegisterFileDBServer(unixGrpcSrv, unixAPI)

	_ = os.Remove(cfg.UnixSocket)
	unixLn, err := net.Listen("unix", cfg.UnixSocket)
	if err != nil {
		log.Printf("filedb: unix socket unavailable (%v), skipping", err)
	} else {
		log.Printf("filedb: gRPC unix socket at %s", cfg.UnixSocket)
		go func() { _ = unixGrpcSrv.Serve(unixLn) }()
	}

	// REST gateway dials the TCP gRPC server using the matching credentials.
	ctx, cancelGW := context.WithCancel(context.Background())
	defer cancelGW()

	restHandler, err := server.NewRESTGateway(ctx, cfg.GRPCAddr, restDialCreds)
	if err != nil {
		return fmt.Errorf("rest gateway: %w", err)
	}
	restSrv := &http.Server{Addr: cfg.RESTAddr, Handler: restHandler}
	go func() {
		log.Printf("filedb: REST listening on %s", cfg.RESTAddr)
		if err := restSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("filedb: REST server error: %v", err)
		}
	}()

	// Start gRPC (TCP).
	go func() { _ = grpcSrv.Serve(tcpLn) }()

	// Graceful shutdown.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("filedb: shutting down...")

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

	log.Println("filedb: stopped")
	return nil
}

// chainUnary returns a single UnaryServerInterceptor that runs first, then second.
func chainUnary(first, second grpc.UnaryServerInterceptor) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		return first(ctx, req, info, func(ctx context.Context, req any) (any, error) {
			return second(ctx, req, info, handler)
		})
	}
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
