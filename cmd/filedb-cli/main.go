package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	pb "github.com/srjn45/filedbv2/internal/pb/proto"
)

type cliFlags struct {
	host   string
	socket string
	apiKey string
	tlsCA  string // path to PEM CA cert; empty = no TLS on TCP
}

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
	flags := &cliFlags{}

	root := &cobra.Command{
		Use:     "filedb-cli",
		Short:   "FileDB command-line client",
		Version: version,
	}
	root.SetVersionTemplate("filedb-cli {{.Version}}\n")

	pf := root.PersistentFlags()
	pf.StringVar(&flags.host, "host", "localhost:5433", "FileDB gRPC address")
	pf.StringVar(&flags.socket, "socket", "/tmp/filedb.sock", "Unix socket path (used if socket file exists)")
	pf.StringVar(&flags.apiKey, "api-key", os.Getenv("FILEDB_API_KEY"), "API key (env: FILEDB_API_KEY)")
	pf.StringVar(&flags.tlsCA, "tls-ca", "", "Path to CA certificate PEM for TLS server verification (enables TLS on TCP)")

	root.AddCommand(
		replCmd(flags),
		runCmd(flags),
		insertCmd(flags),
		findCmd(flags),
		findByIDCmd(flags),
		updateCmd(flags),
		deleteCmd(flags),
		collectionsCmd(flags),
		createCollectionCmd(flags),
		dropCollectionCmd(flags),
		statsCmd(flags),
		compactCmd(flags),
		exportCmd(flags),
		importCmd(flags),
		ensureIndexCmd(flags),
		dropIndexCmd(flags),
		listIndexesCmd(flags),
		beginTxCmd(flags),
		commitTxCmd(flags),
		rollbackTxCmd(flags),
		versionCmd(),
	)
	return root
}

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version, commit, and build date",
		Run: func(cmd *cobra.Command, _ []string) {
			fmt.Printf("filedb-cli %s (commit %s, built %s)\n", version, commit, date)
		},
	}
}

// connect dials the FileDB server, preferring the Unix socket when available.
func connect(flags *cliFlags) (*grpc.ClientConn, pb.FileDBClient, func(), error) {
	var (
		conn *grpc.ClientConn
		err  error
	)

	// Prefer Unix socket if the file exists (always insecure — local transport).
	if _, statErr := os.Stat(flags.socket); statErr == nil {
		opts := []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", flags.socket)
			}),
		}
		conn, err = grpc.NewClient("unix://"+flags.socket, opts...)
	} else {
		// TCP: use TLS when --tls-ca is provided, otherwise insecure.
		var tcpCreds credentials.TransportCredentials
		if flags.tlsCA != "" {
			pem, readErr := os.ReadFile(flags.tlsCA)
			if readErr != nil {
				return nil, nil, nil, fmt.Errorf("read CA cert %q: %w", flags.tlsCA, readErr)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return nil, nil, nil, fmt.Errorf("no valid certificates found in %q", flags.tlsCA)
			}
			tcpCreds = credentials.NewTLS(&tls.Config{RootCAs: pool})
		} else {
			tcpCreds = insecure.NewCredentials()
		}
		conn, err = grpc.NewClient(flags.host, grpc.WithTransportCredentials(tcpCreds))
	}

	if err != nil {
		return nil, nil, nil, fmt.Errorf("connect: %w", err)
	}

	client := pb.NewFileDBClient(conn)
	cleanup := func() { _ = conn.Close() }
	return conn, client, cleanup, nil
}

// ctxWithAuth returns a context carrying the API key metadata.
func ctxWithAuth(flags *cliFlags) context.Context {
	if flags.apiKey == "" {
		return context.Background()
	}
	return metadata.NewOutgoingContext(
		context.Background(),
		metadata.Pairs("x-api-key", flags.apiKey),
	)
}
