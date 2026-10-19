package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/srjn45/scriva/engine"
)

// ReplicationProtocolsHeader carries the comma-separated replication protocol
// versions a peer supports. A follower sends it on every call it makes to the
// leader (ReplicationStatus, Snapshot, Replicate); the leader echoes its own
// supported set back in the response header. Either side refuses — with
// FAILED_PRECONDITION / REPLICATION_PROTOCOL_UNSUPPORTED — when the sets do
// not intersect, instead of silently speaking some other dialect.
const ReplicationProtocolsHeader = "x-scriva-replication-protocols"

// leaderProtocolVersions is what a leader serves.
var leaderProtocolVersions = []uint32{engine.ReplicationProtocolV1, engine.ReplicationProtocolV2}

// ErrReplicationDowngrade is returned when a follower that previously
// replicated over an encrypted transport is started without one.
var ErrReplicationDowngrade = errors.New("replication: refusing to downgrade follower transport from TLS to plaintext")

// ReplicationCredentials is the resolved follower→leader transport.
type ReplicationCredentials struct {
	Creds  credentials.TransportCredentials
	Secure bool // true when the transport is TLS (optionally mutual)
}

// ReplicationAPIKey is the key a follower presents to the leader: the dedicated
// --replicate-api-key, falling back to the node's own --api-key.
func ReplicationAPIKey(cfg Config) string {
	if cfg.ReplicateAPIKey != "" {
		return cfg.ReplicateAPIKey
	}
	return cfg.APIKey
}

func replicationTLSWanted(cfg Config) bool {
	return cfg.ReplicateTLS || cfg.ReplicateCA != "" || cfg.ReplicateCert != "" ||
		cfg.ReplicateKey != "" || cfg.ReplicateServerName != ""
}

// ReplicationTransport validates the follower's leader-connection settings and
// builds its transport credentials. It fails closed: a non-loopback leader
// needs TLS and credentials unless ReplicateInsecure is explicitly set, and
// ReplicateInsecure can never be combined with TLS settings (no silent
// downgrade of an intended-secure configuration).
func ReplicationTransport(cfg Config) (ReplicationCredentials, []string, error) {
	var warnings []string
	wantTLS := replicationTLSWanted(cfg)
	if wantTLS && cfg.ReplicateInsecure {
		return ReplicationCredentials{}, nil, errors.New("replicate_insecure conflicts with replicate TLS settings: refusing to downgrade a TLS configuration to plaintext")
	}
	if (cfg.ReplicateCert == "") != (cfg.ReplicateKey == "") {
		return ReplicationCredentials{}, nil, errors.New("--replicate-cert and --replicate-key must be set together")
	}
	remote := !IsLoopbackAddr(cfg.ReplicateFrom)

	if !wantTLS {
		if remote && !cfg.ReplicateInsecure {
			return ReplicationCredentials{}, nil, fmt.Errorf("refusing to replicate from non-loopback leader %q over plaintext: configure --replicate-tls/--replicate-ca (and --replicate-cert/--replicate-key for mTLS), or pass --replicate-insecure to accept the risk", cfg.ReplicateFrom)
		}
		if remote && ReplicationAPIKey(cfg) == "" {
			warnings = append(warnings, fmt.Sprintf("INSECURE: replicating from non-loopback leader %s over plaintext with no credentials because replicate_insecure is set", cfg.ReplicateFrom))
		} else if remote {
			warnings = append(warnings, fmt.Sprintf("INSECURE: replicating from non-loopback leader %s over plaintext (API key sent in the clear) because replicate_insecure is set", cfg.ReplicateFrom))
		}
		return ReplicationCredentials{Creds: insecure.NewCredentials()}, warnings, nil
	}

	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if cfg.ReplicateCA != "" {
		pool, err := loadCertPool(cfg.ReplicateCA)
		if err != nil {
			return ReplicationCredentials{}, nil, fmt.Errorf("replicate CA: %w", err)
		}
		tlsCfg.RootCAs = pool
	}
	if cfg.ReplicateCert != "" {
		cert, err := tls.LoadX509KeyPair(cfg.ReplicateCert, cfg.ReplicateKey)
		if err != nil {
			return ReplicationCredentials{}, nil, fmt.Errorf("load replicate client key pair: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}
	if cfg.ReplicateServerName != "" {
		tlsCfg.ServerName = cfg.ReplicateServerName
	} else if host, _, err := net.SplitHostPort(cfg.ReplicateFrom); err == nil {
		tlsCfg.ServerName = host
	}
	// Authentication is mandatory over a remote link: a client cert or an API key.
	if remote && cfg.ReplicateCert == "" && ReplicationAPIKey(cfg) == "" && !cfg.ReplicateInsecure {
		return ReplicationCredentials{}, nil, fmt.Errorf("refusing to replicate from non-loopback leader %q without credentials: set --replicate-api-key (or --api-key) or --replicate-cert/--replicate-key", cfg.ReplicateFrom)
	}
	return ReplicationCredentials{Creds: credentials.NewTLS(tlsCfg), Secure: true}, nil, nil
}

const replicationTransportMarker = ".replication-transport"

// CheckReplicationTransportMarker refuses to start a follower in plaintext
// when its data directory records that it has replicated over TLS before. The
// operator must remove the marker file to acknowledge a deliberate downgrade.
func CheckReplicationTransportMarker(dataDir string, secure bool) error {
	b, err := os.ReadFile(filepath.Join(dataDir, replicationTransportMarker))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read replication transport marker: %w", err)
	}
	if strings.TrimSpace(string(b)) == "tls" && !secure {
		return fmt.Errorf("%w (delete %s to acknowledge)", ErrReplicationDowngrade, filepath.Join(dataDir, replicationTransportMarker))
	}
	return nil
}

// WriteReplicationTransportMarker records that this follower replicates over
// TLS (a no-op for plaintext, and never lowers an existing "tls" marker).
func WriteReplicationTransportMarker(dataDir string, secure bool) error {
	if !secure {
		return nil
	}
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dataDir, replicationTransportMarker), []byte("tls\n"), 0o600)
}

// formatProtocols / parseProtocols encode a version set for the header.
func formatProtocols(vs []uint32) string {
	parts := make([]string, len(vs))
	for i, v := range vs {
		parts[i] = strconv.FormatUint(uint64(v), 10)
	}
	return strings.Join(parts, ",")
}

func parseProtocols(vals []string) []uint32 {
	var out []uint32
	for _, val := range vals {
		for p := range strings.SplitSeq(val, ",") {
			if n, err := strconv.ParseUint(strings.TrimSpace(p), 10, 32); err == nil {
				out = append(out, uint32(n))
			}
		}
	}
	return out
}

func protocolsIntersect(a, b []uint32) bool {
	for _, x := range a {
		if slices.Contains(b, x) {
			return true
		}
	}
	return false
}

// protocolMismatchStatus is the typed FAILED_PRECONDITION both sides use when
// the supported replication protocol sets do not overlap.
func protocolMismatchStatus(msg string, peer, local []uint32) error {
	st := status.New(codes.FailedPrecondition, msg)
	withDetails, err := st.WithDetails(&errdetails.ErrorInfo{
		Reason: ReplicationReasonProtocolUnsupported,
		Domain: ReplicationErrorDomain,
		Metadata: map[string]string{
			"peer_versions":  formatProtocols(peer),
			"local_versions": formatProtocols(local),
		},
	})
	if err != nil {
		return st.Err()
	}
	return withDetails.Err()
}

// checkFollowerProtocols is the leader-side negotiation: advertised is the
// follower's version list (ReplicateRequest field or request header). An empty
// list means a pre-negotiation follower, i.e. {1}.
func checkFollowerProtocols(advertised []uint32) error {
	if len(advertised) == 0 {
		return nil
	}
	if !protocolsIntersect(advertised, leaderProtocolVersions) {
		return protocolMismatchStatus(fmt.Sprintf(
			"replication: follower protocol versions [%s] are not supported by this leader (supports [%s]); refusing to downgrade",
			formatProtocols(advertised), formatProtocols(leaderProtocolVersions)),
			advertised, leaderProtocolVersions)
	}
	return nil
}

// checkLeaderProtocols is the follower-side negotiation over the leader's
// response header. A leader that sends no header predates negotiation and is
// treated as v1-only.
func checkLeaderProtocols(md metadata.MD) error {
	leader := parseProtocols(md.Get(ReplicationProtocolsHeader))
	if len(leader) == 0 {
		leader = []uint32{engine.ReplicationProtocolV1}
	}
	if !protocolsIntersect(leader, followerProtocolVersions) {
		return protocolMismatchStatus(fmt.Sprintf(
			"replication: leader protocol versions [%s] are not supported by this follower (supports [%s]); refusing to downgrade",
			formatProtocols(leader), formatProtocols(followerProtocolVersions)),
			leader, followerProtocolVersions)
	}
	return nil
}

// negotiateReplicationProtocols runs the leader-side check against the version
// list in the request body (nil for RPCs without one) and in the incoming
// header, then advertises the leader's supported set in the response header.
func negotiateReplicationProtocols(ctx context.Context, fromBody []uint32, setHeader func(metadata.MD) error) error {
	if err := checkFollowerProtocols(fromBody); err != nil {
		return err
	}
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if err := checkFollowerProtocols(parseProtocols(md.Get(ReplicationProtocolsHeader))); err != nil {
			return err
		}
	}
	return setHeader(metadata.Pairs(ReplicationProtocolsHeader, formatProtocols(leaderProtocolVersions)))
}
