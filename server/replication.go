package server

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/srjn45/filedbv2/engine"
	pb "github.com/srjn45/filedbv2/internal/pb/proto"
)

// DefaultReplicationBackoff is how long a follower waits before reconnecting to
// the leader after the replication stream drops.
const DefaultReplicationBackoff = time.Second

// ReplicationAuthContext returns a context carrying the API key in the metadata
// header the leader's auth interceptor expects. A follower uses it for every
// call it makes to the leader (Snapshot, ReplicationStatus, Replicate). An empty
// key yields the context unchanged (the leader has auth disabled).
func ReplicationAuthContext(ctx context.Context, apiKey string) context.Context {
	if apiKey == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "x-api-key", apiKey)
}

// Bootstrap fetches a full snapshot of the leader into dataDir and returns the
// leader's LSN watermark to resume from. It reads the watermark *before* the
// snapshot (via ReplicationStatus), which guarantees the snapshot contains every
// entry at or below it; the follower then tails from that LSN. Idempotent apply
// on the follower absorbs any entries that raced into the snapshot after the
// watermark was read, so there is neither a gap nor a durable duplicate.
//
// dataDir must be empty (a fresh follower). Existing collection sub-directories
// are left untouched — a follower that already has data resumes from its
// persisted applied-LSN instead of re-bootstrapping.
func Bootstrap(ctx context.Context, client pb.FileDBClient, dataDir string) (uint64, error) {
	st, err := client.ReplicationStatus(ctx, &pb.ReplicationStatusRequest{})
	if err != nil {
		return 0, fmt.Errorf("bootstrap: read leader lsn: %w", err)
	}
	watermark := st.LeaderLsn

	stream, err := client.Snapshot(ctx, &pb.SnapshotRequest{})
	if err != nil {
		return 0, fmt.Errorf("bootstrap: open snapshot: %w", err)
	}

	pr, pw := io.Pipe()
	go func() {
		var perr error
		for {
			chunk, rerr := stream.Recv()
			if errors.Is(rerr, io.EOF) {
				break
			}
			if rerr != nil {
				perr = rerr
				break
			}
			if _, werr := pw.Write(chunk.Data); werr != nil {
				perr = werr
				break
			}
		}
		_ = pw.CloseWithError(perr)
	}()

	if err := extractSnapshot(pr, dataDir); err != nil {
		return 0, fmt.Errorf("bootstrap: extract snapshot: %w", err)
	}
	return watermark, nil
}

// extractSnapshot unpacks a gzip-compressed tar snapshot stream into dataDir. The
// archive lays out one entry per collection file as "<collection>/<file>"; paths
// are validated so a malformed archive cannot escape dataDir.
func extractSnapshot(r io.Reader, dataDir string) error {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return fmt.Errorf("gzip reader: %w", err)
	}
	defer func() { _ = gz.Close() }()

	tr := tar.NewReader(gz)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("tar next: %w", err)
		}
		if hdr.Typeflag != tar.TypeReg {
			continue
		}
		clean := filepath.Clean(hdr.Name)
		if clean == "." || filepath.IsAbs(clean) || strings.HasPrefix(clean, ".."+string(filepath.Separator)) || clean == ".." {
			return fmt.Errorf("unsafe snapshot path %q", hdr.Name)
		}
		dest := filepath.Join(dataDir, clean)
		if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
			return fmt.Errorf("mkdir %q: %w", filepath.Dir(dest), err)
		}
		f, err := os.OpenFile(dest, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			return fmt.Errorf("create %q: %w", dest, err)
		}
		if _, err := io.CopyN(f, tr, hdr.Size); err != nil {
			_ = f.Close()
			return fmt.Errorf("write %q: %w", dest, err)
		}
		if err := f.Close(); err != nil {
			return fmt.Errorf("close %q: %w", dest, err)
		}
	}
	return nil
}

// Follower tails a leader's Replicate feed and applies committed entries to a
// local DB so it stays consistent with the leader. It reconnects with backoff on
// transient stream errors, resuming from the last durably-applied LSN.
type Follower struct {
	db      *engine.DB
	client  pb.FileDBClient
	id      string
	apiKey  string
	logger  *slog.Logger
	backoff time.Duration
}

// NewFollower builds a Follower that applies the leader's feed into db. id is an
// opaque label surfaced in the leader's ReplicationStatus; apiKey authenticates
// to the leader (empty when the leader has auth disabled).
func NewFollower(db *engine.DB, client pb.FileDBClient, id, apiKey string, logger *slog.Logger) *Follower {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Follower{
		db:      db,
		client:  client,
		id:      id,
		apiKey:  apiKey,
		logger:  logger,
		backoff: DefaultReplicationBackoff,
	}
}

// Run tails the leader until ctx is cancelled. On a transient stream error it
// reconnects with backoff, resuming from the last applied LSN. It returns when
// ctx is done, or without retrying when the leader reports FAILED_PRECONDITION
// (the follower has fallen out of the leader's buffer and must be re-bootstrapped
// from a fresh snapshot by the operator).
func (f *Follower) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err := f.stream(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			if status.Code(err) == codes.FailedPrecondition {
				f.logger.Error("replication needs re-bootstrap; stopping follower",
					"err", err, "applied_lsn", f.db.AppliedLSN())
				return err
			}
			f.logger.Warn("replication stream dropped; retrying",
				"err", err, "applied_lsn", f.db.AppliedLSN())
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(f.backoff):
		}
	}
}

// stream opens one Replicate stream from the current applied LSN and applies
// records until it ends or errors.
func (f *Follower) stream(ctx context.Context) error {
	from := f.db.AppliedLSN()
	authCtx := ReplicationAuthContext(ctx, f.apiKey)
	// Learn the leader's current LSN up front so the promotion lag guard (R3) has
	// a fresh last-known-leader watermark even while this follower is catching up
	// or the leader is otherwise idle. Best-effort: a failure just leaves the
	// watermark at its previous value.
	if st, serr := f.client.ReplicationStatus(authCtx, &pb.ReplicationStatusRequest{}); serr == nil {
		f.db.NoteLeaderLSN(st.LeaderLsn)
	}
	rs, err := f.client.Replicate(authCtx, &pb.ReplicateRequest{FromLsn: from, FollowerId: f.id})
	if err != nil {
		return err
	}
	f.logger.Info("replication stream opened", "from_lsn", from, "follower_id", f.id)

	for {
		rec, err := rs.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		// Track the highest LSN the leader is known to hold, for the R3 promotion
		// lag guard, before applying (so a caught-up follower reports zero lag).
		f.db.NoteLeaderLSN(rec.Lsn)
		// Skip anything already applied (idempotency at the LSN level; record-level
		// revision idempotency in the engine is the ultimate safety net).
		if rec.Lsn <= f.db.AppliedLSN() {
			continue
		}
		e := replRecordToEntry(rec)
		if err := f.db.ApplyReplication(rec.Collection, e); err != nil {
			return fmt.Errorf("apply lsn %d (%s): %w", rec.Lsn, rec.Collection, err)
		}
		if err := f.db.SetAppliedLSN(rec.Lsn); err != nil {
			f.logger.Warn("persist applied lsn failed", "err", err, "lsn", rec.Lsn)
		}
	}
}
