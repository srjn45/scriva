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

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/srjn45/scriva/engine"
	pb "github.com/srjn45/scriva/internal/pb/proto"
)

// replicationMaxRecvMsg is the largest Replicate message a follower accepts. It
// must exceed engine.DefaultMaxEnvelopeBytes plus protobuf overhead so a maximal
// transaction envelope is deliverable.
const replicationMaxRecvMsg = 3 * engine.DefaultMaxEnvelopeBytes

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
func Bootstrap(ctx context.Context, client pb.ScrivaClient, dataDir string) (uint64, error) {
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

// Limits applied to an incoming snapshot archive (D2). They are variables so
// tests can lower them; defaults are generous for any real archive.
var (
	maxSnapshotEntries  = 1 << 20
	maxSnapshotFileSize = int64(8) << 30
)

// extractSnapshot unpacks a gzip-compressed tar snapshot stream into dataDir.
//
// The archive is untrusted. It is extracted into a new 0700 temporary directory
// next to dataDir (never into a live root), accepting only regular files at
// depth 1 (coordinator files) or depth 2 ("<collection>/<engine file>"); every
// other entry, a duplicate, an unsafe name or a breached limit is an error and
// the temporary directory is removed. On success the extracted tree is
// promoted by rename, so a hostile or truncated archive can never leave a
// half-restored live directory.
func extractSnapshot(r io.Reader, dataDir string) (err error) {
	dataDir, err = filepath.Abs(dataDir)
	if err != nil {
		return err
	}
	parent := filepath.Dir(dataDir)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return fmt.Errorf("mkdir %q: %w", parent, err)
	}
	prefix := "." + filepath.Base(dataDir) + ".restore-"
	// Remove temp directories a crashed earlier extraction left behind.
	if stale, gerr := filepath.Glob(filepath.Join(parent, prefix+"*")); gerr == nil {
		for _, st := range stale {
			if fi, lerr := os.Lstat(st); lerr == nil && fi.IsDir() {
				_ = os.RemoveAll(st)
			}
		}
	}
	tmp, err := os.MkdirTemp(parent, prefix)
	if err != nil {
		return fmt.Errorf("create restore dir: %w", err)
	}
	defer func() {
		if err != nil {
			_ = os.RemoveAll(tmp)
		}
	}()

	gz, err := gzip.NewReader(r)
	if err != nil {
		return fmt.Errorf("gzip reader: %w", err)
	}
	defer func() { _ = gz.Close() }()

	seen := make(map[string]struct{})
	tr := tar.NewReader(gz)
	for n := 0; ; n++ {
		hdr, nerr := tr.Next()
		if errors.Is(nerr, io.EOF) {
			break
		}
		if nerr != nil {
			return fmt.Errorf("tar next: %w", nerr)
		}
		if n >= maxSnapshotEntries {
			return fmt.Errorf("snapshot has more than %d entries", maxSnapshotEntries)
		}
		if hdr.Typeflag != tar.TypeReg {
			return fmt.Errorf("unsafe snapshot entry %q: type %q is not a regular file", hdr.Name, hdr.Typeflag)
		}
		if hdr.Size < 0 || hdr.Size > maxSnapshotFileSize {
			return fmt.Errorf("snapshot entry %q: size %d exceeds the %d byte limit", hdr.Name, hdr.Size, maxSnapshotFileSize)
		}
		rel, verr := checkSnapshotEntryName(hdr.Name)
		if verr != nil {
			return verr
		}
		if _, dup := seen[rel]; dup {
			return fmt.Errorf("duplicate snapshot entry %q", hdr.Name)
		}
		seen[rel] = struct{}{}

		dest := filepath.Join(tmp, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(dest), 0o700); err != nil {
			return fmt.Errorf("mkdir %q: %w", filepath.Dir(dest), err)
		}
		f, oerr := os.OpenFile(dest, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if oerr != nil {
			return fmt.Errorf("create %q: %w", dest, oerr)
		}
		if _, cerr := io.CopyN(f, tr, hdr.Size); cerr != nil {
			_ = f.Close()
			return fmt.Errorf("write %q: %w", dest, cerr)
		}
		if cerr := f.Close(); cerr != nil {
			return fmt.Errorf("close %q: %w", dest, cerr)
		}
	}

	return promoteRestore(tmp, dataDir)
}

// checkSnapshotEntryName validates a tar entry name and returns it in
// slash-separated form: "<root file>" or "<collection>/<file>".
func checkSnapshotEntryName(name string) (string, error) {
	bad := func(why string) error { return fmt.Errorf("unsafe snapshot path %q: %s", name, why) }
	if name == "" || strings.ContainsAny(name, "\\\x00") || strings.HasPrefix(name, "/") {
		return "", bad("not a relative slash-separated path")
	}
	parts := strings.Split(name, "/")
	for _, p := range parts {
		if p == "" || p == "." || p == ".." {
			return "", bad("empty, '.' or '..' component")
		}
	}
	switch len(parts) {
	case 1:
		if !engine.SnapshotRootFileAllowed(parts[0]) {
			return "", bad("unknown root file")
		}
	case 2:
		if err := engine.ValidateLegacyCollectionName(parts[0]); err != nil {
			return "", bad(err.Error())
		}
		if !engine.SnapshotFileAllowed(parts[1]) {
			return "", bad("unknown collection file")
		}
	default:
		return "", bad("deeper than <collection>/<file>")
	}
	return name, nil
}

// promoteRestore moves the extracted tree from tmp into dataDir. An absent or
// empty dataDir is replaced wholesale by one rename; otherwise each top-level
// entry is renamed in, refusing to overwrite anything that exists.
func promoteRestore(tmp, dataDir string) error {
	entries, err := os.ReadDir(dataDir)
	switch {
	case errors.Is(err, os.ErrNotExist) || (err == nil && len(entries) == 0):
		if err == nil {
			if rerr := os.Remove(dataDir); rerr != nil {
				return fmt.Errorf("replace empty %q: %w", dataDir, rerr)
			}
		}
		if rerr := os.Rename(tmp, dataDir); rerr != nil {
			return fmt.Errorf("promote restore: %w", rerr)
		}
	case err != nil:
		return fmt.Errorf("read %q: %w", dataDir, err)
	default:
		moved, rerr := os.ReadDir(tmp)
		if rerr != nil {
			return rerr
		}
		for _, e := range moved {
			if _, serr := os.Lstat(filepath.Join(dataDir, e.Name())); serr == nil {
				return fmt.Errorf("restore target %q already contains %q", dataDir, e.Name())
			}
		}
		for _, e := range moved {
			if rerr := os.Rename(filepath.Join(tmp, e.Name()), filepath.Join(dataDir, e.Name())); rerr != nil {
				return fmt.Errorf("promote %q: %w", e.Name(), rerr)
			}
		}
		_ = os.Remove(tmp)
	}
	if d, oerr := os.Open(filepath.Dir(dataDir)); oerr == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

// Follower tails a leader's Replicate feed and applies committed entries to a
// local DB so it stays consistent with the leader. It reconnects with backoff on
// transient stream errors, resuming from the last durably-applied LSN.
type Follower struct {
	db      *engine.DB
	client  pb.ScrivaClient
	id      string
	apiKey  string
	logger  *slog.Logger
	backoff time.Duration

	applyObserver EnvelopeApplyObserver
}

// NewFollower builds a Follower that applies the leader's feed into db. id is an
// opaque label surfaced in the leader's ReplicationStatus; apiKey authenticates
// to the leader (empty when the leader has auth disabled).
func NewFollower(db *engine.DB, client pb.ScrivaClient, id, apiKey string, logger *slog.Logger) *Follower {
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
// from a fresh snapshot by the operator), or when a local apply left a
// collection fenced (outcome unknown: the follower must be restarted, and then
// resumes from its applied LSN).
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
			if errors.Is(err, ErrEnvelopeApplyFailed) || errors.Is(err, ErrUnknownRecordVersion) ||
				errors.Is(err, engine.ErrEnvelopeInvalid) || errors.Is(err, engine.ErrEnvelopeDigest) ||
				IsReplicationProtocolError(err) ||
				errors.Is(err, engine.ErrWriteOutcomeUnknown) || errors.Is(err, engine.ErrCollectionFenced) {
				f.logger.Error("replication stopped: cannot process record; applied LSN not advanced",
					"err", err, "applied_lsn", f.db.AppliedLSN())
				return err
			}
			if errors.Is(err, engine.ErrReplicationNeedsBootstrap) {
				f.logger.Error("replication needs re-bootstrap; stopping follower",
					"err", err, "applied_lsn", f.db.AppliedLSN())
				return err
			}
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
	rs, err := f.client.Replicate(authCtx,
		&pb.ReplicateRequest{FromLsn: from, FollowerId: f.id, ProtocolVersions: followerProtocolVersions},
		grpc.MaxCallRecvMsgSize(replicationMaxRecvMsg))
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
		switch rec.RecordVersion {
		case 0, engine.ReplicationProtocolV1:
			if rec.Tx != nil {
				return fmt.Errorf("%w: legacy record %d carries a transaction payload", engine.ErrEnvelopeInvalid, rec.Lsn)
			}
		case engine.ReplicationProtocolV2:
			env, err := txEnvelopeFromProto(rec)
			if err != nil {
				return fmt.Errorf("lsn %d: %w", rec.Lsn, err)
			}
			// Atomic apply or nothing: the applied LSN advances only after the
			// whole transaction is locally committed.
			if err := f.applyEnvelope(rec.Lsn, env); err != nil {
				return fmt.Errorf("apply tx %s (lsn %d): %w", env.TxID, rec.Lsn, err)
			}
			if err := f.db.SetAppliedLSN(rec.Lsn); err != nil {
				f.logger.Warn("persist applied lsn failed", "err", err, "lsn", rec.Lsn)
			}
			continue
		default:
			return fmt.Errorf("%w: %d (lsn %d)", ErrUnknownRecordVersion, rec.RecordVersion, rec.Lsn)
		}
		e := replRecordToEntry(rec)
		if err := f.db.ApplyReplication(rec.Collection, e); err != nil {
			// A post-commit failure (rotation) means the entry is applied: advance
			// past it. Anything else leaves the applied LSN where it is.
			if !errors.Is(err, engine.ErrPostCommit) {
				return fmt.Errorf("apply lsn %d (%s): %w", rec.Lsn, rec.Collection, err)
			}
			f.logger.Warn("replicated entry applied; post-commit step failed",
				"err", err, "lsn", rec.Lsn, "collection", rec.Collection)
		}
		if err := f.db.SetAppliedLSN(rec.Lsn); err != nil {
			f.logger.Warn("persist applied lsn failed", "err", err, "lsn", rec.Lsn)
		}
	}
}
