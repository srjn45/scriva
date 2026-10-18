package engine

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// SnapshotTo writes a consistent, gzip-compressed tar archive of the entire
// database to w. The archive contains one entry per collection file, named
// "<collection>/<file>", so restoring is a plain extract into a --data
// directory:
//
//	tar xzf backup.tar.gz -C ./data
//
// Consistency: a DB-level XTx barrier prevents a snapshot from straddling a
// cross-collection commit. The registry is held read-locked for the whole
// snapshot so no collection is created, dropped, or reopened mid-archive, and
// each collection's files are copied while its own read lock is held so no
// write, rotation, or compaction can mutate them during the copy. Because
// segments are append-only and the on-disk index is rebuilt from segments when
// stale, the extracted directory always opens to a consistent state — even the
// active segment is captured at a valid entry boundary.
func (db *DB) SnapshotTo(w io.Writer) error {
	gz := gzip.NewWriter(w)
	tw := tar.NewWriter(gz)

	// An XTx holds the shared side from before it creates coordinator metadata
	// through applying all participant runs. Taking the exclusive side makes the
	// journal and every collection in this archive one transaction-consistent cut.
	db.xtxSnapshotMu.Lock()
	defer db.xtxSnapshotMu.Unlock()

	// Hold the registry read lock for the whole snapshot so the set of
	// collections cannot change (create/drop/reopen all take the write lock).
	db.mu.RLock()
	defer db.mu.RUnlock()

	names := make([]string, 0, len(db.collections))
	for n := range db.collections {
		names = append(names, n)
	}
	sort.Strings(names) // deterministic archive ordering
	for _, name := range names {
		if err := db.collections[name].checkFenced(); err != nil {
			return fmt.Errorf("snapshot: %w", err)
		}
	}

	// Coordinator metadata is authoritative for stamped entries. A legacy root
	// has no coordinator files and remains byte-format compatible.
	if j := db.journalOrNil(); j != nil {
		if err := db.writeXTxSnapshot(tw, j); err != nil {
			return err
		}
	}

	for _, name := range names {
		if err := db.collections[name].writeSnapshot(tw); err != nil {
			return fmt.Errorf("snapshot: collection %q: %w", name, err)
		}
	}

	if err := tw.Close(); err != nil {
		return fmt.Errorf("snapshot: close tar: %w", err)
	}
	if err := gz.Close(); err != nil {
		return fmt.Errorf("snapshot: close gzip: %w", err)
	}
	return nil
}

// writeXTxSnapshot copies the two durable root coordinator files. The journal
// mutex excludes append/checkpoint while Stat and CopyN establish the tar entry
// boundary. xtx.journal.tmp is an interrupted checkpoint artifact, not state.
func (db *DB) writeXTxSnapshot(tw *tar.Writer, j *xtxJournal) error {
	j.mu.Lock()
	defer j.mu.Unlock()

	for _, base := range []string{xtxFormatFile, xtxJournalFile} {
		if err := writeFileToTar(tw, db.dataDir, base, ""); err != nil {
			return fmt.Errorf("snapshot: coordinator %q: %w", base, err)
		}
	}
	return nil
}

// writeSnapshot copies every persistent file of the collection into tw under the
// collection's read lock, so writes, rotation, and compaction are all excluded
// for the (brief) duration of the copy.
func (c *Collection) writeSnapshot(tw *tar.Writer) error {
	// A fenced collection's files may hold a write of unknown outcome; an
	// archive of it would silently pick one (§2.3). Refuse; reopen first.
	if err := c.checkFenced(); err != nil {
		return err
	}
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Refresh the on-disk secondary indexes so the archived copies reflect the
	// current in-memory state. They map field values to ids (no absolute paths),
	// so they restore verbatim and correctly into a new data directory.
	c.sidxMu.RLock()
	for field, sidx := range c.sidxMap {
		_ = sidx.Persist(sidxFilePath(c.dir, field))
	}
	c.sidxMu.RUnlock()

	entries, err := os.ReadDir(c.dir)
	if err != nil {
		return fmt.Errorf("read dir %q: %w", c.dir, err)
	}

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		base := e.Name()
		// Only regular files of a known engine file kind are archived (D2). A
		// symlink — or any other non-regular entry — is never followed, so the
		// bytes of a file outside the collection can never reach an archive.
		if !e.Type().IsRegular() || !SnapshotFileAllowed(base) {
			if !strings.HasPrefix(base, ".") && base != "index.json" && base != compactManifestFilename && !strings.HasSuffix(base, ".merge") {
				if l := c.cfg.Logger; l != nil {
					l.Warn("snapshot: skipped non-regular or unknown file", "collection", c.name, "file", base)
				}
			}
			continue
		}
		// Skip in-flight compaction temp files (".compact_*", "*.merge") and any
		// other dotfiles — they are not part of the durable state and a
		// half-written temp file must never land in a backup.
		if strings.HasPrefix(base, ".") || strings.HasSuffix(base, ".merge") {
			continue
		}
		// index.json is deliberately excluded: it stores absolute segment paths
		// and its checksum only guards its own contents, so a copied index would
		// point at the source directory and could be silently stale. The restored
		// collection rebuilds a correct index from the segments on first open.
		if base == "index.json" {
			continue
		}
		// The compaction swap manifest is excluded for the same reason: it
		// records absolute paths into the source directory, and the segment set
		// captured here is always swap-consistent (the copy holds the collection
		// lock the swap needs), so a restored archive must never roll a swap
		// "forward" — its removals would delete live segments.
		if base == compactManifestFilename {
			continue
		}
		if err := writeFileToTar(tw, c.dir, base, c.name); err != nil {
			return err
		}
	}
	return nil
}

// SnapshotFileAllowed reports whether base is a file kind a collection
// directory may contribute to (or receive from) a snapshot archive:
// segments, secondary-index files and meta.json.
func SnapshotFileAllowed(base string) bool {
	switch {
	case base == metaFilename:
		return true
	case strings.HasPrefix(base, "seg_") && strings.HasSuffix(base, ".ndjson"):
		return isDigits(base[len("seg_") : len(base)-len(".ndjson")])
	case strings.HasPrefix(base, "sidx_") && strings.HasSuffix(base, ".json"):
		return len(base) > len("sidx_.json") && !strings.ContainsAny(base, "/\\\x00")
	}
	return false
}

// SnapshotRootFileAllowed reports whether base is a coordinator file that may
// sit at the root of a snapshot archive.
func SnapshotRootFileAllowed(base string) bool {
	return base == xtxFormatFile || base == xtxJournalFile
}

// ValidateLegacyCollectionName is the minimum check for a collection name that
// refers to data that may already exist: one plain path component. Archive
// import uses it, since an archive may legitimately carry legacy-named
// collections. Names for new collections use ValidateCollectionName.
func ValidateLegacyCollectionName(name string) error { return validateLegacyName(name) }

func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// writeFileToTar streams the file dir/base into tw under the archive path
// name/base.
func writeFileToTar(tw *tar.Writer, dir, base, name string) error {
	path := filepath.Join(dir, base)
	li, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("lstat %q: %w", path, err)
	}
	if !li.Mode().IsRegular() {
		return fmt.Errorf("%w: %q is not a regular file", ErrUnsafePath, path)
	}
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %q: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat %q: %w", path, err)
	}
	if !os.SameFile(li, info) {
		return fmt.Errorf("%w: %q changed while being archived", ErrUnsafePath, path)
	}

	archiveName := base
	if name != "" {
		archiveName = name + "/" + base
	}
	hdr := &tar.Header{
		Name:    archiveName,
		Mode:    int64(info.Mode().Perm()),
		Size:    info.Size(),
		ModTime: info.ModTime(),
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return fmt.Errorf("tar header %q: %w", hdr.Name, err)
	}
	// Copy exactly Size bytes: the file may grow after Stat only if a writer runs,
	// but writeSnapshot holds the collection read lock so it cannot. Bounding the
	// copy defends the tar invariant regardless.
	if _, err := io.CopyN(tw, f, info.Size()); err != nil {
		return fmt.Errorf("tar copy %q: %w", hdr.Name, err)
	}
	return nil
}
