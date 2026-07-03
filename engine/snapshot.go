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
// Consistency: the DB registry is held read-locked for the whole snapshot so no
// collection is created, dropped, or reopened mid-archive, and each collection's
// files are copied while its own read lock is held so no write, rotation, or
// compaction can mutate them during the copy. Because segments are append-only
// and the on-disk index is rebuilt from segments when stale, the extracted
// directory always opens to a consistent state — even the active segment is
// captured at a valid entry boundary.
func (db *DB) SnapshotTo(w io.Writer) error {
	gz := gzip.NewWriter(w)
	tw := tar.NewWriter(gz)

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

// writeSnapshot copies every persistent file of the collection into tw under the
// collection's read lock, so writes, rotation, and compaction are all excluded
// for the (brief) duration of the copy.
func (c *Collection) writeSnapshot(tw *tar.Writer) error {
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
		if err := writeFileToTar(tw, c.dir, base, c.name); err != nil {
			return err
		}
	}
	return nil
}

// writeFileToTar streams the file dir/base into tw under the archive path
// name/base.
func writeFileToTar(tw *tar.Writer, dir, base, name string) error {
	path := filepath.Join(dir, base)
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %q: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat %q: %w", path, err)
	}

	hdr := &tar.Header{
		Name:    name + "/" + base,
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
