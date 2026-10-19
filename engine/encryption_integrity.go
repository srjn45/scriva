package engine

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// encryptionSentinelFile marks a collection directory that holds (or has held)
// encrypted data. It carries no secret and no policy; its mere existence is
// evidence of encryption that survives loss or corruption of meta.json (hardening
// §3.4, E2). It is written durably before the first encrypted write takes effect,
// is never removed, and travels with snapshots.
const encryptionSentinelFile = "encryption.sentinel"

const encryptionSentinelBody = "scriva: this collection holds encrypted data; do not delete\n"

// encryptionMarkerOnDisk is how the envelope marker ("\x00scrivaenc:", see
// crypto.IsEncrypted) appears in a segment line: encoding/json writes the NUL
// byte of a string as the six characters \u0000.
var encryptionMarkerOnDisk = []byte(`\u0000scrivaenc:`)

// ErrEncryptionMetaIntegrity is returned by open when a collection shows
// evidence of encrypted data but its meta.json is missing, unparseable, lacks
// the encryption block, or records a lower epoch than the stored entries. Open
// never infers plaintext from that state and never rewrites meta.json. It is
// distinct from crypto.ErrWrongEncryptionKey and crypto.ErrKeyUnavailable, and
// no integrity policy tolerates it. Recovery: restore meta.json from a backup
// (see docs/operations.md).
var ErrEncryptionMetaIntegrity = errors.New("engine: encryption metadata integrity violation")

// Evidence kinds reported by EncryptionMetaIntegrityError.
const (
	EvidenceIndexEpoch    = "index-epoch"    // E1: a primary-index entry has epoch > 0
	EvidenceSentinel      = "sentinel"       // E2: encryption.sentinel exists
	EvidenceSegmentMarker = "segment-marker" // E3: an envelope marker is present in a segment
)

// EncryptionMetaIntegrityError is the concrete error behind
// ErrEncryptionMetaIntegrity. It carries only the collection name, the kind of
// evidence, and file names — never key material, plaintext or ciphertext.
type EncryptionMetaIntegrityError struct {
	Collection string
	Evidence   string // one of the Evidence* constants
	File       string // the file that is inconsistent (meta.json)
	Problem    string // "missing", "unparseable", "lacks the encryption block", "epoch behind stored entries"
	// CorruptCopy is the base name of the preserved copy of an unparseable
	// meta.json, when one was made.
	CorruptCopy string
}

func (e *EncryptionMetaIntegrityError) Error() string {
	s := fmt.Sprintf("engine: collection %q holds encrypted data (evidence: %s) but %s is %s; refusing to open", e.Collection, e.Evidence, e.File, e.Problem)
	if e.CorruptCopy != "" {
		s += fmt.Sprintf(" (unparseable file preserved as %s)", e.CorruptCopy)
	}
	return s
}

// Is makes errors.Is(err, ErrEncryptionMetaIntegrity) true.
func (e *EncryptionMetaIntegrityError) Is(target error) bool {
	return target == ErrEncryptionMetaIntegrity
}

// maxEpoch returns the highest encryption epoch stamped on any indexed entry.
func (idx *Index) maxEpoch() uint64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	var m uint64
	for _, e := range idx.entries {
		if e.Epoch > m {
			m = e.Epoch
		}
	}
	return m
}

func sentinelPath(dir string) string { return filepath.Join(dir, encryptionSentinelFile) }

func sentinelExists(dir string) bool {
	st, err := os.Lstat(sentinelPath(dir))
	return err == nil && st.Mode().IsRegular()
}

// ensureSentinel durably creates the sentinel (file fsync, rename, directory
// fsync) if it does not exist. created reports whether this call made it.
func ensureSentinel(dir string) (created bool, err error) {
	if sentinelExists(dir) {
		return false, nil
	}
	if err := writeFileAtomicUnique(sentinelPath(dir), []byte(encryptionSentinelBody), 0o644); err != nil {
		return false, fmt.Errorf("write %s: %w", encryptionSentinelFile, err)
	}
	return true, nil
}

// removeSentinel undoes a sentinel this process just created, best effort.
func removeSentinel(dir string) {
	if os.Remove(sentinelPath(dir)) == nil {
		_ = fsyncDir(dir)
	}
}

// segmentsHoldEnvelope reports whether any of the segment files contains the
// on-disk encryption envelope marker.
func segmentsHoldEnvelope(all []*Segment) bool {
	for _, s := range all {
		if fileContains(s.Path(), encryptionMarkerOnDisk) {
			return true
		}
	}
	return false
}

// fileContains streams path looking for pat, carrying an overlap between chunks
// so a match spanning a chunk boundary is found. Read errors count as no match.
func fileContains(path string, pat []byte) bool {
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer func() { _ = f.Close() }()
	buf := make([]byte, 1<<20)
	keep := len(pat) - 1
	have := 0
	for {
		n, err := f.Read(buf[have:])
		have += n
		if bytes.Contains(buf[:have], pat) {
			return true
		}
		if err != nil { // io.EOF or a read error: nothing more to scan
			return false
		}
		if have > keep {
			copy(buf, buf[have-keep:have])
			have = keep
		}
	}
}

// preserveCorruptMeta copies an unparseable meta.json to
// meta.json.corrupt-<unixnano> (durably) and leaves the original in place.
// It returns the copy's base name, or "" when the copy could not be made.
func preserveCorruptMeta(dir string) string {
	data, err := os.ReadFile(metaPath(dir))
	if err != nil {
		return ""
	}
	name := fmt.Sprintf("%s.corrupt-%d", metaFilename, time.Now().UnixNano())
	if err := writeFileAtomicUnique(filepath.Join(dir, name), data, 0o644); err != nil {
		return ""
	}
	return name
}

// checkEncryptionMeta classifies meta.json against the on-disk evidence of
// encryption before load() may regenerate anything (§3.4 fail-closed). On
// success it returns the parsed meta (ok) or, when the file is missing or
// unparseable for a collection with no evidence of encryption, ok=false so the
// caller regenerates it as before. It never writes meta.json.
func (c *Collection) checkEncryptionMeta(all []*Segment) (m collectionMeta, ok bool, err error) {
	m, lerr := loadMeta(metaPath(c.dir))
	maxEp := c.index.maxEpoch()

	fail := func(evidence, problem string) error {
		ie := &EncryptionMetaIntegrityError{Collection: c.name, Evidence: evidence, File: metaFilename, Problem: problem}
		if lerr != nil && !errors.Is(lerr, os.ErrNotExist) {
			ie.CorruptCopy = preserveCorruptMeta(c.dir)
		}
		if l := c.cfg.Logger; l != nil {
			l.Error("encryption metadata integrity violation; refusing to open",
				"collection", c.name, "evidence", evidence, "file", metaFilename, "problem", problem)
		}
		return ie
	}

	if lerr == nil {
		switch {
		case m.Encryption == nil && maxEp > 0:
			return m, false, fail(EvidenceIndexEpoch, "lacks the encryption block")
		case m.Encryption == nil && sentinelExists(c.dir):
			return m, false, fail(EvidenceSentinel, "lacks the encryption block")
		case m.Encryption != nil && m.Encryption.Epoch < maxEp:
			return m, false, fail(EvidenceIndexEpoch, "epoch behind stored entries")
		}
		return m, true, nil
	}

	problem := "unparseable"
	if errors.Is(lerr, os.ErrNotExist) {
		problem = "missing"
	}
	switch {
	case maxEp > 0:
		return collectionMeta{}, false, fail(EvidenceIndexEpoch, problem)
	case sentinelExists(c.dir):
		return collectionMeta{}, false, fail(EvidenceSentinel, problem)
	case segmentsHoldEnvelope(all):
		return collectionMeta{}, false, fail(EvidenceSegmentMarker, problem)
	}
	return collectionMeta{}, false, nil
}
