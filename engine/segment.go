package engine

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/srjn45/scriva/store"
)

// ErrRecordTooLarge is returned by Append when an encoded record (including its
// trailing newline) exceeds maxScanTokenSize. Such a record could be written to
// disk but never read back — the scan paths cap the line buffer at the same
// limit — so it is rejected at write time rather than left unreadable by offset
// (issue #80, a follow-up to the ReadAt asymmetry fixed in #78).
var ErrRecordTooLarge = errors.New("engine: record exceeds maximum size")

// DefaultSegmentMaxSize is the default maximum file size before a segment is
// sealed and a new active segment is created (4 MiB).
const DefaultSegmentMaxSize int64 = 4 * 1024 * 1024

// maxScanTokenSize is the largest single NDJSON record the segment scanners
// will read (16 MiB). All scan paths — ReadAt, ScanAll and ScanFrom — must use
// this same limit; if they diverge, a record within the larger limit becomes
// writable and visible to full scans but unreadable by offset (issue #78).
const maxScanTokenSize = 16 * 1024 * 1024

// newSegmentScanner returns a bufio.Scanner over r configured with the segment
// line-size limit. Use this everywhere a segment is scanned so the three read
// paths cannot drift apart on their buffer sizes.
func newSegmentScanner(r io.Reader) *bufio.Scanner {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 64*1024), maxScanTokenSize)
	return scanner
}

// Segment represents one NDJSON file in a collection's data directory.
// Sealed segments are immutable; only the active segment accepts appends.
type Segment struct {
	mu     sync.Mutex
	path   string
	size   int64
	sealed bool
	file   segFile // non-nil only for the active (write) segment
	// poisoned is set when a failed Append could not be rolled back, so the
	// file may hold torn bytes past s.size. Every later Append is refused;
	// reopening runs recoverPartialLine, which trims the torn tail.
	poisoned error
}

// ErrSegmentPoisoned is returned by Append on an active segment whose earlier
// failed write could not be rolled back.
var ErrSegmentPoisoned = errors.New("segment: poisoned by unrecoverable partial write")

// segFile is the set of file operations the active segment performs. *os.File
// satisfies it, and an *os.File stored in the interface is a plain pointer, so
// the production path pays one indirect call per op and no allocation. It is a
// test seam: see fileWrapper and the fault harness in faultfs_test.go.
type segFile interface {
	Write(b []byte) (int, error)
	Sync() error
	Truncate(size int64) error
	Close() error
	Stat() (os.FileInfo, error)
	ReadAt(b []byte, off int64) (int, error)
	Seek(offset int64, whence int) (int64, error)
}

// fileWrapper lets tests decorate the file backing an active segment. Nil in
// production (CollectionConfig.wrapFile).
type fileWrapper func(path string, f segFile) segFile

// renameFunc is the rename seam used by compaction; nil means os.Rename.
type renameFunc func(oldpath, newpath string) error

func doRename(fn renameFunc, oldpath, newpath string) error {
	if fn != nil {
		return fn(oldpath, newpath)
	}
	return os.Rename(oldpath, newpath)
}

// openActiveSegment opens (or creates) an active segment at path.
// On open it scans to the last valid newline and truncates any partial
// trailing line left by a previous crash.
func openActiveSegment(path string) (*Segment, error) {
	return openActiveSegmentWith(path, nil)
}

// openActiveSegmentWith is openActiveSegment with an optional file wrapper
// applied before crash recovery, so recovery truncation is interceptable too.
func openActiveSegmentWith(path string, wrap fileWrapper) (*Segment, error) {
	osf, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("segment: open %q: %w", path, err)
	}
	var f segFile = osf
	if wrap != nil {
		f = wrap(path, f)
	}

	size, err := recoverPartialLine(f)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("segment: recover %q: %w", path, err)
	}

	return &Segment{path: path, size: size, file: f}, nil
}

// openSealedSegment opens a sealed (read-only) segment for scanning.
func openSealedSegment(path string, size int64) *Segment {
	return &Segment{path: path, size: size, sealed: true}
}

// recoverPartialLine seeks backwards from EOF to find the last complete line
// (ending in '\n'), truncates any bytes after it, and returns the valid size.
func recoverPartialLine(f segFile) (int64, error) {
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	size := info.Size()
	if size == 0 {
		return 0, nil
	}

	// Read last byte.
	buf := make([]byte, 1)
	if _, err := f.ReadAt(buf, size-1); err != nil {
		return 0, err
	}
	if buf[0] == '\n' {
		// File ends cleanly.
		return size, nil
	}

	// Find the last '\n' by scanning backwards in chunks.
	chunk := int64(512)
	for offset := size - chunk; ; offset -= chunk {
		if offset < 0 {
			offset = 0
		}
		readSize := size - offset
		b := make([]byte, readSize)
		if _, err := f.ReadAt(b, offset); err != nil {
			return 0, err
		}
		for i := len(b) - 1; i >= 0; i-- {
			if b[i] == '\n' {
				validSize := offset + int64(i) + 1
				if err := f.Truncate(validSize); err != nil {
					return 0, fmt.Errorf("truncate partial line: %w", err)
				}
				if _, err := f.Seek(0, io.SeekEnd); err != nil {
					return 0, err
				}
				return validSize, nil
			}
		}
		if offset == 0 {
			break
		}
	}

	// No valid line found — file is entirely corrupt, start fresh.
	if err := f.Truncate(0); err != nil {
		return 0, err
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		return 0, err
	}
	return 0, nil
}

// Append encodes e and appends it to the active segment.
// Returns the byte offset at which this entry starts.
// Callers must not call Append on a sealed segment.
func (s *Segment) Append(e store.Entry) (offset int64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.sealed {
		return 0, fmt.Errorf("segment: append to sealed segment %q", s.path)
	}

	b, err := store.Encode(e)
	if err != nil {
		return 0, err
	}

	// Reject a record that the scan paths could never read back. b already
	// includes the trailing newline, and a line is readable iff its length
	// including that newline is <= maxScanTokenSize (verified against bufio's
	// buffer-growth boundary), so the ceiling is a simple len(b) comparison.
	if int64(len(b)) > maxScanTokenSize {
		return 0, fmt.Errorf("%w: record id=%d encodes to %d bytes, limit is %d", ErrRecordTooLarge, e.ID, len(b), maxScanTokenSize)
	}

	if s.poisoned != nil {
		return 0, fmt.Errorf("%w: %q: %w", ErrSegmentPoisoned, s.path, s.poisoned)
	}

	offset = s.size
	n, err := s.file.Write(b)
	if err == nil && n < len(b) {
		err = io.ErrShortWrite
	}
	if err != nil {
		// Even a reported zero-byte write may have changed the file (and a short
		// write certainly has). Always restore the known-good boundary. If that
		// fails the tail is unknown, so poison instead of appending after it.
		if terr := s.rollbackLocked(s.size); terr != nil {
			return 0, fmt.Errorf("segment: write %q: %w (rollback: %v)", s.path, err, terr)
		}
		return 0, fmt.Errorf("segment: write %q: %w", s.path, err)
	}
	s.size += int64(len(b))
	return offset, nil
}

// rollback truncates an active segment to a previously committed boundary.
// It is used by multi-entry collection writes to discard an already-appended
// prefix when a later append or fsync fails. A failed truncate poisons the
// segment: continuing at an unknown offset could silently corrupt records.
func (s *Segment) rollback(size int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sealed {
		return fmt.Errorf("segment: rollback sealed segment %q", s.path)
	}
	return s.rollbackLocked(size)
}

// rollbackLocked requires s.mu.
func (s *Segment) rollbackLocked(size int64) error {
	if s.poisoned != nil {
		return fmt.Errorf("%w: %q: %w", ErrSegmentPoisoned, s.path, s.poisoned)
	}
	if err := s.file.Truncate(size); err != nil {
		s.poisoned = fmt.Errorf("rollback truncate to %d: %w", size, err)
		return fmt.Errorf("%w: %q: %w", ErrSegmentPoisoned, s.path, s.poisoned)
	}
	s.size = size
	return nil
}

// Sync flushes any buffered writes for the active segment to stable storage
// via fsync(2). It is a no-op on sealed segments (which hold no open file
// handle) and is safe to call concurrently with Append.
func (s *Segment) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sealed || s.file == nil {
		return nil
	}
	if err := s.file.Sync(); err != nil {
		return fmt.Errorf("segment: sync %q: %w", s.path, err)
	}
	return nil
}

// ReadAt decodes the entry starting at the given byte offset.
// Safe to call concurrently on any segment (active or sealed).
func (s *Segment) ReadAt(offset int64) (store.Entry, error) {
	f, err := os.Open(s.path)
	if err != nil {
		return store.Entry{}, fmt.Errorf("segment: open for read %q: %w", s.path, err)
	}
	defer func() { _ = f.Close() }()

	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return store.Entry{}, fmt.Errorf("segment: seek offset %d in %q: %w", offset, s.path, err)
	}

	scanner := newSegmentScanner(f)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return store.Entry{}, fmt.Errorf("segment: scan %q at %d: %w", s.path, offset, err)
		}
		return store.Entry{}, fmt.Errorf("segment: empty at offset %d in %q", offset, s.path)
	}

	e, err := store.Decode(scanner.Bytes())
	if err != nil {
		return store.Entry{}, fmt.Errorf("segment: decode %q at %d: %w", s.path, offset, err)
	}
	return e, nil
}

// ScanAll reads every entry in the segment in order.
func (s *Segment) ScanAll() ([]store.Entry, error) {
	f, err := os.Open(s.path)
	if err != nil {
		return nil, fmt.Errorf("segment: scan open %q: %w", s.path, err)
	}
	defer func() { _ = f.Close() }()

	var entries []store.Entry
	scanner := newSegmentScanner(f)

	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		e, err := store.Decode(line)
		if err != nil {
			return nil, fmt.Errorf("segment: decode line %d in %q: %w", lineNum, s.path, err)
		}
		entries = append(entries, e)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("segment: scan %q: %w", s.path, err)
	}
	return entries, nil
}

// ScanFrom reads every entry in the segment in order, invoking yield with each
// entry's start byte offset. The offset matches the value Append returned for
// that entry, so callers can cross-check liveness against the primary index.
// Returning an error from yield stops the scan and returns that error, which
// lets callers terminate early (e.g. once a limit is reached).
func (s *Segment) ScanFrom(yield func(offset int64, e store.Entry) error) error {
	f, err := os.Open(s.path)
	if err != nil {
		return fmt.Errorf("segment: scanfrom open %q: %w", s.path, err)
	}
	defer func() { _ = f.Close() }()

	scanner := newSegmentScanner(f)

	var off int64
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := scanner.Bytes()
		start := off
		off += int64(len(line)) + 1 // + terminating '\n'
		if len(line) == 0 {
			continue
		}
		e, err := store.Decode(line)
		if err != nil {
			return fmt.Errorf("segment: decode line %d in %q: %w", lineNum, s.path, err)
		}
		if err := yield(start, e); err != nil {
			return err
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("segment: scanfrom %q: %w", s.path, err)
	}
	return nil
}

// Seal marks the segment as immutable and flushes + closes the underlying
// file. After sealing, only ReadAt and ScanAll are valid.
func (s *Segment) Seal() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.sealed {
		return nil
	}
	s.sealed = true

	if s.file != nil {
		if err := s.file.Sync(); err != nil {
			return fmt.Errorf("segment: sync %q: %w", s.path, err)
		}
		if err := s.file.Close(); err != nil {
			return fmt.Errorf("segment: close %q: %w", s.path, err)
		}
		s.file = nil
	}
	return nil
}

// Path returns the file path of this segment.
func (s *Segment) Path() string { return s.path }

// Size returns the current size in bytes.
func (s *Segment) Size() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.size
}

// IsSealed reports whether this segment is immutable.
func (s *Segment) IsSealed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sealed
}

// Close releases resources held by an active segment without sealing it.
// Used during graceful shutdown.
func (s *Segment) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file != nil {
		if err := s.file.Sync(); err != nil {
			return err
		}
		return s.file.Close()
	}
	return nil
}
