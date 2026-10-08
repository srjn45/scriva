package engine

import (
	"errors"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/srjn45/scriva/store"
)

// tornFile writes only a prefix of the next write and then fails, and can
// also fail the rollback truncate.
type tornFile struct {
	segFile
	armed       bool
	failTrunc   bool
	truncCalled int
}

func (f *tornFile) Write(b []byte) (int, error) {
	if f.armed {
		f.armed = false
		n, _ := f.segFile.Write(b[:5])
		return n, syscall.ENOSPC
	}
	return f.segFile.Write(b)
}

func (f *tornFile) Truncate(size int64) error {
	f.truncCalled++
	if f.failTrunc {
		return syscall.EIO
	}
	return f.segFile.Truncate(size)
}

func openTorn(t *testing.T) (*Segment, *tornFile, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "000001.ndjson")
	var tf *tornFile
	seg, err := openActiveSegmentWith(path, func(_ string, f segFile) segFile {
		tf = &tornFile{segFile: f}
		return tf
	})
	if err != nil {
		t.Fatal(err)
	}
	return seg, tf, path
}

func TestSegmentAppendRollsBackPartialWrite(t *testing.T) {
	seg, tf, path := openTorn(t)
	off1, err := seg.Append(store.Entry{ID: 1})
	if err != nil {
		t.Fatal(err)
	}
	size := seg.size
	tf.armed = true
	if _, err := seg.Append(store.Entry{ID: 2}); !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("want ENOSPC, got %v", err)
	}
	if seg.size != size {
		t.Fatalf("size advanced to %d, want %d", seg.size, size)
	}
	off3, err := seg.Append(store.Entry{ID: 3})
	if err != nil {
		t.Fatalf("append after rollback: %v", err)
	}
	if off3 != size {
		t.Fatalf("offset %d, want %d", off3, size)
	}
	for _, off := range []int64{off1, off3} {
		if _, err := seg.ReadAt(off); err != nil {
			t.Fatalf("read at %d: %v", off, err)
		}
	}
	_ = seg.Close()
	s2, err := openActiveSegment(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if es, err := s2.ScanAll(); err != nil || len(es) != 2 {
		t.Fatalf("scan: %d entries, %v", len(es), err)
	}
}

func TestSegmentPoisonedWhenRollbackFails(t *testing.T) {
	seg, tf, path := openTorn(t)
	var poisons, appendErrs int
	seg.onPoison = func(error) { poisons++ }
	seg.onAppend = func(_ int, err error) {
		if err != nil {
			appendErrs++
		}
	}
	defer func() {
		if poisons != 1 || appendErrs != 2 {
			t.Errorf("poison hook fired %d times (want 1), failed-append hook %d (want 2)", poisons, appendErrs)
		}
	}()
	if _, err := seg.Append(store.Entry{ID: 1}); err != nil {
		t.Fatal(err)
	}
	tf.armed, tf.failTrunc = true, true
	if _, err := seg.Append(store.Entry{ID: 2}); !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("want ENOSPC, got %v", err)
	}
	tf.failTrunc = false
	before := tf.truncCalled
	if _, err := seg.Append(store.Entry{ID: 3}); !errors.Is(err, ErrSegmentPoisoned) {
		t.Fatalf("want ErrSegmentPoisoned, got %v", err)
	}
	if tf.truncCalled != before {
		t.Fatal("poisoned append must not touch the file")
	}
	_ = seg.Close()
	// Reopen trims the torn tail and the segment is usable again.
	s2, err := openActiveSegment(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if _, err := s2.Append(store.Entry{ID: 4}); err != nil {
		t.Fatalf("append after reopen: %v", err)
	}
	if es, err := s2.ScanAll(); err != nil || len(es) != 2 {
		t.Fatalf("scan: %d entries, %v", len(es), err)
	}
}
