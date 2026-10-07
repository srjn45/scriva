package engine

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"unicode/utf8"

	"github.com/srjn45/scriva/store"
)

// BadRegionReason describes why bytes in a segment could not be accepted as a
// normal record. The tolerant scanner reports these regions rather than
// changing the segment or stopping at the first one.
type BadRegionReason string

const (
	BadRegionUndecodableJSON BadRegionReason = "undecodable-json"
	BadRegionTornTailLine    BadRegionReason = "torn-tail-line"
	BadRegionGluedLines      BadRegionReason = "glued-lines"
	BadRegionOversizedLine   BadRegionReason = "oversized-line"
	BadRegionWrongOp         BadRegionReason = "wrong-op"
)

// BadRegion identifies a contiguous range of suspect bytes. Error is a
// diagnostic only; callers should branch on Reason rather than its text.
type BadRegion struct {
	Offset int64
	Length int64
	Reason BadRegionReason
	Error  string
}

// SalvagedEntry is a valid entry found while scanning a segment. Offset and
// Length refer to the exact on-disk record, including its terminating newline.
// RecoveredFromGlued is true when the entry was found after corrupt bytes on
// the same physical line. Such entries are intentionally visible to repair
// tooling, but are not silently indistinguishable from ordinary entries.
type SalvagedEntry struct {
	Entry              store.Entry
	Offset             int64
	Length             int64
	RecoveredFromGlued bool
}

// SegmentReport is the read-only result of ScanSegmentTolerant.
type SegmentReport struct {
	Path       string
	Size       int64
	Entries    []SalvagedEntry
	BadRegions []BadRegion
}

// ScanSegmentTolerant scans path without modifying it. Unlike Segment.ScanAll,
// it continues after malformed records and returns every independently valid
// entry it can prove, along with the corrupt byte regions it skipped.
//
// This is a salvage primitive, not an index-replay path: callers must decide
// how (or whether) recovered entries are safe to use.
func ScanSegmentTolerant(path string) (SegmentReport, error) {
	return scanSegmentTolerantLimit(path, -1)
}

// scanSegmentTolerantLimit is ScanSegmentTolerant bounded to the first limit
// bytes of the file (limit < 0 means the whole file). Online verification uses
// it to scan exactly the prefix that existed when its snapshot was taken, so a
// concurrent append can never appear as a torn line.
func scanSegmentTolerantLimit(path string, limit int64) (SegmentReport, error) {
	f, err := os.Open(path)
	if err != nil {
		return SegmentReport{}, fmt.Errorf("salvage: open %q: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil {
		return SegmentReport{}, fmt.Errorf("salvage: stat %q: %w", path, err)
	}
	report := SegmentReport{Path: path, Size: info.Size()}
	var src io.Reader = f
	if limit >= 0 {
		src = io.LimitReader(f, limit)
		if limit < report.Size {
			report.Size = limit
		}
	}
	reader := bufio.NewReaderSize(src, 64*1024)
	var offset int64

	for {
		line, hasNewline, readErr := readPhysicalLine(reader)
		if len(line) != 0 || hasNewline {
			physicalLength := int64(len(line))
			if hasNewline {
				physicalLength++
			}
			scanSalvageLine(&report, line, offset, physicalLength, hasNewline)
			offset += physicalLength
		}
		if readErr == nil {
			continue
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		return SegmentReport{}, fmt.Errorf("salvage: read %q: %w", path, readErr)
	}
	return report, nil
}

// readPhysicalLine returns one line without its newline. It deliberately does
// not use bufio.Scanner: scanner stops permanently on an oversized token,
// whereas salvage must retain its place and inspect later lines.
func readPhysicalLine(r *bufio.Reader) (line []byte, hasNewline bool, err error) {
	line, err = r.ReadBytes('\n')
	if len(line) > 0 && line[len(line)-1] == '\n' {
		return line[:len(line)-1], true, err
	}
	return line, false, err
}

func scanSalvageLine(report *SegmentReport, line []byte, offset, physicalLength int64, hasNewline bool) {
	if !hasNewline {
		report.BadRegions = append(report.BadRegions, BadRegion{Offset: offset, Length: physicalLength, Reason: BadRegionTornTailLine})
		return
	}
	if physicalLength > maxScanTokenSize {
		report.BadRegions = append(report.BadRegions, BadRegion{Offset: offset, Length: physicalLength, Reason: BadRegionOversizedLine})
		return
	}
	if len(line) == 0 {
		report.BadRegions = append(report.BadRegions, BadRegion{Offset: offset, Length: physicalLength, Reason: BadRegionUndecodableJSON, Error: "empty line"})
		return
	}
	if !utf8.Valid(line) {
		report.BadRegions = append(report.BadRegions, BadRegion{Offset: offset, Length: physicalLength, Reason: BadRegionUndecodableJSON, Error: "invalid UTF-8"})
		return
	}

	e, err := store.Decode(line)
	if err == nil {
		if err = validSegmentOp(e.Op); err == nil {
			report.Entries = append(report.Entries, SalvagedEntry{Entry: e, Offset: offset, Length: physicalLength})
			return
		}
		report.BadRegions = append(report.BadRegions, BadRegion{Offset: offset, Length: physicalLength, Reason: BadRegionWrongOp, Error: err.Error()})
		return
	}

	// A failed partial append followed by a successful append can put a prefix
	// of one JSON object immediately before a complete one. Work backwards so
	// the trailing (and therefore complete) object is preferred.
	for start := bytes.LastIndexByte(line, '{'); start > 0; start = bytes.LastIndexByte(line[:start], '{') {
		recovered, decodeErr := store.Decode(line[start:])
		if decodeErr != nil {
			continue
		}
		if opErr := validSegmentOp(recovered.Op); opErr != nil {
			continue
		}
		report.BadRegions = append(report.BadRegions, BadRegion{Offset: offset, Length: int64(start), Reason: BadRegionGluedLines, Error: err.Error()})
		report.Entries = append(report.Entries, SalvagedEntry{
			Entry:              recovered,
			Offset:             offset + int64(start),
			Length:             physicalLength - int64(start),
			RecoveredFromGlued: true,
		})
		return
	}
	report.BadRegions = append(report.BadRegions, BadRegion{Offset: offset, Length: physicalLength, Reason: BadRegionUndecodableJSON, Error: err.Error()})
}

func validSegmentOp(op store.Op) error {
	switch op {
	case store.OpInsert, store.OpUpdate, store.OpDelete:
		return nil
	default:
		return fmt.Errorf("unknown segment op %q", op)
	}
}
