package engine

// The root version gate for cross-collection transactions (design §11.2, §2.1).
//
// xtx.format is a one-line JSON file written (file fsync + dir fsync) before
// the journal and before any stamped byte. A binary that reads it refuses with
// ErrFormatTooNew when min_reader exceeds xtxReaderLevel, and with
// ErrXTxUnsupported when it lists a feature this binary does not know. A root
// without xtx.format and without xtx.journal is a legacy root and is untouched.

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	xtxFormatName = "scriva-xtx"
	// xtxReaderLevel is the highest xtx.format min_reader this binary satisfies.
	xtxReaderLevel = 1
)

// xtxKnownFeatures are the feature tokens this binary understands.
var xtxKnownFeatures = map[string]bool{"journal": true, "stamped-v2": true}

type xtxFormat struct {
	Format    string   `json:"format"`
	MinReader int      `json:"min_reader"`
	Features  []string `json:"features"`
	CreatedBy string   `json:"created_by"`
	CRC       *uint32  `json:"crc,omitempty"`
}

// isReservedXTxName reports whether name is in the reserved xtx. namespace.
func isReservedXTxName(name string) bool {
	return strings.HasPrefix(name, xtxReservedPrefix)
}

func encodeXTxFormat(f xtxFormat) ([]byte, error) {
	f.CRC = nil
	b, err := json.Marshal(f)
	if err != nil {
		return nil, err
	}
	c := crcOf(b)
	f.CRC = &c
	b, err = json.Marshal(f)
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

// decodeXTxFormat parses and gates an xtx.format image. Order matters: a
// min_reader this binary cannot satisfy is reported precisely even if a future
// format changed the rest of the file.
func decodeXTxFormat(data []byte) (*xtxFormat, error) {
	var probe struct {
		MinReader *int `json:"min_reader"`
	}
	if err := json.Unmarshal(data, &probe); err != nil || probe.MinReader == nil {
		return nil, xtxErr("", ErrXTxFormatCorrupt, errors.New("not a JSON object with min_reader"))
	}
	if *probe.MinReader > xtxReaderLevel {
		return nil, xtxErr("", ErrFormatTooNew,
			fmt.Errorf("xtx.format min_reader %d, this binary reads up to %d", *probe.MinReader, xtxReaderLevel))
	}
	var f xtxFormat
	line := data
	if n := len(line); n > 0 && line[n-1] == '\n' {
		line = line[:n-1]
	}
	if err := strictUnmarshal(line, &f); err != nil {
		return nil, xtxErr("", ErrXTxFormatCorrupt, err)
	}
	if f.CRC == nil {
		return nil, xtxErr("", ErrXTxFormatCorrupt, errors.New("no checksum"))
	}
	want := *f.CRC
	f.CRC = nil
	b, _ := json.Marshal(f)
	if crcOf(b) != want {
		return nil, xtxErr("", ErrXTxFormatCorrupt, errors.New("checksum mismatch"))
	}
	f.CRC = &want
	if canon, _ := json.Marshal(f); !bytes.Equal(canon, line) {
		return nil, xtxErr("", ErrXTxFormatCorrupt, errors.New("not in canonical form"))
	}
	f.CRC = nil
	if f.Format != xtxFormatName || f.MinReader < 1 {
		return nil, xtxErr("", ErrXTxUnsupported, fmt.Errorf("unknown format %q min_reader %d", f.Format, f.MinReader))
	}
	for _, ft := range f.Features {
		if !xtxKnownFeatures[ft] {
			return nil, xtxErr("", ErrXTxUnsupported, fmt.Errorf("unknown feature %q", ft))
		}
	}
	return &f, nil
}

// readXTxFormat returns (nil, nil) for a root without xtx.format.
func readXTxFormat(dir string) (*xtxFormat, error) {
	data, err := os.ReadFile(filepath.Join(dir, xtxFormatFile))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("xtx.format: read: %w", err)
	}
	return decodeXTxFormat(data)
}

// ensureXTxFormat makes xtx.format exist and be durable (file + dir fsync) and
// verifies an existing one passes the gate.
func ensureXTxFormat(dir, createdBy string) error {
	f, err := readXTxFormat(dir)
	if err != nil || f != nil {
		return err
	}
	b, err := encodeXTxFormat(xtxFormat{Format: xtxFormatName, MinReader: 1,
		Features: []string{"journal", "stamped-v2"}, CreatedBy: createdBy})
	if err != nil {
		return err
	}
	return writeFileAtomic(filepath.Join(dir, xtxFormatFile), b, 0o644)
}

// gateXTxRoot is Phase 0/1 of open (§7.1) for the pieces implemented so far:
// it deletes a leftover journal tmp, applies the version gate, and opens the
// journal (repairing a torn tail). It returns a nil journal for a legacy root.
// Any error leaves the directory untouched apart from the tmp removal and the
// torn-tail truncation.
func gateXTxRoot(dir string, opt xtxOptions) (*xtxJournal, error) {
	if err := os.Remove(filepath.Join(dir, xtxJournalTmpFile)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("xtx: remove stale %s: %w", xtxJournalTmpFile, err)
	}
	f, err := readXTxFormat(dir)
	if err != nil {
		return nil, err
	}
	if f == nil {
		if _, serr := os.Stat(filepath.Join(dir, xtxJournalFile)); serr == nil {
			return nil, xtxErr("", ErrXTxUnsupported, errors.New("xtx.journal present without xtx.format"))
		}
		return nil, nil
	}
	return openXTxJournal(dir, opt)
}
