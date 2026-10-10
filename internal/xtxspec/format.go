package xtxspec

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
)

// FormatName is the xtx.format discriminator; ReaderVersion is the highest
// min_reader this specification understands (the "R0 fence" reader).
const (
	FormatName    = "scriva-xtx"
	ReaderVersion = 1
)

// ErrFormatTooNew mirrors the planned engine error (design §11.2 L2).
var ErrFormatTooNew = errors.New("xtxspec: xtx.format requires a newer reader")

// Format is the xtx.format root file (design §11.2).
type Format struct {
	Format    string   `json:"format"`
	MinReader int      `json:"min_reader"`
	Features  []string `json:"features"`
	CreatedBy string   `json:"created_by"`
	CRC       *uint32  `json:"crc,omitempty"`
}

// EncodeFormat serialises f with its crc (single line, trailing newline).
func EncodeFormat(f Format) ([]byte, error) {
	f.CRC = nil
	b, err := json.Marshal(f)
	if err != nil {
		return nil, err
	}
	c := crc32.Checksum(b, crcTable)
	f.CRC = &c
	b, err = json.Marshal(f)
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

// ParseFormat verifies the crc and applies the version gate.
func ParseFormat(data []byte) (Format, error) {
	var f Format
	if err := json.Unmarshal(data, &f); err != nil {
		return Format{}, fmt.Errorf("xtxspec: decode xtx.format: %w", err)
	}
	if f.Format != FormatName {
		return Format{}, fmt.Errorf("xtxspec: unexpected format %q", f.Format)
	}
	if f.CRC == nil {
		return Format{}, errors.New("xtxspec: xtx.format has no crc")
	}
	want := *f.CRC
	f.CRC = nil
	b, _ := json.Marshal(f)
	if crc32.Checksum(b, crcTable) != want {
		return Format{}, fmt.Errorf("%w: xtx.format", ErrBadCRC)
	}
	f.CRC = &want
	if f.MinReader > ReaderVersion {
		return f, fmt.Errorf("%w: min_reader=%d > %d", ErrFormatTooNew, f.MinReader, ReaderVersion)
	}
	return f, nil
}
