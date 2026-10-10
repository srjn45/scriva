package xtxspec

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
)

// Journal-level errors (design §4.3, §7.2).
var (
	ErrJournalCorrupt   = errors.New("xtxspec: xtx-journal-corrupt")
	ErrDecisionConflict = errors.New("xtxspec: xtx-decision-conflict")
)

// Record kinds.
const (
	KindCommit = "commit"
	KindAbort  = "abort"
	KindRetire = "retire"
	Magic      = "SCRIVA-XTX"
)

// Header is journal line 1.
type Header struct {
	Magic   string  `json:"magic"`
	V       int     `json:"v"`
	Epoch   string  `json:"epoch"`
	Created string  `json:"created"`
	CRC     *uint32 `json:"crc,omitempty"`
}

// Record is one journal line after the header. Fields not used by a kind are
// omitted. Field order is fixed for deterministic encoding.
type Record struct {
	K     string  `json:"k"`
	Tx    string  `json:"tx"`
	Key   string  `json:"key"`
	Ts    string  `json:"ts"`
	Parts []Part  `json:"parts,omitempty"`
	PD    string  `json:"pd,omitempty"`
	Why   string  `json:"why,omitempty"`
	O     string  `json:"o,omitempty"`
	CRC   *uint32 `json:"crc,omitempty"`
}

func withCRC(v any, set func(uint32)) ([]byte, error) {
	b, err := json.Marshal(v) // crc is nil here => omitted
	if err != nil {
		return nil, err
	}
	set(crc32.Checksum(b, crcTable))
	b, err = json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

// EncodeHeader returns the journal's first line.
func EncodeHeader(h Header) ([]byte, error) {
	h.CRC = nil
	return withCRC(&h, func(c uint32) { h.CRC = &c })
}

// EncodeRecord returns one journal line. Commit records get pd computed.
func EncodeRecord(r Record) ([]byte, error) {
	r.CRC = nil
	if r.K == KindCommit {
		p, err := CanonicalParts(r.Parts)
		if err != nil {
			return nil, err
		}
		r.Parts, r.PD = p, SetDigest(p)
	}
	return withCRC(&r, func(c uint32) { r.CRC = &c })
}

func verifyCRC(line []byte, v any, crc *uint32, clear func()) error {
	if crc == nil {
		return fmt.Errorf("%w: record has no crc", ErrJournalCorrupt)
	}
	want := *crc
	clear()
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if crc32.Checksum(b, crcTable) != want {
		return fmt.Errorf("%w: crc mismatch", ErrJournalCorrupt)
	}
	return nil
}

// Journal is a parsed xtx.journal.
type Journal struct {
	Header  Header
	Records []Record
	// GoodLen is the byte length of the valid prefix; TornTail reports that the
	// file had an incomplete tail beyond it that recovery truncates.
	GoodLen  int
	TornTail bool
}

// ScanJournal implements the §4.3 table. It distinguishes a torn tail (final
// line unterminated or all-zero, nothing valid after it) from corruption
// (anything else), which fails closed with ErrJournalCorrupt.
func ScanJournal(data []byte) (*Journal, error) {
	j := &Journal{}
	pos := 0
	first := true
	for pos < len(data) {
		nl := bytes.IndexByte(data[pos:], '\n')
		if nl < 0 { // unterminated final line
			if first {
				return nil, fmt.Errorf("%w: header missing or torn", ErrJournalCorrupt)
			}
			j.GoodLen, j.TornTail = pos, true
			return j, nil
		}
		line := data[pos : pos+nl]
		isLast := pos+nl+1 == len(data)
		if allZero(line) && isLast && !first {
			j.GoodLen, j.TornTail = pos, true
			return j, nil
		}
		if first {
			var h Header
			if err := json.Unmarshal(line, &h); err != nil || h.Magic != Magic {
				return nil, fmt.Errorf("%w: bad header", ErrJournalCorrupt)
			}
			if err := verifyCRC(line, &h, h.CRC, func() { h.CRC = nil }); err != nil {
				return nil, err
			}
			j.Header = h
			first = false
		} else {
			var r Record
			if err := json.Unmarshal(line, &r); err != nil {
				return nil, fmt.Errorf("%w: unparsable record at byte %d", ErrJournalCorrupt, pos)
			}
			if err := verifyCRC(line, &r, r.CRC, func() { r.CRC = nil }); err != nil {
				return nil, fmt.Errorf("%w (byte %d)", err, pos)
			}
			switch r.K {
			case KindCommit, KindAbort, KindRetire:
			default:
				return nil, fmt.Errorf("%w: unknown kind %q", ErrJournalCorrupt, r.K)
			}
			if r.K == KindCommit {
				if want := SetDigest(r.Parts); want != r.PD {
					return nil, fmt.Errorf("%w: pd does not match parts", ErrJournalCorrupt)
				}
			}
			j.Records = append(j.Records, r)
		}
		pos += nl + 1
	}
	if first {
		return nil, fmt.Errorf("%w: empty journal", ErrJournalCorrupt)
	}
	j.GoodLen = pos
	return j, nil
}

func allZero(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}

// Decision is the effective journal state of one txid.
type Decision int

const (
	DecNone Decision = iota
	DecCommit
	DecAbort
	DecRetireCommit
	DecRetireAbort
)

func (d Decision) String() string {
	return [...]string{"none", "commit", "abort", "retire(commit)", "retire(abort)"}[d]
}

// Decisions folds the journal into txid → decision, applying the §4.3 rules:
// duplicate same-kind is idempotent; commit+abort for one txid is a conflict.
func (j *Journal) Decisions() (map[string]Decision, error) {
	out := map[string]Decision{}
	for _, r := range j.Records {
		var d Decision
		switch {
		case r.K == KindCommit:
			d = DecCommit
		case r.K == KindAbort:
			d = DecAbort
		case r.K == KindRetire && r.O == KindCommit:
			d = DecRetireCommit
		case r.K == KindRetire && r.O == KindAbort:
			d = DecRetireAbort
		default:
			return nil, fmt.Errorf("%w: bad retire outcome %q", ErrJournalCorrupt, r.O)
		}
		prev, ok := out[r.Tx]
		if !ok {
			out[r.Tx] = d
			continue
		}
		pc := prev == DecCommit || prev == DecRetireCommit
		dc := d == DecCommit || d == DecRetireCommit
		if pc != dc {
			return nil, fmt.Errorf("%w: tx %s", ErrDecisionConflict, r.Tx)
		}
		if d > prev { // retire supersedes the bare decision
			out[r.Tx] = d
		}
	}
	return out, nil
}
