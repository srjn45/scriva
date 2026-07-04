// Package query implements filter evaluation for FileDB scan operations.
// Filters are applied in-process after reading a record's data map.
package query

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

// Op is a comparison operator.
type Op string

const (
	OpEq       Op = "eq"
	OpNeq      Op = "neq"
	OpGt       Op = "gt"
	OpGte      Op = "gte"
	OpLt       Op = "lt"
	OpLte      Op = "lte"
	OpContains Op = "contains"
	OpRegex    Op = "regex"
)

// Filter is the common interface for all filter nodes.
type Filter interface {
	Match(record map[string]any) bool
}

// FieldFilter tests a single field against a value.
type FieldFilter struct {
	Field string
	Op    Op
	Value string // JSON-encoded comparison value
}

// AndFilter passes only if all child filters match.
type AndFilter struct{ Filters []Filter }

// OrFilter passes if any child filter matches.
type OrFilter struct{ Filters []Filter }

// MatchAll is a filter that accepts every record.
var MatchAll Filter = matchAllFilter{}

type matchAllFilter struct{}

func (matchAllFilter) Match(_ map[string]any) bool { return true }

func (f *AndFilter) Match(r map[string]any) bool {
	for _, sub := range f.Filters {
		if !sub.Match(r) {
			return false
		}
	}
	return true
}

func (f *OrFilter) Match(r map[string]any) bool {
	for _, sub := range f.Filters {
		if sub.Match(r) {
			return true
		}
	}
	return false
}

func (f *FieldFilter) Match(record map[string]any) bool {
	fieldVal, ok := record[f.Field]
	if !ok {
		return false
	}

	// Decode the comparison value from JSON.
	var cmp any
	if err := json.Unmarshal([]byte(f.Value), &cmp); err != nil {
		// Treat as plain string if not valid JSON.
		cmp = f.Value
	}

	switch f.Op {
	case OpEq:
		return equal(fieldVal, cmp)
	case OpNeq:
		return !equal(fieldVal, cmp)
	case OpGt:
		return Compare(fieldVal, cmp) > 0
	case OpGte:
		return Compare(fieldVal, cmp) >= 0
	case OpLt:
		return Compare(fieldVal, cmp) < 0
	case OpLte:
		return Compare(fieldVal, cmp) <= 0
	case OpContains:
		return strings.Contains(fmt.Sprintf("%v", fieldVal), fmt.Sprintf("%v", cmp))
	case OpRegex:
		re, err := regexp.Compile(fmt.Sprintf("%v", cmp))
		if err != nil {
			return false
		}
		return re.MatchString(fmt.Sprintf("%v", fieldVal))
	}
	return false
}

// Comparison semantics (eq/neq/gt/gte/lt/lte)
//
// Both the stored field value and the filter comparison value carry their
// JSON type: numbers decode to float64, strings to string. The operators
// honour those types rather than blindly stringifying:
//
//   - Both values are numbers  -> compared NUMERICALLY.
//     So `age gt 9` matches a record with age 10 (10 > 9), instead of the
//     lexicographic surprise where "10" < "9".
//   - Both values are strings  -> compared LEXICOGRAPHICALLY (byte order).
//     A numeric-looking string such as "10" is NOT coerced to a number; if
//     you stored a field as a string it keeps string ordering.
//   - The types differ (one number, one string) -> the two values are
//     compared lexicographically by their string representation. This is
//     deterministic but rarely meaningful; comparing a number against a
//     string is a query mistake, so we degrade predictably rather than
//     guessing an intended coercion.

// equal performs a type-aware equality check between a record field value
// (which may be float64 from JSON decode) and a comparison value.
func equal(a, b any) bool {
	af, aIsNum := asNumber(a)
	bf, bIsNum := asNumber(b)
	if aIsNum && bIsNum {
		return af == bf
	}
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

// Compare returns -1, 0, or 1 for a < b, a == b, a > b following the
// comparison semantics documented above. It is the single comparison used by
// both filter operators (gt/gte/lt/lte) and by order_by sorting in the engine,
// so a query and a sort always agree on how two values relate.
func Compare(a, b any) int {
	af, aIsNum := asNumber(a)
	bf, bIsNum := asNumber(b)
	if aIsNum && bIsNum {
		switch {
		case af < bf:
			return -1
		case af > bf:
			return 1
		default:
			return 0
		}
	}
	as, bs := fmt.Sprintf("%v", a), fmt.Sprintf("%v", b)
	return strings.Compare(as, bs)
}

// AsNumber reports whether v is a JSON/Go numeric value and, if so, returns it as
// a float64. It applies the exact same type rules as Compare and the gt/lt filter
// operators — numeric-looking strings are deliberately not coerced — so a numeric
// aggregation (sum/avg/min/max) agrees with how the same field filters and sorts.
func AsNumber(v any) (float64, bool) { return asNumber(v) }

// asNumber reports whether v is a JSON/Go numeric value and returns it as a
// float64. Numeric-looking strings are deliberately NOT treated as numbers:
// a value's JSON type determines whether numeric or lexicographic comparison
// applies, so a string field always keeps string ordering.
func asNumber(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int32:
		return float64(x), true
	case int64:
		return float64(x), true
	case json.Number:
		if f, err := x.Float64(); err == nil {
			return f, true
		}
	}
	return 0, false
}
