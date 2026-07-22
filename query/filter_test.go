package query_test

import (
	"testing"

	"github.com/srjn45/scriva/query"
)

// record is a helper to build a data map concisely.
func record(pairs ...any) map[string]any {
	m := make(map[string]any, len(pairs)/2)
	for i := 0; i < len(pairs)-1; i += 2 {
		m[pairs[i].(string)] = pairs[i+1]
	}
	return m
}

// ---- FieldFilter ------------------------------------------------------------

func TestFieldFilter_Eq(t *testing.T) {
	f := &query.FieldFilter{Field: "role", Op: query.OpEq, Value: `"admin"`}
	if !f.Match(record("role", "admin")) {
		t.Error("expected match for equal string")
	}
	if f.Match(record("role", "user")) {
		t.Error("expected no match for different string")
	}
}

func TestFieldFilter_Eq_Numeric(t *testing.T) {
	f := &query.FieldFilter{Field: "age", Op: query.OpEq, Value: "30"}
	if !f.Match(record("age", float64(30))) {
		t.Error("expected match for equal number")
	}
	if f.Match(record("age", float64(31))) {
		t.Error("expected no match for different number")
	}
}

func TestFieldFilter_Neq(t *testing.T) {
	f := &query.FieldFilter{Field: "role", Op: query.OpNeq, Value: `"admin"`}
	if !f.Match(record("role", "user")) {
		t.Error("expected match for neq")
	}
	if f.Match(record("role", "admin")) {
		t.Error("expected no match when equal")
	}
}

func TestFieldFilter_Gt(t *testing.T) {
	f := &query.FieldFilter{Field: "age", Op: query.OpGt, Value: "25"}
	if !f.Match(record("age", float64(30))) {
		t.Error("expected match for 30 > 25")
	}
	if f.Match(record("age", float64(25))) {
		t.Error("expected no match for 25 > 25")
	}
	if f.Match(record("age", float64(20))) {
		t.Error("expected no match for 20 > 25")
	}
}

func TestFieldFilter_Gte(t *testing.T) {
	f := &query.FieldFilter{Field: "age", Op: query.OpGte, Value: "25"}
	if !f.Match(record("age", float64(25))) {
		t.Error("expected match for 25 >= 25")
	}
	if !f.Match(record("age", float64(26))) {
		t.Error("expected match for 26 >= 25")
	}
	if f.Match(record("age", float64(24))) {
		t.Error("expected no match for 24 >= 25")
	}
}

func TestFieldFilter_Lt(t *testing.T) {
	f := &query.FieldFilter{Field: "age", Op: query.OpLt, Value: "25"}
	if !f.Match(record("age", float64(20))) {
		t.Error("expected match for 20 < 25")
	}
	if f.Match(record("age", float64(25))) {
		t.Error("expected no match for 25 < 25")
	}
}

func TestFieldFilter_Lte(t *testing.T) {
	f := &query.FieldFilter{Field: "age", Op: query.OpLte, Value: "25"}
	if !f.Match(record("age", float64(25))) {
		t.Error("expected match for 25 <= 25")
	}
	if f.Match(record("age", float64(26))) {
		t.Error("expected no match for 26 <= 25")
	}
}

func TestFieldFilter_Contains(t *testing.T) {
	f := &query.FieldFilter{Field: "email", Op: query.OpContains, Value: `"@example"`}
	if !f.Match(record("email", "alice@example.com")) {
		t.Error("expected match for contains")
	}
	if f.Match(record("email", "alice@other.com")) {
		t.Error("expected no match when not contained")
	}
}

func TestFieldFilter_Regex(t *testing.T) {
	f := &query.FieldFilter{Field: "name", Op: query.OpRegex, Value: `"^Al"`}
	if !f.Match(record("name", "Alice")) {
		t.Error("expected match for regex ^Al")
	}
	if f.Match(record("name", "Bob")) {
		t.Error("expected no match for Bob against ^Al")
	}
}

func TestFieldFilter_Regex_Invalid(t *testing.T) {
	// Invalid regex should not panic — just returns false.
	f := &query.FieldFilter{Field: "name", Op: query.OpRegex, Value: `"[invalid"`}
	if f.Match(record("name", "Alice")) {
		t.Error("expected false for invalid regex")
	}
}

func TestFieldFilter_MissingField(t *testing.T) {
	f := &query.FieldFilter{Field: "missing", Op: query.OpEq, Value: `"x"`}
	if f.Match(record("name", "Alice")) {
		t.Error("expected no match when field absent")
	}
}

// ---- Numeric comparison correctness -----------------------------------------

// TestFieldFilter_Gt_NumericNotLexical is the regression test for the headline
// bug: "10" < "9" lexicographically, but 10 > 9 numerically. With JSON numbers
// on both sides the comparison must be numeric.
func TestFieldFilter_Gt_NumericNotLexical(t *testing.T) {
	f := &query.FieldFilter{Field: "age", Op: query.OpGt, Value: "9"}
	if !f.Match(record("age", float64(10))) {
		t.Error("expected match: 10 > 9 numerically (lexical compare would fail because \"10\" < \"9\")")
	}
	if f.Match(record("age", float64(9))) {
		t.Error("expected no match for 9 > 9")
	}
	if f.Match(record("age", float64(8))) {
		t.Error("expected no match for 8 > 9")
	}
}

// TestFieldFilter_NumericBoundaries exercises gt/gte/lt/lte around the boundary
// value for the multi-digit case that breaks lexical ordering.
func TestFieldFilter_NumericBoundaries(t *testing.T) {
	cases := []struct {
		name  string
		op    query.Op
		val   string
		field float64
		want  bool
	}{
		{"gt below", query.OpGt, "100", 99, false},
		{"gt equal", query.OpGt, "100", 100, false},
		{"gt above", query.OpGt, "100", 101, true},

		{"gte below", query.OpGte, "100", 99, false},
		{"gte equal", query.OpGte, "100", 100, true},
		{"gte above", query.OpGte, "100", 101, true},

		{"lt below", query.OpLt, "100", 99, true},
		{"lt equal", query.OpLt, "100", 100, false},
		{"lt above", query.OpLt, "100", 101, false},

		{"lte below", query.OpLte, "100", 99, true},
		{"lte equal", query.OpLte, "100", 100, true},
		{"lte above", query.OpLte, "100", 101, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := &query.FieldFilter{Field: "n", Op: tc.op, Value: tc.val}
			if got := f.Match(record("n", tc.field)); got != tc.want {
				t.Errorf("%s %s %s with field %v: got %v want %v",
					"n", tc.op, tc.val, tc.field, got, tc.want)
			}
		})
	}
}

func TestFieldFilter_NegativeNumbers(t *testing.T) {
	gt := &query.FieldFilter{Field: "t", Op: query.OpGt, Value: "-5"}
	if !gt.Match(record("t", float64(-3))) {
		t.Error("expected match: -3 > -5")
	}
	if gt.Match(record("t", float64(-10))) {
		t.Error("expected no match: -10 > -5 is false")
	}

	lt := &query.FieldFilter{Field: "t", Op: query.OpLt, Value: "0"}
	if !lt.Match(record("t", float64(-1))) {
		t.Error("expected match: -1 < 0")
	}
	if lt.Match(record("t", float64(1))) {
		t.Error("expected no match: 1 < 0 is false")
	}
}

func TestFieldFilter_Floats(t *testing.T) {
	f := &query.FieldFilter{Field: "score", Op: query.OpGt, Value: "9.5"}
	if !f.Match(record("score", float64(9.51))) {
		t.Error("expected match: 9.51 > 9.5")
	}
	if f.Match(record("score", float64(9.5))) {
		t.Error("expected no match: 9.5 > 9.5 is false")
	}
	if f.Match(record("score", float64(9.49))) {
		t.Error("expected no match: 9.49 > 9.5 is false")
	}

	// Float field against an integer comparison value still compares numerically.
	lte := &query.FieldFilter{Field: "score", Op: query.OpLte, Value: "10"}
	if !lte.Match(record("score", float64(9.99))) {
		t.Error("expected match: 9.99 <= 10")
	}
}

// TestFieldFilter_IntFieldValue ensures non-JSON numeric Go types (int, int64)
// that may reach the filter are also treated numerically.
func TestFieldFilter_IntFieldValue(t *testing.T) {
	f := &query.FieldFilter{Field: "age", Op: query.OpGt, Value: "9"}
	if !f.Match(record("age", 10)) {
		t.Error("expected match: int 10 > 9")
	}
	if !f.Match(record("age", int64(10))) {
		t.Error("expected match: int64 10 > 9")
	}
}

// ---- String comparison stays lexicographic ----------------------------------

func TestFieldFilter_StringComparisonLexical(t *testing.T) {
	gt := &query.FieldFilter{Field: "name", Op: query.OpGt, Value: `"m"`}
	if !gt.Match(record("name", "n")) {
		t.Error("expected match: \"n\" > \"m\" lexically")
	}
	if gt.Match(record("name", "a")) {
		t.Error("expected no match: \"a\" > \"m\" is false")
	}

	lt := &query.FieldFilter{Field: "name", Op: query.OpLt, Value: `"banana"`}
	if !lt.Match(record("name", "apple")) {
		t.Error("expected match: \"apple\" < \"banana\" lexically")
	}
}

// TestFieldFilter_NumericStringsStayLexical proves that string-typed fields keep
// lexicographic ordering even when they look numeric — they are NOT coerced to
// numbers. "10" < "9" as strings.
func TestFieldFilter_NumericStringsStayLexical(t *testing.T) {
	f := &query.FieldFilter{Field: "code", Op: query.OpGt, Value: `"9"`}
	if f.Match(record("code", "10")) {
		t.Error("expected no match: string \"10\" > \"9\" is false lexically")
	}
	if !f.Match(record("code", "95")) {
		t.Error("expected match: string \"95\" > \"9\" lexically")
	}
}

// TestFieldFilter_MismatchedTypes documents the cross-type fallback: a numeric
// field compared against a string comparison value (and vice versa) degrades to
// a deterministic lexicographic comparison of their string forms.
func TestFieldFilter_MismatchedTypes(t *testing.T) {
	// Numeric field, string comparison value.
	f := &query.FieldFilter{Field: "age", Op: query.OpEq, Value: `"30"`}
	if !f.Match(record("age", float64(30))) {
		t.Error("expected match: number 30 vs string \"30\" stringify-equal")
	}

	// gt across types is lexicographic on string forms: "30" > "9".
	gt := &query.FieldFilter{Field: "age", Op: query.OpGt, Value: `"9"`}
	if gt.Match(record("age", float64(30))) {
		t.Error("expected no match: \"30\" > \"9\" is false lexically")
	}
}

// ---- AndFilter / OrFilter ---------------------------------------------------

func TestAndFilter_AllMatch(t *testing.T) {
	f := &query.AndFilter{Filters: []query.Filter{
		&query.FieldFilter{Field: "role", Op: query.OpEq, Value: `"admin"`},
		&query.FieldFilter{Field: "age", Op: query.OpGt, Value: "20"},
	}}
	if !f.Match(record("role", "admin", "age", float64(30))) {
		t.Error("expected match when all sub-filters match")
	}
}

func TestAndFilter_OneFails(t *testing.T) {
	f := &query.AndFilter{Filters: []query.Filter{
		&query.FieldFilter{Field: "role", Op: query.OpEq, Value: `"admin"`},
		&query.FieldFilter{Field: "age", Op: query.OpGt, Value: "50"},
	}}
	if f.Match(record("role", "admin", "age", float64(30))) {
		t.Error("expected no match when one sub-filter fails")
	}
}

func TestOrFilter_OneMatches(t *testing.T) {
	f := &query.OrFilter{Filters: []query.Filter{
		&query.FieldFilter{Field: "role", Op: query.OpEq, Value: `"admin"`},
		&query.FieldFilter{Field: "role", Op: query.OpEq, Value: `"superuser"`},
	}}
	if !f.Match(record("role", "admin")) {
		t.Error("expected match when one sub-filter matches")
	}
}

func TestOrFilter_NoneMatch(t *testing.T) {
	f := &query.OrFilter{Filters: []query.Filter{
		&query.FieldFilter{Field: "role", Op: query.OpEq, Value: `"admin"`},
		&query.FieldFilter{Field: "role", Op: query.OpEq, Value: `"superuser"`},
	}}
	if f.Match(record("role", "user")) {
		t.Error("expected no match when no sub-filter matches")
	}
}

func TestMatchAll(t *testing.T) {
	if !query.MatchAll.Match(record()) {
		t.Error("MatchAll should match empty record")
	}
	if !query.MatchAll.Match(record("x", "y")) {
		t.Error("MatchAll should match any record")
	}
}

// ---- Nested (And inside Or) -------------------------------------------------

func TestNestedFilter(t *testing.T) {
	// (role=admin AND age>25) OR (role=superuser)
	f := &query.OrFilter{Filters: []query.Filter{
		&query.AndFilter{Filters: []query.Filter{
			&query.FieldFilter{Field: "role", Op: query.OpEq, Value: `"admin"`},
			&query.FieldFilter{Field: "age", Op: query.OpGt, Value: "25"},
		}},
		&query.FieldFilter{Field: "role", Op: query.OpEq, Value: `"superuser"`},
	}}

	if !f.Match(record("role", "admin", "age", float64(30))) {
		t.Error("expected match: admin age 30")
	}
	if f.Match(record("role", "admin", "age", float64(20))) {
		t.Error("expected no match: admin age 20 (fails age>25)")
	}
	if !f.Match(record("role", "superuser", "age", float64(10))) {
		t.Error("expected match: superuser regardless of age")
	}
}

// ---- Compare (shared by filter operators and engine order_by) ---------------

// TestCompare exercises the exported Compare directly. It is the single
// comparison used by both the gt/gte/lt/lte operators and by order_by sorting,
// so these cases pin the semantics both paths rely on.
func TestCompare(t *testing.T) {
	cases := []struct {
		name string
		a, b any
		want int // sign: -1 a<b, 0 equal, +1 a>b
	}{
		{"numeric less, not lexical", float64(2), float64(10), -1},
		{"numeric greater", float64(10), float64(2), 1},
		{"numeric equal", float64(7), float64(7), 0},
		{"negative numbers", float64(-5), float64(-1), -1},
		{"string lexical less", "apple", "banana", -1},
		{"string lexical greater", "banana", "apple", 1},
		{"string equal", "x", "x", 0},
		// A numeric-looking string keeps string ordering — "10" < "9" lexically.
		{"numeric-looking strings stay lexical", "10", "9", -1},
		// Mixed types degrade to a deterministic string comparison.
		{"mixed number vs string is deterministic", float64(5), "5", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := sign(query.Compare(tc.a, tc.b))
			if got != tc.want {
				t.Errorf("Compare(%v, %v) = %d, want sign %d", tc.a, tc.b, got, tc.want)
			}
			// Compare must be antisymmetric: swapping the arguments flips the sign.
			if rev := sign(query.Compare(tc.b, tc.a)); rev != -tc.want {
				t.Errorf("Compare(%v, %v) = %d, want %d (antisymmetry)", tc.b, tc.a, rev, -tc.want)
			}
		})
	}
}

func sign(n int) int {
	switch {
	case n < 0:
		return -1
	case n > 0:
		return 1
	default:
		return 0
	}
}
