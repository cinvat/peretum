package base

import "testing"

func TestGetBool(t *testing.T) {
	m := map[string]any{"yes": true, "no": false}
	if !GetBool(m, "yes") {
		t.Fatal("expected true")
	}
	if GetBool(m, "no") {
		t.Fatal("expected false")
	}
	if GetBool(m, "missing") {
		t.Fatal("expected false for missing key")
	}
	if GetBool(map[string]any{"s": "true"}, "s") {
		t.Fatal("expected false for non-bool value")
	}
}

func TestGetInt(t *testing.T) {
	m := map[string]any{"i": 42, "f": 3.9}
	if got := GetInt(m, "i"); got != 42 {
		t.Fatalf("GetInt int = %d", got)
	}
	if got := GetInt(m, "f"); got != 3 {
		t.Fatalf("GetInt float = %d", got)
	}
	if got := GetInt(m, "missing"); got != 0 {
		t.Fatalf("GetInt missing = %d", got)
	}
	if got := GetInt(map[string]any{"s": "5"}, "s"); got != 0 {
		t.Fatalf("GetInt string = %d", got)
	}
}

func TestGetIntOrDefault(t *testing.T) {
	m := map[string]any{"i": 7, "f": 2.8}
	if got := GetIntOrDefault(m, "i", 99); got != 7 {
		t.Fatalf("GetIntOrDefault int = %d", got)
	}
	if got := GetIntOrDefault(m, "f", 99); got != 2 {
		t.Fatalf("GetIntOrDefault float = %d", got)
	}
	if got := GetIntOrDefault(m, "missing", 99); got != 99 {
		t.Fatalf("GetIntOrDefault missing = %d", got)
	}
	if got := GetIntOrDefault(map[string]any{"s": "5"}, "s", 99); got != 99 {
		t.Fatalf("GetIntOrDefault string = %d", got)
	}
}

func TestGetString(t *testing.T) {
	m := map[string]any{"s": "hello"}
	if got := GetString(m, "s"); got != "hello" {
		t.Fatalf("GetString = %q", got)
	}
	if got := GetString(m, "missing"); got != "" {
		t.Fatalf("GetString missing = %q", got)
	}
	if got := GetString(map[string]any{"i": 1}, "i"); got != "" {
		t.Fatalf("GetString non-string = %q", got)
	}
}

func TestGetStringSlice(t *testing.T) {
	m := map[string]any{
		"mixed": []any{"a", 1, "b"},
		"typed": []string{"x", "y"},
	}
	if got := GetStringSlice(m, "mixed"); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("GetStringSlice mixed = %v", got)
	}
	if got := GetStringSlice(m, "typed"); len(got) != 2 || got[0] != "x" || got[1] != "y" {
		t.Fatalf("GetStringSlice typed = %v", got)
	}
	if got := GetStringSlice(m, "missing"); got != nil {
		t.Fatalf("GetStringSlice missing = %v", got)
	}
	if got := GetStringSlice(map[string]any{"s": "z"}, "s"); got != nil {
		t.Fatalf("GetStringSlice non-slice = %v", got)
	}
}

func TestGetInt64(t *testing.T) {
	m := map[string]any{
		"int64":          int64(42),
		"float":          43.9,
		"int":            44,
		"numeric_string": "45",
	}
	if got := GetInt64(m, "int64"); got != 42 {
		t.Fatalf("GetInt64 int64 = %d, want 42", got)
	}
	if got := GetInt64(m, "float"); got != 43 {
		t.Fatalf("GetInt64 float64 = %d, want 43 (truncated)", got)
	}
	if got := GetInt64(m, "int"); got != 44 {
		t.Fatalf("GetInt64 int = %d, want 44", got)
	}
	if got := GetInt64(m, "missing"); got != 0 {
		t.Fatalf("GetInt64 missing = %d, want 0", got)
	}
	if got := GetInt64(m, "numeric_string"); got != 0 {
		t.Fatalf("GetInt64 string = %d, want 0", got)
	}
}
