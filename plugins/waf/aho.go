package waf

import "github.com/coregx/ahocorasick"

// ahoMatcher wraps the coregx ahocorasick library for WAF signature matching.
// It reports every pattern that occurs as a substring in a single linear pass.
type ahoMatcher struct {
	automaton *ahocorasick.Automaton
	// condMap maps each pattern index to the condition IDs it satisfies.
	condMap [][]int
}

// buildAhoMatcher constructs the automaton from the given patterns and
// condition-ID mappings.  Empty patterns are silently skipped; if all patterns
// are empty, nil is returned.
func buildAhoMatcher(patterns []string, condMap [][]int) *ahoMatcher {
	var nonEmpty []string
	var nonEmptyCondMap [][]int
	for i, p := range patterns {
		if p == "" {
			continue
		}
		nonEmpty = append(nonEmpty, p)
		nonEmptyCondMap = append(nonEmptyCondMap, condMap[i])
	}
	if len(nonEmpty) == 0 {
		return nil
	}
	b := ahocorasick.NewBuilder()
	b.AddStrings(nonEmpty)
	// Build cannot fail here: nonEmpty holds only valid non-empty patterns.
	a, _ := b.Build()
	return &ahoMatcher{automaton: a, condMap: nonEmptyCondMap}
}

// match scans text and returns the condition IDs of every pattern that occurs
// as a substring.  The same condition ID is only reported once even if the
// pattern matches at multiple positions.
func (m *ahoMatcher) match(text []byte) []int {
	if m == nil || len(text) == 0 {
		return nil
	}
	var result []int
	seen := make(map[int]struct{})
	for _, match := range m.automaton.FindAllOverlapping(text) {
		if _, dup := seen[match.PatternID]; dup {
			continue
		}
		seen[match.PatternID] = struct{}{}
		result = append(result, m.condMap[match.PatternID]...)
	}
	return result
}
