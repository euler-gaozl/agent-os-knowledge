package lexical

import (
	"github.com/euler-gaozl/agent-os-sdk/contracts"
	"strings"
	"unicode"
)

// SearchText produces deterministic lexical tokens without a resident tokenizer
// model. Han bigrams support Chinese phrases; Latin/digit words stay intact.
// This is lexical retrieval, not semantic understanding or Chinese word parsing.
func SearchText(text string) string {
	runes := []rune(strings.ToLower(text))
	seen := map[string]bool{}
	tokens := []string{}
	add := func(s string) {
		if s != "" && !seen[s] {
			seen[s] = true
			tokens = append(tokens, s)
		}
	}
	for i := 0; i < len(runes); {
		if unicode.Is(unicode.Han, runes[i]) {
			if i+1 < len(runes) && unicode.Is(unicode.Han, runes[i+1]) {
				add(string(runes[i : i+2]))
			}
			i++
			continue
		}
		if unicode.IsLetter(runes[i]) || unicode.IsDigit(runes[i]) {
			start := i
			for i < len(runes) && !unicode.Is(unicode.Han, runes[i]) && (unicode.IsLetter(runes[i]) || unicode.IsDigit(runes[i])) {
				i++
			}
			add(string(runes[start:i]))
			continue
		}
		i++
	}
	return strings.Join(tokens, " ")
}
func ScopeToken(ref contracts.Ref) string { return "scope" + contracts.Hash(contracts.JSON(ref))[:24] }
