package knowledge

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/euler-gaozl/agent-os-sdk/contracts"
)

func TestModelRetrievalPreservesHandlesWithinInlineBudget(t *testing.T) {
	for _, text := range []string{strings.Repeat("库存😀", 1200), strings.Repeat("\x00<>&", 1200)} {
		result := contracts.Retrieval{Engine: "local"}
		for i := 0; i < 8; i++ {
			result.Hits = append(result.Hits, map[string]any{
				"document_id": fmt.Sprintf("%032d", i), "chunk_id": fmt.Sprintf("%064d", i),
				"title": text, "content": text, "source": "https://example.test/original",
				"document_offset": 2147483646,
			})
		}
		view := retrievalForModel(result).(map[string]any)
		encoded, err := json.Marshal(view)
		if err != nil || len(encoded) > 5500 || !utf8.Valid(encoded) {
			t.Fatalf("invalid or oversized model view: %d bytes, %v", len(encoded), err)
		}
		hits := view["hits"].([]map[string]any)
		if len(hits) != 8 || view["preview_only"] != true || view["coverage"] != "matching_candidates_only" {
			t.Fatal("lost references or retrieval caveat")
		}
		for i, hit := range hits {
			if hit["document_id"] != result.Hits[i]["document_id"] || hit["chunk_id"] != result.Hits[i]["chunk_id"] {
				t.Fatal("document handle changed")
			}
			if hit["document_offset"] != 2147483646 {
				t.Fatal("lost original-document coordinate")
			}
			if result.Hits[i]["content"] != text || result.Hits[i]["source"] != "https://example.test/original" {
				t.Fatal("Web/original retrieval was modified")
			}
		}
	}
}

func TestModelRetrievalDoesNotInventOffsets(t *testing.T) {
	view := retrievalForModel(contracts.Retrieval{Engine: "local", Hits: []map[string]any{
		{"document_id": "missing"}, {"document_id": "negative", "document_offset": -1},
		{"document_id": "start", "document_offset": 0},
	}}).(map[string]any)
	hits := view["hits"].([]map[string]any)
	for _, hit := range hits[:2] {
		if _, exists := hit["document_offset"]; exists {
			t.Fatal("unlocated text was assigned a location")
		}
	}
	if offset, exists := hits[2]["document_offset"]; !exists || offset != 0 {
		t.Fatal("actual zero offset was lost")
	}
}

func TestModelRetrievalDeduplicatesDocumentsAndKeepsEngineSemantics(t *testing.T) {
	result := contracts.Retrieval{Engine: "local", Hits: []map[string]any{{"document_id": "one", "content": "first"}, {"document_id": "one", "content": "second"}}}
	view := retrievalForModel(result).(map[string]any)
	if len(view["hits"].([]map[string]any)) != 1 || view["query_mode"] != "all_terms" {
		t.Fatal("duplicate document or wrong search mode")
	}
	result.Engine = "lightrag"
	if _, ok := retrievalForModel(result).(contracts.Retrieval); !ok {
		t.Fatal("graph retrieval semantics changed")
	}
}
