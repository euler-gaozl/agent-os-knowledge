package knowledge

import (
	"context"
	"encoding/json"
	. "github.com/euler-gaozl/agent-os-sdk/contracts"
	"strings"
	"testing"
)

func TestGraphRejectsForeignMixedAndInventedProvenance(t *testing.T) {
	s := knowledgeStore(t)
	ctx := context.Background()
	kb := KnowledgeBase{Name: "Graph fixture", Engine: "lightrag", Endpoint: "https://graph.example.test", ChunkSize: 200}
	s.ResourceReader = liveKBResources{kb}
	ref := Ref{ID: "graph-scope", Revision: 1}
	foreignRef := Ref{ID: ref.ID, Revision: 2}
	owned, e := s.UploadKnowledge(ctx, ref, "owned.txt", "fixture://owned", []byte("OWNED842 inventory evidence. A plan is not actual production."))
	if e != nil {
		t.Fatal(e)
	}
	foreign, e := s.UploadKnowledge(ctx, foreignRef, "foreign.txt", "fixture://foreign", []byte("FOREIGN953 confidential data from a different revision."))
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.DB.Exec(ctx, "UPDATE knowledge_files SET status='ready'"); e != nil {
		t.Fatal(e)
	}
	p1, p2 := owned.ID+"-owned.txt", foreign.ID+"-foreign.txt"
	raw := JSON(map[string]any{
		"entities": []any{
			map[string]any{"entity_name": "Owned", "description": "OWNED842 extracted claim", "file_path": p1, "extra": "FOREIGN_HIDDEN"},
			map[string]any{"entity_name": "Foreign", "description": "FOREIGN953", "file_path": p2},
			map[string]any{"entity_name": "Mixed", "description": "FOREIGN_MERGED", "file_path": p1 + "<SEP>" + p2},
			map[string]any{"entity_name": "Missing", "description": "FOREIGN_MISSING"},
		},
		"relationships": []any{map[string]any{"src_id": "A", "tgt_id": "B", "description": "FOREIGN_RELATION", "file_path": p2}},
		"chunks": []any{
			map[string]any{"content": "OWNED842 inventory evidence.", "file_path": p1},
			map[string]any{"content": "FOREIGN953 confidential data from a different revision.", "file_path": p2},
			map[string]any{"content": "INVENTED777", "file_path": p1},
		},
		"references": []any{map[string]any{"file_path": "FOREIGN_REFERENCE"}},
		"metadata":   map[string]any{"debug": "FOREIGN_METADATA"},
	})
	clean, e := s.scopedGraph(ctx, ref, kb.Endpoint, raw)
	if e != nil {
		t.Fatal(e)
	}
	if strings.Contains(string(clean), "FOREIGN") || strings.Contains(string(clean), "INVENTED") || !strings.Contains(string(clean), owned.DocumentID) {
		t.Fatal("unsafe graph provenance", string(clean))
	}
	var result struct {
		Entities, Chunks, References []any
		Discarded                    int `json:"discarded_items"`
	}
	if e = json.Unmarshal(clean, &result); e != nil {
		t.Fatal(e)
	}
	if len(result.Entities) != 1 || len(result.Chunks) != 1 || len(result.References) != 1 || result.Discarded != 6 {
		t.Fatal("wrong scoped graph", string(clean))
	}
	// A valid path cannot launder foreign derived fields into context. Names
	// absent from local text are rejected; valid names only survive as hints,
	// with original spelling and per-field locally verified document IDs.
	forged := JSON(map[string]any{
		"entities": []any{
			map[string]any{"entity_name": "FOREIGN953", "description": "FOREIGN_DESCRIPTION", "file_path": p1},
			map[string]any{"entity_name": "owned842", "entity_type": "FOREIGN_TYPE", "description": "FOREIGN_DESCRIPTION", "file_path": p1},
		},
		"relationships": []any{
			map[string]any{"src_id": "OWNED842", "tgt_id": "FOREIGN953", "file_path": p1},
			map[string]any{"src_id": "owned842", "tgt_id": "Inventory", "description": "FOREIGN_RELATION", "keywords": "FOREIGN_KEYWORD", "file_path": p1},
		},
	})
	verified, e := s.scopedGraph(ctx, ref, kb.Endpoint, forged)
	if e != nil || strings.Contains(string(verified), "FOREIGN") {
		t.Fatal("forged authorized path leaked remote text", string(verified), e)
	}
	var hints struct{ Entities, Relationships []map[string]json.RawMessage }
	if e = json.Unmarshal(verified, &hints); e != nil || len(hints.Entities) != 1 || len(hints.Relationships) != 1 {
		t.Fatal("verified retrieval hints missing", string(verified), e)
	}
	if string(hints.Entities[0]["entity_name"]) != `"OWNED842"` || string(hints.Relationships[0]["tgt_id"]) != `"inventory"` || !strings.Contains(string(hints.Relationships[0]["field_sources"]), owned.DocumentID) || string(hints.Relationships[0]["status"]) != `"retrieval_hint_only"` {
		t.Fatal("hints must return original spelling and local field evidence", string(verified))
	}
	// A different configured backend cannot claim a source uploaded elsewhere.
	empty, e := s.scopedGraph(ctx, ref, "https://other.example.test", raw)
	if e != nil || strings.Contains(string(empty), "OWNED842") {
		t.Fatal("backend scope not checked", e)
	}
	if _, e = s.DB.Exec(ctx, "UPDATE knowledge_files SET status='external_pending' WHERE id=$1", owned.ID); e != nil {
		t.Fatal(e)
	}
	empty, e = s.scopedGraph(ctx, ref, kb.Endpoint, raw)
	if e != nil || strings.Contains(string(empty), "OWNED842") {
		t.Fatal("unfinished index accepted", e)
	}
}
