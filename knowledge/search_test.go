package knowledge

import (
	"context"
	"encoding/json"
	. "github.com/euler-gaozl/agent-os-sdk/contracts"
	"strings"
	"testing"
)

type fixtureResources struct{}

func (fixtureResources) Resource(ctx context.Context, kind string, ref Ref, dst any) error {
	return json.Unmarshal(JSON(KnowledgeBase{Name: "Fixture", Engine: "local", ChunkSize: 200}), dst)
}
func knowledgeStore(t *testing.T) *Store {
	t.Helper()
	db, role, indexerRole := fixtureDatabase(t)
	if err := Migrate(context.Background(), db, role, indexerRole); err != nil {
		t.Fatal(err)
	}
	return &Store{DB: db, ResourceReader: fixtureResources{}}
}
func TestIncrementalBackfillAndScopedOriginalDocument(t *testing.T) {
	s := knowledgeStore(t)
	ctx := context.Background()
	ref := Ref{ID: "test", Revision: 1}
	body := []byte(strings.Repeat("甲醇库存 controlled source. ", 1400))
	file, e := s.UploadKnowledge(ctx, ref, "source.txt", "fixture://original", body)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.DB.Exec(ctx, "UPDATE knowledge_chunks SET search_vector=NULL"); e != nil {
		t.Fatal(e)
	}
	first, e := s.IndexLocalBatch(ctx)
	if e != nil || first != 128 {
		t.Fatal(first, e)
	}
	second, e := s.IndexLocalBatch(ctx)
	if e != nil || second == 0 {
		t.Fatal(second, e)
	}
	for {
		n, e := s.IndexLocalBatch(ctx)
		if e != nil {
			t.Fatal(e)
		}
		if n == 0 {
			break
		}
	}
	hits, e := s.Retrieve(ctx, ref, KnowledgeBase{Engine: "local"}, "甲醇库存")
	if e != nil || len(hits.Hits) == 0 || len(hits.Hits) > 8 {
		t.Fatal(hits, e)
	}
	wrong, e := s.Retrieve(ctx, Ref{ID: "test", Revision: 2}, KnowledgeBase{Engine: "local"}, "甲醇库存")
	if e != nil || len(wrong.Hits) != 0 {
		t.Fatal(wrong, e)
	}
	run := Run{Snapshot: Snapshot{Agent: Agent{Knowledge: []Ref{ref}}}}
	out, e := s.ReadDocument(ctx, run, file.DocumentID, 0, 100)
	if e != nil || !strings.Contains(string(JSON(out)), "fixture://original") {
		t.Fatal(out, e)
	}
	run.Snapshot.Agent.Knowledge[0].Revision = 2
	if _, e = s.ReadDocument(ctx, run, file.DocumentID, 0, 100); e == nil {
		t.Fatal("read crossed knowledge revision")
	}
	if _, e = s.Retrieve(ctx, ref, KnowledgeBase{Engine: "local"}, "甲"); e == nil {
		t.Fatal("unindexable query accepted")
	}
}

func TestFileNameSearchAndLegacyBackfillRemainVersionScoped(t *testing.T) {
	s := knowledgeStore(t)
	ctx := context.Background()
	ref := Ref{ID: "titles", Revision: 1}
	f, e := s.UploadKnowledge(ctx, ref, "验收口径.txt", "fixture://title", []byte("AZ-731: 120 -> 105. Planned +20."))
	if e != nil {
		t.Fatal(e)
	}
	check := func() {
		t.Helper()
		result, e := s.Retrieve(ctx, ref, KnowledgeBase{Engine: "local"}, "验收口径")
		if e != nil || len(result.Hits) != 1 || result.Hits[0]["document_id"] != f.DocumentID || result.Hits[0]["matched_on"] != "file_name" {
			t.Fatal(result, e)
		}
		if offset, ok := result.Hits[0]["document_offset"].(int); !ok || offset != 0 {
			t.Fatal("filename fallback has wrong document location", result.Hits[0])
		}
	}
	check()
	if _, e = s.DB.Exec(ctx, "UPDATE knowledge_files SET name_vector=NULL"); e != nil {
		t.Fatal(e)
	}
	if n, e := s.IndexNamesBatch(ctx); e != nil || n != 1 {
		t.Fatal(n, e)
	}
	check()
	for _, wrong := range []Ref{{ID: "titles", Revision: 2}, {ID: "another", Revision: 1}} {
		result, e := s.Retrieve(ctx, wrong, KnowledgeBase{Engine: "local"}, "验收口径")
		if e != nil || len(result.Hits) != 0 {
			t.Fatal("name query crossed scope", result, e)
		}
	}
}
