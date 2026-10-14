package knowledge

import (
	"context"
	"strings"
	"testing"
	"unicode/utf8"

	. "github.com/euler-gaozl/agent-os-sdk/contracts"
)

// Imported capture text has a generated prefix. The returned coordinate must
// address the retained document, not HTML bytes, capture text or UTF-16 units.
func TestCaptureSearchOffsetReadsExactRetainedDocument(t *testing.T) {
	s, admin, b := captureImportFixture(t)
	ctx := context.Background()
	b.Text = strings.Repeat("导航🙂其他栏目\n", 180) + "\n核心库存证据：本期120万吨。\n"
	b.TextHash = Hash([]byte(b.Text))
	b.TextBytes = len(b.Text)
	s.CaptureReader = captureFixtureReader{b}
	ref := Ref{ID: "offset-capture", Revision: 1}
	imported, err := s.importCapture(ctx, ref, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	var original string
	if err = admin.QueryRow(ctx, "SELECT content FROM documents WHERE id=$1", imported.File.DocumentID).Scan(&original); err != nil {
		t.Fatal(err)
	}
	result, err := s.Retrieve(ctx, ref, imported.File.Config, "核心库存")
	if err != nil || len(result.Hits) != 1 {
		t.Fatal("missing scoped match", result, err)
	}
	hit := result.Hits[0]
	offset, ok := hit["document_offset"].(int)
	chunk := hit["content"].(string)
	if !ok || offset < 1200 || offset != utf8.RuneCountInString(original[:strings.Index(original, chunk)]) {
		t.Fatal("wrong retained document coordinate", offset)
	}
	run := Run{Snapshot: Snapshot{Agent: Agent{Knowledge: []Ref{ref}}}}
	page, err := s.ReadDocument(ctx, run, imported.File.DocumentID, offset, 1200)
	if err != nil || !strings.HasPrefix(page.(map[string]any)["document"].(Document).Content, chunk) {
		t.Fatal("model cannot read located evidence", page, err)
	}
	view := retrievalForModel(result).(map[string]any)["hits"].([]map[string]any)[0]
	if view["document_offset"] != offset {
		t.Fatal("model location differs from Web location")
	}
	run.Snapshot.Agent.Knowledge[0].Revision = 2
	if _, err = s.ReadDocument(ctx, run, imported.File.DocumentID, offset, 1200); err == nil {
		t.Fatal("offset bypassed knowledge authorization")
	}
	wrong, err := s.Retrieve(ctx, Ref{ID: ref.ID, Revision: 2}, imported.File.Config, "核心库存")
	if err != nil || len(wrong.Hits) != 0 {
		t.Fatal("offset query exposed another revision", wrong, err)
	}
	// Legacy/inconsistent chunks still give a candidate, never a false zero.
	if _, err = admin.Exec(ctx, "UPDATE documents SET content='已变更的资料' WHERE id=$1", imported.File.DocumentID); err != nil {
		t.Fatal(err)
	}
	result, err = s.Retrieve(ctx, ref, imported.File.Config, "核心库存")
	if err != nil || len(result.Hits) != 1 {
		t.Fatal(result, err)
	}
	if _, exists := result.Hits[0]["document_offset"]; exists {
		t.Fatal("stale chunk produced a false position")
	}
}

func TestDuplicateChunksKeepTheirOwnDocumentCoordinates(t *testing.T) {
	s := knowledgeStore(t)
	ctx := context.Background()
	ref := Ref{ID: "repeated-paragraph", Revision: 1}
	text := strings.Repeat("甲", 160) + strings.Repeat("重复库存🙂", 40) + strings.Repeat("乙", 120) + strings.Repeat("重复库存🙂", 40)
	f, err := s.UploadKnowledge(ctx, ref, "dates.txt", "fixture://duplicate-periods", []byte(text))
	if err != nil {
		t.Fatal(err)
	}
	result, err := s.Retrieve(ctx, ref, KnowledgeBase{Engine: "local"}, "重复库存")
	if err != nil || len(result.Hits) < 2 {
		t.Fatal(result, err)
	}
	seen := map[int]bool{}
	for _, hit := range result.Hits {
		var ordinal int
		if err = s.DB.QueryRow(ctx, "SELECT ordinal FROM knowledge_chunks WHERE id=$1", hit["chunk_id"]).Scan(&ordinal); err != nil {
			t.Fatal(err)
		}
		offset, ok := hit["document_offset"].(int)
		if !ok || offset != ordinal*160 {
			t.Fatalf("duplicate chunk moved: ordinal %d, offset %v", ordinal, hit["document_offset"])
		}
		if hit["content"] == strings.Repeat("重复库存🙂", 40) {
			seen[offset] = true
		}
	}
	if !seen[160] || !seen[480] {
		t.Fatal("fixture must expose equal text at different positions", seen)
	}
	if _, err = s.DB.Exec(ctx, "UPDATE knowledge_files SET config=config-'chunk_size' WHERE id=$1", f.ID); err != nil {
		t.Fatal(err)
	}
	result, err = s.Retrieve(ctx, ref, KnowledgeBase{Engine: "local"}, "重复库存")
	if err != nil {
		t.Fatal(err)
	}
	for _, hit := range result.Hits {
		if _, exists := hit["document_offset"]; exists {
			t.Fatal("invented offset for unversioned chunk settings")
		}
	}
}
