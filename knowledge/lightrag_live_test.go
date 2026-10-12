package knowledge

import (
	"context"
	"encoding/json"
	. "github.com/euler-gaozl/agent-os-sdk/contracts"
	"os"
	"strings"
	"testing"
	"time"
)

func TestLightRAGLiveContract(t *testing.T) {
	endpoint := os.Getenv("TEST_LIGHTRAG_URL")
	if endpoint == "" {
		t.Skip("explicit real LightRAG endpoint required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	s := knowledgeStore(t)
	kb := KnowledgeBase{Name: "Live graph adapter", Engine: "lightrag", Endpoint: endpoint, ChunkSize: 200}
	resources := liveKBResources{kb}
	s.ResourceReader = resources
	s.SecretReader = resources
	ref := Ref{ID: "live-graph-" + ID(), Revision: 1}
	file, e := s.UploadKnowledge(ctx, ref, "graph.txt", "fixture://graph-original", []byte("Methanol inventory is 108 controlled units. GRAPH842 is a protocol test marker, not a market fact. A plan is not actual production."))
	if e != nil {
		t.Fatal(e)
	}
	if e = s.IndexFile(ctx, file.ID); e != nil {
		t.Fatal(e)
	}
	pending, e := s.KnowledgeFile(ctx, file.ID)
	if e != nil || pending.TrackID == "" || pending.Status != "external_pending" {
		t.Fatal("actual upstream import receipt missing", pending.Status, pending.Error, e)
	}
	t.Log("PASS real LightRAG accepted async import; not marked ready before track completion")
	for {
		if e = s.IndexFile(ctx, file.ID); e != nil {
			t.Fatal(e)
		}
		status, e := s.KnowledgeFile(ctx, file.ID)
		if e != nil {
			t.Fatal(e)
		}
		if status.Status == "ready" {
			break
		}
		if status.Status == "failed" || status.Status == "unknown" {
			t.Fatal("real LightRAG indexing failed", status.Status, status.Error)
		}
		select {
		case <-ctx.Done():
			t.Fatal("actual graph indexing deadline")
		case <-time.After(time.Second):
		}
	}
	t.Log("PASS real LightRAG processed document with controlled LLM/embedding fixtures")
	result, e := s.Retrieve(ctx, ref, kb, "Methanol inventory")
	if e != nil {
		t.Fatal(e)
	}
	var graph struct{ Entities, Relationships, Chunks, References []json.RawMessage }
	if e = json.Unmarshal(result.Graph, &graph); e != nil {
		t.Fatal(e)
	}
	if len(graph.Chunks) == 0 || len(graph.References) == 0 || !strings.Contains(string(result.Graph), file.DocumentID) || !strings.Contains(string(result.Graph), "GRAPH842") {
		t.Fatal("actual upstream context lacks validated original source", string(result.Graph))
	}
	if len(graph.Entities) == 0 || len(graph.Relationships) == 0 {
		t.Fatal("graph indexing did not produce controlled entities and relationships", string(result.Graph))
	}
	t.Log("PASS actual graph and vector contexts resolve retained source IDs")
	// Querying the same upstream with an unbound revision must not expose its
	// graph. This checks the production API adapter, not a fabricated response.
	other, e := s.Retrieve(ctx, Ref{ID: ref.ID, Revision: 2}, kb, "Methanol inventory")
	if e != nil || strings.Contains(string(other.Graph), "GRAPH842") || strings.Contains(string(other.Graph), file.DocumentID) {
		t.Fatal("actual backend leaked across revision", e)
	}
	if e = json.Unmarshal(other.Graph, &graph); e != nil {
		t.Fatal(e)
	}
	if len(graph.Entities)+len(graph.Relationships)+len(graph.Chunks)+len(graph.References) != 0 {
		t.Fatal("unbound graph data returned", string(other.Graph))
	}
	t.Log("PASS real backend data from an unbound revision is excluded")
}
