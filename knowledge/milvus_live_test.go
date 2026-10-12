package knowledge

import (
	"context"
	"encoding/json"
	"fmt"
	. "github.com/euler-gaozl/agent-os-sdk/contracts"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type liveKBResources struct{ kb KnowledgeBase }

func (r liveKBResources) Resource(_ context.Context, _ string, _ Ref, dst any) error {
	return json.Unmarshal(JSON(r.kb), dst)
}
func (liveKBResources) Secret(_ context.Context, name string) (string, error) {
	if name != "" {
		return "", fmt.Errorf("live contract does not accept provider credentials")
	}
	return "", nil
}

// This test uses the actual service storage/index/retrieval methods and real
// Milvus. Only embedding vectors are deterministic fixtures: it measures API
// correctness, persistence and isolation, not semantic quality or capacity.
func TestMilvusLiveContract(t *testing.T) {
	endpoint := os.Getenv("TEST_MILVUS_URL")
	if endpoint == "" {
		t.Skip("explicit real Milvus endpoint required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s := knowledgeStore(t)
	embedding := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			Input []string `json:"input"`
		}
		if r.URL.Path != "/embeddings" || json.NewDecoder(r.Body).Decode(&in) != nil {
			http.Error(w, "fixture request rejected", 400)
			return
		}
		data := []any{}
		for i, text := range in.Input {
			v := []float64{0, 1, 0, 0}
			if strings.Contains(text, "库存") {
				v = []float64{1, 0, 0, 0}
			}
			data = append(data, map[string]any{"index": i, "embedding": v})
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": data})
	}))
	t.Cleanup(embedding.Close)
	// Lose one upsert response AFTER the real Milvus commit. Recovery must use
	// stable IDs, not create duplicate vectors or pretend it never ran.
	var lose atomic.Bool
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req, e := http.NewRequestWithContext(r.Context(), r.Method, endpoint+r.URL.Path, r.Body)
		if e != nil {
			http.Error(w, "request", 500)
			return
		}
		req.Header.Set("Content-Type", "application/json")
		resp, e := (&http.Client{Timeout: 25 * time.Second}).Do(req)
		if e != nil {
			http.Error(w, "backend", 502)
			return
		}
		defer resp.Body.Close()
		var body json.RawMessage
		if e = json.NewDecoder(resp.Body).Decode(&body); e != nil {
			http.Error(w, "body", 502)
			return
		}
		if r.URL.Path == "/v2/vectordb/entities/upsert" && lose.Swap(false) {
			http.Error(w, "controlled lost reply after commit", 502)
			return
		}
		w.WriteHeader(resp.StatusCode)
		_, _ = w.Write(body)
	}))
	t.Cleanup(proxy.Close)
	kb := KnowledgeBase{Name: "Live adapter fixture", Engine: "milvus", Endpoint: proxy.URL, Embedding: ModelConfig{BaseURL: embedding.URL, Model: "controlled-fixture"}, Dimensions: 4, ChunkSize: 200}
	resources := liveKBResources{kb}
	s.ResourceReader, s.SecretReader = resources, resources
	ref := Ref{ID: "live-" + ID(), Revision: 1}
	other := Ref{ID: ref.ID, Revision: 2}
	for _, r := range []Ref{ref, other} {
		r := r
		t.Cleanup(func() {
			_, _ = s.milvus(context.Background(), kb, "collections/drop", map[string]any{"collectionName": collection(r)})
		})
	}
	file, e := s.UploadKnowledge(ctx, ref, "inventory.txt", "fixture://inventory-v1", []byte(strings.Repeat("库存 FACT842 controlled evidence, not market facts. ", 15)))
	if e != nil {
		t.Fatal(e)
	}
	lose.Store(true)
	if e = s.IndexFile(ctx, file.ID); e != nil {
		t.Fatal(e)
	}
	failed, e := s.KnowledgeFile(ctx, file.ID)
	if e != nil || failed.Status != "failed" || !strings.Contains(failed.Error, "502") {
		t.Fatal("lost upsert reply not retained", failed.Status, failed.Error, e)
	}
	if _, e = s.DB.Exec(ctx, "UPDATE knowledge_files SET status='queued' WHERE id=$1", file.ID); e != nil {
		t.Fatal(e)
	}
	if e = s.IndexFile(ctx, file.ID); e != nil {
		t.Fatal(e)
	}
	ready, e := s.KnowledgeFile(ctx, file.ID)
	if e != nil || ready.Status != "ready" {
		t.Fatal("real Milvus indexing failed", ready.Error, e)
	}
	var chunks int
	if e = s.DB.QueryRow(ctx, "SELECT count(*) FROM knowledge_chunks WHERE file_id=$1", file.ID).Scan(&chunks); e != nil {
		t.Fatal(e)
	}
	count, e := s.milvus(ctx, kb, "entities/query", map[string]any{"collectionName": collection(ref), "filter": "", "outputFields": []string{"count(*)"}, "consistencyLevel": "Strong"})
	if e != nil {
		t.Fatal(e)
	}
	var aggregate struct {
		Data []map[string]int `json:"data"`
	}
	if e = json.Unmarshal(count, &aggregate); e != nil || len(aggregate.Data) != 1 || aggregate.Data[0]["count(*)"] != chunks {
		t.Fatal("retry duplicated or lost real vectors", string(count), chunks, e)
	}
	t.Log("PASS real Milvus create/upsert; lost response recovery preserves vector cardinality")
	second, e := s.UploadKnowledge(ctx, other, "inventory-new.txt", "fixture://inventory-v2", []byte("库存 OTHER953 belongs only to another knowledge revision."))
	if e != nil {
		t.Fatal(e)
	}
	if e = s.IndexFile(ctx, second.ID); e != nil {
		t.Fatal(e)
	}
	verify := func() {
		t.Helper()
		for _, c := range []struct {
			ref         Ref
			doc, marker string
		}{{ref, file.DocumentID, "FACT842"}, {other, second.DocumentID, "OTHER953"}} {
			result, e := s.Retrieve(ctx, c.ref, kb, "库存")
			if e != nil || len(result.Hits) == 0 {
				t.Fatal("real Milvus returned no owned source", e)
			}
			for _, hit := range result.Hits {
				if hit["document_id"] != c.doc || !strings.Contains(hit["content"].(string), c.marker) {
					t.Fatal("cross revision or invalid backreference", hit)
				}
			}
		}
	}
	verify()
	t.Log("PASS real vector search resolves retained original sources and isolates knowledge revisions")
	if target := os.Getenv("TEST_MILVUS_RESTART"); target != "" {
		if os.Getenv("GITHUB_ACTIONS") != "true" || target != "agentos-rag-milvus" {
			t.Fatal("restart only allowed for named disposable CI container")
		}
		if e = exec.CommandContext(ctx, "docker", "restart", "--time", "30", target).Run(); e != nil {
			t.Fatal(e)
		}
		deadline := time.Now().Add(90 * time.Second)
		for {
			_, e = s.milvus(ctx, kb, "collections/has", map[string]any{"collectionName": collection(ref)})
			if e == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("Milvus restart did not recover", e)
			}
			time.Sleep(time.Second)
		}
		verify()
		t.Log("PASS real Milvus restart retains vectors; production adapter reconnects")
	}
}
