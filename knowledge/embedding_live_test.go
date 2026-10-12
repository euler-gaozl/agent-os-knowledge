package knowledge

import (
	"context"
	. "github.com/euler-gaozl/agent-os-sdk/contracts"
	"os"
	"testing"
	"time"
)

func TestRealEmbeddingAndMilvusRetrieval(t *testing.T) {
	endpoint, embedding := os.Getenv("TEST_MILVUS_URL"), os.Getenv("TEST_EMBEDDING_URL")
	if endpoint == "" || embedding == "" {
		t.Skip("real Milvus and embedding inference endpoints required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s := knowledgeStore(t)
	kb := KnowledgeBase{Name: "Real embedding contract", Engine: "milvus", Endpoint: endpoint, Embedding: ModelConfig{BaseURL: embedding, Model: "qwen3-embedding-0.6b-q8"}, Dimensions: 1024, ChunkSize: 200, QueryPrefix: "Instruct: Given a Chinese research question, retrieve relevant passages that answer the query\nQuery: "}
	resources := liveKBResources{kb}
	s.ResourceReader = resources
	s.SecretReader = resources
	ref := Ref{ID: "semantic-fixture-" + ID(), Revision: 1}
	t.Cleanup(func() {
		_, _ = s.milvus(context.Background(), kb, "collections/drop", map[string]any{"collectionName": collection(ref)})
	})
	fixtures := []struct{ title, text, question string }{
		{"化工供给", "甲醇工厂因设备停车检修而降低产量。本周甲醇装置开工率为六成。EMB-MA-101。受控测试资料，不是市场事实。", "哪种化工原料因为设备维护而减少供应？"},
		{"油籽加工", "豆粕来自大豆压榨。油厂开机率上升，大豆进口到港充足，饲料蛋白库存增加。EMB-M-202。受控测试资料，不是市场事实。", "油籽压榨厂的开工情况和饲料蛋白库存怎样？"},
		{"金属冶炼", "铜冶炼企业的精炼铜产出增加。有色金属的社会仓库铜库存也随之增加。EMB-CU-303。受控测试资料，不是市场事实。", "冶炼厂的产出和有色金属显性库存有何变化？"},
		{"谷物产区", "东北地区玉米收购加快，农民手中的粮食余量减少。粮库持续收购玉米。EMB-C-404。受控测试资料，不是市场事实。", "东北谷物的收购进度以及产区农户余粮是什么情况？"},
	}
	ids := []string{}
	start := time.Now()
	for _, sample := range fixtures {
		file, e := s.UploadKnowledge(ctx, ref, sample.title+".txt", "fixture://semantic/"+sample.title, []byte(sample.text))
		if e != nil {
			t.Fatal(e)
		}
		if e = s.IndexFile(ctx, file.ID); e != nil {
			t.Fatal(e)
		}
		status, e := s.KnowledgeFile(ctx, file.ID)
		if e != nil || status.Status != "ready" {
			t.Fatal("actual embedding inference/indexing failed", status.Error, e)
		}
		ids = append(ids, file.DocumentID)
	}
	t.Logf("Indexed four Chinese controlled source files with real 1024-dimensional Qwen inference in %s", time.Since(start))
	for i, sample := range fixtures {
		start = time.Now()
		result, e := s.Retrieve(ctx, ref, kb, sample.question)
		if e != nil || len(result.Hits) == 0 || result.Hits[0]["document_id"] != ids[i] {
			t.Fatalf("top result mismatched annotated fixture %d: hits=%v err=%v", i, result.Hits, e)
		}
		t.Logf("PASS annotated Chinese fixture %d top-1 original document; query latency %s", i, time.Since(start))
	}
	t.Log("LIMIT: four authored fixtures are a model/API smoke test, not domain-wide relevance, throughput or model superiority evidence")
}
