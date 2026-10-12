package knowledge

import (
	"context"
	"encoding/json"
	"github.com/euler-gaozl/agent-os-sdk/contracts"
	"strings"
	"testing"
)

func TestFileNameMissUsesIndexAndBroadQueryConsumesBoundedRows(t *testing.T) {
	s := knowledgeStore(t)
	ctx := context.Background()
	ref := contracts.Ref{ID: "plan-fixture", Revision: 1}
	tx, e := s.DB.Begin(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback(ctx)
	queries := []string{
		`INSERT INTO artifacts(hash,body,media_type) SELECT encode(sha256(convert_to(i::text,'UTF8')),'hex'),convert_to(i::text,'UTF8'),'text/plain' FROM generate_series(1,20000)i`,
		`INSERT INTO documents(id,kind,title,content,source,metadata) SELECT md5(i::text),'evidence','fixture','fixture','fixture://name-plan','{}' FROM generate_series(1,20000)i`,
		`INSERT INTO knowledge_files(id,kb_id,kb_revision,document_id,artifact_hash,name,status,config,name_vector) SELECT md5(i::text),'plan-fixture',1,md5(i::text),encode(sha256(convert_to(i::text,'UTF8')),'hex'),'common record'||i,'ready','{}',to_tsvector('simple',$1||' common record'||i) FROM generate_series(1,20000)i`,
		`INSERT INTO knowledge_chunks(id,file_id,ordinal,content,search_vector) SELECT md5(i::text),md5(i::text),0,'fixture',to_tsvector('simple','fixture') FROM generate_series(1,20000)i`,
		`ANALYZE knowledge_files`,
		`ANALYZE knowledge_chunks`,
		`ANALYZE documents`,
	}
	for _, q := range queries {
		var args []any
		if strings.Contains(q, "$1") {
			args = []any{scopeToken(ref)}
		}
		if _, e = tx.Exec(ctx, q, args...); e != nil {
			t.Fatal(e)
		}
	}
	for _, mode := range []string{"force_custom_plan", "force_generic_plan"} {
		if _, e = tx.Exec(ctx, "SET LOCAL plan_cache_mode="+mode); e != nil {
			t.Fatal(e)
		}
		var miss []byte
		if e = tx.QueryRow(ctx, "EXPLAIN (ANALYZE,FORMAT JSON) "+localNameQuery, ref.ID, ref.Revision, scopeToken(ref)+" absent-record").Scan(&miss); e != nil {
			t.Fatal(e)
		}
		if !strings.Contains(string(miss), "knowledge_files_name_search") {
			t.Fatalf("%s used full filename scan for missing term", mode)
		}
		var broad []byte
		if e = tx.QueryRow(ctx, "EXPLAIN (ANALYZE,FORMAT JSON) "+localNameQuery, ref.ID, ref.Revision, scopeToken(ref)+" common").Scan(&broad); e != nil {
			t.Fatal(e)
		}
		var tree any
		if e = json.Unmarshal(broad, &tree); e != nil {
			t.Fatal(e)
		}
		found := false
		var walk func(any)
		walk = func(v any) {
			switch x := v.(type) {
			case []any:
				for _, n := range x {
					walk(n)
				}
			case map[string]any:
				if x["Subplan Name"] == "CTE matches" {
					found = true
					if x["Actual Rows"].(float64) > 64 {
						t.Fatal("broad filename search materialized all matches")
					}
				}
				for _, n := range x {
					walk(n)
				}
			}
		}
		walk(tree)
		if !found {
			t.Fatal("missing observed filename candidate plan")
		}
	}
}
