// capacity measures the real local knowledge retrieval path on a disposable
// database. SQL bulk fixtures measure storage/query capacity, not upload speed,
// relevance, embeddings, or complete platform capacity.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/euler-gaozl/agent-os-knowledge/knowledge"
	"github.com/euler-gaozl/agent-os-sdk/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, "capacity check failed:", e)
		os.Exit(1)
	}
}
func run() error {
	if os.Getenv("CAPACITY_DISPOSABLE") != "agentos-capacity-fixtures-only" {
		return fmt.Errorf("explicit disposable fixture acknowledgement required")
	}
	n, e := strconv.Atoi(os.Getenv("CAPACITY_DOCUMENTS"))
	if e != nil || n < 1000 || n > 1000000 || n%1000 != 0 {
		return fmt.Errorf("documents must be a multiple of 1000 within 1000..1000000")
	}
	cfg, e := pgxpool.ParseConfig(os.Getenv("CAPACITY_DATABASE_URL"))
	if e != nil {
		return fmt.Errorf("invalid database configuration")
	}
	if cfg.ConnConfig.Database != "agentos_capacity" {
		return fmt.Errorf("dedicated agentos_capacity database required")
	}
	cfg.MaxConns = 4
	cfg.ConnConfig.RuntimeParams["search_path"] = "knowledge"
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = "0"
	ctx, cancel := context.WithTimeout(context.Background(), 70*time.Minute)
	defer cancel()
	db, e := pgxpool.NewWithConfig(ctx, cfg)
	if e != nil {
		return e
	}
	defer db.Close()
	reportPath := os.Getenv("CAPACITY_REPORT")
	if reportPath == "" {
		reportPath = "/evidence/capacity.json"
	}
	if !filepath.IsAbs(reportPath) {
		return fmt.Errorf("absolute CAPACITY_REPORT path required")
	}
	if _, e = os.Lstat(reportPath); !os.IsNotExist(e) {
		return fmt.Errorf("capacity report path must be new")
	}
	if info, err := os.Stat(filepath.Dir(reportPath)); err != nil || !info.IsDir() {
		return fmt.Errorf("capacity report directory must exist")
	}
	queryDB, e := prepareFixture(ctx, db)
	if e != nil {
		return e
	}
	defer queryDB.Close()
	// Each document has ten 800-character synthetic chunks. Distributed bucket
	// and unique record terms exercise selective and broad GIN retrieval.
	ref := contracts.Ref{ID: "capacity", Revision: 1}
	scope := "scope" + contracts.Hash(contracts.JSON(ref))[:24]
	start := time.Now()
	for first := 1; first <= n; first += 1000 {
		tx, e := db.Begin(ctx)
		if e != nil {
			return e
		}
		queries := []string{
			`INSERT INTO artifacts(hash,body,media_type) SELECT encode(sha256(convert_to(repeat(md5(i::text),32),'UTF8')),'hex'),convert_to(repeat(md5(i::text),32),'UTF8'),'text/plain' FROM generate_series($1::int,$2::int) i`,
			`INSERT INTO documents(id,kind,title,content,source,metadata,scope) SELECT md5(i::text),'evidence','容量样本 record'||i,repeat(md5(i::text),32),'fixture://capacity/'||i,'{}','knowledge:capacity' FROM generate_series($1::int,$2::int) i`,
			`INSERT INTO knowledge_files(id,kb_id,kb_revision,document_id,artifact_hash,name,status,config,name_vector) SELECT md5(i::text),'capacity',1,md5(i::text),encode(sha256(convert_to(repeat(md5(i::text),32),'UTF8')),'hex'),'容量样本 record'||i,'ready','{"engine":"local"}',to_tsvector('simple',$3||' 容量 量样 样本 record'||i) FROM generate_series($1::int,$2::int) i`,
			`INSERT INTO artifact_refs(root,hash) SELECT 'knowledge:'||md5(i::text),encode(sha256(convert_to(repeat(md5(i::text),32),'UTF8')),'hex') FROM generate_series($1::int,$2::int) i`,
			`INSERT INTO knowledge_chunks(id,file_id,ordinal,content,search_vector) SELECT md5(i||':'||j),md5(i::text),j,left('甲醇供给 库存 record'||i||' bucket'||(i%1000)||' ordinal'||j||' '||repeat(md5(i||':'||j)||' ',30),800),to_tsvector('simple',$3||' 甲醇 醇供 供给 库存 record'||i||' bucket'||(i%1000)||' ordinal'||j) FROM generate_series($1::int,$2::int) i CROSS JOIN generate_series(0,9) j`,
		}
		for _, q := range queries {
			var args = []any{first, first + 999}
			if strings.Contains(q, "$3") {
				args = append(args, scope)
			}
			if _, e = tx.Exec(ctx, q, args...); e != nil {
				tx.Rollback(ctx)
				return e
			}
		}
		if e = tx.Commit(ctx); e != nil {
			return e
		}
		if first == 1 || (first+999)%100000 == 0 {
			fmt.Fprintf(os.Stderr, "loaded %d documents / %d chunks\n", first+999, (first+999)*10)
		}
	}
	loadSeconds := time.Since(start).Seconds()
	if _, e = db.Exec(ctx, "ANALYZE"); e != nil {
		return e
	}
	var documents, chunks, bytes int64
	if e = db.QueryRow(ctx, "SELECT (SELECT count(*) FROM knowledge_files),(SELECT count(*) FROM knowledge_chunks),(SELECT sum(pg_total_relation_size(c.oid)) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='knowledge' AND c.relkind='r')").Scan(&documents, &chunks, &bytes); e != nil {
		return e
	}
	if documents != int64(n) || chunks != int64(n*10) {
		return fmt.Errorf("loaded row counts differ")
	}
	store := knowledge.Store{DB: queryDB}
	plans := map[string]any{}
	for _, q := range []string{"record" + strconv.Itoa(n/2), "missingrecord99999999", "容量样本"} {
		p, e := store.ExplainLocal(ctx, ref, q)
		if e != nil {
			return e
		}
		plans[q] = p
	}
	type sample struct {
		Query        string  `json:"query"`
		Milliseconds float64 `json:"ms"`
		Hits         int     `json:"hits"`
		Error        string  `json:"error,omitempty"`
	}
	measure := func(query string) sample {
		t := time.Now()
		r, e := store.Retrieve(ctx, ref, contracts.KnowledgeBase{Engine: "local"}, query)
		x := sample{Query: query, Milliseconds: float64(time.Since(t).Microseconds()) / 1000, Hits: len(r.Hits)}
		if e != nil {
			x.Error = e.Error()
		}
		return x
	}
	queries := []string{"record" + strconv.Itoa(n/2), "bucket42 库存", "甲醇供给", "missingrecord99999999", "容量样本"}
	initial := []sample{}
	for _, q := range queries {
		initial = append(initial, measure(q))
	}
	results := make(chan sample, 200)
	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				results <- measure(queries[i%len(queries)])
			}
		}()
	}
	wg.Wait()
	close(results)
	samples := []sample{}
	times := []float64{}
	errors := 0
	for s := range results {
		samples = append(samples, s)
		times = append(times, s.Milliseconds)
		if s.Error != "" {
			errors++
		}
	}
	sort.Float64s(times)
	report := map[string]any{"format": 2, "documents": documents, "chunks": chunks, "chunk_characters": 800, "source_bytes_per_document": 1024, "fixture": "SQL generated; repeated seeded text; original documents intentionally shorter than aggregate chunks; not an upload/relevance test", "schema": "knowledge", "query_identity": "restricted Knowledge role via SET ROLE; not an independent login benchmark", "database_table_and_index_bytes": bytes, "load_seconds": loadSeconds, "concurrency": 4, "samples": samples, "initial_queries_after_load": initial, "cache": "warm/mixed after load; no cold-cache claim", "p50_ms": times[len(times)/2], "p95_ms": times[len(times)*95/100], "max_ms": times[len(times)-1], "errors": errors, "limits": "Not set or verified by this command. Caller must retain actual container/host limits and resource samples. Other platform services excluded."}
	report["query_plans"] = plans
	var failure error
	if errors > 0 {
		failure = fmt.Errorf("%d retrieval timeouts/errors; report retained", errors)
	}
	for _, s := range initial {
		missing := strings.HasPrefix(s.Query, "missing")
		if s.Error != "" || (!missing && s.Hits == 0) || (missing && s.Hits != 0) {
			failure = fmt.Errorf("initial retrieval assertion failed for %s", s.Query)
		}
	}
	// An unbound revision must not return any matching title or chunk.
	wrong, e := store.Retrieve(ctx, contracts.Ref{ID: "capacity", Revision: 2}, contracts.KnowledgeBase{Engine: "local"}, "库存")
	if e != nil || len(wrong.Hits) != 0 {
		failure = fmt.Errorf("revision isolation failed")
	}
	report["revision_isolation"] = e == nil && len(wrong.Hits) == 0
	report["passed"] = failure == nil
	if failure != nil {
		report["failure"] = failure.Error()
	}
	body, e := json.MarshalIndent(report, "", "  ")
	if e != nil {
		return e
	}
	f, e := os.OpenFile(reportPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	defer f.Close()
	if _, e = f.Write(body); e != nil {
		return e
	}
	if e = f.Sync(); e != nil {
		return e
	}
	fmt.Printf("documents=%d chunks=%d bytes=%d p95_ms=%.2f errors=%d\n", documents, chunks, bytes, times[len(times)*95/100], errors)
	return failure
}
