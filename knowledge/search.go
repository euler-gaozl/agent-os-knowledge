package knowledge

import (
	"context"
	"encoding/json"
	"github.com/euler-gaozl/agent-os-knowledge/internal/lexical"
	"github.com/euler-gaozl/agent-os-sdk/contracts"
)

func SearchText(text string) string       { return lexical.SearchText(text) }
func scopeToken(ref contracts.Ref) string { return lexical.ScopeToken(ref) }

// Scope is part of the indexed query, then checked against authoritative file
// metadata again. Materializing a bounded candidate set prevents PostgreSQL's
// generic prepared plan from choosing a file-first nested scan under LIMIT.
// This remains bounded lexical retrieval, not relevance-ranked full recall.
const localChunkQuery = `WITH candidates AS MATERIALIZED (
 SELECT id,file_id,content FROM knowledge_chunks WHERE search_vector @@ plainto_tsquery('simple',$3) LIMIT 64
) SELECT c.id,c.content,d.source,d.id,f.name FROM candidates c
 JOIN knowledge_files f ON f.id=c.file_id JOIN documents d ON d.id=f.document_id
 WHERE f.kb_id=$1 AND f.kb_revision=$2 AND f.status='ready' LIMIT 8`

// Plan the indexed filename predicate independently of LIMIT. Combining the two
// caused an optimistic sequential scan for absent terms at one million files.
// The second materialization still bounds rows consumed by the joins to 64.
const localNameQuery = `WITH matches AS MATERIALIZED (
 SELECT id,name,document_id,kb_id,kb_revision,status FROM knowledge_files WHERE name_vector @@ plainto_tsquery('simple',$3)
), candidates AS MATERIALIZED (
 SELECT * FROM matches LIMIT 64
) SELECT c.id,c.content,d.source,d.id,f.name FROM candidates f
 JOIN documents d ON d.id=f.document_id JOIN knowledge_chunks c ON c.file_id=f.id AND c.ordinal=0
 WHERE f.kb_id=$1 AND f.kb_revision=$2 AND f.status='ready' LIMIT 8`

// ExplainLocal is used by the disposable capacity runner, never by model tools.
func (s *Store) ExplainLocal(ctx context.Context, ref contracts.Ref, query string) (map[string]any, error) {
	out := map[string]any{}
	for name, sql := range map[string]string{"chunks": localChunkQuery, "names": localNameQuery} {
		var plan []byte
		if e := s.DB.QueryRow(ctx, "EXPLAIN (FORMAT JSON) "+sql, ref.ID, ref.Revision, scopeToken(ref)+" "+SearchText(query)).Scan(&plan); e != nil {
			return nil, e
		}
		var decoded any
		if e := json.Unmarshal(plan, &decoded); e != nil {
			return nil, e
		}
		out[name] = decoded
	}
	return out, nil
}
