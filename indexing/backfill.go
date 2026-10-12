package indexing

import (
	"context"
	"github.com/euler-gaozl/agent-os-knowledge/internal/lexical"
	"github.com/euler-gaozl/agent-os-sdk/contracts"
	"github.com/jackc/pgx/v5"
)

func (s *Processor) BackfillNames(ctx context.Context) (int, error) {
	tx, e := s.db.Begin(ctx)
	if e != nil {
		return 0, e
	}
	defer tx.Rollback(ctx)
	rows, e := tx.Query(ctx, "SELECT id,name,kb_id,kb_revision FROM knowledge_files WHERE name_vector IS NULL ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 128")
	if e != nil {
		return 0, e
	}
	batch := &pgx.Batch{}
	n := 0
	for rows.Next() {
		var id, name string
		var ref contracts.Ref
		if e = rows.Scan(&id, &name, &ref.ID, &ref.Revision); e != nil {
			rows.Close()
			return 0, e
		}
		batch.Queue("UPDATE knowledge_files SET name_vector=to_tsvector('simple',$2) WHERE id=$1", id, lexical.ScopeToken(ref)+" "+lexical.SearchText(name))
		n++
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return 0, e
	}
	if n > 0 {
		if e = tx.SendBatch(ctx, batch).Close(); e != nil {
			return 0, e
		}
	}
	return n, tx.Commit(ctx)
}
func (s *Processor) BackfillChunks(ctx context.Context) (int, error) {
	tx, e := s.db.Begin(ctx)
	if e != nil {
		return 0, e
	}
	defer tx.Rollback(ctx)
	rows, e := tx.Query(ctx, "SELECT c.id,c.content,f.kb_id,f.kb_revision FROM knowledge_chunks c JOIN knowledge_files f ON f.id=c.file_id WHERE c.search_vector IS NULL ORDER BY c.id FOR UPDATE OF c SKIP LOCKED LIMIT 128")
	if e != nil {
		return 0, e
	}
	batch := &pgx.Batch{}
	n := 0
	for rows.Next() {
		var id, text string
		var ref contracts.Ref
		if e = rows.Scan(&id, &text, &ref.ID, &ref.Revision); e != nil {
			rows.Close()
			return 0, e
		}
		batch.Queue("UPDATE knowledge_chunks SET search_vector=to_tsvector('simple',$2) WHERE id=$1", id, lexical.ScopeToken(ref)+" "+lexical.SearchText(text))
		n++
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return 0, e
	}
	if n > 0 {
		results := tx.SendBatch(ctx, batch)
		if e = results.Close(); e != nil {
			return 0, e
		}
	}
	return n, tx.Commit(ctx)
}
