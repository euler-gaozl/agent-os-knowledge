package knowledge

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ImportLegacy is an offline, one-time copy within one PostgreSQL instance.
// Both schemas retain originals; the marker prevents re-importing stale state.
func ImportLegacy(ctx context.Context, p *pgxpool.Pool) error {
	tx, e := p.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	if _, e = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(8843923)"); e != nil {
		return e
	}
	var done bool
	if e = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM service_migrations WHERE version=1)").Scan(&done); e != nil {
		return e
	}
	if done {
		return tx.Commit(ctx)
	}
	for _, q := range []string{
		`INSERT INTO documents SELECT d.* FROM public.documents d WHERE d.id IN(SELECT document_id FROM public.knowledge_files) ON CONFLICT DO NOTHING`,
		`INSERT INTO artifacts SELECT a.* FROM public.artifacts a WHERE a.hash IN(SELECT artifact_hash FROM public.knowledge_files) ON CONFLICT DO NOTHING`,
		`INSERT INTO artifact_refs SELECT r.* FROM public.artifact_refs r WHERE r.root IN(SELECT 'knowledge:'||id FROM public.knowledge_files) ON CONFLICT DO NOTHING`,
		`INSERT INTO knowledge_files(id,kb_id,kb_revision,document_id,artifact_hash,name,status,error,track_id,config,receipt,created_at,updated_at) SELECT id,kb_id,kb_revision,document_id,artifact_hash,name,status,error,track_id,config,receipt,created_at,updated_at FROM public.knowledge_files ON CONFLICT DO NOTHING`,
		`INSERT INTO knowledge_chunks(id,file_id,ordinal,content) SELECT id,file_id,ordinal,content FROM public.knowledge_chunks ON CONFLICT DO NOTHING`,
		`INSERT INTO service_migrations(version) VALUES(1)`,
	} {
		if _, e = tx.Exec(ctx, q); e != nil {
			return e
		}
	}
	return tx.Commit(ctx)
}
