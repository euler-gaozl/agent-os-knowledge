package knowledge

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5/pgxpool"
)

const readinessQuery = "SELECT d.content,a.body,r.root,f.config,f.name_vector,c.search_vector FROM documents d,artifacts a,artifact_refs r,knowledge_files f,knowledge_chunks c WHERE false"

func Readiness(ctx context.Context, db *pgxpool.Pool) error {
	if db == nil {
		return errors.New("database required")
	}
	_, err := db.Exec(ctx, readinessQuery)
	if err != nil {
		return err
	}
	_, err = archiveRequirement(ctx, db, 0)
	return err
}
