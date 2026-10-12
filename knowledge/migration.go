package knowledge

import (
	"context"
	_ "embed"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var migrationSQL string

// Migrate is the only DDL owner for Knowledge and Indexer. Both application
// roles must already exist. Consumers never migrate at process startup.
// Grants and DDL commit together; no cross-domain import or down migration.
func Migrate(ctx context.Context, db *pgxpool.Pool, knowledgeRole, indexerRole string) error {
	if knowledgeRole == "" || indexerRole == "" || knowledgeRole == indexerRole {
		return errors.New("distinct existing Knowledge and Indexer roles required")
	}
	tx, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "SET LOCAL search_path=knowledge; SET LOCAL lock_timeout='3s'; SET LOCAL statement_timeout='120s'"); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(8843923)"); err != nil {
		return err
	}
	for _, role := range []string{knowledgeRole, indexerRole} {
		var exists bool
		if err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)", role).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			return errors.New("application role is not provisioned")
		}
	}
	if _, err = tx.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS knowledge"); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, migrationSQL); err != nil {
		return err
	}
	for _, role := range []string{knowledgeRole, indexerRole} {
		if _, err = tx.Exec(ctx, "GRANT USAGE ON SCHEMA knowledge TO "+pgx.Identifier{role}.Sanitize()); err != nil {
			return err
		}
	}
	for _, table := range []string{"documents", "artifacts", "artifact_refs", "knowledge_files", "knowledge_chunks", "service_migrations"} {
		if _, err = tx.Exec(ctx, "GRANT SELECT,INSERT,UPDATE,DELETE ON TABLE "+pgx.Identifier{"knowledge", table}.Sanitize()+" TO "+pgx.Identifier{knowledgeRole}.Sanitize()); err != nil {
			return err
		}
	}
	for _, table := range []string{"documents", "knowledge_files", "knowledge_chunks"} {
		if _, err = tx.Exec(ctx, "GRANT SELECT ON TABLE "+pgx.Identifier{"knowledge", table}.Sanitize()+" TO "+pgx.Identifier{indexerRole}.Sanitize()); err != nil {
			return err
		}
	}
	for _, table := range []string{"knowledge_files", "knowledge_chunks"} {
		if _, err = tx.Exec(ctx, "GRANT UPDATE ON TABLE "+pgx.Identifier{"knowledge", table}.Sanitize()+" TO "+pgx.Identifier{indexerRole}.Sanitize()); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
