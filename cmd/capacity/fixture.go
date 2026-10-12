package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/euler-gaozl/agent-os-knowledge/knowledge"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// A fixed disposable database and new marker are required. This command never
// drops, truncates, adopts or resumes an existing Knowledge schema. Failure
// leaves its marker and rows for diagnosis; rerunning requires a fresh fixture.
func prepareFixture(ctx context.Context, db *pgxpool.Pool) (*pgxpool.Pool, error) {
	tx, err := db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "SET LOCAL lock_timeout='3s'; SET LOCAL statement_timeout='10s'; SELECT pg_advisory_xact_lock(8843941)"); err != nil {
		return nil, err
	}
	var database string
	var existing bool
	if err = tx.QueryRow(ctx, "SELECT current_database(),EXISTS(SELECT 1 FROM pg_namespace WHERE nspname IN ('knowledge','capacity'))").Scan(&database, &existing); err != nil {
		return nil, err
	}
	if database != "agentos_capacity" || existing {
		return nil, fmt.Errorf("new disposable agentos_capacity database required; no schema adopted")
	}
	var nonce [10]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		return nil, err
	}
	role := "capacity_" + hex.EncodeToString(nonce[:])
	indexer := role + "_indexer"
	if _, err = tx.Exec(ctx, "CREATE SCHEMA capacity; CREATE ROLE "+pgx.Identifier{role}.Sanitize()+" NOLOGIN; CREATE ROLE "+pgx.Identifier{indexer}.Sanitize()+" NOLOGIN"); err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	if err = knowledge.Migrate(ctx, db, role, indexer); err != nil {
		return nil, err
	}
	cfg := db.Config().Copy()
	cfg.MaxConns = 4
	cfg.ConnConfig.RuntimeParams["search_path"] = "knowledge"
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, "SET ROLE "+pgx.Identifier{role}.Sanitize())
		return err
	}
	queries, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	var actual string
	var canDDL bool
	if err = queries.QueryRow(ctx, "SELECT current_user,has_schema_privilege(current_user,'knowledge','CREATE')").Scan(&actual, &canDDL); err != nil || actual != role || canDDL {
		queries.Close()
		return nil, fmt.Errorf("capacity query identity/privileges invalid")
	}
	return queries, nil
}
