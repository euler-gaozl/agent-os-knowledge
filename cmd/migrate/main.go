package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/euler-gaozl/agent-os-knowledge/knowledge"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"time"
)

func main() {
	role := flag.String("role", "agentos_knowledge", "existing application database role")
	indexerRole := flag.String("indexer-role", "agentos_indexer", "existing Indexer database role in the same knowledge domain")
	flag.Parse()
	dsn := os.Getenv("MIGRATION_DATABASE_URL")
	config, err := pgxpool.ParseConfig(dsn)
	if dsn == "" || err != nil {
		fail()
	}
	config.MaxConns = 2
	config.ConnConfig.ConnectTimeout = 5 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	db, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		fail()
	}
	defer db.Close()
	if err = knowledge.Migrate(ctx, db, *role, *indexerRole); err != nil {
		fail()
	}
	fmt.Println("knowledge migration committed")
}
func fail() {
	fmt.Fprintln(os.Stderr, "knowledge migration failed; verify migration connection, existing role, permissions and lock availability")
	os.Exit(1)
}
