package knowledge

import (
	"context"
	"errors"
	"github.com/euler-gaozl/agent-os-knowledge/indexing"
	. "github.com/euler-gaozl/agent-os-sdk/contracts"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"strings"
	"testing"
)

func rolePool(t *testing.T, db *pgxpool.Pool, role string) *pgxpool.Pool {
	t.Helper()
	cfg := db.Config().Copy()
	cfg.AfterConnect = func(ctx context.Context, c *pgx.Conn) error {
		_, err := c.Exec(ctx, "SET ROLE "+pgx.Identifier{role}.Sanitize())
		return err
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func permissionDenied(t *testing.T, err error) {
	t.Helper()
	var p *pgconn.PgError
	if !errors.As(err, &p) || p.Code != "42501" {
		t.Fatal("expected permission denied", err)
	}
}

func TestMigrationOwnsOneDomainAndIndexerPrivileges(t *testing.T) {
	db, role, idxRole := fixtureDatabase(t)
	ctx := context.Background()
	if Readiness(ctx, db) == nil {
		t.Fatal("empty schema ready")
	}
	if _, err := db.Exec(ctx, "CREATE TABLE public.foreign_domain_sentinel(value text); INSERT INTO public.foreign_domain_sentinel VALUES('preserve')"); err != nil {
		t.Fatal(err)
	}
	for _, roles := range [][2]string{{role, role}, {role, "missing_p2_indexer_role"}, {"missing_p2_knowledge_role", idxRole}} {
		if err := Migrate(ctx, db, roles[0], roles[1]); err == nil {
			t.Fatal("invalid roles accepted")
		}
	}
	for i := 0; i < 2; i++ {
		if err := Migrate(ctx, db, role, idxRole); err != nil {
			t.Fatal(err)
		}
	}
	var count int
	if err := db.QueryRow(ctx, "SELECT count(*) FROM pg_tables WHERE schemaname='knowledge'").Scan(&count); err != nil || count != 6 {
		t.Fatal("wrong ownership", err, count)
	}
	app, idx := rolePool(t, db, role), rolePool(t, db, idxRole)
	if err := Readiness(ctx, app); err != nil {
		t.Fatal(err)
	}
	if err := indexing.Readiness(ctx, idx); err != nil {
		t.Fatal(err)
	}
	for _, p := range []*pgxpool.Pool{app, idx} {
		_, err := p.Exec(ctx, "SELECT * FROM public.foreign_domain_sentinel")
		permissionDenied(t, err)
		_, err = p.Exec(ctx, "CREATE TABLE knowledge.must_not_create(id int)")
		permissionDenied(t, err)
	}
	for _, q := range []string{"SELECT * FROM artifacts", "INSERT INTO documents(id) VALUES('forbidden')", "DELETE FROM knowledge_files", "UPDATE documents SET content='forbidden'", "INSERT INTO knowledge_files(id) VALUES('forbidden')", "SELECT * FROM service_migrations"} {
		_, err := idx.Exec(ctx, q)
		permissionDenied(t, err)
	}
	// Real upload/dedup/retention transaction through the restricted Knowledge role.
	store := &Store{DB: app, ResourceReader: fixtureResources{}}
	ref := Ref{ID: "owner-fixture", Revision: 1}
	body := []byte(strings.Repeat("资料库存 bounded evidence. ", 400))
	file, err := store.UploadKnowledge(ctx, ref, "库存.txt", "fixture://ownership", body)
	if err != nil {
		t.Fatal(err)
	}
	again, err := store.UploadKnowledge(ctx, ref, "库存.txt", "fixture://ownership", body)
	if err != nil || again.ID != file.ID {
		t.Fatal("dedup failed", err)
	}
	if _, err = app.Exec(ctx, "UPDATE knowledge_files SET name_vector=NULL; UPDATE knowledge_chunks SET search_vector=NULL"); err != nil {
		t.Fatal(err)
	}
	processor := indexing.New(idx, nil)
	if n, err := processor.BackfillNames(ctx); err != nil || n != 1 {
		t.Fatal("indexer filename grant", n, err)
	}
	if n, err := processor.BackfillChunks(ctx); err != nil || n == 0 {
		t.Fatal("indexer chunk grant", n, err)
	}
	if ids, err := processor.PendingFiles(ctx); err != nil || len(ids) != 0 {
		t.Fatal("local files wrongly scheduled", ids, err)
	}
	if _, err := store.ReadDocument(ctx, Run{Snapshot: Snapshot{Agent: Agent{Knowledge: []Ref{ref}}}}, file.DocumentID, 0, 20); err != nil {
		t.Fatal("original read", err)
	}
	var sentinel string
	if err := db.QueryRow(ctx, "SELECT value FROM public.foreign_domain_sentinel").Scan(&sentinel); err != nil || sentinel != "preserve" {
		t.Fatal("foreign mutation", err)
	}
}

func TestFailedOwnerMigrationRollsBackSchema(t *testing.T) {
	db, role, idxRole := fixtureDatabase(t)
	saved := migrationSQL
	migrationSQL += "\nSELECT 1/0;"
	defer func() { migrationSQL = saved }()
	if err := Migrate(context.Background(), db, role, idxRole); err == nil {
		t.Fatal("injected failure ignored")
	}
	var exists bool
	if err := db.QueryRow(context.Background(), "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname='knowledge')").Scan(&exists); err != nil || exists {
		t.Fatal("partial schema survived failed migration", err)
	}
}

func TestGrantFailureRollsBackEarlierGrantsOnExistingTables(t *testing.T) {
	db, role, idxRole := fixtureDatabase(t)
	ctx := context.Background()
	if err := Migrate(ctx, db, role, idxRole); err != nil {
		t.Fatal(err)
	}
	for _, r := range []string{role, idxRole} {
		if _, err := db.Exec(ctx, "REVOKE ALL ON ALL TABLES IN SCHEMA knowledge FROM "+pgx.Identifier{r}.Sanitize()+"; REVOKE ALL ON SCHEMA knowledge FROM "+pgx.Identifier{r}.Sanitize()); err != nil {
			t.Fatal(err)
		}
	}
	// This disposable database already has its tables. Fail only after every
	// Knowledge table GRANT, when the first Indexer SELECT GRANT executes.
	if _, err := db.Exec(ctx, `CREATE FUNCTION public.fail_indexer_grant() RETURNS event_trigger LANGUAGE plpgsql AS $$
 BEGIN IF position('GRANT SELECT ON TABLE' in current_query())>0 THEN RAISE EXCEPTION 'controlled fixture grant failure'; END IF; END $$;
 CREATE EVENT TRIGGER fail_indexer_grant ON ddl_command_end WHEN TAG IN ('GRANT') EXECUTE FUNCTION public.fail_indexer_grant();`); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, db, role, idxRole); err == nil || !strings.Contains(err.Error(), "controlled fixture grant failure") {
		t.Fatal("grant failure was not reached", err)
	}
	for _, r := range []string{role, idxRole} {
		var allowed bool
		if err := db.QueryRow(ctx, "SELECT has_schema_privilege($1,'knowledge','USAGE')", r).Scan(&allowed); err != nil || allowed {
			t.Fatal("schema grant survived rollback", err)
		}
		for _, table := range []string{"documents", "artifacts", "artifact_refs", "knowledge_files", "knowledge_chunks", "service_migrations"} {
			for _, privilege := range []string{"SELECT", "INSERT", "UPDATE", "DELETE"} {
				if err := db.QueryRow(ctx, "SELECT has_table_privilege($1,$2,$3)", r, "knowledge."+table, privilege).Scan(&allowed); err != nil || allowed {
					t.Fatal("partial table grant survived rollback", table, privilege, err)
				}
			}
		}
	}
	var count int
	if err := db.QueryRow(ctx, "SELECT count(*) FROM pg_tables WHERE schemaname='knowledge'").Scan(&count); err != nil || count != 6 {
		t.Fatal("pre-existing tables lost", err, count)
	}
}
