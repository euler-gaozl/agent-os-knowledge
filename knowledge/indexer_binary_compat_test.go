package knowledge

import (
	"context"
	"encoding/json"
	"errors"
	. "github.com/euler-gaozl/agent-os-sdk/contracts"
	"github.com/euler-gaozl/agent-os-sdk/servicehttp"
	"github.com/jackc/pgx/v5"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type indexerProcess struct {
	command *exec.Cmd
	done    chan error
	stopped bool
}

func (p *indexerProcess) stop(t *testing.T) {
	t.Helper()
	if p.stopped {
		return
	}
	_ = p.command.Process.Kill()
	select {
	case <-p.done:
		p.stopped = true
	case <-time.After(5 * time.Second):
		t.Fatal("fixture Indexer did not terminate")
	}
}

func (p *indexerProcess) killAtFault(t *testing.T, ctx context.Context) {
	t.Helper()
	if ctx.Err() != nil {
		t.Fatal("fault deadline expired before explicit kill")
	}
	select {
	case <-p.done:
		p.stopped = true
		t.Fatal("fixture process exited before fault injection")
	default:
	}
	if err := p.command.Process.Kill(); err != nil {
		t.Fatal("explicit fault kill failed")
	}
	select {
	case err := <-p.done:
		p.stopped = true
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.Success() {
			t.Fatal("fault did not produce a failed process exit")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fault-killed process did not exit")
	}
}

func indexerFixtureDSN(base, database, role string) (string, error) {
	u, err := url.Parse(base)
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		return "", errors.New("fixture requires explicit PostgreSQL URL")
	}
	u.User = url.UserPassword(role, "fixture-only")
	u.Path = "/" + database
	query := u.Query()
	for key := range query {
		switch strings.ToLower(key) {
		case "user", "password", "dbname", "database":
			query.Del(key)
		}
	}
	query.Set("search_path", "knowledge,public")
	u.RawQuery = query.Encode()
	cfg, err := pgx.ParseConfig(u.String())
	if err != nil || cfg.User != role || cfg.Database != database || cfg.Password != "fixture-only" {
		return "", errors.New("fixture connection scope validation failed")
	}
	return u.String(), nil
}

func TestIndexerFixtureDSNRemovesIdentityOverrides(t *testing.T) {
	dsn, err := indexerFixtureDSN("postgres://initial:initial@127.0.0.1:55591/postgres?user=override&password=override&dbname=original&database=original&sslmode=disable", "isolated_fixture", "restricted_fixture")
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil || cfg.User != "restricted_fixture" || cfg.Database != "isolated_fixture" || cfg.Password != "fixture-only" {
		t.Fatal("identity query parameters escaped fixture scope")
	}
}

func startIndexerBinary(t *testing.T, ctx context.Context, binary, dsn, peerURL string) *indexerProcess {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	token := strings.Repeat("i", 40)
	control := strings.Repeat("c", 40)
	peers := map[string]string{"indexer": servicehttp.TokenHash(token), "control": servicehttp.TokenHash(control)}
	peerJSON, _ := json.Marshal(peers)
	config := map[string]string{"LISTEN": address, "DATABASE_URL": dsn, "SERVICE_TOKEN": token, "SERVICE_PEERS": string(peerJSON), "REGISTRY_URL": peerURL, "RUNTIME_URL": peerURL, "INSTANCE_ID": "fixture-" + ID(), "DB_MAX_CONNS": "3", "HTTP_MAX_CONCURRENT": "4"}
	directory := t.TempDir()
	configPath := filepath.Join(directory, "service.json")
	if err = os.WriteFile(configPath, JSON(config), 0600); err != nil {
		t.Fatal(err)
	}
	output, err := os.Create(filepath.Join(directory, "process.log"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { output.Close() })
	command := exec.CommandContext(ctx, binary, "-config", configPath)
	// No real parent service configuration is inherited by the fixture binary.
	var environment []string
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if _, provided := config[strings.ToUpper(key)]; !provided {
			environment = append(environment, entry)
		}
	}
	command.Env = environment
	command.Stdout = output
	command.Stderr = output
	if err = command.Start(); err != nil {
		t.Fatal("fixture binary could not start")
	}
	p := &indexerProcess{command: command, done: make(chan error, 1)}
	go func() { p.done <- command.Wait() }()
	t.Cleanup(func() { p.stop(t) })
	client := &http.Client{Timeout: time.Second}
	for {
		request, _ := http.NewRequestWithContext(ctx, "GET", "http://"+address+"/readyz", nil)
		request.Header.Set("Authorization", "Bearer "+control)
		response, e := client.Do(request)
		if e == nil {
			response.Body.Close()
			if response.StatusCode == 200 {
				return p
			}
		}
		select {
		case <-p.done:
			p.stopped = true
			t.Fatal("fixture Indexer exited before readiness; no private output printed")
		case <-ctx.Done():
			t.Fatal("fixture Indexer never became ready")
		case <-time.After(30 * time.Millisecond):
		}
	}
}

func waitFileStatus(t *testing.T, ctx context.Context, store *Store, id, want string) {
	t.Helper()
	for {
		f, err := store.KnowledgeFile(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if f.Status == want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("file status %s did not reach %s", f.Status, want)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// Explicitly opt-in to execute actual archived old/new Indexer executables.
// All data and peers are disposable fixtures. No deployed service is stopped.
func TestIndexerBinaryCompatibility(t *testing.T) {
	oldBinary, newBinary := os.Getenv("TEST_OLD_INDEXER_BINARY"), os.Getenv("TEST_NEW_INDEXER_BINARY")
	if oldBinary == "" || newBinary == "" {
		t.Skip("actual old/new Indexer binaries not configured")
	}
	for _, binary := range []string{oldBinary, newBinary} {
		if !filepath.IsAbs(binary) {
			t.Fatal("absolute fixture binary path required")
		}
		if st, err := os.Stat(binary); err != nil || st.IsDir() {
			t.Fatal("fixture binary missing")
		}
	}
	for _, tc := range []struct{ name, first, second, mode string }{
		{"old_to_new_unknown", oldBinary, newBinary, "unknown"},
		{"new_to_old_saved_track", newBinary, oldBinary, "tracked"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, role, idxRole := fixtureDatabase(t)
			ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
			defer cancel()
			if err := Migrate(ctx, db, role, idxRole); err != nil {
				t.Fatal(err)
			}
			// Legacy Indexer probes the whole Knowledge schema at startup, even
			// though its work never reads artifacts. Keep these two read grants
			// while testing rollback to that binary. Fresh new-only deployments
			// and the separate restricted-role tests need neither grant.
			if _, err := db.Exec(ctx, "GRANT SELECT ON knowledge.artifacts,knowledge.artifact_refs TO "+pgx.Identifier{idxRole}.Sanitize()); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(ctx, "ALTER ROLE "+pgx.Identifier{idxRole}.Sanitize()+" LOGIN PASSWORD 'fixture-only'"); err != nil {
				t.Fatal(err)
			}
			fixtureDSN, err := indexerFixtureDSN(os.Getenv("TEST_DATABASE_URL"), db.Config().ConnConfig.Database, idxRole)
			if err != nil {
				t.Fatal(err)
			}
			identity, err := pgx.Connect(ctx, fixtureDSN)
			if err != nil {
				t.Fatal("fixture role could not connect")
			}
			var actualUser, actualDatabase string
			err = identity.QueryRow(ctx, "SELECT current_user,current_database()").Scan(&actualUser, &actualDatabase)
			identity.Close(ctx)
			if err != nil || actualUser != idxRole || actualDatabase != db.Config().ConnConfig.Database {
				t.Fatal("fixture connected outside expected identity or database")
			}
			var uploads, polls atomic.Int32
			var handoff atomic.Bool
			entered := make(chan struct{})
			release := make(chan struct{})
			peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/v1/instances":
					io.Copy(io.Discard, r.Body)
					io.WriteString(w, `{"ok":true}`)
				case "/documents/text":
					io.Copy(io.Discard, r.Body)
					if uploads.Add(1) == 1 {
						close(entered)
					}
					if tc.mode == "unknown" {
						select {
						case <-r.Context().Done():
						case <-release:
						}
						return
					}
					io.WriteString(w, `{"track_id":"binary-track"}`)
				case "/documents/track_status/binary-track":
					polls.Add(1)
					if handoff.Load() {
						io.WriteString(w, `{"documents":[{"status":"processed"}]}`)
					} else {
						io.WriteString(w, `{"documents":[{"status":"processing"}]}`)
					}
				default:
					t.Error("unexpected fixture peer request")
					w.WriteHeader(400)
				}
			}))
			defer peer.Close()
			defer close(release)
			resources := savedResources{kb: KnowledgeBase{Name: "binary fixture", Engine: "lightrag", Endpoint: peer.URL, ChunkSize: 200}}
			store := &Store{DB: rolePool(t, db, role), ResourceReader: resources, SecretReader: resources}
			file, err := store.UploadKnowledge(ctx, Ref{ID: "binary-fixture", Revision: 1}, "source.txt", "fixture://binary", []byte("controlled source evidence"))
			if err != nil {
				t.Fatal(err)
			}
			first := startIndexerBinary(t, ctx, tc.first, fixtureDSN, peer.URL)
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("first binary did not issue upload")
			}
			expected := "running"
			if tc.mode == "tracked" {
				expected = "external_pending"
			}
			waitFileStatus(t, ctx, store, file.ID, expected)
			beforeKill, err := store.KnowledgeFile(ctx, file.ID)
			if err != nil {
				t.Fatal(err)
			}
			first.killAtFault(t, ctx)
			afterKill, err := store.KnowledgeFile(ctx, file.ID)
			if err != nil || afterKill.Status != expected || afterKill.TrackID != beforeKill.TrackID {
				t.Fatal("persisted fault boundary changed before replacement started", err)
			}
			handoff.Store(true)
			second := startIndexerBinary(t, ctx, tc.second, fixtureDSN, peer.URL)
			expected = "unknown"
			if tc.mode == "tracked" {
				expected = "ready"
			}
			waitFileStatus(t, ctx, store, file.ID, expected)
			second.stop(t)
			if uploads.Load() != 1 {
				t.Fatal("binary handoff repeated an external upload")
			}
			if tc.mode == "tracked" && polls.Load() < 1 {
				t.Fatal("saved receipt was not polled")
			}
			if tc.mode == "unknown" && polls.Load() != 0 {
				t.Fatal("unreceipted upload unexpectedly polled")
			}
			t.Logf("actual executable handoff: final=%s uploads=%d polls=%d", expected, uploads.Load(), polls.Load())
		})
	}
}
