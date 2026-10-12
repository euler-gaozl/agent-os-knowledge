package knowledge

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/euler-gaozl/agent-os-knowledge/indexing"
	. "github.com/euler-gaozl/agent-os-sdk/contracts"
	"github.com/jackc/pgx/v5/pgxpool"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type savedResources struct{ kb KnowledgeBase }

func (r savedResources) Resource(_ context.Context, _ string, _ Ref, out any) error {
	return json.Unmarshal(JSON(r.kb), out)
}
func (savedResources) Secret(context.Context, string) (string, error) { return "", nil }

func TestIndexerAbruptExitHelper(t *testing.T) {
	if os.Getenv("INDEXER_FAULT_HELPER") != "1" {
		t.Skip("subprocess helper")
	}
	cfg, err := pgxpool.ParseConfig(os.Getenv("INDEXER_FIXTURE_DSN"))
	if err != nil {
		t.Fatal("fixture connection invalid")
	}
	// ConnString retains the original parsed DSN, not the mutable Database and
	// RuntimeParams set by the disposable fixture. Pass that scope explicitly.
	cfg.ConnConfig.Database = os.Getenv("INDEXER_FIXTURE_DATABASE")
	cfg.ConnConfig.RuntimeParams["search_path"] = "knowledge,public"
	cfg.MaxConns = 3
	db, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal("fixture connection invalid")
	}
	defer db.Close()
	idx := rolePool(t, db, os.Getenv("INDEXER_FIXTURE_ROLE"))
	if err = indexing.New(idx, savedResources{}).ProcessFile(context.Background(), os.Getenv("INDEXER_FIXTURE_FILE")); err != nil {
		t.Fatal("fixture indexing failed", err)
	}
}

func TestAbruptIndexerExitReleasesLockWithoutRepeatingUnknownUpload(t *testing.T) {
	db, role, idxRole := fixtureDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	if err := Migrate(ctx, db, role, idxRole); err != nil {
		t.Fatal(err)
	}
	var uploads atomic.Int32
	entered := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/documents/text" {
			t.Error("unexpected provider request")
			w.WriteHeader(400)
			return
		}
		io.Copy(io.Discard, r.Body)
		if uploads.Add(1) == 1 {
			close(entered)
		}
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer server.Close()
	defer close(release)
	resources := savedResources{kb: KnowledgeBase{Name: "fault fixture", Engine: "lightrag", Endpoint: server.URL, ChunkSize: 200}}
	app := rolePool(t, db, role)
	store := &Store{DB: app, ResourceReader: resources, SecretReader: resources}
	file, err := store.UploadKnowledge(ctx, Ref{ID: "fault-kb", Revision: 1}, "source.txt", "fixture://fault", []byte("controlled inventory source"))
	if err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	child := exec.CommandContext(ctx, executable, "-test.run=^TestIndexerAbruptExitHelper$")
	child.Env = append(os.Environ(), "INDEXER_FAULT_HELPER=1", "INDEXER_FIXTURE_DSN="+db.Config().ConnString(), "INDEXER_FIXTURE_DATABASE="+db.Config().ConnConfig.Database, "INDEXER_FIXTURE_ROLE="+idxRole, "INDEXER_FIXTURE_FILE="+file.ID)
	var output bytes.Buffer
	child.Stdout = &output
	child.Stderr = &output
	if err = child.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	done := make(chan error, 1)
	go func() { done <- child.Wait() }()
	defer func() {
		if !waited {
			_ = child.Process.Kill()
			<-done
		}
	}()
	select {
	case <-entered:
	case <-done:
		waited = true
		t.Fatalf("fixture child exited before upload: %s", output.String())
	case <-ctx.Done():
		t.Fatal("child did not reach external upload")
	}
	idx := rolePool(t, db, idxRole)
	processor := indexing.New(idx, resources)
	// Another live Indexer must not process the in-flight file or call the provider.
	if err = processor.ProcessFile(ctx, file.ID); err != nil {
		t.Fatal(err)
	}
	if uploads.Load() != 1 {
		t.Fatal("concurrent indexer repeated upload")
	}
	current, err := store.KnowledgeFile(ctx, file.ID)
	if err != nil || current.Status != "running" {
		t.Fatal("missing write-before-effect state", err, current.Status)
	}
	if err = child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	<-done
	waited = true
	for {
		if err = processor.ProcessFile(ctx, file.ID); err != nil {
			t.Fatal(err)
		}
		current, err = store.KnowledgeFile(ctx, file.ID)
		if err != nil {
			t.Fatal(err)
		}
		if current.Status == "unknown" {
			break
		}
		select {
		case <-time.After(20 * time.Millisecond):
		case <-ctx.Done():
			t.Fatal("dead process retained file lock")
		}
	}
	if err = processor.ProcessFile(ctx, file.ID); err != nil {
		t.Fatal(err)
	}
	if uploads.Load() != 1 {
		t.Fatal("unknown external effect blindly retried")
	}
}

func TestTrackReceiptPollingUsesSavedConfigWithoutReupload(t *testing.T) {
	db, role, idxRole := fixtureDatabase(t)
	ctx := context.Background()
	if err := Migrate(ctx, db, role, idxRole); err != nil {
		t.Fatal(err)
	}
	var uploads, polls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == "POST" && r.URL.Path == "/documents/text" {
			uploads.Add(1)
			io.WriteString(w, `{"track_id":"fixture-track"}`)
			return
		}
		if r.Method == "GET" && r.URL.Path == "/documents/track_status/fixture-track" {
			polls.Add(1)
			io.WriteString(w, `{"documents":[{"status":"processed"}]}`)
			return
		}
		w.WriteHeader(400)
	}))
	defer server.Close()
	resources := savedResources{kb: KnowledgeBase{Name: "track fixture", Engine: "lightrag", Endpoint: server.URL, ChunkSize: 200}}
	store := &Store{DB: rolePool(t, db, role), ResourceReader: resources, SecretReader: resources}
	file, err := store.UploadKnowledge(ctx, Ref{ID: "tracked", Revision: 1}, "source.txt", "fixture://track", []byte("controlled inventory evidence"))
	if err != nil {
		t.Fatal(err)
	}
	processor := indexing.New(rolePool(t, db, idxRole), savedResources{}) // no ResourceReader is provided or available
	if err = processor.ProcessFile(ctx, file.ID); err != nil {
		t.Fatal(err)
	}
	current, err := store.KnowledgeFile(ctx, file.ID)
	if err != nil || current.Status != "external_pending" || current.TrackID != "fixture-track" {
		t.Fatal("durable track receipt missing", err)
	}
	if err = processor.ProcessFile(ctx, file.ID); err != nil {
		t.Fatal(err)
	}
	current, err = store.KnowledgeFile(ctx, file.ID)
	if err != nil || current.Status != "ready" || uploads.Load() != 1 || polls.Load() != 1 {
		t.Fatal("receipt polling failed", err, current.Status, uploads.Load(), polls.Load())
	}
}

// The remote transport is synthetic; PostgreSQL, restricted roles, saved
// config, state transitions and stable upsert identities are real.
func TestMilvusLostReceiptRetryRetainsChunkIdentities(t *testing.T) {
	db, role, idxRole := fixtureDatabase(t)
	ctx := context.Background()
	if err := Migrate(ctx, db, role, idxRole); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var attempts [][]string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/embeddings":
			io.WriteString(w, `{"data":[{"index":0,"embedding":[1,0]}]}`)
		case "/v2/vectordb/collections/has":
			io.WriteString(w, `{"code":0,"data":{"has":true}}`)
		case "/v2/vectordb/entities/upsert":
			var in struct {
				Data []struct {
					ID    string `json:"id"`
					Chunk string `json:"chunk_id"`
				} `json:"data"`
			}
			if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
				t.Error(err)
				w.WriteHeader(400)
				return
			}
			ids := []string{}
			for _, d := range in.Data {
				if d.ID != d.Chunk {
					t.Error("chunk identity mismatch")
				}
				ids = append(ids, d.ID)
			}
			mu.Lock()
			attempts = append(attempts, ids)
			first := len(attempts) == 1
			mu.Unlock()
			if first {
				w.WriteHeader(502)
				io.WriteString(w, `{"error":"fixture receipt lost after effect"}`)
				return
			}
			io.WriteString(w, `{"code":0,"data":{"upsertCount":1}}`)
		default:
			t.Error("unexpected provider path")
			w.WriteHeader(400)
		}
	}))
	defer server.Close()
	resources := savedResources{kb: KnowledgeBase{Name: "vector fixture", Engine: "milvus", Endpoint: server.URL, Embedding: ModelConfig{BaseURL: server.URL, Model: "fixture"}, Dimensions: 2, ChunkSize: 200}}
	store := &Store{DB: rolePool(t, db, role), ResourceReader: resources, SecretReader: resources}
	file, err := store.UploadKnowledge(ctx, Ref{ID: "vector", Revision: 1}, "source.txt", "fixture://vector", []byte("controlled evidence"))
	if err != nil {
		t.Fatal(err)
	}
	processor := indexing.New(rolePool(t, db, idxRole), savedResources{})
	if err = processor.ProcessFile(ctx, file.ID); err != nil {
		t.Fatal(err)
	}
	current, err := store.KnowledgeFile(ctx, file.ID)
	if err != nil || current.Status != "failed" {
		t.Fatal("lost receipt state", err, current.Status)
	}
	// Exercise the existing administrative retry route with the application DB role.
	mux := http.NewServeMux()
	(&Server{Store: store}).Routes(mux)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("POST", "/v1/files/"+file.ID+"/retry", nil))
	if w.Code != 200 {
		t.Fatal("operator retry rejected", w.Code)
	}
	if err = processor.ProcessFile(ctx, file.ID); err != nil {
		t.Fatal(err)
	}
	current, err = store.KnowledgeFile(ctx, file.ID)
	if err != nil || current.Status != "ready" {
		t.Fatal("retry did not finish", err, current.Status)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(attempts) != 2 || len(attempts[0]) != 1 || !reflect.DeepEqual(attempts[0], attempts[1]) {
		t.Fatal("upsert IDs changed on retry", attempts)
	}
}
