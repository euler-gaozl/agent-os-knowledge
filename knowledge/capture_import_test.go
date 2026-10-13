package knowledge

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/euler-gaozl/agent-os-sdk/contracts"
	"github.com/euler-gaozl/agent-os-sdk/servicehttp"
	"github.com/jackc/pgx/v5/pgxpool"
)

type captureFixtureReader struct{ bundle CaptureBundle }

func (r captureFixtureReader) ReadCapture(context.Context, string) (CaptureBundle, error) {
	return r.bundle, nil
}

func fixtureCapture() CaptureBundle {
	b := CaptureBundle{Format: 1, ID: strings.Repeat("a", 32), RequestedURL: "https://source.invalid/evidence", FinalURL: "https://source.invalid/evidence", Title: "独立资料🙂", FetchedAt: time.Date(2026, 1, 1, 0, 0, 0, 123456000, time.UTC), MediaType: "text/html; charset=utf-8", ExtractionVersion: "agentos-text-html-v1", Raw: []byte("<p>证据🙂库存120吨</p>"), Text: "证据🙂库存120吨"}
	b.RawHash = Hash(b.Raw)
	b.TextHash = Hash([]byte(b.Text))
	b.RawBytes = len(b.Raw)
	b.TextBytes = len(b.Text)
	return b
}

func captureImportFixture(t *testing.T) (*Store, *pgxpool.Pool, CaptureBundle) {
	t.Helper()
	ctx := context.Background()
	admin, role, idx := fixtureDatabase(t)
	if e := Migrate(ctx, admin, role, idx); e != nil {
		t.Fatal(e)
	}
	c := admin.Config().Copy()
	c.ConnConfig.RuntimeParams["role"] = role
	c.MaxConns = 3
	db, e := pgxpool.NewWithConfig(ctx, c)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(db.Close)
	b := fixtureCapture()
	return &Store{DB: db, ResourceReader: fixtureResources{}, CaptureReader: captureFixtureReader{b}}, admin, b
}

func TestCaptureImportAtomicCommitFailureAndRecovery(t *testing.T) {
	s, admin, b := captureImportFixture(t)
	ctx := context.Background()
	ref := Ref{ID: "capture-test", Revision: 1}
	_, e := admin.Exec(ctx, `CREATE FUNCTION knowledge.reject_capture_manifest() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.media_type='application/vnd.agentos.source-archive+json' THEN RAISE EXCEPTION 'fixture deferred final write failure'; END IF; RETURN NEW; END $$; CREATE CONSTRAINT TRIGGER reject_capture_manifest AFTER INSERT ON knowledge.artifacts DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION knowledge.reject_capture_manifest()`)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = s.importCapture(ctx, ref, b.ID); e == nil {
		t.Fatal("expected deferred commit failure")
	}
	for _, table := range []string{"documents", "artifacts", "artifact_refs", "knowledge_files", "knowledge_chunks"} {
		var n int
		if e = s.DB.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&n); e != nil || n != 0 {
			t.Fatal("partial commit", table, n, e)
		}
	}
	if _, e = admin.Exec(ctx, "DROP TRIGGER reject_capture_manifest ON knowledge.artifacts"); e != nil {
		t.Fatal(e)
	}
	out, e := s.importCapture(ctx, ref, b.ID)
	if e != nil {
		t.Fatal(e)
	}
	if out.File.Status != "ready" || out.Source.Archive.Format != 2 || out.Source.Archive.Capture.ID != b.ID || out.Source.Archive.CapturedAt != "2026-01-01T00:00:00.123456Z" {
		t.Fatal("wrong capture receipt", out)
	}
	for member, want := range map[string][]byte{"raw": b.Raw, "text": []byte(b.Text), "manifest": JSON(out.Source.Archive)} {
		body, hash, e := s.SourceArchiveMember(ctx, out.File.ID, out.Source.Hash, member)
		if e != nil || !bytes.Equal(body, want) || hash != Hash(want) {
			t.Fatal("incomplete committed association", member, e)
		}
	}
	result, e := s.Retrieve(ctx, ref, out.File.Config, "库存")
	if e != nil || len(result.Hits) == 0 || result.Hits[0]["document_id"] != out.File.DocumentID {
		t.Fatal("committed capture not searchable", e, result)
	}
}

func TestCaptureImportConcurrentRetryRepairsMissingArchiveAndDetectsCorruption(t *testing.T) {
	s, admin, b := captureImportFixture(t)
	ctx := context.Background()
	ref := Ref{ID: "capture-test", Revision: 1}
	// A previous ordinary upload of exactly this document must not cause the
	// capture import to return early without adding all provenance attachments.
	name, body, _, _, e := prepareCapture(b.ID, b)
	if e != nil {
		t.Fatal(e)
	}
	file, e := s.UploadKnowledge(ctx, ref, name, b.RequestedURL, body)
	if e != nil {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	results := make(chan CaptureImportResult, 6)
	failures := make(chan error, 6)
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, e := s.importCapture(ctx, ref, b.ID)
			if e != nil {
				failures <- e
			} else {
				results <- out
			}
		}()
	}
	wg.Wait()
	close(results)
	close(failures)
	for e := range failures {
		t.Fatal(e)
	}
	var hash string
	for out := range results {
		if out.File.ID != file.ID || (hash != "" && hash != out.Source.Hash) {
			t.Fatal("duplicate capture import")
		}
		hash = out.Source.Hash
	}
	var n int
	for table, want := range map[string]int{"knowledge_files": 1, "documents": 1, "artifacts": 4, "artifact_refs": 4} {
		if e = s.DB.QueryRow(ctx, "SELECT count(*) FROM "+table).Scan(&n); e != nil || n != want {
			t.Fatal(table, n, e)
		}
	}
	if _, e = admin.Exec(ctx, "DELETE FROM knowledge.artifact_refs WHERE root=$1 AND hash=$2", "knowledge-source:"+file.ID+":payloads", b.RawHash); e != nil {
		t.Fatal(e)
	}
	out, e := s.importCapture(ctx, ref, b.ID)
	if e != nil || out.Source.Hash != hash {
		t.Fatal("retry did not repair retention", e)
	}
	if _, _, e = s.SourceArchiveMember(ctx, file.ID, hash, "raw"); e != nil {
		t.Fatal(e)
	}
	other, e := s.importCapture(ctx, Ref{ID: ref.ID, Revision: 2}, b.ID)
	if e != nil || other.File.ID == file.ID || other.Source.Archive.Knowledge.Revision != 2 {
		t.Fatal("revision isolation lost", e)
	}
	if _, e = admin.Exec(ctx, "UPDATE knowledge.artifacts SET body='corrupt'::bytea WHERE hash=$1", file.Hash); e != nil {
		t.Fatal(e)
	}
	_, e = s.importCapture(ctx, ref, b.ID)
	requireArchiveStatus(t, e, 409)
}

func TestCaptureImportHTTPDiscardedReplyAndPermissions(t *testing.T) {
	s, _, b := captureImportFixture(t)
	token := strings.Repeat("c", 40)
	runtimeToken := strings.Repeat("r", 40)
	server := &Server{Store: s}
	handler := servicehttp.New(servicehttp.Config{Role: "knowledge", Peers: map[string]string{"control": servicehttp.TokenHash(token), "runtime": servicehttp.TokenHash(runtimeToken)}, Authorize: Authorize}, s.DB.Ping, func(m *http.ServeMux) {
		m.HandleFunc("POST /v1/import-capture", servicehttp.Wrap(server.importCaptureHTTP))
	})
	srv := httptest.NewServer(handler)
	defer srv.Close()
	input := JSON(map[string]any{"knowledge": Ref{ID: "capture-test", Revision: 1}, "capture_id": b.ID})
	call := func(key string, discard bool) (int, CaptureImportResult) {
		req, e := http.NewRequest("POST", srv.URL+"/v1/import-capture", bytes.NewReader(input))
		if e != nil {
			t.Fatal(e)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		resp, e := http.DefaultClient.Do(req)
		if e != nil {
			t.Fatal(e)
		}
		defer resp.Body.Close()
		var out CaptureImportResult
		if !discard && resp.StatusCode == 200 {
			if e = json.NewDecoder(resp.Body).Decode(&out); e != nil {
				t.Fatal(e)
			}
		}
		return resp.StatusCode, out
	}
	if status, _ := call(runtimeToken, false); status != 403 {
		t.Fatal("model runtime may import", status)
	}
	if status, _ := call(token, true); status != 200 {
		t.Fatal(status)
	}
	var committedID string
	if e := s.DB.QueryRow(context.Background(), "SELECT id FROM knowledge_files").Scan(&committedID); e != nil {
		t.Fatal(e)
	}
	status, out := call(token, false)
	if status != 200 || out.File.ID != committedID {
		t.Fatal("lost response retry duplicated import", status)
	}
	page, e := s.SourceArchives(context.Background(), out.File.ID, "")
	if e != nil || len(page.Items) != 1 {
		t.Fatal("lost reply attachment", e)
	}
}

func TestCaptureBundleValidationAndFixedAuthenticatedTransport(t *testing.T) {
	b := fixtureCapture()
	for _, tc := range []struct {
		name   string
		mutate func(*CaptureBundle)
	}{
		{"wrong identity", func(x *CaptureBundle) { x.ID = strings.Repeat("b", 32) }},
		{"raw tamper", func(x *CaptureBundle) { x.Raw = []byte("modified"); x.RawBytes = len(x.Raw) }},
		{"text tamper", func(x *CaptureBundle) { x.Text = "modified"; x.TextBytes = len(x.Text) }},
		{"size tamper", func(x *CaptureBundle) { x.TextBytes++ }},
		{"redirect metadata", func(x *CaptureBundle) { x.FinalURL = "https://other.invalid/" }},
		{"future version", func(x *CaptureBundle) { x.ExtractionVersion = "unknown-v99" }},
		{"credential url", func(x *CaptureBundle) {
			x.RequestedURL = "https://source.invalid/?token=private"
			x.FinalURL = x.RequestedURL
		}},
		{"future capture", func(x *CaptureBundle) { x.FetchedAt = time.Now().Add(24 * time.Hour) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := b
			tc.mutate(&bad)
			if _, _, _, _, e := prepareCapture(b.ID, bad); e == nil {
				t.Fatal("invalid capture accepted")
			}
		})
	}
	var peerCalls atomic.Int32
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { peerCalls.Add(1) }))
	defer peer.Close()
	var mode atomic.Int32
	var sourceCalls atomic.Int32
	var capabilityCalls atomic.Int32
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/capture-capabilities" && r.Header.Get("Authorization") == "Bearer fixture-peer-only" {
			capabilityCalls.Add(1)
			if mode.Load() == 4 {
				w.WriteHeader(404)
				return
			}
			io.WriteString(w, `{"protocol":"source-capture-v1","durable_capture_ids":true}`)
			return
		}
		sourceCalls.Add(1)
		if r.Header.Get("Authorization") != "Bearer fixture-peer-only" || r.URL.Path != "/captures/"+b.ID+"/bundle" || r.Method != "GET" {
			t.Error("wrong source identity or endpoint")
			w.WriteHeader(403)
			return
		}
		switch mode.Load() {
		case 1:
			http.Redirect(w, r, peer.URL, 302)
		case 2:
			io.WriteString(w, strings.Repeat("x", (4<<20)+1))
		case 3:
			fmt.Fprintf(w, "%s {}", JSON(b))
		default:
			json.NewEncoder(w).Encode(b)
		}
	}))
	defer source.Close()
	r := CaptureHTTPReader{BaseURL: source.URL, Token: "fixture-peer-only"}
	for i := int32(0); i < 5; i++ {
		mode.Store(i)
		out, e := r.ReadCapture(context.Background(), b.ID)
		if i == 0 && (e != nil || out.ID != b.ID) {
			t.Fatal(e)
		}
		if i > 0 && e == nil {
			t.Fatal("untrusted transport response accepted", i)
		}
	}
	if peerCalls.Load() != 0 || sourceCalls.Load() != 4 || capabilityCalls.Load() != 5 {
		t.Fatal("redirect leaked service credential or wrong endpoint")
	}
	if _, e := r.ReadCapture(context.Background(), "../foreign"); e == nil || sourceCalls.Load() != 4 {
		t.Fatal("unsafe path reached Sources")
	}
}

func TestReaderBridgeDisablesCaptureWrites(t *testing.T) {
	if CaptureImportWritesEnabled {
		t.Fatal("this reader bridge must not enable format 2 writes")
	}
	m := http.NewServeMux()
	(&Server{Store: &Store{}}).captureImportRoutes(m)
	w := httptest.NewRecorder()
	m.ServeHTTP(w, httptest.NewRequest("POST", "/v1/import-capture", strings.NewReader(`{}`)))
	if w.Code != 503 {
		t.Fatal("writer gate bypass", w.Code)
	}
}

type captureExternalResources struct{}

func (captureExternalResources) Resource(ctx context.Context, kind string, ref Ref, dst any) error {
	return json.Unmarshal(JSON(KnowledgeBase{Name: "external fixture", Engine: "lightrag", Endpoint: "https://provider.invalid", ChunkSize: 200}), dst)
}
func TestCaptureImportExternalIndexRemainsQueued(t *testing.T) {
	s, _, b := captureImportFixture(t)
	s.ResourceReader = captureExternalResources{}
	out, e := s.importCapture(context.Background(), Ref{ID: "external", Revision: 1}, b.ID)
	if e != nil || out.File.Status != "queued" {
		t.Fatal("external import falsely ready", e, out.File.Status)
	}
	if _, _, e = s.SourceArchiveMember(context.Background(), out.File.ID, out.Source.Hash, "text"); e != nil {
		t.Fatal("queued file source not durable", e)
	}
}
