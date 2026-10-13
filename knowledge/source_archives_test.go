package knowledge

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	. "github.com/euler-gaozl/agent-os-sdk/contracts"
	"github.com/euler-gaozl/agent-os-sdk/servicehttp"
	"github.com/jackc/pgx/v5/pgxpool"
)

func archiveFixture(t *testing.T) (*Store, *pgxpool.Pool, KnowledgeFile, SourceArchiveInput) {
	t.Helper()
	admin, role, indexer := fixtureDatabase(t)
	if err := Migrate(context.Background(), admin, role, indexer); err != nil {
		t.Fatal(err)
	}
	c := admin.Config().Copy()
	c.ConnConfig.RuntimeParams["role"] = role
	db, err := pgxpool.NewWithConfig(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	s := &Store{DB: db, ResourceReader: fixtureResources{}}
	doc := []byte("标题\n证据🙂库存120吨\n结束")
	f, err := s.UploadKnowledge(context.Background(), Ref{ID: "archive", Revision: 1}, "selected.md", "https://source.invalid/original", doc)
	if err != nil {
		t.Fatal(err)
	}
	in := SourceArchiveInput{FileHash: f.Hash, URL: "https://source.invalid/original", CapturedAt: "2026-01-01T08:00:00+08:00", MediaType: "text/html; charset=utf-8", ExtractionMethod: "historical fixture extraction retained without regeneration", Raw: []byte("<p>证据🙂库存120吨</p>"), Text: "前言\n证据🙂库存120吨\n尾页", Spans: []SourceSpan{{Start: 3, End: 12, DocumentStart: 3, DocumentEnd: 12}}}
	in.RawHash = Hash(in.Raw)
	in.RawEncoding = "identity"
	in.TextHash = Hash([]byte(in.Text))
	return s, admin, f, in
}

func TestSourceArchiveAtomicIdempotentAndImmutable(t *testing.T) {
	s, _, f, in := archiveFixture(t)
	ctx := context.Background()
	before, err := s.KnowledgeFile(ctx, f.ID)
	if err != nil {
		t.Fatal(err)
	}
	var docBefore, chunksBefore string
	if err = s.DB.QueryRow(ctx, "SELECT row_to_json(d)::text FROM documents d WHERE id=$1", f.DocumentID).Scan(&docBefore); err != nil {
		t.Fatal(err)
	}
	if err = s.DB.QueryRow(ctx, "SELECT jsonb_agg(row_to_json(c) ORDER BY ordinal)::text FROM knowledge_chunks c WHERE file_id=$1", f.ID).Scan(&chunksBefore); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	results := make(chan SourceArchiveRecord, 6)
	failures := make(chan error, 6)
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, e := s.AttachSourceArchive(ctx, f.ID, in)
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
	var expected string
	for out := range results {
		if expected != "" && expected != out.Hash {
			t.Fatal("non-deterministic retry")
		}
		expected = out.Hash
		if out.Archive.CapturedAt != "2026-01-01T00:00:00Z" {
			t.Fatal("capture time changed")
		}
	}
	page, err := s.SourceArchives(ctx, f.ID, "")
	if err != nil || len(page.Items) != 1 || page.Items[0].Hash != expected {
		t.Fatal(page, err)
	}
	for _, item := range []struct {
		hash string
		body []byte
		root string
	}{{in.RawHash, in.Raw, ":payloads"}, {in.TextHash, []byte(in.Text), ":payloads"}, {expected, JSON(page.Items[0].Archive), ":manifests"}} {
		var saved []byte
		err = s.DB.QueryRow(ctx, "SELECT a.body FROM artifacts a JOIN artifact_refs r USING(hash) WHERE r.root=$1 AND a.hash=$2", "knowledge-source:"+f.ID+item.root, item.hash).Scan(&saved)
		if err != nil || !bytes.Equal(saved, item.body) {
			t.Fatal("lost archive bytes/root", err)
		}
	}
	after, err := s.KnowledgeFile(ctx, f.ID)
	if err != nil || string(JSON(before)) != string(JSON(after)) {
		t.Fatal("attachment changed file", err)
	}
	var docAfter, chunksAfter string
	_ = s.DB.QueryRow(ctx, "SELECT row_to_json(d)::text FROM documents d WHERE id=$1", f.DocumentID).Scan(&docAfter)
	_ = s.DB.QueryRow(ctx, "SELECT jsonb_agg(row_to_json(c) ORDER BY ordinal)::text FROM knowledge_chunks c WHERE file_id=$1", f.ID).Scan(&chunksAfter)
	if docAfter != docBefore || chunksAfter != chunksBefore {
		t.Fatal("attachment changed document/index")
	}
}

func requireArchiveStatus(t *testing.T, err error, status int) {
	t.Helper()
	var e *servicehttp.StatusError
	if !errors.As(err, &e) || e.Status != status {
		t.Fatalf("wanted status %d, got %v", status, err)
	}
}

func TestSourceArchiveRejectsFalseAssociationsAndPartialCommit(t *testing.T) {
	s, admin, f, in := archiveFixture(t)
	ctx := context.Background()
	for _, tc := range []struct {
		name   string
		mutate func(*SourceArchiveInput)
		status int
	}{
		{"wrong target", func(x *SourceArchiveInput) { x.FileHash = strings.Repeat("0", 64) }, 409},
		{"wrong raw hash", func(x *SourceArchiveInput) { x.RawHash = strings.Repeat("0", 64) }, 400},
		{"wrong text hash", func(x *SourceArchiveInput) { x.TextHash = strings.Repeat("0", 64) }, 400},
		{"no spans", func(x *SourceArchiveInput) { x.Spans = nil }, 400},
		{"bad Unicode span", func(x *SourceArchiveInput) {
			x.Spans = []SourceSpan{{Start: 3, End: 99, DocumentStart: 3, DocumentEnd: 99}}
		}, 400},
		{"different text", func(x *SourceArchiveInput) {
			x.Spans = []SourceSpan{{Start: 0, End: 2, DocumentStart: 0, DocumentEnd: 2}}
		}, 400},
		{"credential URL", func(x *SourceArchiveInput) { x.URL = "https://a:b@source.invalid/" }, 400},
		{"query credential", func(x *SourceArchiveInput) { x.URL = "https://source.invalid/?api_key=x" }, 400},
		{"malformed query", func(x *SourceArchiveInput) { x.URL = "https://source.invalid/?api_key=x%ZZ" }, 400},
		{"query semicolon", func(x *SourceArchiveInput) { x.URL = "https://source.invalid/?api_key=x;y" }, 400},
		{"future time", func(x *SourceArchiveInput) { x.CapturedAt = time.Now().Add(24 * time.Hour).Format(time.RFC3339) }, 400},
		{"oversize", func(x *SourceArchiveInput) { x.Raw = bytes.Repeat([]byte{'x'}, (1<<20)+1); x.RawHash = Hash(x.Raw) }, 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := in
			tc.mutate(&bad)
			_, err := s.AttachSourceArchive(ctx, f.ID, bad)
			requireArchiveStatus(t, err, tc.status)
		})
	}
	_, err := s.AttachSourceArchive(ctx, strings.Repeat("a", 32), in)
	requireArchiveStatus(t, err, 404)
	// A deferred failure occurs at commit after all payload/ref statements ran.
	_, err = admin.Exec(ctx, `CREATE FUNCTION knowledge.reject_manifest() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.media_type='application/vnd.agentos.source-archive+json' THEN RAISE EXCEPTION 'fixture final write failure'; END IF; RETURN NEW; END $$; CREATE CONSTRAINT TRIGGER reject_archive_manifest AFTER INSERT ON knowledge.artifacts DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION knowledge.reject_manifest()`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.AttachSourceArchive(ctx, f.ID, in); err == nil {
		t.Fatal("injected failure missed")
	}
	var count int
	if err = s.DB.QueryRow(ctx, "SELECT count(*) FROM artifacts").Scan(&count); err != nil || count != 1 {
		t.Fatal("partial artifact commit", count, err)
	}
	if err = s.DB.QueryRow(ctx, "SELECT count(*) FROM artifact_refs WHERE root LIKE 'knowledge-source:%'").Scan(&count); err != nil || count != 0 {
		t.Fatal("partial refs", count, err)
	}
	if _, err = admin.Exec(ctx, "DROP TRIGGER reject_archive_manifest ON knowledge.artifacts"); err != nil {
		t.Fatal(err)
	}
	// A pre-existing hash whose bytes are corrupt must not be silently adopted.
	if _, err = admin.Exec(ctx, "INSERT INTO knowledge.artifacts(hash,body,media_type) VALUES($1,$2,'fixture')", in.RawHash, []byte("corrupt")); err != nil {
		t.Fatal(err)
	}
	_, err = s.AttachSourceArchive(ctx, f.ID, in)
	requireArchiveStatus(t, err, 409)
}

func TestSourceArchiveListingBoundsAndHTTPRoles(t *testing.T) {
	s, admin, f, in := archiveFixture(t)
	ctx := context.Background()
	for i := 0; i < 23; i++ {
		in.ExtractionMethod = fmt.Sprintf("retained fixture extraction %d", i)
		if _, err := s.AttachSourceArchive(ctx, f.ID, in); err != nil {
			t.Fatal(err)
		}
	}
	first, err := s.SourceArchives(ctx, f.ID, "")
	if err != nil || len(first.Items) != 20 || first.NextCursor == "" {
		t.Fatal(first, err)
	}
	second, err := s.SourceArchives(ctx, f.ID, first.NextCursor)
	if err != nil || len(second.Items) != 3 || second.NextCursor != "" {
		t.Fatal(second, err)
	}
	seen := map[string]bool{}
	for _, r := range append(first.Items, second.Items...) {
		if seen[r.Hash] {
			t.Fatal("duplicate page")
		}
		seen[r.Hash] = true
	}
	peers := map[string]string{}
	for _, role := range []string{"control", "runtime", "knowledge", "futures", "sources", "indexer"} {
		peers[role] = servicehttp.TokenHash(strings.Repeat(role, 12))
	}
	server := &Server{Store: s}
	handler := servicehttp.New(servicehttp.Config{Role: "knowledge", Peers: peers, Authorize: Authorize}, s.DB.Ping, server.Routes)
	path := "/v1/files/" + f.ID + "/source-archives"
	for role := range peers {
		for _, method := range []string{"GET", "POST"} {
			r := httptest.NewRequest(method, path, bytes.NewReader(JSON(in)))
			r.Header.Set("Authorization", "Bearer "+strings.Repeat(role, 12))
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			expected := 403
			if role == "control" {
				expected = 200
			}
			if w.Code != expected {
				t.Fatal(role, method, w.Code, w.Body.String())
			}
		}
	}
	for _, query := range []string{"?after=", "?after=x", "?after=" + first.NextCursor + "&after=" + first.NextCursor, "?limit=999"} {
		r := httptest.NewRequest("GET", path+query, nil)
		r.Header.Set("Authorization", "Bearer "+strings.Repeat("control", 12))
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != 400 {
			t.Fatal(query, w.Code)
		}
	}
	if _, err = admin.Exec(ctx, "UPDATE knowledge.artifacts SET body=$2 WHERE hash=$1", first.Items[0].Hash, []byte(`{"format":1}`)); err != nil {
		t.Fatal(err)
	}
	_, err = s.SourceArchives(ctx, f.ID, "")
	requireArchiveStatus(t, err, 409)
}

func TestSourceArchiveStrictTransport(t *testing.T) {
	server := &Server{Store: &Store{}}
	handler := servicehttp.New(servicehttp.Config{Role: "knowledge", Peers: map[string]string{"control": servicehttp.TokenHash(strings.Repeat("control", 12))}, Authorize: Authorize}, nil, server.Routes)
	for _, body := range []string{`{"unrecognized":"value"}`, `{} {}`, `{"text":"` + strings.Repeat("x", 2<<20) + `"}`} {
		r := httptest.NewRequest("POST", "/v1/files/"+strings.Repeat("a", 32)+"/source-archives", strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+strings.Repeat("control", 12))
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != 400 {
			t.Fatal(w.Code)
		}
	}
}

func TestSourceArchiveCompressedBytesAndBomb(t *testing.T) {
	s, _, f, in := archiveFixture(t)
	ctx := context.Background()
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	if _, err := w.Write(in.Raw); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	in.Raw = b.Bytes()
	in.RawEncoding = "gzip"
	out, err := s.AttachSourceArchive(ctx, f.ID, in)
	if err != nil {
		t.Fatal(err)
	}
	if out.Archive.StoredRawHash != Hash(in.Raw) || out.Archive.RawHash != in.RawHash || out.Archive.RawHash == out.Archive.StoredRawHash {
		t.Fatal("compressed and expanded identities conflated")
	}
	retained, hash, err := s.SourceArchiveMember(ctx, f.ID, out.Hash, "raw")
	if err != nil || hash != Hash(in.Raw) || !bytes.Equal(retained, in.Raw) {
		t.Fatal("gzip bytes not retained exactly", err)
	}
	for _, bad := range [][]byte{in.Raw[:len(in.Raw)-1], append(bytes.Clone(in.Raw), 'x'), append(bytes.Clone(in.Raw[:len(in.Raw)-8]), make([]byte, 8)...)} {
		x := in
		x.Raw = bad
		_, err = s.AttachSourceArchive(ctx, f.ID, x)
		requireArchiveStatus(t, err, 400)
	}
	b.Reset()
	w = gzip.NewWriter(&b)
	_, _ = w.Write(bytes.Repeat([]byte{'x'}, (1<<20)+1))
	_ = w.Close()
	in.Raw = b.Bytes()
	in.RawHash = Hash(bytes.Repeat([]byte{'x'}, (1<<20)+1))
	_, err = s.AttachSourceArchive(ctx, f.ID, in)
	requireArchiveStatus(t, err, 400)
}

func TestSourceArchiveScopedDownloadsAndCorruption(t *testing.T) {
	s, admin, f, in := archiveFixture(t)
	ctx := context.Background()
	out, err := s.AttachSourceArchive(ctx, f.ID, in)
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.UploadKnowledge(ctx, Ref{ID: "another", Revision: 2}, "other.md", "fixture", []byte("different document"))
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = s.SourceArchiveMember(ctx, second.ID, out.Hash, "raw")
	requireArchiveStatus(t, err, 404)
	_, _, err = s.SourceArchiveMember(ctx, f.ID, out.Hash, in.RawHash)
	requireArchiveStatus(t, err, 400)
	server := &Server{Store: s}
	peers := map[string]string{"control": servicehttp.TokenHash(strings.Repeat("control", 12)), "runtime": servicehttp.TokenHash(strings.Repeat("runtime", 12))}
	handler := servicehttp.New(servicehttp.Config{Role: "knowledge", Peers: peers, Authorize: Authorize}, s.DB.Ping, server.Routes)
	for _, member := range []struct {
		name string
		body []byte
	}{{"raw", in.Raw}, {"text", []byte(in.Text)}, {"manifest", JSON(out.Archive)}} {
		for role := range peers {
			r := httptest.NewRequest("GET", "/v1/files/"+f.ID+"/source-archives/"+out.Hash+"/"+member.name, nil)
			r.Header.Set("Authorization", "Bearer "+strings.Repeat(role, 12))
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, r)
			if role != "control" {
				if w.Code != 403 {
					t.Fatal(w.Code)
				}
				continue
			}
			if w.Code != 200 || !bytes.Equal(w.Body.Bytes(), member.body) || w.Header().Get("Content-Type") != "application/octet-stream" || w.Header().Get("Content-Disposition") != "attachment" || w.Header().Get("X-Content-Type-Options") != "nosniff" || w.Header().Get("Cache-Control") != "no-store" || w.Header().Get("X-Content-SHA256") != Hash(member.body) {
				t.Fatal("unsafe or incorrect download", member.name, w.Code)
			}
		}
	}
	// Keep global artifact bytes but remove just this file's retention membership.
	if _, err = admin.Exec(ctx, "DELETE FROM knowledge.artifact_refs WHERE root=$1 AND hash=$2", "knowledge-source:"+f.ID+":payloads", in.RawHash); err != nil {
		t.Fatal(err)
	}
	_, _, err = s.SourceArchiveMember(ctx, f.ID, out.Hash, "text")
	requireArchiveStatus(t, err, 409)
	// A retry restores the missing root and has the same stable manifest identity.
	again, err := s.AttachSourceArchive(ctx, f.ID, in)
	if err != nil || again.Hash != out.Hash {
		t.Fatal(again, err)
	}
	if _, err = admin.Exec(ctx, "UPDATE knowledge.artifacts SET body=$2 WHERE hash=$1", in.TextHash, []byte("damaged")); err != nil {
		t.Fatal(err)
	}
	_, _, err = s.SourceArchiveMember(ctx, f.ID, out.Hash, "manifest")
	requireArchiveStatus(t, err, 409)
}
