package knowledge

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"github.com/euler-gaozl/agent-os-knowledge/internal/files"
	. "github.com/euler-gaozl/agent-os-sdk/contracts"
	"github.com/euler-gaozl/agent-os-sdk/servicehttp"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"
)

func extractUpload(name string, b []byte) (string, error) {
	if len(b) == 0 || len(b) > 1<<20 {
		return "", errors.New("file must contain 1 byte to 1 MiB")
	}
	switch strings.ToLower(filepath.Ext(name)) {
	case ".txt", ".md", ".csv", ".json":
		if !utf8.Valid(b) || bytes.IndexByte(b, 0) >= 0 {
			return "", errors.New("text file must use UTF-8")
		}
		return string(b), nil
	case ".docx":
		z, e := zip.NewReader(bytes.NewReader(b), int64(len(b)))
		if e != nil {
			return "", e
		}
		for _, f := range z.File {
			if f.Name == "word/document.xml" {
				if f.UncompressedSize64 > 4<<20 {
					return "", errors.New("DOCX text exceeds extraction limit")
				}
				r, e := f.Open()
				if e != nil {
					return "", e
				}
				defer r.Close()
				decoder := xml.NewDecoder(io.LimitReader(r, 4<<20))
				var out strings.Builder
				inText := false
				for {
					token, e := decoder.Token()
					if e == io.EOF {
						break
					}
					if e != nil {
						return "", e
					}
					switch v := token.(type) {
					case xml.StartElement:
						if v.Name.Local == "t" {
							inText = true
						}
					case xml.EndElement:
						if v.Name.Local == "t" {
							inText = false
						}
						if v.Name.Local == "p" {
							out.WriteByte('\n')
						}
					case xml.CharData:
						if inText {
							out.Write(v)
						}
					}
				}
				return out.String(), nil
			}
		}
		return "", errors.New("DOCX has no main document")
	default:
		return "", errors.New("supported files: UTF-8 TXT, Markdown, CSV, JSON and DOCX text; PDF/OCR needs an external parser")
	}
}
func chunks(text string, size int) []string {
	chars := []rune(text)
	out := []string{}
	overlap := min(100, size/5)
	for start := 0; start < len(chars); {
		end := min(start+size, len(chars))
		out = append(out, string(chars[start:end]))
		if end == len(chars) {
			break
		}
		start = end - overlap
	}
	return out
}
func (s *Store) UploadKnowledge(ctx context.Context, ref Ref, name, source string, body []byte) (KnowledgeFile, error) {
	var kb KnowledgeBase
	f := KnowledgeFile{}
	if e := s.Resource(ctx, "knowledge", ref, &kb); e != nil {
		return f, e
	}
	if e := kb.Validate(); e != nil {
		return f, e
	}
	content, e := extractUpload(name, body)
	if e != nil {
		return f, e
	}
	if len(content) > 1000000 {
		return f, errors.New("extracted document exceeds 1 MB")
	}
	if strings.TrimSpace(source) == "" {
		source = "upload:" + name
	}
	tx, e := s.DB.Begin(ctx)
	if e != nil {
		return f, e
	}
	defer tx.Rollback(ctx)
	f, e = uploadKnowledgeTx(ctx, tx, ref, kb, name, source, body, content)
	if e != nil {
		return f, e
	}
	return f, tx.Commit(ctx)
}

// The caller owns commit so capture imports can retain the complete source in
// this same transaction. Duplicate reads also use tx, never a second pool slot.
func uploadKnowledgeTx(ctx context.Context, tx pgx.Tx, ref Ref, kb KnowledgeBase, name, source string, body []byte, content string) (KnowledgeFile, error) {
	hash := Hash(body)
	f := KnowledgeFile{ID: ID(), KBID: ref.ID, Revision: ref.Revision, Name: name, Hash: hash, Config: kb, Status: "queued"}
	// Serializes duplicate uploads before creating documents or retention roots.
	if _, e := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1,0))", fmt.Sprint(ref.ID, ref.Revision, hash)); e != nil {
		return f, e
	}
	var existing string
	e := tx.QueryRow(ctx, "SELECT id FROM knowledge_files WHERE kb_id=$1 AND kb_revision=$2 AND artifact_hash=$3", ref.ID, ref.Revision, hash).Scan(&existing)
	if e == nil {
		return files.Read(ctx, tx, existing)
	}
	if !errors.Is(e, pgx.ErrNoRows) {
		return f, e
	}
	if _, e = putArtifact(ctx, tx, body, "application/octet-stream", "knowledge:"+f.ID); e != nil {
		return f, e
	}
	d, e := putDocument(ctx, tx, Document{Kind: "evidence", Title: name, Content: content, Source: source, Scope: "knowledge:" + ref.ID, Metadata: map[string]any{"original_hash": hash, "kb_revision": ref.Revision}})
	if e != nil {
		return f, e
	}
	f.DocumentID = d.ID
	if kb.Engine == "local" {
		f.Status = "ready"
	}
	if _, e = tx.Exec(ctx, "INSERT INTO knowledge_files(id,kb_id,kb_revision,document_id,artifact_hash,name,status,config) VALUES($1,$2,$3,$4,$5,$6,$7,$8)", f.ID, ref.ID, ref.Revision, d.ID, hash, name, f.Status, JSON(kb)); e != nil {
		return f, e
	}
	batch := &pgx.Batch{}
	batch.Queue("UPDATE knowledge_files SET name_vector=to_tsvector('simple',$2) WHERE id=$1", f.ID, scopeToken(ref)+" "+SearchText(name))
	for i, text := range chunks(content, kb.ChunkSize) {
		batch.Queue("INSERT INTO knowledge_chunks(id,file_id,ordinal,content,search_vector) VALUES($1,$2,$3,$4,to_tsvector('simple',$5))", Hash([]byte(fmt.Sprint(f.ID, ":", i))), f.ID, i, text, scopeToken(ref)+" "+SearchText(text))
	}
	if e = tx.SendBatch(ctx, batch).Close(); e != nil {
		return f, e
	}
	return f, nil
}
func (s *Store) Retrieve(ctx context.Context, ref Ref, kb KnowledgeBase, query string) (Retrieval, error) {
	result := Retrieval{Engine: kb.Engine, Hits: []map[string]any{}}
	if strings.TrimSpace(query) == "" || len(query) > 4000 {
		return result, errors.New("query must be 1–4000 bytes")
	}
	if kb.Engine == "local" {
		terms := strings.Fields(SearchText(query))
		if len(terms) == 0 {
			return result, errors.New("query requires at least two adjacent Han characters or a word/number")
		}
		if len(terms) > 64 {
			return result, errors.New("lexical query exceeds 64 terms")
		}
		queryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		rows, e := s.DB.Query(queryCtx, localChunkQuery, ref.ID, ref.Revision, scopeToken(ref)+" "+strings.Join(terms, " "))
		if e != nil {
			return result, e
		}
		for rows.Next() {
			var id, content, source, document, name string
			var offset int
			if e = rows.Scan(&id, &content, &source, &document, &name, &offset); e != nil {
				rows.Close()
				return result, e
			}
			hit := map[string]any{"chunk_id": id, "content": content, "source": source, "document_id": document, "title": name}
			if offset >= 0 {
				hit["document_offset"] = offset
			}
			result.Hits = append(result.Hits, hit)
		}
		e = rows.Err()
		rows.Close()
		if e != nil || len(result.Hits) > 0 {
			return result, e
		}
		// File names are searchable independently from chunk contents, including
		// legacy uploads backfilled by the indexer. Return the first chunk as a
		// preview; the caller can read the complete document using document_id.
		rows, e = s.DB.Query(queryCtx, localNameQuery, ref.ID, ref.Revision, scopeToken(ref)+" "+strings.Join(terms, " "))
		if e != nil {
			return result, e
		}
		defer rows.Close()
		for rows.Next() {
			var id, content, source, document, name string
			var offset int
			if e = rows.Scan(&id, &content, &source, &document, &name, &offset); e != nil {
				return result, e
			}
			hit := map[string]any{"chunk_id": id, "content": content, "source": source, "document_id": document, "title": name, "matched_on": "file_name"}
			if offset >= 0 {
				hit["document_offset"] = offset
			}
			result.Hits = append(result.Hits, hit)
		}
		return result, rows.Err()
	}
	if kb.Engine == "lightrag" {
		key, e := s.Secret(ctx, kb.KeyEnv)
		if e != nil {
			return result, e
		}
		b, e := ProviderJSON(ctx, "POST", strings.TrimRight(kb.Endpoint, "/")+"/query/data", key, "X-API-Key", map[string]any{"query": query, "mode": "mix", "top_k": 10, "chunk_top_k": 8})
		if e != nil {
			return result, e
		}
		var response struct {
			Status string          `json:"status"`
			Data   json.RawMessage `json:"data"`
		}
		if e = json.Unmarshal(b, &response); e != nil {
			return result, e
		}
		if response.Status != "success" {
			return result, errors.New("LightRAG query failed")
		}
		result.Graph, e = s.scopedGraph(ctx, ref, kb.Endpoint, response.Data)
		return result, e
	}
	vectors, e := s.Embed(ctx, kb, []string{query}, true)
	if e != nil {
		return result, e
	}
	b, e := s.milvus(ctx, kb, "entities/search", map[string]any{"collectionName": collection(ref), "data": vectors, "annsField": "vector", "limit": 8, "outputFields": []string{"chunk_id"}, "consistencyLevel": "Strong"})
	if e != nil {
		return result, e
	}
	var response struct {
		Data []struct {
			ID       string  `json:"id"`
			Distance float64 `json:"distance"`
		} `json:"data"`
	}
	if e = json.Unmarshal(b, &response); e != nil {
		return result, e
	}
	for _, hit := range response.Data {
		var text, source, doc, name string
		e = s.DB.QueryRow(ctx, `SELECT c.content,d.source,d.id,f.name FROM knowledge_chunks c JOIN knowledge_files f ON f.id=c.file_id JOIN documents d ON d.id=f.document_id WHERE c.id=$1 AND f.kb_id=$2 AND f.kb_revision=$3 AND f.status='ready'`, hit.ID, ref.ID, ref.Revision).Scan(&text, &source, &doc, &name)
		if errors.Is(e, pgx.ErrNoRows) {
			continue
		}
		if e != nil {
			return result, e
		}
		result.Hits = append(result.Hits, map[string]any{"chunk_id": hit.ID, "content": text, "source": source, "document_id": doc, "title": name, "score": hit.Distance})
	}
	return result, nil
}
func (s *Server) Routes(mux *http.ServeMux) {
	s.documentRoutes(mux)
	s.sourceArchiveRoutes(mux)
	s.captureImportRoutes(mux)
	mux.HandleFunc("GET /v1/status", servicehttp.Wrap(func(w http.ResponseWriter, r *http.Request) error {
		var count int64
		if e := s.Store.DB.QueryRow(r.Context(), "SELECT count(*) FROM knowledge_files WHERE status='ready'").Scan(&count); e != nil {
			return e
		}
		servicehttp.JSONResponse(w, 200, map[string]any{"indexed_documents": count})
		return nil
	}))
	mux.HandleFunc("GET /v1/files", servicehttp.Wrap(func(w http.ResponseWriter, r *http.Request) error {

		rows, e := s.Store.DB.Query(r.Context(), "SELECT id,kb_id,kb_revision,document_id,artifact_hash,name,status,error,track_id,config FROM knowledge_files WHERE kb_id=$1 AND kb_revision=$2 AND ($3='' OR id<$3) ORDER BY id DESC LIMIT 100", r.URL.Query().Get("id"), r.URL.Query().Get("revision"), r.URL.Query().Get("after"))
		if e != nil {
			return e
		}
		defer rows.Close()
		list := []KnowledgeFile{}
		for rows.Next() {
			var f KnowledgeFile
			var cfg []byte
			if e = rows.Scan(&f.ID, &f.KBID, &f.Revision, &f.DocumentID, &f.Hash, &f.Name, &f.Status, &f.Error, &f.TrackID, &cfg); e != nil {
				return e
			}
			if e = json.Unmarshal(cfg, &f.Config); e != nil {
				return e
			}
			list = append(list, f)
		}
		if e = rows.Err(); e != nil {
			return e
		}
		servicehttp.JSONResponse(w, 200, list)
		return nil
	}))
	mux.HandleFunc("POST /v1/upload", servicehttp.Wrap(func(w http.ResponseWriter, r *http.Request) error {
		var in struct {
			Knowledge Ref    `json:"knowledge"`
			Name      string `json:"name"`
			Source    string `json:"source"`
			Content   []byte `json:"content"`
		}
		if e := servicehttp.Decode(w, r, &in); e != nil {
			return e
		}
		f, e := s.Store.UploadKnowledge(r.Context(), in.Knowledge, in.Name, in.Source, in.Content)
		if e != nil {
			return e
		}
		servicehttp.JSONResponse(w, 201, f)
		return nil
	}))
	mux.HandleFunc("POST /v1/query", servicehttp.Wrap(func(w http.ResponseWriter, r *http.Request) error {
		var in struct {
			Knowledge Ref    `json:"knowledge"`
			Query     string `json:"query"`
		}
		if e := servicehttp.Decode(w, r, &in); e != nil {
			return e
		}
		var kb KnowledgeBase
		if e := s.Store.Resource(r.Context(), "knowledge", in.Knowledge, &kb); e != nil {
			return e
		}
		v, e := s.Store.Retrieve(r.Context(), in.Knowledge, kb, in.Query)
		if e != nil {
			return e
		}
		servicehttp.JSONResponse(w, 200, v)
		return nil
	}))
	mux.HandleFunc("POST /v1/retrieve", servicehttp.Wrap(func(w http.ResponseWriter, r *http.Request) error {
		var in struct {
			RunID string `json:"run_id"`
			KBID  string `json:"kb_id"`
			Query string `json:"query"`
		}
		if e := servicehttp.Decode(w, r, &in); e != nil {
			return e
		}
		run, e := s.Store.Run(r.Context(), in.RunID)
		if e != nil {
			return e
		}
		kb, ok := run.Snapshot.Knowledge[in.KBID]
		if !ok {
			return errors.New("knowledge base not bound to agent")
		}
		var ref Ref
		for _, candidate := range run.Snapshot.Agent.Knowledge {
			if candidate.ID == in.KBID {
				ref = candidate
			}
		}
		v, e := s.Store.Retrieve(r.Context(), ref, kb, in.Query)
		if e != nil {
			return e
		}
		servicehttp.JSONResponse(w, 200, retrievalForModel(v))
		return nil
	}))
	mux.HandleFunc("POST /v1/files/{id}/retry", servicehttp.Wrap(func(w http.ResponseWriter, r *http.Request) error {
		tag, e := s.Store.DB.Exec(r.Context(), "UPDATE knowledge_files SET status='queued',error='',updated_at=now() WHERE id=$1 AND status='failed' AND config->>'engine'='milvus'", r.PathValue("id"))
		if e != nil {
			return e
		}
		if tag.RowsAffected() != 1 {
			return errors.New("only failed Milvus upserts can be retried; LightRAG needs external receipt reconciliation")
		}
		servicehttp.JSONResponse(w, 200, map[string]bool{"queued": true})
		return nil
	}))
	mux.HandleFunc("POST /v1/files/{id}/reconcile", servicehttp.Wrap(func(w http.ResponseWriter, r *http.Request) error {
		var in struct {
			TrackID  string `json:"track_id"`
			Evidence string `json:"evidence"`
		}
		if e := servicehttp.Decode(w, r, &in); e != nil {
			return e
		}
		if !SafeName.MatchString(in.TrackID) || in.Evidence == "" {
			return errors.New("verified track id and evidence required")
		}
		tag, e := s.Store.DB.Exec(r.Context(), "UPDATE knowledge_files SET track_id=$2,status='external_pending',receipt=$3,error='',updated_at=now() WHERE id=$1 AND status='unknown'", r.PathValue("id"), in.TrackID, JSON(in))
		if e != nil {
			return e
		}
		if tag.RowsAffected() != 1 {
			return errors.New("unknown import not found")
		}
		servicehttp.JSONResponse(w, 200, map[string]bool{"tracking": true})
		return nil
	}))
}

type ResourceReader interface {
	Resource(context.Context, string, Ref, any) error
}
type SecretReader interface {
	Secret(context.Context, string) (string, error)
}
type RunReader interface {
	Run(context.Context, string) (Run, error)
}
type Store struct {
	DB *pgxpool.Pool
	ResourceReader
	SecretReader
	RunReader
	CaptureReader
}
type Server struct {
	Store *Store
	HTTP  servicehttp.Config
}

func (s *Server) Handler() http.Handler { return servicehttp.New(s.HTTP, s.Store.DB.Ping, s.Routes) }
