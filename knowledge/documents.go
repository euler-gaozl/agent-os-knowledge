package knowledge

import (
	"context"
	"encoding/json"
	"errors"
	. "github.com/euler-gaozl/agent-os-sdk/contracts"
	"github.com/euler-gaozl/agent-os-sdk/servicehttp"
	"net/http"
)

func (s *Store) ReadDocument(ctx context.Context, run Run, id string, offset, limit int) (any, error) {
	if offset < 0 || limit < 0 || limit > 1200 {
		return nil, errors.New("invalid document window")
	}
	if limit == 0 {
		limit = 1200
	}
	var doc Document
	var ref Ref
	var metadata []byte
	e := s.DB.QueryRow(ctx, `SELECT d.id,d.kind,d.title,d.content,d.source,d.metadata,f.kb_id,f.kb_revision FROM documents d JOIN knowledge_files f ON f.document_id=d.id WHERE d.id=$1`, id).Scan(&doc.ID, &doc.Kind, &doc.Title, &doc.Content, &doc.Source, &metadata, &ref.ID, &ref.Revision)
	if e != nil {
		return nil, e
	}
	allowed := false
	for _, bound := range run.Snapshot.Agent.Knowledge {
		if bound == ref {
			allowed = true
			break
		}
	}
	if !allowed {
		return nil, errors.New("document knowledge revision not bound to run")
	}
	if e = json.Unmarshal(metadata, &doc.Metadata); e != nil {
		return nil, e
	}
	chars := []rune(doc.Content)
	if offset > len(chars) {
		return nil, errors.New("offset exceeds document length")
	}
	end := min(offset+limit, len(chars))
	doc.Content = string(chars[offset:end])
	return map[string]any{"document": doc, "next_offset": end, "has_more": end < len(chars), "total_characters": len(chars)}, nil
}
func (s *Server) documentRoutes(m *http.ServeMux) {
	m.HandleFunc("POST /v1/document", servicehttp.Wrap(func(w http.ResponseWriter, r *http.Request) error {
		var in struct {
			RunID  string `json:"run_id"`
			ID     string `json:"id"`
			Offset int    `json:"offset"`
			Limit  int    `json:"limit"`
		}
		if e := servicehttp.Decode(w, r, &in); e != nil {
			return e
		}
		run, e := s.Store.Run(r.Context(), in.RunID)
		if e != nil {
			return e
		}
		result, e := s.Store.ReadDocument(r.Context(), run, in.ID, in.Offset, in.Limit)
		if e != nil {
			return e
		}
		servicehttp.JSONResponse(w, 200, result)
		return nil
	}))
	m.HandleFunc("GET /v1/artifacts/{hash}", servicehttp.Wrap(func(w http.ResponseWriter, r *http.Request) error {
		var body []byte
		e := s.Store.DB.QueryRow(r.Context(), "SELECT body FROM artifacts WHERE hash=$1", r.PathValue("hash")).Scan(&body)
		if e != nil {
			return e
		}
		if Hash(body) != r.PathValue("hash") {
			return errors.New("artifact hash mismatch")
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Disposition", "attachment")
		_, e = w.Write(body)
		return e
	}))
}
