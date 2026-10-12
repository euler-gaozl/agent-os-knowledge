package indexing

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/euler-gaozl/agent-os-knowledge/internal/files"
	"github.com/euler-gaozl/agent-os-knowledge/internal/provider"
	. "github.com/euler-gaozl/agent-os-sdk/contracts"
	"net/url"
	"path/filepath"
	"strings"
	"time"
)

func (s *Processor) ProcessFile(ctx context.Context, id string) error {
	conn, e := s.db.Acquire(ctx)
	if e != nil {
		return e
	}
	defer conn.Release()
	var locked bool
	if e = conn.QueryRow(ctx, "SELECT pg_try_advisory_lock(hashtextextended($1,42))", id).Scan(&locked); e != nil {
		return e
	}
	if !locked {
		return nil
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if _, err := conn.Exec(cleanup, "SELECT pg_advisory_unlock(hashtextextended($1,42))", id); err != nil {
			// A pooled session must never retain this file's advisory lock.
			_ = conn.Conn().Close(cleanup)
		}
	}()
	// Move attempted work behind other queued files even if its provider stays unavailable.
	if _, e = conn.Exec(ctx, "UPDATE knowledge_files SET updated_at=now() WHERE id=$1", id); e != nil {
		return e
	}
	f, e := files.Read(ctx, s.db, id)
	if e != nil {
		return e
	}
	if !Has([]string{"queued", "running", "external_pending"}, f.Status) {
		return nil
	}
	if f.Status == "running" && f.Config.Engine == "lightrag" {
		_, e = s.db.Exec(ctx, "UPDATE knowledge_files SET status='unknown',error='Upload interrupted before receipt; verify LightRAG track id.',updated_at=now() WHERE id=$1", id)
		return e
	}
	if f.Status == "external_pending" {
		key, e := s.providers.Secret(ctx, f.Config.KeyEnv)
		if e != nil {
			return e
		}
		b, e := provider.JSONRequest(ctx, "GET", strings.TrimRight(f.Config.Endpoint, "/")+"/documents/track_status/"+url.PathEscape(f.TrackID), key, "X-API-Key", nil)
		if e != nil {
			return e
		}
		var tracked struct {
			Documents []struct {
				Status string `json:"status"`
			} `json:"documents"`
		}
		if e = json.Unmarshal(b, &tracked); e != nil {
			return e
		}
		status := "ready"
		if len(tracked.Documents) == 0 {
			status = "external_pending"
		}
		for _, d := range tracked.Documents {
			if d.Status == "failed" {
				status = "failed"
				break
			}
			if d.Status != "processed" {
				status = "external_pending"
			}
		}
		_, e = s.db.Exec(ctx, "UPDATE knowledge_files SET status=$2,receipt=$3,updated_at=now() WHERE id=$1", id, status, b)
		return e
	}
	if _, e = s.db.Exec(ctx, "UPDATE knowledge_files SET status='running',updated_at=now() WHERE id=$1", id); e != nil {
		return e
	}
	finish := func(status string, receipt []byte, err error) error {
		msg := ""
		if err != nil {
			msg = err.Error()
		}
		if receipt == nil {
			receipt = JSON(nil)
		}
		_, e := s.db.Exec(ctx, "UPDATE knowledge_files SET status=$2,receipt=$3,error=$4,updated_at=now() WHERE id=$1", id, status, receipt, msg)
		return e
	}
	if f.Config.Engine == "lightrag" {
		key, e := s.providers.Secret(ctx, f.Config.KeyEnv)
		if e != nil {
			return finish("failed", nil, e)
		}
		var text string
		if e = s.db.QueryRow(ctx, "SELECT content FROM documents WHERE id=$1", f.DocumentID).Scan(&text); e != nil {
			return e
		}
		b, e := provider.JSONRequest(ctx, "POST", strings.TrimRight(f.Config.Endpoint, "/")+"/documents/text", key, "X-API-Key", map[string]string{"text": text, "file_source": f.ID + "-" + filepath.Base(f.Name)})
		if e != nil {
			return finish("unknown", nil, e)
		}
		var receipt struct {
			TrackID string `json:"track_id"`
		}
		if e = json.Unmarshal(b, &receipt); e != nil || receipt.TrackID == "" {
			return finish("unknown", b, errors.New("LightRAG accepted response lacks track id"))
		}
		_, e = s.db.Exec(ctx, "UPDATE knowledge_files SET status='external_pending',track_id=$2,receipt=$3,updated_at=now() WHERE id=$1", id, receipt.TrackID, b)
		return e
	}
	ref := Ref{ID: f.KBID, Revision: f.Revision}
	name := provider.Collection(ref)
	b, e := s.providers.Milvus(ctx, f.Config, "collections/has", map[string]any{"collectionName": name})
	if e != nil {
		return finish("failed", nil, e)
	}
	var has struct {
		Data struct {
			Has bool `json:"has"`
		} `json:"data"`
	}
	if e = json.Unmarshal(b, &has); e != nil {
		return finish("failed", nil, e)
	}
	if !has.Data.Has {
		_, e = s.providers.Milvus(ctx, f.Config, "collections/create", map[string]any{"collectionName": name, "dimension": f.Config.Dimensions, "metricType": "COSINE", "idType": "VarChar", "primaryFieldName": "id", "vectorFieldName": "vector", "autoId": false, "params": map[string]any{"max_length": 64, "enableDynamicField": true}})
		if e != nil {
			return finish("failed", nil, e)
		}
	}
	rows, e := s.db.Query(ctx, "SELECT id,content FROM knowledge_chunks WHERE file_id=$1 ORDER BY ordinal", id)
	if e != nil {
		return e
	}
	ids, texts := []string{}, []string{}
	for rows.Next() {
		var chunk, text string
		if e = rows.Scan(&chunk, &text); e != nil {
			rows.Close()
			return e
		}
		ids = append(ids, chunk)
		texts = append(texts, text)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	for start := 0; start < len(texts); start += 16 {
		end := min(start+16, len(texts))
		vectors, e := s.providers.Embed(ctx, f.Config, texts[start:end], false)
		if e != nil {
			return finish("failed", nil, e)
		}
		data := []any{}
		for i, v := range vectors {
			data = append(data, map[string]any{"id": ids[start+i], "chunk_id": ids[start+i], "vector": v})
		}
		b, e = s.providers.Milvus(ctx, f.Config, "entities/upsert", map[string]any{"collectionName": name, "data": data})
		if e != nil {
			return finish("failed", nil, e)
		}
	}
	return finish("ready", b, nil)
}
