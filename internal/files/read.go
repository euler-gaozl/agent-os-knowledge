package files

import (
	"context"
	"encoding/json"
	. "github.com/euler-gaozl/agent-os-sdk/contracts"
	"github.com/jackc/pgx/v5"
)

type Reader interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func Read(ctx context.Context, db Reader, id string) (KnowledgeFile, error) {
	var f KnowledgeFile
	var cfg []byte
	e := db.QueryRow(ctx, "SELECT id,kb_id,kb_revision,document_id,artifact_hash,name,status,error,track_id,config FROM knowledge_files WHERE id=$1", id).Scan(&f.ID, &f.KBID, &f.Revision, &f.DocumentID, &f.Hash, &f.Name, &f.Status, &f.Error, &f.TrackID, &cfg)
	if e == nil {
		e = json.Unmarshal(cfg, &f.Config)
	}
	return f, e
}
