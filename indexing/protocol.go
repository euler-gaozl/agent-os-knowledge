// Package indexing is Knowledge's versioned indexing transaction protocol.
// It contains no upload/retrieval/admin HTTP API, migration, or scheduler.
package indexing

import (
	"context"
	"errors"
	"github.com/euler-gaozl/agent-os-knowledge/internal/provider"
	"github.com/jackc/pgx/v5/pgxpool"
)

const Protocol = "knowledge-indexing-v1"

type SecretReader interface {
	Secret(context.Context, string) (string, error)
}

// Processor never exposes its database or individual transaction steps.
// ProcessFile holds the file advisory lock through the external request and
// receipt transition. Callers must not release/reacquire that lock themselves.
type Processor struct {
	db        *pgxpool.Pool
	providers provider.Client
}

func New(db *pgxpool.Pool, secrets SecretReader) *Processor {
	return &Processor{db: db, providers: provider.Client{SecretReader: secrets}}
}

func Readiness(ctx context.Context, db *pgxpool.Pool) error {
	if db == nil {
		return errors.New("database required")
	}
	_, err := db.Exec(ctx, "SELECT f.status,f.config,f.track_id,f.receipt,f.name_vector,c.search_vector,d.content FROM knowledge_files f,knowledge_chunks c,documents d WHERE false")
	return err
}

// PendingFiles returns at most ten IDs. It does not claim them: ProcessFile
// must obtain the session advisory lock and re-read the authoritative state.
func (s *Processor) PendingFiles(ctx context.Context) ([]string, error) {
	rows, err := s.db.Query(ctx, "SELECT id FROM knowledge_files WHERE status IN('queued','running','external_pending') ORDER BY updated_at LIMIT 10")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
