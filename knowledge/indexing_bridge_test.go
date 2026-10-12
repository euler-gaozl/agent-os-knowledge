package knowledge

import (
	"context"
	"github.com/euler-gaozl/agent-os-knowledge/indexing"
)

func (s *Store) IndexFile(ctx context.Context, id string) error {
	return indexing.New(s.DB, s.SecretReader).ProcessFile(ctx, id)
}
func (s *Store) IndexNamesBatch(ctx context.Context) (int, error) {
	return indexing.New(s.DB, s.SecretReader).BackfillNames(ctx)
}
func (s *Store) IndexLocalBatch(ctx context.Context) (int, error) {
	return indexing.New(s.DB, s.SecretReader).BackfillChunks(ctx)
}
