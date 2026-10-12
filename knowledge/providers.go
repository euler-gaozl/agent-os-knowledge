package knowledge

import (
	"context"
	"github.com/euler-gaozl/agent-os-knowledge/internal/files"
	"github.com/euler-gaozl/agent-os-knowledge/internal/provider"
	. "github.com/euler-gaozl/agent-os-sdk/contracts"
)

func ProviderJSON(ctx context.Context, method, endpoint, key, header string, body any) ([]byte, error) {
	return provider.JSONRequest(ctx, method, endpoint, key, header, body)
}
func (s *Store) Embed(ctx context.Context, kb KnowledgeBase, text []string, query bool) ([][]float64, error) {
	return (provider.Client{SecretReader: s.SecretReader}).Embed(ctx, kb, text, query)
}
func (s *Store) milvus(ctx context.Context, kb KnowledgeBase, path string, body any) ([]byte, error) {
	return (provider.Client{SecretReader: s.SecretReader}).Milvus(ctx, kb, path, body)
}
func collection(ref Ref) string { return provider.Collection(ref) }
func (s *Store) KnowledgeFile(ctx context.Context, id string) (KnowledgeFile, error) {
	return files.Read(ctx, s.DB, id)
}
