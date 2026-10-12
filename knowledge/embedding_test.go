package knowledge

import (
	"context"
	. "github.com/euler-gaozl/agent-os-sdk/contracts"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestEmbeddingRejectsInvalidCosineInputs(t *testing.T) {
	for _, body := range []string{
		`{"data":[{"index":0,"embedding":[null,1]}]}`,
		`{"data":[{"index":0,"embedding":[0,0]}]}`,
		`{"data":[{"index":0,"embedding":[1e50,1]}]}`,
		`{"data":[{"index":0,"embedding":[1e-50,1e-50]}]}`,
	} {
		t.Run(body, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(body))
			}))
			defer server.Close()
			s := Store{SecretReader: liveKBResources{}}
			_, e := s.Embed(context.Background(), KnowledgeBase{Dimensions: 2, Embedding: ModelConfig{BaseURL: server.URL, Model: "fixture"}}, []string{"test"}, false)
			if e == nil {
				t.Fatal("invalid vector would enter a float32 COSINE index")
			}
		})
	}
}
