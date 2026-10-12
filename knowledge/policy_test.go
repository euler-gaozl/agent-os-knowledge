package knowledge

import (
	"github.com/euler-gaozl/agent-os-sdk/servicehttp"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPeerCannotCrossUploadAndRunReadBoundary(t *testing.T) {
	tokens := map[string]string{"control": strings.Repeat("c", 40), "runtime": strings.Repeat("r", 40), "futures": strings.Repeat("f", 40), "indexer": strings.Repeat("i", 40)}
	peers := map[string]string{}
	for k, v := range tokens {
		peers[k] = servicehttp.TokenHash(v)
	}
	handler := servicehttp.New(servicehttp.Config{Role: "knowledge", Peers: peers, Authorize: Authorize}, nil, func(m *http.ServeMux) {
		m.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })
	})
	for _, tc := range []struct {
		role, method, path string
		want               int
	}{
		{"control", "POST", "/v1/upload", 204}, {"control", "POST", "/v1/document", 403}, {"runtime", "POST", "/v1/document", 204},
		{"runtime", "POST", "/v1/upload", 403}, {"futures", "GET", "/v1/status", 204}, {"futures", "GET", "/v1/artifacts/hash", 403},
		{"indexer", "GET", "/v1/artifacts/hash", 403}, {"indexer", "POST", "/v1/upload", 403}, {"", "GET", "/v1/status", 401},
	} {
		req := httptest.NewRequest(tc.method, tc.path, nil)
		req.Header.Set("Authorization", "Bearer "+tokens[tc.role])
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		if w.Code != tc.want {
			t.Fatalf("%+v got %d", tc, w.Code)
		}
	}
}
