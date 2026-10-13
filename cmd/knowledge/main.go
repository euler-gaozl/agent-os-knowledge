package main

import (
	"context"
	"encoding/json"
	"github.com/euler-gaozl/agent-os-knowledge/knowledge"
	"github.com/euler-gaozl/agent-os-sdk/servicehost"
	"github.com/euler-gaozl/agent-os-sdk/servicehttp"
	"github.com/jackc/pgx/v5/pgxpool"
	"net/http"
	"os"
)

func main() {
	if len(os.Args) == 2 && os.Args[1] == "--source-archive-profile" {
		_ = json.NewEncoder(os.Stdout).Encode(knowledge.ArchiveProfile())
		return
	}
	servicehost.Main(servicehost.Options{Role: "knowledge", Port: "8095", Database: true, Concurrency: 4, Authorize: knowledge.Authorize, Readiness: knowledge.Readiness}, func(ctx context.Context, p *pgxpool.Pool, c servicehttp.Config) (http.Handler, func(context.Context) error, error) {
		if err := knowledge.CheckArchiveCompatibility(ctx, p); err != nil {
			return nil, nil, err
		}
		if err := applyLocalDefaults(); err != nil {
			return nil, nil, err
		}
		client := servicehost.Client()
		store := &knowledge.Store{DB: p, ResourceReader: client, SecretReader: client, RunReader: client, CaptureReader: knowledge.CaptureHTTPReader{BaseURL: servicehost.Env("SOURCES_URL", "http://127.0.0.1:8101"), Token: client.Token}}
		return (&knowledge.Server{Store: store, HTTP: c}).Handler(), nil, nil
	})
}
