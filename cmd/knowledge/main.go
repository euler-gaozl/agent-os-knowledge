package main

import (
	"context"
	"github.com/euler-gaozl/agent-os-knowledge/knowledge"
	"github.com/euler-gaozl/agent-os-sdk/servicehost"
	"github.com/euler-gaozl/agent-os-sdk/servicehttp"
	"github.com/jackc/pgx/v5/pgxpool"
	"net/http"
)

func main() {
	servicehost.Main(servicehost.Options{Role: "knowledge", Port: "8095", Database: true, Concurrency: 4, Authorize: knowledge.Authorize, Readiness: knowledge.Readiness}, func(ctx context.Context, p *pgxpool.Pool, c servicehttp.Config) (http.Handler, func(context.Context) error, error) {
		if err := applyLocalDefaults(); err != nil {
			return nil, nil, err
		}
		client := servicehost.Client()
		store := &knowledge.Store{DB: p, ResourceReader: client, SecretReader: client, RunReader: client}
		return (&knowledge.Server{Store: store, HTTP: c}).Handler(), nil, nil
	})
}
