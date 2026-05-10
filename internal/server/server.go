// Package server builds the api binary's HTTP listener. Phase 1 exposes
// GET /healthz and GET /metrics; Phase 2+ adds /v1/notifications endpoints.
package server

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/tarkandikmen/notifications/internal/config"
)

// New builds the api binary's *http.Server. The mux is wrapped in
// otelhttp.NewHandler so every request becomes a span.
//
// docs/phases/01-foundation.md §6.
func New(cfg *config.Config, reg *prometheus.Registry) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", healthz)
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

	return &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: otelhttp.NewHandler(mux, "api"),
	}
}

// healthz is the Phase 1 minimum per docs/design/03-api.md §`GET /healthz`:
// process-up only, no dependency checks. The body is written as a raw byte
// slice rather than via json.Encode to avoid the trailing newline that
// breaks acceptance test 5's exact-byte match.
func healthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}
