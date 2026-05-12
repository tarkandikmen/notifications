// Package metricsserver builds the tiny *http.Server every binary
// runs on cfg.MetricsAddr (default :9090) to serve /metrics and
// /healthz from a shared *prometheus.Registry.
//
// Phase 5 keeps the server intentionally minimal: stdlib net/http,
// no middleware, no graceful per-request timeout. The
// caller (each binary's cmd.go) owns lifecycle: launching the
// listener in a goroutine and calling Shutdown on the returned
// *http.Server during graceful shutdown.
//
// The api binary keeps Phase 1's /metrics route on :8080 (which the
// Phase-1 acceptance test 5 still pokes at) and additionally runs
// this server on :9090 so every binary exposes a uniform
// per-binary endpoint Prometheus can scrape — both routes share the
// same metrics.Registry().
//
// docs/phases/05-observability.md §2.
package metricsserver

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// New builds an *http.Server that serves /metrics from reg and
// /healthz via the supplied handler. When healthz is nil the server
// installs a default 200 handler that writes the Phase-1
// byte-exact body `{"status":"ok"}` so non-api binaries (whose
// /healthz is "binary process is up" only — deeper probes are
// Phase 7 polish) match the api binary's 200 path byte-for-byte.
//
// The returned *http.Server is not started; the caller launches it
// in a goroutine and calls Shutdown on graceful shutdown.
//
// docs/phases/05-observability.md §2 locks the surface.
func New(addr string, reg *prometheus.Registry, healthz http.HandlerFunc) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	if healthz == nil {
		healthz = defaultHealthz
	}
	mux.HandleFunc("GET /healthz", healthz)

	return &http.Server{
		Addr:    addr,
		Handler: mux,
	}
}

// defaultHealthz writes the Phase 1 byte-exact 200 body. Mirrors
// internal/api/handlers.go handleHealthz exactly so the non-api
// binaries' /healthz endpoints respond identically to the api
// binary's healthy path.
//
// json.Encode is intentionally avoided so the body has no trailing
// newline.
func defaultHealthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}
