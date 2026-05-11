// Package server builds the api binary's HTTP listener. Phase 2 makes
// New a thin builder: route registration moves to the api package
// (api.RegisterRoutes), and the only behavior left here is binding the
// configured address and wrapping the supplied handler with otelhttp.
//
// docs/phases/02-walking-skeleton.md §6.
package server

import (
	"net/http"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/tarkandikmen/notifications/internal/config"
)

// New wraps handler in otelhttp and returns an *http.Server bound to
// cfg.HTTPAddr. The api package owns route registration via
// api.RegisterRoutes; this function is intentionally trivial so the
// chunked Phase-2 plan can keep the lifecycle skeleton from Phase 1
// while the API surface grows.
func New(cfg *config.Config, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: otelhttp.NewHandler(handler, "api"),
	}
}
