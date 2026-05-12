// Package server builds the api binary's HTTP listener. New is a thin
// builder: route registration lives in the api package
// (api.RegisterRoutes), and the only behavior here is binding the
// configured address and wrapping the supplied handler with otelhttp.
package server

import (
	"net/http"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/tarkandikmen/notifications/internal/config"
)

// New wraps handler in otelhttp and returns an *http.Server bound to
// cfg.HTTPAddr. The api package owns route registration via
// api.RegisterRoutes; this function is intentionally trivial so the
// lifecycle skeleton stays small as the API surface grows.
func New(cfg *config.Config, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: otelhttp.NewHandler(handler, "api"),
	}
}
