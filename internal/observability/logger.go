// Package observability builds the binary's logger, Prometheus registry, and
// OpenTelemetry tracer. The plumbing exists in Phase 1; later phases add the
// pipeline-specific metrics and spans without touching this skeleton.
package observability

import (
	"log/slog"
	"os"
)

// NewLogger returns a JSON-handler-backed *slog.Logger at the requested
// level. Callers install it as the process-wide default via slog.SetDefault.
//
// docs/phases/01-foundation.md §7.
func NewLogger(level slog.Level) *slog.Logger {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	return slog.New(handler)
}
