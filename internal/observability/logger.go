// Package observability builds the binary's logger, Prometheus registry, and
// OpenTelemetry tracer.
package observability

import (
	"log/slog"
	"os"
)

// NewLogger returns a JSON-handler-backed *slog.Logger at the requested
// level. Callers install it as the process-wide default via slog.SetDefault.
//
// The JSON handler is wrapped in NewTraceHandler so every record emitted
// under an active span context automatically carries trace_id and span_id
// attributes — the structured-logging correlation surface. Call sites that
// want the correlation must use the Context-bearing slog variants
// (InfoContext, LogAttrs, etc.); call sites that pass no context emit the
// same JSON shape as before.
func NewLogger(level slog.Level) *slog.Logger {
	base := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	return slog.New(NewTraceHandler(base))
}
