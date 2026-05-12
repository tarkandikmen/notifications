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
// Phase 5 wraps the JSON handler in NewTraceHandler so every record
// emitted under an active span context automatically carries trace_id
// and span_id attributes — the assessment brief's "Structured logging
// with correlation IDs" line. Call sites that want the correlation
// must use the Context-bearing slog variants (InfoContext, LogAttrs,
// etc.); call sites that pass no context emit the same JSON shape as
// before.
//
// Signature unchanged from Phase 1 (docs/phases/01-foundation.md §7);
// docs/phases/05-observability.md §5 layers the trace-aware wrap.
func NewLogger(level slog.Level) *slog.Logger {
	base := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level})
	return slog.New(NewTraceHandler(base))
}
