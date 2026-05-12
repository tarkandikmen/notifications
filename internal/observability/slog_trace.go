package observability

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
)

// NewTraceHandler returns a slog.Handler that delegates record
// formatting to next, but adds trace_id and span_id attributes to
// every record emitted under a context with an active span.
//
// The handler is intentionally allocation-free on the no-span path
// (the IsValid early-return skips AddAttrs entirely) so wrapping
// every binary's logger has no measurable cost on background polling
// loops that emit logs without an active span.
//
// Implements slog.Handler:
//   - Enabled: forwards to next.
//   - Handle(ctx, r): if a span is active in ctx, adds trace_id +
//     span_id attrs to r before forwarding to next.
//   - WithAttrs / WithGroup: forward to next, wrap the result in
//     another traceHandler so the attribute / group propagates
//     without losing the trace-context behavior.
//
// docs/phases/00-phases.md §Cross-cutting decisions: trace_id is the
// correlation ID required by the assessment brief's "Structured
// logging with correlation IDs" line. This handler is the
// implementation; docs/phases/05-observability.md §5 locks the surface.
func NewTraceHandler(next slog.Handler) slog.Handler {
	return &traceHandler{next: next}
}

type traceHandler struct {
	next slog.Handler
}

func (h *traceHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

func (h *traceHandler) Handle(ctx context.Context, r slog.Record) error {
	sc := trace.SpanContextFromContext(ctx)
	if sc.IsValid() {
		r.AddAttrs(
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
	}
	return h.next.Handle(ctx, r)
}

func (h *traceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &traceHandler{next: h.next.WithAttrs(attrs)}
}

func (h *traceHandler) WithGroup(name string) slog.Handler {
	return &traceHandler{next: h.next.WithGroup(name)}
}
