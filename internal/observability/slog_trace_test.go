package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// TestHandle_AddsTraceID_AndSpanID_UnderActiveSpan asserts a record
// emitted under a span context renders with trace_id + span_id
// attributes. Round-trips through a JSON handler so the assertion
// reads the wire shape rather than poking at slog internals.
func TestHandle_AddsTraceID_AndSpanID_UnderActiveSpan(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := slog.New(NewTraceHandler(slog.NewJSONHandler(buf, nil)))

	ctx, span := newTestTracerProvider().Tracer("test").Start(context.Background(), "test.span")
	defer span.End()

	logger.InfoContext(ctx, "msg with span")

	rec := decodeRecord(t, buf.Bytes())

	traceID, ok := rec["trace_id"].(string)
	require.True(t, ok, "trace_id must be present and a string")
	assert.Len(t, traceID, 32, "trace_id is a 32-hex-char W3C trace ID")
	assert.Equal(t, span.SpanContext().TraceID().String(), traceID)

	spanID, ok := rec["span_id"].(string)
	require.True(t, ok, "span_id must be present and a string")
	assert.Len(t, spanID, 16, "span_id is a 16-hex-char W3C span ID")
	assert.Equal(t, span.SpanContext().SpanID().String(), spanID)
}

// TestHandle_NoSpan_NoExtraAttrs asserts a record without an active
// span context renders the same as the underlying handler — no
// trace_id / span_id attribute is added. Guards the "no span = no
// extra cost" invariant from §5.
func TestHandle_NoSpan_NoExtraAttrs(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := slog.New(NewTraceHandler(slog.NewJSONHandler(buf, nil)))

	logger.InfoContext(context.Background(), "msg without span")

	rec := decodeRecord(t, buf.Bytes())
	_, hasTrace := rec["trace_id"]
	_, hasSpan := rec["span_id"]
	assert.False(t, hasTrace, "trace_id must be absent without an active span")
	assert.False(t, hasSpan, "span_id must be absent without an active span")
}

// TestWithAttrs_PreservesTraceContext asserts the wrapper-on-WithAttrs
// pattern: a logger.With("k","v").InfoContext(ctx,"msg") record still
// carries trace_id from the active span. Without the wrap, the
// returned handler would be the underlying JSON handler with attrs
// attached and the span context attribute would be lost.
func TestWithAttrs_PreservesTraceContext(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := slog.New(NewTraceHandler(slog.NewJSONHandler(buf, nil)))

	ctx, span := newTestTracerProvider().Tracer("test").Start(context.Background(), "test.span")
	defer span.End()

	logger.With("custom_attr", "custom_value").InfoContext(ctx, "msg with attr")

	rec := decodeRecord(t, buf.Bytes())
	assert.Equal(t, "custom_value", rec["custom_attr"], "WithAttrs attribute must round-trip")

	traceID, ok := rec["trace_id"].(string)
	require.True(t, ok, "trace_id must still be present after WithAttrs")
	assert.Equal(t, span.SpanContext().TraceID().String(), traceID)
}

// TestWithGroup_PreservesTraceContext asserts the wrapper-on-WithGroup
// pattern: a logger.WithGroup("g").InfoContext(ctx,"msg",...) record
// still carries trace_id. Per slog's WithGroup semantics every
// subsequent attribute on the record (including the trace_id added
// by traceHandler.Handle) is nested under the group, so the JSON
// shape carries trace_id inside the group rather than at the top
// level. The wrap-on-WithGroup contract holds either way: the
// trace context attribute is preserved (not silently dropped).
func TestWithGroup_PreservesTraceContext(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := slog.New(NewTraceHandler(slog.NewJSONHandler(buf, nil)))

	ctx, span := newTestTracerProvider().Tracer("test").Start(context.Background(), "test.span")
	defer span.End()

	logger.WithGroup("inner").InfoContext(ctx, "msg with group", "k", "v")

	rec := decodeRecord(t, buf.Bytes())

	inner, ok := rec["inner"].(map[string]any)
	require.True(t, ok, "inner group must be a JSON object")

	traceID, ok := inner["trace_id"].(string)
	require.True(t, ok, "trace_id must be present inside the WithGroup-nested attributes")
	assert.Equal(t, span.SpanContext().TraceID().String(), traceID)

	// The user-supplied k=v also lands inside the group.
	assert.Equal(t, "v", inner["k"], "WithGroup-attached attribute must round-trip inside the group")
}

// newTestTracerProvider builds an in-memory tracer provider so the
// tests don't depend on the global default state.
func newTestTracerProvider() *trace.TracerProvider {
	exp := tracetest.NewInMemoryExporter()
	return trace.NewTracerProvider(trace.WithSyncer(exp))
}

// decodeRecord parses the JSON-handler's last emitted line into a
// map[string]any so the test can assert on individual attributes
// without depending on field ordering.
func decodeRecord(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	line := strings.TrimSpace(string(raw))
	require.NotEmpty(t, line, "expected at least one log record")

	var rec map[string]any
	require.NoError(t, json.Unmarshal([]byte(line), &rec), "log record must be valid JSON")
	return rec
}
