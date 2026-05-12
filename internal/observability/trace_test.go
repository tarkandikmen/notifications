package observability_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/tarkandikmen/notifications/internal/observability"
)

func setupPropagator(t *testing.T) {
	t.Helper()
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
}

func TestRoundTrip_HeadersFromContext_ToContext(t *testing.T) {
	setupPropagator(t)
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(exp)))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	tr := tp.Tracer("test")

	ctx := context.Background()
	ctx, sp := tr.Start(ctx, "parent")
	headers, err := observability.TraceHeadersFromContext(ctx)
	sp.End()
	require.NoError(t, err)
	require.NotNil(t, headers)

	childCtx := observability.ContextFromTraceHeaders(context.Background(), headers)
	_, child := tr.Start(childCtx, "child")
	child.End()

	stubs := exp.GetSpans()
	require.Len(t, stubs, 2)
	require.Equal(t, "child", stubs[1].Name)
	require.True(t, stubs[1].Parent.SpanID().IsValid())
	require.Equal(t, stubs[0].SpanContext.SpanID(), stubs[1].Parent.SpanID())
}

func TestNoActiveSpan_ReturnsNil(t *testing.T) {
	setupPropagator(t)
	headers, err := observability.TraceHeadersFromContext(context.Background())
	require.NoError(t, err)
	require.Nil(t, headers)
}

func TestEmptyHeaders_ReturnsParent(t *testing.T) {
	setupPropagator(t)
	parent := context.Background()
	ctx := observability.ContextFromTraceHeaders(parent, nil)
	require.Equal(t, parent, ctx)

	ctx2 := observability.ContextFromTraceHeaders(parent, json.RawMessage(`{}`))
	sc := trace.SpanContextFromContext(ctx2)
	require.False(t, sc.IsValid())
}

func TestMalformedHeaders_ReturnsParent(t *testing.T) {
	setupPropagator(t)
	parent := context.Background()
	ctx := observability.ContextFromTraceHeaders(parent, json.RawMessage(`not-json`))
	require.Equal(t, parent, ctx)
}

func TestKafkaHeaders_RoundTrip(t *testing.T) {
	setupPropagator(t)
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(exp)))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	tr := tp.Tracer("test")

	ctx := context.Background()
	ctx, sp := tr.Start(ctx, "kafka-producer")
	raw, err := observability.TraceHeadersFromContext(ctx)
	sp.End()
	require.NoError(t, err)
	require.NotNil(t, raw)

	kh := observability.KafkaHeadersFromOutboxHeaders(raw)
	require.NotEmpty(t, kh)

	consumerCtx := observability.ContextFromKafkaHeaders(context.Background(), kh)
	_, cons := tr.Start(consumerCtx, "consumer")
	cons.End()

	stubs := exp.GetSpans()
	require.Len(t, stubs, 2)
	require.Equal(t, "consumer", stubs[1].Name)
	require.Equal(t, stubs[0].SpanContext.SpanID(), stubs[1].Parent.SpanID())

	var found bool
	for _, h := range kh {
		if h.Key == "traceparent" {
			found = true
			break
		}
	}
	require.True(t, found)

	ctx2 := observability.ContextFromKafkaHeaders(context.Background(), []kgo.RecordHeader{
		{Key: "traceparent", Value: []byte("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")},
	})
	ctx2, sp2 := tr.Start(ctx2, "with-extracted")
	sp2.End()
	require.True(t, trace.SpanContextFromContext(ctx2).IsValid())
}

func TestSetSpanNotificationID_RecordingSpan(t *testing.T) {
	setupPropagator(t)
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(exp)))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	tr := tp.Tracer("test")

	id := uuid.MustParse("11110000-0000-7000-8000-000000000001")
	ctx, sp := tr.Start(context.Background(), "http")
	observability.SetSpanNotificationID(ctx, id)
	sp.End()

	stubs := exp.GetSpans()
	require.Len(t, stubs, 1)
	var got string
	for _, kv := range stubs[0].Attributes {
		if string(kv.Key) == "notification.id" {
			got = kv.Value.AsString()
			break
		}
	}
	require.Equal(t, id.String(), got)
}

func TestSetSpanBatchID_RecordingSpan(t *testing.T) {
	setupPropagator(t)
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(exp)))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	tr := tp.Tracer("test")

	bid := uuid.MustParse("22220000-0000-7000-8000-000000000002")
	ctx, sp := tr.Start(context.Background(), "http")
	observability.SetSpanBatchID(ctx, bid)
	sp.End()

	stubs := exp.GetSpans()
	require.Len(t, stubs, 1)
	var got string
	for _, kv := range stubs[0].Attributes {
		if string(kv.Key) == "batch.id" {
			got = kv.Value.AsString()
			break
		}
	}
	require.Equal(t, bid.String(), got)
}

func TestSetSpanNotificationID_NoSpan_NoPanic(t *testing.T) {
	setupPropagator(t)
	observability.SetSpanNotificationID(context.Background(), uuid.MustParse("33330000-0000-7000-8000-000000000003"))
}
