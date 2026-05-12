package observability

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// Init bootstraps OpenTelemetry tracing. When endpoint is empty the SDK
// writes spans to stdout (Phase 1 default); when set, spans are exported via
// OTLP/gRPC. Production docker-compose sets OTEL_EXPORTER_OTLP_ENDPOINT to
// jaeger:4317 (Jaeger all-in-one OTLP receiver) per
// docs/phases/05-observability.md §9–§10. The W3C trace-context propagator is installed globally so
// outgoing HTTP, Kafka, and DB spans link cleanly to incoming traces.
//
// The returned shutdown function flushes the exporter and must be deferred
// by the caller.
//
// docs/phases/01-foundation.md §7. otel_sampling_rate locked at 1.0 in
// docs/design/07-constants.md §I.
func Init(ctx context.Context, serviceName string, endpoint string) (func(context.Context) error, error) {
	exporter, err := newExporter(ctx, endpoint)
	if err != nil {
		return nil, fmt.Errorf("observability: build exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName(serviceName)),
	)
	if err != nil {
		return nil, fmt.Errorf("observability: build resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithSampler(sdktrace.TraceIDRatioBased(1.0)),
		sdktrace.WithResource(res),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return tp.Shutdown, nil
}

func newExporter(ctx context.Context, endpoint string) (sdktrace.SpanExporter, error) {
	if endpoint == "" {
		return stdouttrace.New(stdouttrace.WithPrettyPrint())
	}
	return otlptrace.New(ctx, otlptracegrpc.NewClient(
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
	))
}
