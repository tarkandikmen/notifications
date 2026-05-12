package observability

import (
	"context"
	"encoding/json"
	"sort"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// mapCarrier adapts map[string]string to propagation.TextMapCarrier.
type mapCarrier map[string]string

func (m mapCarrier) Get(key string) string {
	return m[key]
}

func (m mapCarrier) Set(key, value string) {
	m[key] = value
}

func (m mapCarrier) Keys() []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// TraceHeadersFromContext serializes the W3C Trace Context (traceparent
// + optional tracestate) from ctx's active span into a JSON object
// suitable for the outbox.headers JSONB column.
//
// Returns nil (which the store layer translates to SQL NULL) when ctx
// has no active span.
//
// Marshal failure returns nil + the error; callers log and proceed with
// a null headers column rather than failing the surrounding transaction.
func TraceHeadersFromContext(ctx context.Context) (json.RawMessage, error) {
	carrier := mapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	if len(carrier) == 0 {
		return nil, nil
	}
	b, err := json.Marshal(carrier)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// ContextFromTraceHeaders hydrates a parent context from outbox.headers
// JSONB. Used by the worker (after extracting headers from the inbound
// Kafka record) and would be used by any future consumer of
// events.notification.
//
// Returns parent unchanged when headers is empty / unparseable / has no
// traceparent.
func ContextFromTraceHeaders(parent context.Context, headers json.RawMessage) context.Context {
	if len(headers) == 0 {
		return parent
	}
	var carrier mapCarrier
	if err := json.Unmarshal(headers, &carrier); err != nil {
		return parent
	}
	if len(carrier) == 0 {
		return parent
	}
	return otel.GetTextMapPropagator().Extract(parent, carrier)
}

// KafkaHeadersFromOutboxHeaders converts the JSONB shape into the
// franz-go header slice. Used by the relay to attach to outbound
// records.
func KafkaHeadersFromOutboxHeaders(headers json.RawMessage) []kgo.RecordHeader {
	if len(headers) == 0 {
		return nil
	}
	var m map[string]string
	if err := json.Unmarshal(headers, &m); err != nil {
		return nil
	}
	if len(m) == 0 {
		return nil
	}
	out := make([]kgo.RecordHeader, 0, len(m))
	for k, v := range m {
		out = append(out, kgo.RecordHeader{Key: k, Value: []byte(v)})
	}
	return out
}

// ContextFromKafkaHeaders rebuilds trace context from a kgo.Record's
// Headers slice and returns the enriched context.
func ContextFromKafkaHeaders(parent context.Context, headers []kgo.RecordHeader) context.Context {
	if len(headers) == 0 {
		return parent
	}
	carrier := mapCarrier{}
	for _, h := range headers {
		if h.Key == "" {
			continue
		}
		carrier[h.Key] = string(h.Value)
	}
	if len(carrier) == 0 {
		return parent
	}
	return otel.GetTextMapPropagator().Extract(parent, carrier)
}

// SetSpanNotificationID sets notification.id on the active span in ctx when
// it is recording. Matches dispatcher/worker attribute naming so Jaeger tag
// search can correlate API requests with downstream spans.
func SetSpanNotificationID(ctx context.Context, id uuid.UUID) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}
	span.SetAttributes(attribute.String("notification.id", id.String()))
}

// SetSpanBatchID sets batch.id on the active span in ctx when it is recording.
// Used for POST /v1/notifications/batch (individual rows still have
// notification.id on worker/dispatcher spans).
func SetSpanBatchID(ctx context.Context, batchID uuid.UUID) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}
	span.SetAttributes(attribute.String("batch.id", batchID.String()))
}
