package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// providerRequestTimeout caps a single provider HTTP call per
// docs/design/07-constants.md §H (provider_request_timeout = 30 s). A
// timeout firing here surfaces to Classify as RequestErr and routes to
// the transient branch (T5 or T7 depending on attempt).
const providerRequestTimeout = 30 * time.Second

// providerResponseBodyMaxBytes caps how much of the provider's response
// body the worker reads. delivery_attempts.response is a JSONB column
// (docs/design/01-schema.md §2) and a webhook returning a multi-MB body
// would otherwise blow up the row. 64 KiB matches the typical "small
// JSON response" envelope and tolerates the assessment provider
// (webhook.site returns ~few-hundred bytes).
const providerResponseBodyMaxBytes int64 = 64 * 1024

// HTTPDoer is the slim subset of *http.Client the provider uses.
// Defining it as an interface lets loop_test.go drive the loop with a
// fake provider when it wants finer control than httptest.NewServer
// gives. Production wiring uses *http.Client directly.
type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// Provider issues the worker's outgoing HTTP call to the webhook. A
// single Provider is shared across worker iterations so the underlying
// http.Client's connection pool is reused.
type Provider struct {
	client     HTTPDoer
	webhookURL string
}

// NewProvider builds a Provider with the per-attempt timeout baked into
// the underlying *http.Client. The Timeout field covers the entire
// request lifecycle (DNS + connect + TLS + write + read) so the worker
// loop never blocks longer than providerRequestTimeout on a hung
// provider.
//
// webhookURL is the absolute URL passed via cfg.WebhookURL; the
// provider posts every SMS to this single URL (Phase 2 has no
// per-recipient routing).
//
// Phase 5 wraps the transport with otelhttp.NewTransport so the
// provider call automatically opens an `HTTP POST` span as a child of
// the active span on the request's context. The worker's
// `provider.Send(ctx, ...)` already passes ctx from handleRecord, so
// the span chains under worker.handleRecord per
// docs/phases/05-observability.md §6. No metric duplication — the
// otelhttp span lives in the trace; the
// worker_provider_request_duration_seconds histogram is what
// dashboards graph.
//
// notificationSpanAttrRT sits inside otelhttp so each outbound span can
// copy notification.id from context (set in Send) onto the HTTP client
// span for Jaeger tag search.
func NewProvider(webhookURL string) *Provider {
	return &Provider{
		client: &http.Client{
			Timeout: providerRequestTimeout,
			Transport: otelhttp.NewTransport(&notificationSpanAttrRT{
				base: http.DefaultTransport,
			}),
		},
		webhookURL: webhookURL,
	}
}

type providerNotifIDKey struct{}

// notificationSpanAttrRT runs after otelhttp creates the client span;
// r.Context() is the span context, so attributes apply to the HTTP POST span.
type notificationSpanAttrRT struct {
	base http.RoundTripper
}

func (t *notificationSpanAttrRT) RoundTrip(r *http.Request) (*http.Response, error) {
	if id, _ := r.Context().Value(providerNotifIDKey{}).(string); id != "" {
		if span := trace.SpanFromContext(r.Context()); span.IsRecording() {
			span.SetAttributes(attribute.String("notification.id", id))
		}
	}
	if t.base == nil {
		t.base = http.DefaultTransport
	}
	return t.base.RoundTrip(r)
}

// NewProviderWithClient is the test seam — accepts an HTTPDoer so unit
// tests can wire a fake. Production callers use NewProvider.
func NewProviderWithClient(client HTTPDoer, webhookURL string) *Provider {
	return &Provider{client: client, webhookURL: webhookURL}
}

// providerRequest is the JSON body the worker POSTs to the webhook per
// docs/phases/02-walking-skeleton.md §9 step 2.2. The fields are
// exactly those documented; the Phase 6 template path adds rendered
// content under the same `content` key.
type providerRequest struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

// Send posts the SMS message to the configured webhook URL and returns
// a ProviderResult shaped for Classify. Behavior:
//
//   - HTTP response received (any status code): result.HTTPStatus is
//     the response code; result.Body holds up to
//     providerResponseBodyMaxBytes of the response body.
//   - No HTTP response (timeout, connect refused, DNS, TLS): result.RequestErr
//     is the underlying error; HTTPStatus stays 0 and Body stays nil.
//
// Send never returns an error itself — every failure mode is encoded
// in the returned ProviderResult so the caller's classify branch fires
// uniformly regardless of HTTP-vs-network failure.
func (p *Provider) Send(ctx context.Context, notificationID, recipient, channel, content string) ProviderResult {
	ctx = context.WithValue(ctx, providerNotifIDKey{}, notificationID)

	body, err := json.Marshal(providerRequest{
		To:      recipient,
		Channel: channel,
		Content: content,
	})
	if err != nil {
		// json.Marshal of three strings cannot fail in normal flow;
		// surface the error as a transient request-side failure so the
		// worker logs and retries rather than panicking.
		return ProviderResult{RequestErr: fmt.Errorf("worker: marshal provider request: %w", err)}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.webhookURL, bytes.NewReader(body))
	if err != nil {
		return ProviderResult{RequestErr: fmt.Errorf("worker: build provider request: %w", err)}
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		return ProviderResult{RequestErr: err}
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, providerResponseBodyMaxBytes))
	if err != nil {
		// We received an HTTP response but the body read failed
		// mid-stream. The status code is meaningful (the headers
		// landed); record it but flag the body as missing via the
		// error path so Classify routes us to the transient branch.
		return ProviderResult{HTTPStatus: resp.StatusCode, RequestErr: fmt.Errorf("worker: read provider response body: %w", err)}
	}

	return ProviderResult{
		HTTPStatus: resp.StatusCode,
		Body:       respBody,
	}
}
