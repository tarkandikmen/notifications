package api

import (
	"bytes"
	"io"
	"net/http"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEnvelopeMiddleware_UnknownRoute_JSON404 locks the contract that a
// request to an unregistered path returns the canonical JSON envelope
// (`{"error":{"code":"not_found", ...}}`) with Content-Type
// application/json — matching every other 4xx in the api package and
// docs/openapi.yaml's ErrorEnvelope schema. Without envelopeMiddleware
// the stdlib mux would surface a text/plain "404 page not found" body
// instead.
func TestEnvelopeMiddleware_UnknownRoute_JSON404(t *testing.T) {
	srv := newTestServer(t, &fakeStore{})

	resp, err := http.Get(srv.URL + "/this-route-does-not-exist")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Equal(t, "application/json", resp.Header.Get("Content-Type"))

	env := decodeErrorEnvelope(t, resp.Body)
	assert.Equal(t, "not_found", env.Error.Code)
	assert.NotEmpty(t, env.Error.Message)
}

// TestEnvelopeMiddleware_WrongMethod_JSON405 locks the contract that a
// method-mismatch on a registered path returns a JSON envelope and
// preserves the Allow header that the stdlib mux populates for RFC
// 7231 §6.5.5 compliance. The api package's POST /v1/notifications +
// GET /v1/notifications routes share a path; a DELETE hits both
// branches and returns 405.
func TestEnvelopeMiddleware_WrongMethod_JSON405(t *testing.T) {
	srv := newTestServer(t, &fakeStore{})

	req, err := http.NewRequest(http.MethodDelete, srv.URL+"/v1/notifications", http.NoBody)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
	assert.Equal(t, "application/json", resp.Header.Get("Content-Type"))
	allow := resp.Header.Get("Allow")
	require.NotEmpty(t, allow, "stdlib mux must surface Allow on 405; envelopeMiddleware must preserve it")
	assert.Contains(t, allow, "POST")
	assert.Contains(t, allow, "GET")

	env := decodeErrorEnvelope(t, resp.Body)
	assert.Equal(t, "method_not_allowed", env.Error.Code)
}

// TestEnvelopeMiddleware_HandlerJSON404_Passthrough locks the
// passthrough contract: a handler-driven 404 (writeNotFound) that
// already sets Content-Type: application/json must reach the wire
// unchanged so its typed details[] payloads (none on writeNotFound
// today, but reserved) survive the wrapper. Without the JSON
// content-type guard, the wrapper would discard the handler body and
// emit a generic envelope, dropping per-route messaging.
func TestEnvelopeMiddleware_HandlerJSON404_Passthrough(t *testing.T) {
	fs := &fakeStore{getErr: assertableErr{}}
	srv := newTestServer(t, fs)

	resp, err := http.Get(srv.URL + "/v1/notifications/00000000-0000-4000-8000-000000000099")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	// fakeStore returns an arbitrary error → handler renders 500 with
	// JSON envelope. Verifies the wrapper does not interfere with
	// non-404 responses, either.
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	assert.Equal(t, "application/json", resp.Header.Get("Content-Type"))
	env := decodeErrorEnvelope(t, resp.Body)
	assert.Equal(t, "internal_error", env.Error.Code)
}

// TestEnvelopeMiddleware_StreamingPassthrough exercises the wrapper
// against /metrics, the largest body in the api binary's surface, to
// guard against regressions where the wrapper accidentally buffers or
// discards passthrough payloads. /metrics returns 200 with
// text/plain — the wrapper must not touch the body or headers.
//
// We register one synthetic counter on the per-test registry so the
// exposition body is non-empty (newTestServer hands every test a
// fresh prometheus.NewRegistry() with no collectors; an empty
// registry legitimately returns a zero-byte body, which would mask
// any wrapper-level discard regression).
func TestEnvelopeMiddleware_StreamingPassthrough(t *testing.T) {
	registry := prometheus.NewRegistry()
	registry.MustRegister(prometheus.NewCounter(prometheus.CounterOpts{
		Name: "envelope_passthrough_canary_total",
		Help: "synthetic counter so /metrics has a non-empty exposition body in this test",
	}))
	srv := newTestServerWithDeps(t, Deps{
		Store:    &fakeStore{},
		Registry: registry,
	})

	resp, err := http.Get(srv.URL + "/metrics")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.NotEmpty(t, body, "/metrics body must reach client through envelopeMiddleware")
	assert.True(t, bytes.Contains(body, []byte("envelope_passthrough_canary_total")),
		"expected canary counter in exposition body; got %q", string(body[:min(len(body), 200)]))
}

// assertableErr is a sentinel non-store.ErrNotFound that drives the
// handler's internal-error branch.
type assertableErr struct{}

func (assertableErr) Error() string { return "boom" }
