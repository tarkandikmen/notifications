package health

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandler_AllProbesPass_200ExactBody locks the byte-exact 200
// path. Multiple probes that all return nil produce the byte-exact
// body `{"status":"ok"}` with no trailing newline.
func TestHandler_AllProbesPass_200ExactBody(t *testing.T) {
	h := Handler(map[string]ProbeFunc{
		"postgres": func(_ context.Context) error { return nil },
		"redis":    func(_ context.Context) error { return nil },
		"kafka":    func(_ context.Context) error { return nil },
	})

	rr := httptest.NewRecorder()
	h(rr, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	resp := rr.Result()
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "application/json", resp.Header.Get("Content-Type"))
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, `{"status":"ok"}`, string(body),
		"200 body must be byte-exact (no trailing newline)")
}

// TestHandler_OneProbeFails_503MultiComponent asserts a single failing
// probe produces 503 with the failing component's name + verbatim
// error message in the components map. Other (passing) probes are
// absent from the components map: a successful probe never appears
// in the components map.
func TestHandler_OneProbeFails_503MultiComponent(t *testing.T) {
	pingErr := errors.New("connection refused")
	h := Handler(map[string]ProbeFunc{
		"postgres": func(_ context.Context) error { return nil },
		"redis":    func(_ context.Context) error { return pingErr },
		"kafka":    func(_ context.Context) error { return nil },
	})

	rr := httptest.NewRecorder()
	h(rr, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	resp := rr.Result()
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	assert.Equal(t, "application/json", resp.Header.Get("Content-Type"))

	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, "unhealthy", body["status"])

	components, ok := body["components"].(map[string]any)
	require.True(t, ok, "components must be a JSON object")
	assert.Equal(t, "connection refused", components["redis"],
		"failing component carries the probe error message verbatim")
	_, present := components["postgres"]
	assert.False(t, present, "passing components must not appear in the map")
	_, present = components["kafka"]
	assert.False(t, present, "passing components must not appear in the map")
}

// TestHandler_MultipleProbesFails_503AlphabeticalOrder asserts the
// 503 body emits both the top-level keys (`components`, `status`) and
// the inner failing-component names in alphabetical order. The JSON
// order follows encoding/json's default alphabetical map ordering, so
// the test asserts on the literal byte sequence rather than just on
// map presence.
func TestHandler_MultipleProbesFails_503AlphabeticalOrder(t *testing.T) {
	h := Handler(map[string]ProbeFunc{
		"postgres": func(_ context.Context) error { return errors.New("pg down") },
		"redis":    func(_ context.Context) error { return errors.New("redis down") },
	})

	rr := httptest.NewRecorder()
	h(rr, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	resp := rr.Result()
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	got := strings.TrimRight(string(body), "\n")

	// Top-level keys: components < status (alphabetical).
	// Inner keys: postgres < redis.
	assert.Equal(t,
		`{"components":{"postgres":"pg down","redis":"redis down"},"status":"unhealthy"}`,
		got,
		"503 body keys must be in alphabetical order at every level")
}

// TestHandler_NilProbes_200 asserts a nil probe map produces the
// byte-exact 200 body. Mirrors metricsserver.defaultHealthz's
// behavior so a binary that wires health.Handler(nil) is identical
// to one that uses the metricsserver default.
func TestHandler_NilProbes_200(t *testing.T) {
	h := Handler(nil)

	rr := httptest.NewRecorder()
	h(rr, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	resp := rr.Result()
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, `{"status":"ok"}`, string(body))
}

// TestHandler_EmptyProbes_200 asserts an empty (non-nil) probe map
// produces the byte-exact 200 body. The empty-map case is asserted
// alongside the nil case so the empty-but-allocated map cannot
// regress the "no probes → always 200" contract on a path nil cannot
// reach.
func TestHandler_EmptyProbes_200(t *testing.T) {
	h := Handler(map[string]ProbeFunc{})

	rr := httptest.NewRecorder()
	h(rr, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	resp := rr.Result()
	defer func() { _ = resp.Body.Close() }()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, `{"status":"ok"}`, string(body))
}

// TestHandler_ProbeRespectsTimeout asserts a probe that exceeds the
// per-request 2 s budget surfaces ctx.DeadlineExceeded in the
// components map. Uses an inner deadline shorter than the outer 2 s
// so the test does not actually wait the full budget — the request
// context is wrapped at the http.Request level, and Handler's own
// context.WithTimeout uses the inherited deadline (the earliest of
// the two wins).
func TestHandler_ProbeRespectsTimeout(t *testing.T) {
	h := Handler(map[string]ProbeFunc{
		"slow": func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(5 * time.Second):
				return nil
			}
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	ctx, cancel := context.WithTimeout(req.Context(), 100*time.Millisecond)
	defer cancel()
	req = req.WithContext(ctx)

	rr := httptest.NewRecorder()
	start := time.Now()
	h(rr, req)
	elapsed := time.Since(start)

	resp := rr.Result()
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	assert.Less(t, elapsed, 2*time.Second,
		"handler must return as soon as ctx.Done fires (before the 5 s sleep finishes)")

	var body map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, "unhealthy", body["status"])

	components, ok := body["components"].(map[string]any)
	require.True(t, ok)
	slow, ok := components["slow"].(string)
	require.True(t, ok, "slow component must be present in the components map")
	assert.Contains(t, slow, "context",
		"deadline-exceeded probe surfaces a context error in the components map")
}

// TestHandler_ProbesRunInParallel asserts two slow-but-cheap probes
// finish in roughly max(probe duration), not sum(probe duration).
// Each probe sleeps probeWait; serialized execution would take
// 2*probeWait, parallel execution takes ~probeWait. The 1.5*probeWait
// upper bound gives plenty of slack for goroutine scheduling jitter
// without permitting serial execution.
func TestHandler_ProbesRunInParallel(t *testing.T) {
	const probeWait = 200 * time.Millisecond

	var calls atomic.Int32
	probe := func(_ context.Context) error {
		calls.Add(1)
		time.Sleep(probeWait)
		return nil
	}

	h := Handler(map[string]ProbeFunc{
		"a": probe,
		"b": probe,
	})

	rr := httptest.NewRecorder()
	start := time.Now()
	h(rr, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	elapsed := time.Since(start)

	resp := rr.Result()
	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.EqualValues(t, 2, calls.Load(), "both probes must run")
	assert.Less(t, elapsed, probeWait*3/2,
		"parallel execution must finish in ~max(probe), not sum(probe)")
	assert.GreaterOrEqual(t, elapsed, probeWait,
		"handler waits for every probe to return (no early-exit)")
}
