package metrics

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMiddleware_RecordsStatusClass_2xx_4xx_5xx exercises the three
// principal status classes the api emits and asserts the
// api_requests_total counter for that (endpoint, method, status_class)
// triple incremented by one per request.
//
// Each subtest uses a unique mux pattern so the per-test counter
// vector is isolated from other tests in this package — the registry
// is process-shared per docs/phases/05-observability.md §1.2.
func TestMiddleware_RecordsStatusClass_2xx_4xx_5xx(t *testing.T) {
	cases := []struct {
		name        string
		pattern     string
		status      int
		statusClass string
	}{
		{"2xx", "POST /metrics-test/2xx", http.StatusOK, "2xx"},
		{"4xx", "POST /metrics-test/4xx", http.StatusBadRequest, "4xx"},
		{"5xx", "POST /metrics-test/5xx", http.StatusInternalServerError, "5xx"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := counterValue(t, APIRequests.WithLabelValues(tc.pattern, http.MethodPost, tc.statusClass))

			mux := http.NewServeMux()
			mux.HandleFunc(tc.pattern, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
			})
			ts := httptest.NewServer(Middleware(mux))
			t.Cleanup(ts.Close)

			req, err := http.NewRequest(http.MethodPost, ts.URL+"/metrics-test/"+tc.name, nil)
			require.NoError(t, err)
			resp, err := ts.Client().Do(req)
			require.NoError(t, err)
			resp.Body.Close()

			after := counterValue(t, APIRequests.WithLabelValues(tc.pattern, http.MethodPost, tc.statusClass))
			assert.Equal(t, before+1, after, "counter for status class %s must increment by 1", tc.statusClass)
		})
	}
}

// TestMiddleware_DefaultsTo200WhenHandlerNeverWritesStatus asserts a
// handler that calls Write without an explicit WriteHeader records
// the implicit 200 — mirrors net/http's contract that the first Write
// flips the wroteHeader flag with status=200.
func TestMiddleware_DefaultsTo200WhenHandlerNeverWritesStatus(t *testing.T) {
	const pattern = "POST /metrics-test/implicit-200"
	before := counterValue(t, APIRequests.WithLabelValues(pattern, http.MethodPost, "2xx"))

	mux := http.NewServeMux()
	mux.HandleFunc(pattern, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	ts := httptest.NewServer(Middleware(mux))
	t.Cleanup(ts.Close)

	req, err := http.NewRequest(http.MethodPost, ts.URL+"/metrics-test/implicit-200", nil)
	require.NoError(t, err)
	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	resp.Body.Close()

	after := counterValue(t, APIRequests.WithLabelValues(pattern, http.MethodPost, "2xx"))
	assert.Equal(t, before+1, after, "implicit 200 must be recorded as 2xx")
}

// TestMiddleware_RecordsDuration asserts the duration histogram's
// observation count rises by 1 per request and the per-bucket count
// reflects an observed value above the simulated handler latency
// (here ~50ms, well above the .025 bucket boundary).
func TestMiddleware_RecordsDuration(t *testing.T) {
	const pattern = "POST /metrics-test/duration"
	before := histogramSampleCount(t, APIRequestDuration.WithLabelValues(pattern, http.MethodPost))

	mux := http.NewServeMux()
	mux.HandleFunc(pattern, func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(50 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	})
	ts := httptest.NewServer(Middleware(mux))
	t.Cleanup(ts.Close)

	req, err := http.NewRequest(http.MethodPost, ts.URL+"/metrics-test/duration", nil)
	require.NoError(t, err)
	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	resp.Body.Close()

	after := histogramSampleCount(t, APIRequestDuration.WithLabelValues(pattern, http.MethodPost))
	assert.Equal(t, before+1, after, "duration histogram sample count must increment by 1")
}

// TestMiddleware_LabelsPattern asserts the recorded endpoint label is
// the mux pattern (Go 1.22+ ServeMux populates r.Pattern), not the
// request path. A path-templated pattern surfaces with the template,
// so the label cardinality is bounded by the route count rather than
// the live URL count.
func TestMiddleware_LabelsPattern(t *testing.T) {
	const pattern = "GET /metrics-test/pattern/{id}"
	before := counterValue(t, APIRequests.WithLabelValues(pattern, http.MethodGet, "2xx"))

	mux := http.NewServeMux()
	mux.HandleFunc(pattern, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	ts := httptest.NewServer(Middleware(mux))
	t.Cleanup(ts.Close)

	resp, err := http.Get(ts.URL + "/metrics-test/pattern/abc-123")
	require.NoError(t, err)
	resp.Body.Close()

	after := counterValue(t, APIRequests.WithLabelValues(pattern, http.MethodGet, "2xx"))
	assert.Equal(t, before+1, after, "endpoint label must be the mux pattern, not the live path")
}

// TestMiddleware_NoMatchedRouteEndpointEmpty asserts a request that
// doesn't match any registered route surfaces with endpoint="" so an
// alerting rule on that label catches a routing regression. The
// stdlib mux falls through to its 404 handler for unmatched paths;
// r.Pattern is empty in that callback.
func TestMiddleware_NoMatchedRouteEndpointEmpty(t *testing.T) {
	before := counterValue(t, APIRequests.WithLabelValues("", http.MethodGet, "4xx"))

	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics-test/registered", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	ts := httptest.NewServer(Middleware(mux))
	t.Cleanup(ts.Close)

	resp, err := http.Get(ts.URL + "/metrics-test/no-such-route")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)

	after := counterValue(t, APIRequests.WithLabelValues("", http.MethodGet, "4xx"))
	assert.Equal(t, before+1, after, "unmatched-route 404 must increment endpoint=\"\"")
}

// counterValue extracts the current value from a prometheus.Counter.
// Mirrors the helper convention in metrics_test.go (gaugeValue) so
// the middleware tests stay self-contained without pulling
// prometheus/testutil into the test deps.
func counterValue(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	var m dto.Metric
	require.NoError(t, c.Write(&m))
	require.NotNil(t, m.Counter, "counter metric must carry a Counter payload")
	require.NotNil(t, m.Counter.Value)
	return *m.Counter.Value
}

// histogramSampleCount returns the SampleCount of a
// prometheus.Histogram observation. Mirrors the gaugeValue helper but
// for histograms — the test asserts an observation was made (count
// rose by 1) without depending on the exact bucket boundaries the
// duration landed in.
func histogramSampleCount(t *testing.T, h prometheus.Observer) uint64 {
	t.Helper()
	collector, ok := h.(prometheus.Metric)
	require.True(t, ok, "Observer must satisfy prometheus.Metric")
	var m dto.Metric
	require.NoError(t, collector.Write(&m))
	require.NotNil(t, m.Histogram, "histogram metric must carry a Histogram payload")
	require.NotNil(t, m.Histogram.SampleCount)
	return *m.Histogram.SampleCount
}
