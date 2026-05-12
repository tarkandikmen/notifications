package metricsserver

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNew_ServesMetrics asserts /metrics returns a Prometheus body
// containing the metric registered against the supplied registry.
// Mirrors what every binary's :9090/metrics endpoint produces.
func TestNew_ServesMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	c := promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "test_metricsserver_demo_total",
		Help: "Test counter used to verify /metrics surfaces the registered family.",
	})
	c.Inc()

	srv := New("", reg, nil)
	ts := launch(t, srv)

	resp, err := http.Get(ts.URL + "/metrics")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "test_metricsserver_demo_total")
}

// TestNew_DefaultHealthz_Returns200 asserts the nil-healthz default
// path returns 200 with the Phase 1 byte-exact body. The non-api
// binaries (worker / dispatcher / relay / reaper) pass nil and rely
// on this contract.
func TestNew_DefaultHealthz_Returns200(t *testing.T) {
	srv := New("", prometheus.NewRegistry(), nil)
	ts := launch(t, srv)

	resp, err := http.Get(ts.URL + "/healthz")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "application/json", resp.Header.Get("Content-Type"))
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, `{"status":"ok"}`, string(body), "default healthz body must match Phase 1's byte-exact contract")
}

// TestNew_CustomHealthz_IsCalled asserts the supplied healthz
// handler runs in place of the default. The api binary will pass
// its rich handler (with the Postgres ping) here in Chunk 2.
func TestNew_CustomHealthz_IsCalled(t *testing.T) {
	var called atomic.Int32
	custom := func(w http.ResponseWriter, _ *http.Request) {
		called.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"status":"custom"}`))
	}

	srv := New("", prometheus.NewRegistry(), custom)
	ts := launch(t, srv)

	resp, err := http.Get(ts.URL + "/healthz")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, int32(1), called.Load(), "custom healthz handler must be called")
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), `"custom"`)
}

// TestNew_GracefulShutdown asserts Shutdown returns within the
// caller-supplied deadline so cmd.go's defer wiring drains cleanly
// without leaking the listener goroutine.
func TestNew_GracefulShutdown(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := New(ln.Addr().String(), prometheus.NewRegistry(), nil)

	go func() {
		_ = srv.Serve(ln)
	}()

	resp, err := http.Get("http://" + ln.Addr().String() + "/healthz")
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	assert.NoError(t, srv.Shutdown(ctx))
	assert.Less(t, time.Since(start), 5*time.Second, "Shutdown must return within the supplied deadline")
}

// launch wraps the *http.Server in an httptest.Server so the tests
// don't have to bind a real port unless lifecycle behavior is the
// SUT. Mirrors the convention in internal/api/handlers_test.go.
func launch(t *testing.T, srv *http.Server) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(srv.Handler)
	t.Cleanup(ts.Close)
	return ts
}
