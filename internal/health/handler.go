// Package health builds the per-binary /healthz HTTP handler. Every
// binary in this repo (api on :8080, all binaries on cfg.MetricsAddr)
// serves /healthz via this handler: it takes a map of probes and
// reports each component's status. Binaries with no component-level
// probes pass nil and inherit metricsserver.defaultHealthz behavior.
package health

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// probeBudget is the per-request ceiling for every probe in a Handler
// invocation. Kept on a const so the unit tests in handler_test.go
// and the implementer can reason about it from one place.
const probeBudget = 2 * time.Second

// ProbeFunc returns nil when the named component is reachable from the
// binary's perspective and an error otherwise. Probes run inside a
// shared 2 s context timeout per request; a probe that exceeds that
// timeout reports as unhealthy with the deadline-exceeded error.
//
// Implementations should be cheap (one round trip) and side-effect free.
// The api binary's three probes are pgxpool.Pool.Ping, redis.Client.Ping,
// and kgo.Client.Ping (via kafkaadmin.LagClient.Ping); each is a single
// network round trip per call.
type ProbeFunc func(ctx context.Context) error

// Handler returns an http.HandlerFunc that runs every probe in probes
// in parallel inside a 2 s context timeout per request, then writes:
//
//   - 200 + {"status":"ok"} (no trailing newline) when every probe
//     returns nil. The 200 path body is byte-exact across the api
//     binary's :8080 endpoint and every binary's metricsserver
//     :9090 endpoint.
//   - 503 + {"components":{"<name>":"<error>",...},"status":"unhealthy"}
//     when at least one probe returns non-nil. Only failing components
//     appear in the components map. Keys at every nesting level land in
//     encoding/json's default map[string]X alphabetical order, which is
//     deterministic across runs without any explicit sort step.
//
// Empty / nil probes map → always 200. Mirrors
// metricsserver.defaultHealthz behavior so a binary that wires
// health.Handler(nil) (or an empty map) is identical to one that uses
// the metricsserver default.
func Handler(probes map[string]ProbeFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if len(probes) == 0 {
			writeOK(w)
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), probeBudget)
		defer cancel()

		// Each goroutine writes a single (name, err) pair into results
		// once probe(ctx) returns. The channel is buffered to len(probes)
		// so a slow probe does not block the others on send and so the
		// collector loop completes deterministically without a select
		// over ctx.Done — every probe inherits the same 2 s deadline so
		// the wait is bounded by the deadline, not by the collector
		// loop's own timeout.
		type result struct {
			name string
			err  error
		}
		results := make(chan result, len(probes))

		var wg sync.WaitGroup
		wg.Add(len(probes))
		for name, probe := range probes {
			name, probe := name, probe
			go func() {
				defer wg.Done()
				results <- result{name: name, err: probe(ctx)}
			}()
		}
		wg.Wait()
		close(results)

		failures := make(map[string]string)
		for res := range results {
			if res.err != nil {
				failures[res.name] = res.err.Error()
			}
		}

		if len(failures) == 0 {
			writeOK(w)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		// json.NewEncoder.Encode emits a trailing newline; the 503 path
		// accepts the newline (no exact-byte contract). encoding/json
		// marshals map[string]X with alphabetically sorted keys, which
		// gives {"components":{...},"status":"unhealthy"}
		// deterministically.
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":     "unhealthy",
			"components": failures,
		})
	}
}

// writeOK writes the byte-exact 200 body that metricsserver.defaultHealthz
// mirrors. json.Encode is intentionally avoided so the body has no trailing
// newline.
func writeOK(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}
