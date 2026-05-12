package metrics

import (
	"fmt"
	"net/http"
	"time"
)

// Middleware wraps next with api_requests_total +
// api_request_duration_seconds instrumentation. Designed to be wrapped
// around the api package's mux in api/cmd.go (or inside RegisterRoutes)
// after route registration so http.Request.Pattern is populated by the
// time the middleware reads it.
//
// Endpoint label: r.Pattern (Go 1.22+ ServeMux). When no route matched
// the field is the empty string — surfaces as endpoint="" so an
// alerting rule on that label catches a routing regression.
//
// Status class derived from the captured response code via a thin
// responseRecorder. First WriteHeader wins (subsequent calls are
// ignored, matching net/http's standard behavior). When the handler
// never explicitly writes a status the implicit 200 is recorded.
//
// Per docs/phases/05-observability.md §1.3, the middleware does NOT
// introspect request bodies; the per-handler size histograms
// (APIBatchSize, APIListResultSize) are observed inside the handlers
// themselves where the parsed request / response shape is in scope.
//
// docs/phases/05-observability.md §1.3.
func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		rec := &responseRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)

		elapsed := time.Since(start)

		endpoint := r.Pattern
		method := r.Method
		statusClass := fmt.Sprintf("%dxx", rec.status/100)

		APIRequests.WithLabelValues(endpoint, method, statusClass).Inc()
		APIRequestDuration.WithLabelValues(endpoint, method).Observe(elapsed.Seconds())
	})
}

// responseRecorder captures the response status without buffering the
// body. wroteHeader guards against the "first WriteHeader wins"
// stdlib semantic — subsequent calls are ignored.
//
// When the handler calls Write before WriteHeader, the implicit 200
// is recorded by Write's wroteHeader-flip; this mirrors net/http's
// ResponseWriter contract so the recorded status matches what the
// client actually saw on the wire.
type responseRecorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (r *responseRecorder) WriteHeader(status int) {
	if r.wroteHeader {
		return
	}
	r.status = status
	r.wroteHeader = true
	r.ResponseWriter.WriteHeader(status)
}

func (r *responseRecorder) Write(b []byte) (int, error) {
	if !r.wroteHeader {
		r.wroteHeader = true
	}
	return r.ResponseWriter.Write(b)
}
