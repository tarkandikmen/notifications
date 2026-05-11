package worker

import (
	"errors"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fixedNow is the reference clock every classify test pins so the
// NewEligibleAt assertions can compare absolute timestamps without
// chasing clock skew. UTC chosen to mirror the api layer's
// formatTime() convention.
var fixedNow = time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)

// transientBackoffRange returns the closed [floor, ceiling] interval
// that TransientBackoff(attempt) must lie within per
// docs/design/05-retry.md §3. Used by the per-case range assertions
// below — the worker classification tests can no longer pin exact
// eligible_at values because Phase 3 swaps in equal-jitter backoff.
func transientBackoffRange(attempt int) (floor, ceiling time.Duration) {
	det := time.Duration(math.Pow(2, float64(attempt))) * time.Second
	return det / 2, det
}

// TestClassify_HTTP2xx_Success exercises docs/design/05-retry.md §1
// row 1: any HTTP 2xx is success / T4 (DELIVERED). NewEligibleAt is
// the passed-in now per the Outcome doc ("eligible_at unchanged"
// semantics).
func TestClassify_HTTP2xx_Success(t *testing.T) {
	cases := []struct {
		name string
		code int
	}{
		{"200 OK", 200},
		{"201 Created", 201},
		{"202 Accepted", 202},
		{"204 No Content", 204},
		{"299 boundary", 299},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := []byte(`{"messageId":"abc","status":"accepted"}`)
			out := Classify(ProviderResult{HTTPStatus: tc.code, Body: body}, 1, fixedNow)
			assert.Equal(t, classificationSuccess, out.Classification)
			assert.Equal(t, statusDelivered, out.NewStatus)
			assert.Equal(t, fixedNow, out.NewEligibleAt)
			assert.Nil(t, out.FailureReason)
			assert.Equal(t, body, out.ResponseBody)
			assert.Nil(t, out.ErrorMessage)
		})
	}
}

// TestClassify_HTTP2xx_EmptyBody confirms a 2xx with no body still
// classifies as success — the body field is forensic, not load-bearing.
func TestClassify_HTTP2xx_EmptyBody(t *testing.T) {
	out := Classify(ProviderResult{HTTPStatus: 204}, 1, fixedNow)
	assert.Equal(t, classificationSuccess, out.Classification)
	assert.Equal(t, statusDelivered, out.NewStatus)
	assert.Empty(t, out.ResponseBody)
}

// TestClassify_HTTPTransient_BelowMaxAttempts exercises
// docs/design/05-retry.md §1 rows for HTTP 408 / 425 / 429 / 5xx with
// attempt < max_attempts: classification is transient (T5) and
// NewStatus is PENDING with NewEligibleAt advanced by a jittered
// TransientBackoff(attempt) — asserted as a range per §3 since the
// equal-jitter draw replaces Phase 2's deterministic backoff.
func TestClassify_HTTPTransient_BelowMaxAttempts(t *testing.T) {
	cases := []struct {
		name    string
		code    int
		attempt int
	}{
		{"408 Request Timeout, attempt=1", 408, 1},
		{"425 Too Early, attempt=2", 425, 2},
		{"429 Too Many Requests, attempt=3", 429, 3},
		{"500 Internal Server Error, attempt=1", 500, 1},
		{"502 Bad Gateway, attempt=2", 502, 2},
		{"503 Service Unavailable, attempt=3", 503, 3},
		{"504 Gateway Timeout, attempt=6", 504, 6},
		{"599 boundary, attempt=4", 599, 4},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := []byte(`{"error":"oops"}`)
			out := Classify(ProviderResult{HTTPStatus: tc.code, Body: body}, tc.attempt, fixedNow)
			assert.Equal(t, classificationTransient, out.Classification)
			assert.Equal(t, statusPending, out.NewStatus)
			assert.Nil(t, out.FailureReason)
			assert.Equal(t, body, out.ResponseBody)
			assert.Nil(t, out.ErrorMessage)

			floor, ceiling := transientBackoffRange(tc.attempt)
			lo := fixedNow.Add(floor)
			hi := fixedNow.Add(ceiling)
			assert.False(t, out.NewEligibleAt.Before(lo),
				"eligible_at %s below floor %s for attempt=%d", out.NewEligibleAt, lo, tc.attempt)
			assert.False(t, out.NewEligibleAt.After(hi),
				"eligible_at %s above ceiling %s for attempt=%d", out.NewEligibleAt, hi, tc.attempt)
		})
	}
}

// TestClassify_HTTPTransient_AtMaxAttempts exercises
// docs/design/05-retry.md §1 + §4: a transient outcome on
// attempt == max_attempts terminal-fails the row (T7). The
// classification stays "transient" per the table — the row's
// failure_reason is "max_attempts_exceeded", not the (T6-only)
// "permanent_error".
func TestClassify_HTTPTransient_AtMaxAttempts(t *testing.T) {
	body := []byte(`{"error":"final"}`)
	out := Classify(ProviderResult{HTTPStatus: 500, Body: body}, maxAttempts, fixedNow)
	assert.Equal(t, classificationTransient, out.Classification)
	assert.Equal(t, statusFailed, out.NewStatus)
	assert.Equal(t, fixedNow, out.NewEligibleAt)
	require.NotNil(t, out.FailureReason)
	assert.Equal(t, failureReasonMaxAttempts, *out.FailureReason)
	assert.Equal(t, body, out.ResponseBody)
	assert.Nil(t, out.ErrorMessage)
}

// TestClassify_HTTPTransient_BeyondMaxAttempts proves the >= guard,
// not a strict ==. A theoretically out-of-range attempt still
// terminal-fails rather than scheduling another retry.
func TestClassify_HTTPTransient_BeyondMaxAttempts(t *testing.T) {
	out := Classify(ProviderResult{HTTPStatus: 500}, maxAttempts+5, fixedNow)
	assert.Equal(t, classificationTransient, out.Classification)
	assert.Equal(t, statusFailed, out.NewStatus)
	require.NotNil(t, out.FailureReason)
	assert.Equal(t, failureReasonMaxAttempts, *out.FailureReason)
}

// TestClassify_HTTPPermanent_AnyAttempt exercises docs/design/05-retry.md
// §1 row "HTTP 4xx other": the per-row T6 path. The notification
// terminal-fails immediately regardless of attempt count, classification
// is "permanent", and failure_reason is "permanent_error" (not
// "max_attempts_exceeded" — that's T7's reason). The provider response
// body is preserved on the delivery_attempts row.
func TestClassify_HTTPPermanent_AnyAttempt(t *testing.T) {
	cases := []struct {
		name    string
		code    int
		attempt int
	}{
		{"400 Bad Request, attempt=1", 400, 1},
		{"401 Unauthorized, attempt=2", 401, 2},
		{"403 Forbidden, attempt=3", 403, 3},
		{"404 Not Found, attempt=4", 404, 4},
		{"410 Gone, attempt=5", 410, 5},
		{"422 Unprocessable Entity, attempt=6", 422, 6},
		{"451 Unavailable For Legal Reasons, attempt=7", 451, 7},
		{"400 on first attempt — terminal even at attempt=1", 400, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := []byte(`{"error":"bad request"}`)
			out := Classify(ProviderResult{HTTPStatus: tc.code, Body: body}, tc.attempt, fixedNow)
			assert.Equal(t, classificationPermanent, out.Classification)
			assert.Equal(t, statusFailed, out.NewStatus)
			assert.Equal(t, fixedNow, out.NewEligibleAt,
				"permanent terminal-fail leaves eligible_at = now (forensic only)")
			require.NotNil(t, out.FailureReason)
			assert.Equal(t, failureReasonPermanent, *out.FailureReason)
			assert.Equal(t, body, out.ResponseBody)
			assert.Nil(t, out.ErrorMessage,
				"a permanent outcome from an HTTP response carries the body, not error_message")
		})
	}
}

// TestClassify_HTTPPermanent_BoundaryAt400 pins the lower edge of the
// permanent range. 399 is not a 4xx (it's a 3xx redirect) so it routes
// through the defensive default branch as transient; 400 is the first
// real 4xx and routes to permanent.
func TestClassify_HTTPPermanent_BoundaryAt400(t *testing.T) {
	at400 := Classify(ProviderResult{HTTPStatus: 400}, 1, fixedNow)
	assert.Equal(t, classificationPermanent, at400.Classification,
		"400 is the first permanent code in the 4xx-other range")

	at399 := Classify(ProviderResult{HTTPStatus: 399}, 1, fixedNow)
	assert.Equal(t, classificationTransient, at399.Classification,
		"399 is a 3xx; the defensive default treats unknown statuses as transient")
}

// TestClassify_HTTPTransient_BoundaryAt500 pins the lower edge of the
// transient 5xx range. 499 is a 4xx (Cloudflare's "client closed
// request" lives here) and routes to permanent; 500 is the first
// transient 5xx.
func TestClassify_HTTPTransient_BoundaryAt500(t *testing.T) {
	at500 := Classify(ProviderResult{HTTPStatus: 500, Body: []byte(`{}`)}, 1, fixedNow)
	assert.Equal(t, classificationTransient, at500.Classification,
		"500 is the first transient 5xx")
	assert.Equal(t, statusPending, at500.NewStatus)

	at499 := Classify(ProviderResult{HTTPStatus: 499, Body: []byte(`{}`)}, 1, fixedNow)
	assert.Equal(t, classificationPermanent, at499.Classification,
		"499 is the last 4xx-other before the transient 5xx range")
	assert.Equal(t, statusFailed, at499.NewStatus)
}

// TestClassify_DefaultUnknownStatusTreatedAsTransient covers the
// Classify default branch — codes that don't match any of the explicit
// success / transient / permanent predicates (1xx informational, 3xx
// redirect, the unlikely <100 / >599 path). The defensive disposition
// is transient so a misconfigured proxy returning a 1xx or a
// platform-routing bug returning a 3xx leaves the row recoverable.
func TestClassify_DefaultUnknownStatusTreatedAsTransient(t *testing.T) {
	cases := []int{100, 199, 300, 301, 304, 399, 0, -1, 600}
	for _, code := range cases {
		t.Run("status="+strconv.Itoa(code), func(t *testing.T) {
			out := Classify(ProviderResult{HTTPStatus: code}, 1, fixedNow)
			assert.Equal(t, classificationTransient, out.Classification,
				"defensive default routes unknown status %d to transient", code)
			assert.Equal(t, statusPending, out.NewStatus)
		})
	}
}

// TestClassify_RequestErr_BelowMaxAttempts exercises
// docs/design/05-retry.md §1's "no HTTP response" row — connection
// refused, DNS failure, timeout before the server responds, TLS
// handshake failure all classify transient with NewEligibleAt
// jittered into the [now+det/2, now+det] range when the retry chain
// still has slots.
func TestClassify_RequestErr_BelowMaxAttempts(t *testing.T) {
	cases := []struct {
		name    string
		err     error
		attempt int
	}{
		{"connect timeout", errors.New("dial tcp: i/o timeout"), 1},
		{"connection refused", errors.New("dial tcp 127.0.0.1:8080: connection refused"), 3},
		{"dns failure", errors.New("dial tcp: lookup webhook.invalid: no such host"), 5},
		{"tls handshake", errors.New("remote error: tls: handshake failure"), 6},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := Classify(ProviderResult{RequestErr: tc.err}, tc.attempt, fixedNow)
			assert.Equal(t, classificationTransient, out.Classification)
			assert.Equal(t, statusPending, out.NewStatus)
			assert.Nil(t, out.FailureReason)
			assert.Empty(t, out.ResponseBody, "no HTTP response → no body")
			require.NotNil(t, out.ErrorMessage)
			assert.Equal(t, tc.err.Error(), *out.ErrorMessage)

			floor, ceiling := transientBackoffRange(tc.attempt)
			lo := fixedNow.Add(floor)
			hi := fixedNow.Add(ceiling)
			assert.False(t, out.NewEligibleAt.Before(lo),
				"eligible_at %s below floor %s for attempt=%d", out.NewEligibleAt, lo, tc.attempt)
			assert.False(t, out.NewEligibleAt.After(hi),
				"eligible_at %s above ceiling %s for attempt=%d", out.NewEligibleAt, hi, tc.attempt)
		})
	}
}

// TestClassify_RequestErr_AtMaxAttempts is the no-response half of
// docs/design/05-retry.md §1 + §4: a network error on the final
// attempt terminal-fails the row (T7) with
// failure_reason=max_attempts_exceeded. The error string is preserved
// on the delivery_attempts row for forensic review.
func TestClassify_RequestErr_AtMaxAttempts(t *testing.T) {
	out := Classify(ProviderResult{RequestErr: errors.New("connect timeout")}, maxAttempts, fixedNow)
	assert.Equal(t, classificationTransient, out.Classification)
	assert.Equal(t, statusFailed, out.NewStatus)
	assert.Equal(t, fixedNow, out.NewEligibleAt)
	require.NotNil(t, out.FailureReason)
	assert.Equal(t, failureReasonMaxAttempts, *out.FailureReason)
	require.NotNil(t, out.ErrorMessage)
	assert.Equal(t, "connect timeout", *out.ErrorMessage)
}

// TestClassify_RequestErr_TruncatesLongMessage proves the column-size
// guard on error_message — a multi-kilobyte error string from the
// stdlib (e.g., a wrapped url.Error chain) gets clipped to
// errMessageMaxBytes so delivery_attempts.error_message never blows up.
func TestClassify_RequestErr_TruncatesLongMessage(t *testing.T) {
	long := strings.Repeat("x", 1024)
	out := Classify(ProviderResult{RequestErr: errors.New(long)}, 1, fixedNow)
	require.NotNil(t, out.ErrorMessage)
	assert.Len(t, *out.ErrorMessage, errMessageMaxBytes)
}

// TestClassify_RequestErrTakesPriority covers the defensive behavior
// when both HTTPStatus and RequestErr are set (can't happen in real
// flow, but the function shouldn't crash and shouldn't classify as
// success). The error path wins so the worker still retries and the
// error string lands in the forensic row.
func TestClassify_RequestErrTakesPriority(t *testing.T) {
	out := Classify(ProviderResult{
		HTTPStatus: 200,
		Body:       []byte(`{"status":"accepted"}`),
		RequestErr: errors.New("network error"),
	}, 1, fixedNow)
	assert.Equal(t, classificationTransient, out.Classification)
	assert.Equal(t, statusPending, out.NewStatus)
	require.NotNil(t, out.ErrorMessage)
	assert.Equal(t, "network error", *out.ErrorMessage)
	assert.Empty(t, out.ResponseBody)
}

// TestIsSuccessStatus pins the 2xx range boundaries.
func TestIsSuccessStatus(t *testing.T) {
	cases := map[int]bool{
		199: false,
		200: true,
		201: true,
		250: true,
		299: true,
		300: false,
		301: false,
		400: false,
		500: false,
	}
	for code, want := range cases {
		assert.Equal(t, want, isSuccessStatus(code), "status=%d", code)
	}
}

// TestIsTransientStatus pins the explicit transient 4xx allowlist
// (408, 425, 429) and the 5xx range. Codes outside both sets return
// false; isPermanentStatus is the catch-all 4xx predicate the caller
// reaches after this check.
func TestIsTransientStatus(t *testing.T) {
	cases := map[int]bool{
		400: false,
		407: false,
		408: true,
		409: false,
		424: false,
		425: true,
		428: false,
		429: true,
		430: false,
		499: false,
		500: true,
		503: true,
		599: true,
		600: false,
		200: false,
	}
	for code, want := range cases {
		assert.Equal(t, want, isTransientStatus(code), "status=%d", code)
	}
}

// TestIsPermanentStatus is the catch-all 4xx predicate. The caller
// (Classify) checks isTransientStatus first so 408 / 425 / 429 still
// route to transient; this test asserts the predicate's "any 4xx"
// behavior in isolation — the routing-order guarantee belongs to
// TestClassify_*.
func TestIsPermanentStatus(t *testing.T) {
	cases := map[int]bool{
		399: false,
		400: true,
		401: true,
		403: true,
		404: true,
		408: true, // predicate says yes; Classify routes via isTransientStatus first.
		410: true,
		422: true,
		451: true,
		499: true,
		500: false,
		200: false,
	}
	for code, want := range cases {
		assert.Equal(t, want, isPermanentStatus(code), "status=%d", code)
	}
}
