package worker

import (
	"errors"
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

// TestClassify_HTTP2xx_Success exercises the §10 first row: any HTTP
// 2xx is success / T4 (DELIVERED). NewEligibleAt is the passed-in now
// per the Outcome doc ("eligible_at unchanged" semantics).
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

// TestClassify_HTTPNon2xx_BelowMaxAttempts exercises the §10 second row:
// any non-2xx with attempt < max_attempts is transient / T5 (PENDING).
// NewEligibleAt = now + backoff(attempt).
func TestClassify_HTTPNon2xx_BelowMaxAttempts(t *testing.T) {
	cases := []struct {
		name    string
		code    int
		attempt int
	}{
		// Real provider sad statuses Phase 2 lumps together into
		// transient. Phase 3's full classification splits 4xx-other
		// into permanent (T6).
		{"400 Bad Request, attempt=1", 400, 1},
		{"401 Unauthorized, attempt=2", 401, 2},
		{"403 Forbidden, attempt=3", 403, 3},
		{"404 Not Found, attempt=4", 404, 4},
		{"408 Request Timeout, attempt=5", 408, 5},
		{"429 Too Many Requests, attempt=6", 429, 6},
		{"500 Internal Server Error, attempt=1", 500, 1},
		{"502 Bad Gateway, attempt=2", 502, 2},
		{"503 Service Unavailable, attempt=3", 503, 3},
		{"504 Gateway Timeout, attempt=6", 504, 6},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := []byte(`{"error":"oops"}`)
			out := Classify(ProviderResult{HTTPStatus: tc.code, Body: body}, tc.attempt, fixedNow)
			assert.Equal(t, classificationTransient, out.Classification)
			assert.Equal(t, statusPending, out.NewStatus)
			expected := fixedNow.Add(backoffFor(tc.attempt))
			assert.Equal(t, expected, out.NewEligibleAt)
			assert.Nil(t, out.FailureReason)
			assert.Equal(t, body, out.ResponseBody)
			assert.Nil(t, out.ErrorMessage)
		})
	}
}

// TestClassify_HTTPNon2xx_AtMaxAttempts exercises §10's third row: a
// non-2xx outcome on attempt == max_attempts terminal-fails the row
// (T7). The classification stays "transient" per the table — the
// row's failure_reason is "max_attempts_exceeded", not the
// (Phase-3-only) "permanent_error".
func TestClassify_HTTPNon2xx_AtMaxAttempts(t *testing.T) {
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

// TestClassify_HTTPNon2xx_BeyondMaxAttempts proves the >= guard, not a
// strict ==. A theoretically-out-of-range attempt still terminal-fails
// rather than scheduling another retry.
func TestClassify_HTTPNon2xx_BeyondMaxAttempts(t *testing.T) {
	out := Classify(ProviderResult{HTTPStatus: 500}, maxAttempts+5, fixedNow)
	assert.Equal(t, classificationTransient, out.Classification)
	assert.Equal(t, statusFailed, out.NewStatus)
	require.NotNil(t, out.FailureReason)
	assert.Equal(t, failureReasonMaxAttempts, *out.FailureReason)
}

// TestClassify_RequestErr_BelowMaxAttempts exercises §10's second row's
// "no HTTP response" half — connection refused, DNS failure, timeout
// before the server responds, TLS handshake failure all classify
// transient with NewEligibleAt = now + backoff(attempt) when the
// retry chain still has slots.
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
			expected := fixedNow.Add(backoffFor(tc.attempt))
			assert.Equal(t, expected, out.NewEligibleAt)
			assert.Nil(t, out.FailureReason)
			assert.Empty(t, out.ResponseBody, "no HTTP response → no body")
			require.NotNil(t, out.ErrorMessage)
			assert.Equal(t, tc.err.Error(), *out.ErrorMessage)
		})
	}
}

// TestClassify_RequestErr_AtMaxAttempts is §10's third row's "no
// response" half: a network error on the final attempt terminal-fails
// the row (T7) with failure_reason=max_attempts_exceeded. The error
// string is preserved on the delivery_attempts row for forensic
// review.
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

// TestBackoffFor_Curve locks the deterministic backoff curve so a
// regression in the math.Pow call surfaces immediately. Values come
// from docs/design/07-constants.md §D and the docs/design/05-retry.md
// "deterministic(attempt) = backoff_base * 2^attempt" formula.
func TestBackoffFor_Curve(t *testing.T) {
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{3, 8 * time.Second},
		{4, 16 * time.Second},
		{5, 32 * time.Second},
		{6, 64 * time.Second},
		{7, 128 * time.Second},
	}
	for _, tc := range cases {
		got := backoffFor(tc.attempt)
		assert.Equal(t, tc.want, got, "attempt=%d", tc.attempt)
	}
}

// TestBackoffFor_ClampsBelowOne is the defensive clamp — a stray
// attempt=0 (shouldn't happen since the dispatcher always increments
// to >= 1 before the worker sees a row) shouldn't divide by zero or
// produce a sub-second backoff.
func TestBackoffFor_ClampsBelowOne(t *testing.T) {
	assert.Equal(t, 2*time.Second, backoffFor(0))
	assert.Equal(t, 2*time.Second, backoffFor(-3))
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
