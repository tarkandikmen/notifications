package worker

import (
	"math"
	"time"
)

// Phase 2 retry constants. Values inlined from docs/design/07-constants.md
// §D so the classify rules read declaratively. Phase 3 replaces this file
// with the full four-value taxonomy + equal-jitter backoff from
// docs/design/05-retry.md §1 + §3.
const (
	// maxAttempts caps the worker's retry chain
	// (docs/design/07-constants.md §D, max_attempts = 7). On a transient
	// outcome with attempt >= maxAttempts the worker terminal-fails the
	// row (T7) instead of scheduling another T5 retry.
	maxAttempts = 7

	// backoffBase is the deterministic exponential base
	// (docs/design/07-constants.md §D, backoff_base = 1 s). Phase 2
	// computes backoff(attempt) = backoffBase * 2^attempt; Phase 3 swaps
	// in the equal-jitter transient_backoff(attempt) from
	// docs/design/05-retry.md §3.
	backoffBase = 1 * time.Second
)

// Classification labels per docs/design/01-schema.md §Domain values for
// delivery_attempts.classification. Phase 2 uses only the first two of
// the four possible values; Phase 3 introduces "permanent" (T6) and
// "unprocessable" (T8 + DLQ).
const (
	classificationSuccess   = "success"
	classificationTransient = "transient"
)

// notifications.status values per docs/design/01-schema.md §Domain values.
// Inlined here rather than imported from the api package so the worker
// has no dependency on api (the worker is a downstream consumer of the
// rows api creates, not a peer).
const (
	statusPending   = "PENDING"
	statusDelivered = "DELIVERED"
	statusFailed    = "FAILED"
)

// failureReasonMaxAttempts is the only failure_reason Phase 2 writes
// (T7). Phase 3 adds "permanent_error" (T6) and "unprocessable_message"
// (T8); the reaper's T10 already writes this same value via its inline
// SQL in store.ReapStuck.
const failureReasonMaxAttempts = "max_attempts_exceeded"

// errMessageMaxBytes caps the length of the short error string written
// to delivery_attempts.error_message when the provider call fails
// without an HTTP response. Keeps the column small vs. the raw err
// string, which can pull in the full request URL and cause/wrap chain.
const errMessageMaxBytes = 256

// ProviderResult is the worker's view of a provider HTTP call. Exactly
// one of (HTTPStatus > 0, RequestErr != nil) is populated in normal
// flow:
//
//   - HTTPStatus > 0, RequestErr == nil: HTTP response received; Body
//     carries the response body bytes (possibly empty).
//   - HTTPStatus == 0, RequestErr != nil: no HTTP response (timeout,
//     connection refused, DNS failure, TLS handshake failure, etc).
//
// Classify treats a populated RequestErr as "no HTTP response"
// regardless of HTTPStatus, so a real provider that surfaces both never
// confuses the classification.
type ProviderResult struct {
	HTTPStatus int
	Body       []byte
	RequestErr error
}

// Outcome is the classified per-attempt result. Every field maps onto a
// store.OutcomeInput field in loop.go, where it gets combined with
// NotificationID/Attempt/StartedAt/FinishedAt/EventPayload to build the
// full RecordOutcome input.
//
// NewEligibleAt semantics per docs/phases/02-walking-skeleton.md §10:
//
//   - T4 (success / DELIVERED): "eligible_at unchanged". The store's
//     UPDATE always rewrites the column, so we pass `now`. Terminal
//     rows are never re-read by the dispatcher (status filter on
//     PENDING) or the reaper (status filter on DISPATCHED), so the
//     stored value is forensic only.
//   - T5 (transient / PENDING): now + backoff(attempt). The dispatcher
//     re-claims at this time per docs/design/02-state-machine.md
//     §Counter discipline.
//   - T7 (transient terminal / FAILED): same as T4 — terminal row,
//     value not re-read.
type Outcome struct {
	Classification string
	NewStatus      string
	NewEligibleAt  time.Time
	FailureReason  *string
	ResponseBody   []byte
	ErrorMessage   *string
}

// Classify maps a ProviderResult to the Phase 2 outcome per
// docs/phases/02-walking-skeleton.md §10. Three branches:
//
//	HTTP 2xx                                  → success / T4 (DELIVERED)
//	non-2xx OR no response, attempt < 7       → transient / T5 (PENDING)
//	non-2xx OR no response, attempt >= 7      → transient / T7 (FAILED)
//
// `attempt` is the notification's current attempt number — the value
// the dispatcher set with `attempt = attempt + 1` at T2. `now` is the
// reference clock used both for T5's backoff timestamp and as the
// passed-through value for T4/T7's NewEligibleAt (see Outcome doc).
func Classify(result ProviderResult, attempt int, now time.Time) Outcome {
	if result.RequestErr == nil && isSuccessStatus(result.HTTPStatus) {
		return Outcome{
			Classification: classificationSuccess,
			NewStatus:      statusDelivered,
			NewEligibleAt:  now,
			ResponseBody:   result.Body,
		}
	}

	body, errMsg := splitResponseAndError(result)

	if attempt >= maxAttempts {
		reason := failureReasonMaxAttempts
		return Outcome{
			Classification: classificationTransient,
			NewStatus:      statusFailed,
			NewEligibleAt:  now,
			FailureReason:  &reason,
			ResponseBody:   body,
			ErrorMessage:   errMsg,
		}
	}

	return Outcome{
		Classification: classificationTransient,
		NewStatus:      statusPending,
		NewEligibleAt:  now.Add(backoffFor(attempt)),
		ResponseBody:   body,
		ErrorMessage:   errMsg,
	}
}

// isSuccessStatus is the 2xx predicate. Pulled out so the (success,
// non-success) branching reads at the top of Classify in plain English.
func isSuccessStatus(code int) bool {
	return code >= 200 && code < 300
}

// splitResponseAndError partitions a non-success result into the two
// columns delivery_attempts records: response (body bytes) when an HTTP
// response was received, error_message (short string) when the request
// errored before a response. Exactly one of the two is non-empty.
//
// A request error takes priority — a real worker never produces both,
// but the safer ordering is "if we couldn't talk to the provider, the
// error is the more diagnostic value."
func splitResponseAndError(result ProviderResult) (body []byte, errMsg *string) {
	if result.RequestErr != nil {
		s := truncateErrMessage(result.RequestErr.Error())
		return nil, &s
	}
	return result.Body, nil
}

// backoffFor returns the deterministic exponential delay for the
// just-completed attempt: backoff_base * 2^attempt seconds. No jitter
// in Phase 2 — Phase 3 swaps in transient_backoff(attempt) from
// docs/design/05-retry.md §3.
//
// `attempt` is clamped to >= 0 defensively; in normal flow the
// dispatcher always increments attempt to >= 1 before the worker sees
// a row.
func backoffFor(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	exp := math.Pow(2, float64(attempt))
	return time.Duration(float64(backoffBase) * exp)
}

// truncateErrMessage caps a request error string at errMessageMaxBytes.
// Operates on bytes (not runes) since net/http error strings are ASCII
// in practice; a UTF-8-aware truncation here would buy nothing.
func truncateErrMessage(s string) string {
	if len(s) <= errMessageMaxBytes {
		return s
	}
	return s[:errMessageMaxBytes]
}
