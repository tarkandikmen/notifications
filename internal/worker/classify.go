package worker

import "time"

// maxAttempts caps the worker's retry chain
// (docs/design/07-constants.md §D, max_attempts = 7). On a transient
// outcome with attempt >= maxAttempts the worker terminal-fails the
// row (T7) instead of scheduling another T5 retry. The reaper applies
// the same cap at T9 / T10.
const maxAttempts = 7

// Classification labels per docs/design/01-schema.md §Domain values for
// delivery_attempts.classification. Phase 3 ships the full four-value
// taxonomy from docs/design/05-retry.md §1; Phase 2 used only the first
// two of these (success / transient).
//
// classificationUnprocessable is wired by the T8 path
// (docs/design/06-idempotency.md §T8) which lands in Chunk 4. Defining
// the constant here keeps every classification literal in one place.
const (
	classificationSuccess       = "success"
	classificationTransient     = "transient"
	classificationPermanent     = "permanent"
	classificationUnprocessable = "unprocessable"
)

// notifications.status values per docs/design/01-schema.md §Domain values.
// Inlined here rather than imported from the api package so the worker
// has no dependency on api (the worker is a downstream consumer of the
// rows api creates, not a peer). statusDispatched + statusCancelled
// are read by the Layer 1 state guard in idempotency.go.
const (
	statusPending    = "PENDING"
	statusDispatched = "DISPATCHED"
	statusDelivered  = "DELIVERED"
	statusFailed     = "FAILED"
	statusCancelled  = "CANCELLED"
)

// notifications.failure_reason values per docs/design/01-schema.md §Domain
// values. max_attempts_exceeded is shared across:
//
//   - worker T7 (transient outcome on the final attempt);
//   - reaper T10 (stuck-row sweep on the final attempt; written by the
//     SQL in store.ReapStuck).
//
// permanent_error is worker T6 only (4xx-other from the provider).
// unprocessable_message is worker T8 only (pre-INSERT validation
// failure → DLQ); wired by Chunk 4.
const (
	failureReasonMaxAttempts   = "max_attempts_exceeded"
	failureReasonPermanent     = "permanent_error"
	failureReasonUnprocessable = "unprocessable_message"
)

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
// NewEligibleAt semantics per docs/design/05-retry.md §5 + §3:
//
//   - T4 (success / DELIVERED): "eligible_at unchanged". The store's
//     UPDATE always rewrites the column, so we pass `now`. Terminal
//     rows are never re-read by the dispatcher (status filter on
//     PENDING) or the reaper (status filter on DISPATCHED), so the
//     stored value is forensic only.
//   - T5 (transient / PENDING): now + TransientBackoff(attempt). The
//     dispatcher re-claims at this time per docs/design/02-state-machine.md
//     §Counter discipline.
//   - T6 (permanent / FAILED): same as T4 — terminal row.
//   - T7 (transient terminal / FAILED): same as T4 — terminal row.
type Outcome struct {
	Classification string
	NewStatus      string
	NewEligibleAt  time.Time
	FailureReason  *string
	ResponseBody   []byte
	ErrorMessage   *string
}

// Classify maps a ProviderResult to the four-value Outcome taxonomy
// per docs/design/05-retry.md §1. Branches in evaluation order; the
// first matching row wins:
//
//	RequestErr != nil                  → transient (T5 / T7)
//	HTTP 2xx                           → success   (T4)
//	HTTP 408 / 425 / 429 / 5xx          → transient (T5 / T7)
//	HTTP 4xx other                     → permanent (T6)
//	default (unknown 1xx / 3xx)        → transient (T5 / T7) — defensive
//
// `attempt` is the notification's current attempt number — the value
// the dispatcher set with `attempt = attempt + 1` at T2. `now` is the
// reference clock used both for T5's backoff timestamp and as the
// passed-through value for T4 / T6 / T7's NewEligibleAt (see Outcome
// doc).
func Classify(result ProviderResult, attempt int, now time.Time) Outcome {
	switch {
	case result.RequestErr != nil:
		return classifyTransient(result, attempt, now)
	case isSuccessStatus(result.HTTPStatus):
		return classifySuccess(result, now)
	case isTransientStatus(result.HTTPStatus):
		return classifyTransient(result, attempt, now)
	case isPermanentStatus(result.HTTPStatus):
		return classifyPermanent(result, now)
	default:
		// Unknown status (1xx informational, 3xx redirect, or a code
		// outside the 100–599 range). The worker never expects to see
		// these against the assessment provider, and a real provider
		// returning one is signaling a routing or proxy bug at the
		// platform layer rather than a per-message defect. Defensive
		// transient classification keeps the row recoverable (the
		// next attempt may see the misconfiguration resolved) rather
		// than terminal-failing on a status we did not explicitly
		// model.
		return classifyTransient(result, attempt, now)
	}
}

// classifySuccess builds the T4 Outcome — DELIVERED, no failure_reason,
// the provider body retained for forensic display (the api layer
// surfaces it via GET /v1/notifications/{id}.attempts[].response).
func classifySuccess(result ProviderResult, now time.Time) Outcome {
	return Outcome{
		Classification: classificationSuccess,
		NewStatus:      statusDelivered,
		NewEligibleAt:  now,
		ResponseBody:   result.Body,
	}
}

// classifyTransient handles every "should retry" outcome — HTTP
// 408 / 425 / 429, HTTP 5xx, no-HTTP-response request errors, and the
// defensive default. The branching on attempt count picks T5 (retry,
// PENDING with TransientBackoff) vs T7 (terminal FAILED with
// max_attempts_exceeded) per docs/design/05-retry.md §4.
//
// The transient path is the only one that consults TransientBackoff;
// success and permanent are terminal so the eligible_at they emit is
// a forensic timestamp rather than a re-poll deadline.
func classifyTransient(result ProviderResult, attempt int, now time.Time) Outcome {
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
		NewEligibleAt:  now.Add(TransientBackoff(attempt)),
		ResponseBody:   body,
		ErrorMessage:   errMsg,
	}
}

// classifyPermanent handles the HTTP 4xx-other branch — T6 per
// docs/design/05-retry.md §1. Terminal-fail the row regardless of
// attempt count; a 400 / 401 / 403 / 404 / 410 / 422 etc. will fail
// identically on every retry, so retrying wastes provider quota and
// per-channel rate-limit budget without making progress.
//
// The provider response body is retained on delivery_attempts.response
// so an operator inspecting "why did this row terminal-fail at attempt
// 1?" sees the provider's own error envelope (e.g., a Twilio 400 with
// `{"code": 21211, "message": "Invalid 'To' number"}`).
func classifyPermanent(result ProviderResult, now time.Time) Outcome {
	reason := failureReasonPermanent
	return Outcome{
		Classification: classificationPermanent,
		NewStatus:      statusFailed,
		NewEligibleAt:  now,
		FailureReason:  &reason,
		ResponseBody:   result.Body,
	}
}

// isSuccessStatus is the 2xx predicate.
func isSuccessStatus(code int) bool {
	return code >= 200 && code < 300
}

// isTransientStatus is the predicate for HTTP statuses that classify
// as transient per docs/design/05-retry.md §1: 408 (Request Timeout),
// 425 (Too Early), 429 (Too Many Requests), and the entire 5xx range
// ("server having a bad day"). Order in Classify matters — this check
// runs before isPermanentStatus so a 408 / 425 / 429 routes to the
// transient branch even though it is technically a 4xx code.
func isTransientStatus(code int) bool {
	if code >= 500 && code < 600 {
		return true
	}
	return code == 408 || code == 425 || code == 429
}

// isPermanentStatus is the catch-all 4xx predicate. By convention the
// caller (Classify) checks isTransientStatus first, so the
// transient-classified 4xx codes (408, 425, 429) never reach this
// branch. Examples that do route here: 400, 401, 403, 404, 410, 422,
// 451 — all unrecoverable from the worker's perspective.
func isPermanentStatus(code int) bool {
	return code >= 400 && code < 500
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

// truncateErrMessage caps a request error string at errMessageMaxBytes.
// Operates on bytes (not runes) since net/http error strings are ASCII
// in practice; a UTF-8-aware truncation here would buy nothing.
func truncateErrMessage(s string) string {
	if len(s) <= errMessageMaxBytes {
		return s
	}
	return s[:errMessageMaxBytes]
}
