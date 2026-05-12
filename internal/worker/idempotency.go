package worker

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/tarkandikmen/notifications/internal/store"
)

// GuardOutcome is the result of the worker's Layer 1 state guard from
// docs/design/06-idempotency.md §Layer 1. The guard reads
// (status, attempt) for the message's notification id and decides
// whether the worker should proceed to Layer 2 + the provider call or
// short-circuit.
//
// The four values are exhaustive — every (status, attempt) pair the
// schema allows maps onto exactly one of them per the table in
// docs/phases/03-resilience.md §2.1. New status values would require a
// new GuardOutcome variant; the default branch in CheckStateGuard
// surfaces unknown statuses as GuardSkipStale defensively rather than
// panicking.
type GuardOutcome int

const (
	// GuardProceed is the only outcome that authorizes the worker to
	// continue: status='DISPATCHED' AND attempt=msg.attempt — the
	// message is for the row's current authoritative attempt and no
	// terminal transition has fired yet. The caller proceeds to
	// Layer 2 (BeginAttempt).
	GuardProceed GuardOutcome = iota

	// GuardSkipStale fires when the row is non-terminal but the message
	// is for a superseded attempt. Two sub-cases:
	//   - status='DISPATCHED' AND attempt!=msg.attempt: the reaper reset
	//     and the dispatcher re-claimed; the current attempt has moved on.
	//   - status='PENDING' (any attempt): the reaper has reset the row
	//     but no new dispatcher claim has incremented attempt yet; the
	//     new claim will produce a fresh Kafka message.
	// Both dispositions are the same: ack + skip. The new attempt's
	// own message will run separately.
	GuardSkipStale

	// GuardSkipTerminal fires when the row reached DELIVERED, FAILED, or
	// CANCELLED before this message was processed. An earlier worker
	// already finalized the notification (or a future cancel API
	// pre-empted it). Ack + skip; running the provider call would be
	// wasteful and a Tx B against a terminal row is harmless but useless.
	GuardSkipTerminal

	// GuardSkipMissing fires when ReadStateForGuard returns ErrNotFound.
	// Should not occur in practice — notifications are not deleted at
	// this scope per docs/design/06-idempotency.md §Layer 1. Defensive
	// ack + skip so a pathological replay doesn't park the partition
	// indefinitely.
	GuardSkipMissing
)

// String renders the GuardOutcome's name for slog log lines. Without
// this, slog formats the value as the int constant, which makes
// debugging which branch fired unnecessarily painful.
func (g GuardOutcome) String() string {
	switch g {
	case GuardProceed:
		return "proceed"
	case GuardSkipStale:
		return "skip_stale"
	case GuardSkipTerminal:
		return "skip_terminal"
	case GuardSkipMissing:
		return "skip_missing"
	default:
		return "unknown"
	}
}

// guardReader is the slim subset of *store.Store that CheckStateGuard
// needs. Defining it as an unexported interface keeps the public
// signature simple (*store.Store satisfies it transparently in
// production) while letting the unit test drive every GuardOutcome
// branch with a fake — no Postgres testcontainer required for the
// branch table per docs/phases/03-resilience.md §Chunk 2 (Files to
// create — internal/worker/idempotency_test.go).
//
// Phase 5 widened the return shape with `createdAt` so the worker can
// compute the end-to-end delivery latency without a second round
// trip after T4. Tests that don't care about the latency observation
// supply the zero time.Time and the worker handles it gracefully.
type guardReader interface {
	ReadStateForGuard(ctx context.Context, id uuid.UUID) (status string, attempt int, createdAt time.Time, err error)
}

// GuardResult bundles the GuardOutcome with the created_at timestamp
// the worker needs for the end-to-end delivery latency observation
// (notification_delivery_latency_seconds histogram per
// docs/phases/05-observability.md §1.1). CreatedAt is the zero
// time.Time on every non-Proceed outcome (the latency observation
// fires only on T4); the worker checks for the zero value defensively.
type GuardResult struct {
	Outcome   GuardOutcome
	CreatedAt time.Time
}

// CheckStateGuard runs the Layer 1 state guard for a single Kafka
// record per docs/design/06-idempotency.md §Layer 1. Reads the
// (status, attempt, createdAt) tuple via st.ReadStateForGuard and
// maps the result to a GuardOutcome per the locked table in
// docs/phases/03-resilience.md §2.1.
//
// Returns a non-nil error only on a Postgres call failure — the
// caller leaves the Kafka offset uncommitted so the next poll
// retries. ErrNotFound surfaces as GuardSkipMissing (rather than an
// error) so the caller's branch table reads uniformly without an
// errors.Is check.
//
// docs/phases/03-resilience.md §2.1.
func CheckStateGuard(ctx context.Context, st guardReader, id uuid.UUID, msgAttempt int) (GuardResult, error) {
	status, attempt, createdAt, err := st.ReadStateForGuard(ctx, id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return GuardResult{Outcome: GuardSkipMissing}, nil
		}
		return GuardResult{}, err
	}

	switch status {
	case statusDelivered, statusFailed, statusCancelled:
		// Terminal states pre-empt the worker regardless of attempt
		// (an earlier worker delivered, max_attempts terminal-failed,
		// or a future cancel API fired between dispatch and now).
		return GuardResult{Outcome: GuardSkipTerminal}, nil
	case statusDispatched:
		if attempt == msgAttempt {
			return GuardResult{Outcome: GuardProceed, CreatedAt: createdAt}, nil
		}
		return GuardResult{Outcome: GuardSkipStale}, nil
	case statusPending:
		// The reaper reset the row but the dispatcher has not yet
		// re-claimed (or the message we're holding is from before
		// the reset). The new claim will produce a new Kafka
		// message; this one is stale.
		return GuardResult{Outcome: GuardSkipStale}, nil
	default:
		// Unknown status — the schema constraint covers PENDING /
		// DISPATCHED / DELIVERED / FAILED / CANCELLED so this branch
		// is unreachable in practice. Defensive ack + skip rather
		// than panicking: a single weird row should not block the
		// partition.
		return GuardResult{Outcome: GuardSkipStale}, nil
	}
}

// Unprocessable error codes per docs/design/04-kafka.md §3 + docs/design/05-retry.md §2.
// Centralized so every code path uses the exact strings the DLQ
// payload's `error` field expects.
const (
	errCodeDecodeFailed   = "decode_failed"
	errCodeSchemaMismatch = "schema_mismatch"
	errCodeMissingField   = "missing_field"
	errCodePanic          = "panic"
)

// dlqPayload is the JSON shape the worker emits to send.<channel>.dlq
// per docs/design/04-kafka.md §3. Built by BuildUnprocessable; the
// store's RecordUnprocessable inserts the marshaled bytes verbatim
// into the outbox row.
//
// Exactly one of OriginalMessage / OriginalMessageRaw is non-null per
// docs/design/04-kafka.md §3:
//
//   - OriginalMessage is the decoded JSON payload (rec.Value passed
//     through as json.RawMessage). Used when JSON decode succeeded
//     but validation failed.
//   - OriginalMessageRaw is the base64-encoded raw bytes. Used when
//     JSON decode failed (the bytes aren't valid JSON; embedding them
//     directly would break the surrounding payload).
type dlqPayload struct {
	Version            int             `json:"version"`
	NotificationID     *string         `json:"notification_id"`
	Channel            string          `json:"channel"`
	Attempt            *int            `json:"attempt"`
	OriginalMessage    json.RawMessage `json:"original_message"`
	OriginalMessageRaw *string         `json:"original_message_raw"`
	Error              string          `json:"error"`
	ErrorDetails       *string         `json:"error_details"`
	FailedAt           string          `json:"failed_at"`
}

// dlqPayloadVersion is the schema version stamped into every dlqPayload.
// Bumping this is a breaking change to send.<channel>.dlq consumers
// (currently only the future replay tool per docs/design/04-kafka.md §3
// and the assessment-time human investigator).
const dlqPayloadVersion = 1

// decodeAndValidatePanicHook is a test seam that lets internal tests
// inject a panic into decodeAndValidate's body to exercise the panic
// recovery branch deterministically. Production never sets this — the
// nil check at the call site short-circuits the no-op case to a single
// branch instruction.
//
// The hook fires AFTER the deferred recover is installed and BEFORE
// json.Unmarshal runs, so a panic from the hook is caught by the
// recover exactly the way a panic from decoding would be. Tests set
// this via SetDecodeAndValidatePanicHook + restore via t.Cleanup.
var decodeAndValidatePanicHook func()

// SetDecodeAndValidatePanicHook installs hook as the panic-injection
// seam decodeAndValidate fires before its real work. Returns the
// previous hook so callers can restore via t.Cleanup. Production never
// calls this; only loop_internal_test.go does.
func SetDecodeAndValidatePanicHook(hook func()) (previous func()) {
	previous = decodeAndValidatePanicHook
	decodeAndValidatePanicHook = hook
	return previous
}

// decodeAndValidate runs the worker's pre-INSERT decode + schema
// validation per docs/phases/03-resilience.md §2.4 steps 1–2. The
// helper is wrapped in a deferred recover so a panic during JSON
// decoding (or in the test panic hook) surfaces as a normal
// (errCode='panic', panicked=true) return rather than unwinding the
// goroutine — head-of-line blocking on a corrupt Kafka message would
// defeat the DLQ's purpose per docs/design/05-retry.md §2 +
// ARCHITECTURE_v3.md §5.9.
//
// Returns (per docs/phases/03-resilience.md §4 BuildUnprocessable
// notes):
//
//   - msg=nil, errCode="decode_failed" — bytes failed json.Unmarshal;
//     no decoded payload exists. BuildUnprocessable's no-target branch
//     consumes this shape and uses original_message_raw (base64).
//   - msg=nil, errCode="panic", panicked=true — a panic fired during
//     decode (or via the test hook); we have no decoded payload (the
//     panic likely interrupted Unmarshal mid-flight). Treated like
//     decode_failed by BuildUnprocessable.
//   - msg=&p, errCode="schema_mismatch" — JSON parsed but the schema
//     differs from this worker's expected shape (`version != 1` or
//     `id` failed uuid.Parse). The decoded payload is preserved so the
//     DLQ stores original_message (rather than original_message_raw)
//     and BuildUnprocessable can see msg.ID / msg.Attempt for
//     forensic enrichment of the DLQ payload's notification_id /
//     attempt fields.
//   - msg=&p, errCode="missing_field" — JSON parsed but a required
//     field is empty / zero / nil. msg is preserved for the same
//     reason as schema_mismatch; when msg.ID parses as a UUID and
//     msg.Attempt > 0, BuildUnprocessable takes the targeted T8
//     branch (the row's authoritative state moves to FAILED).
//   - msg=&p, errCode="" — every check passed; caller proceeds to
//     Layer 1.
//
// Returning the parsed msg on validation failures lets the targeted
// T8 path (docs/design/06-idempotency.md §T8) fire whenever the
// corrupt message identifies a real notification, even when the
// payload is otherwise invalid (e.g., a producer bug emits a payload
// missing recipient but with a correct id + attempt — the row still
// terminal-fails so the operator sees the failure in the
// notifications table, not just buried in the DLQ).
//
// Steps 3+ panics are NOT caught here — those operate on validated
// data and a panic indicates a programmer bug that should crash the
// process loudly so monitoring catches it.
//
// docs/phases/03-resilience.md §2.4.
func decodeAndValidate(value []byte) (msg *sendPayload, errCode, errDetails string, panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			msg = nil
			errCode = errCodePanic
			errDetails = fmt.Sprintf("%v", r)
			panicked = true
		}
	}()

	if hook := decodeAndValidatePanicHook; hook != nil {
		hook()
	}

	var p sendPayload
	if err := json.Unmarshal(value, &p); err != nil {
		return nil, errCodeDecodeFailed, err.Error(), false
	}

	if p.Version != sendPayloadVersion {
		return &p, errCodeSchemaMismatch,
			fmt.Sprintf("unsupported version %d (expected %d)", p.Version, sendPayloadVersion),
			false
	}

	if p.ID == "" {
		return &p, errCodeMissingField, "id is required", false
	}
	if _, err := uuid.Parse(p.ID); err != nil {
		return &p, errCodeSchemaMismatch,
			fmt.Sprintf("invalid id %q: %v", p.ID, err),
			false
	}

	if p.Attempt <= 0 {
		return &p, errCodeMissingField,
			fmt.Sprintf("attempt must be > 0 (got %d)", p.Attempt),
			false
	}

	if p.Recipient == "" {
		return &p, errCodeMissingField, "recipient is required", false
	}

	if p.Content == nil {
		return &p, errCodeMissingField, "content is required", false
	}

	return &p, "", "", false
}

// BuildUnprocessable constructs the store.UnprocessableInput for a
// validation failure per docs/phases/03-resilience.md §4.
// handleRecord calls decodeAndValidate, gets back (msg, errCode,
// errDetails, panicked), and feeds the result here to package the T8
// transaction's input.
//
// Target extraction (the (NotificationID, Attempt) pair) follows the
// rules in docs/phases/03-resilience.md §4 BuildUnprocessable notes:
//
//   - msg == nil (decode_failed or panic) → no target. The DLQ payload
//     uses OriginalMessageRaw (base64 of rec.Value). NotificationID
//     and Attempt are nil.
//   - msg != nil, msg.ID is empty or fails uuid.Parse → no target. The
//     DLQ payload uses OriginalMessage (the decoded JSON, rec.Value
//     verbatim).
//   - msg != nil, valid id, msg.Attempt <= 0 → no target. (The layer-3
//     guard needs a real attempt; without one we can't safely UPDATE
//     the row, so we fall back to no-target. DLQ payload still
//     populates msg.ID for forensic value via the targeted-style id
//     field — but NotificationID stays nil so RecordUnprocessable
//     skips statements 1, 2, and 4.)
//   - All fields valid → targeted. NotificationID = &id, Attempt =
//     &msg.Attempt.
//
// The Kafka record's key (rec.Key) is intentionally NOT consulted per
// the locked spec: the key carries the notification ID by convention
// (docs/design/04-kafka.md §1) but the same value lives in msg.id when
// the payload is decodable, and we have no recovery path when the
// payload is undecodable (we lack the attempt regardless of whether
// the key is intact). Including key-based fallback is documented as a
// Phase 7 polish item in docs/phases/03-resilience.md §Out of scope.
//
// The channel argument is authoritative — passed by handleRecord from
// deps.Channel rather than from msg.Channel. A corrupt msg.Channel
// (or no msg at all) must not redirect the DLQ to the wrong topic.
//
// Returns a non-nil error only if the DLQ payload fails to marshal,
// which shouldn't happen for a fixed-shape struct but the explicit
// branch keeps the worker from silently producing a corrupt outbox
// row if it ever does.
//
// docs/phases/03-resilience.md §4.
func BuildUnprocessable(rec *kgo.Record, msg *sendPayload, errCode, errDetails string, channel string, now time.Time) (store.UnprocessableInput, error) {
	failedAt := now.UTC().Format(occurredAtFormat)

	dlq := dlqPayload{
		Version:      dlqPayloadVersion,
		Channel:      channel,
		Error:        errCode,
		ErrorDetails: optionalString(errDetails),
		FailedAt:     failedAt,
	}

	in := store.UnprocessableInput{
		Channel:      channel,
		StartedAt:    now,
		ErrorCode:    errCode,
		ErrorDetails: errDetails,
		// EventPayload populated below only on the targeted branch
		// (statement 4 is skipped on no-target per
		// docs/design/06-idempotency.md §T8 edge case).
	}

	switch {
	case msg == nil:
		// No-target: payload undecodable (decode_failed or panic).
		// DLQ uses base64-encoded raw bytes. NotificationID +
		// Attempt stay nil; in.NotificationID == nil triggers the
		// no-target branch in store.RecordUnprocessable.
		raw := base64.StdEncoding.EncodeToString(rec.Value)
		dlq.OriginalMessageRaw = &raw
		dlq.OriginalMessage = nil

	default:
		// msg decoded; the rec.Value is valid JSON (it decoded into
		// msg). Pass it through verbatim as original_message.
		dlq.OriginalMessage = json.RawMessage(rec.Value)
		dlq.OriginalMessageRaw = nil

		// Best-effort populate msg-derived fields for forensics, even
		// when we fall back to the no-target branch below.
		if msg.ID != "" {
			id := msg.ID
			dlq.NotificationID = &id
		}
		if msg.Attempt != 0 {
			a := msg.Attempt
			dlq.Attempt = &a
		}

		// Targeted branch is reachable only when every field the
		// layer-3 guard needs is well-formed (valid uuid id,
		// attempt > 0). Re-validate here rather than trusting that
		// decodeAndValidate already filtered everything — the spec
		// documents BuildUnprocessable as robust to either input
		// shape (per docs/phases/03-resilience.md §4 BuildUnprocessable
		// notes).
		notifID, idErr := uuid.Parse(msg.ID)
		if idErr == nil && msg.Attempt > 0 {
			in.NotificationID = &notifID
			in.Attempt = &msg.Attempt
		}
	}

	dlqBytes, err := json.Marshal(dlq)
	if err != nil {
		return store.UnprocessableInput{}, fmt.Errorf("worker: marshal dlq payload: %w", err)
	}
	in.DLQPayload = dlqBytes

	if in.NotificationID != nil && in.Attempt != nil {
		eventBytes, err := buildUnprocessableEventPayload(*in.NotificationID, *in.Attempt, channel, now)
		if err != nil {
			return store.UnprocessableInput{}, fmt.Errorf("worker: marshal events payload: %w", err)
		}
		in.EventPayload = eventBytes
	}

	return in, nil
}

// buildUnprocessableEventPayload assembles the events.notification body
// emitted by T8 statement 4 per docs/design/04-kafka.md §2. Every
// targeted T8 transition lands the same shape: previous=DISPATCHED,
// current=FAILED, classification=unprocessable, failure_reason=
// unprocessable_message. Channel and attempt come from the message;
// id is the parsed uuid the caller already validated.
//
// The shape mirrors loop.go's buildEventPayload but with the four
// T8-locked discriminator values inlined — pulling them through a
// shared helper would require widening Outcome's surface for one
// extra branch and obscure the locked constants.
func buildUnprocessableEventPayload(id uuid.UUID, attempt int, channel string, now time.Time) (json.RawMessage, error) {
	failureReason := failureReasonUnprocessable
	payload := eventPayload{
		Version:        eventPayloadVersion,
		ID:             id.String(),
		BatchID:        nil,
		Channel:        channel,
		Attempt:        attempt,
		PreviousStatus: previousStatusDispatched,
		CurrentStatus:  statusFailed,
		Classification: classificationUnprocessable,
		FailureReason:  &failureReason,
		OccurredAt:     now.UTC().Format(occurredAtFormat),
	}
	return json.Marshal(payload)
}

// optionalString returns nil for an empty string and a pointer
// otherwise. Used so the DLQ payload's error_details field renders as
// JSON null when the validator didn't produce a detail string (rare,
// but defensive — every code path in decodeAndValidate currently
// produces a non-empty detail).
func optionalString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
