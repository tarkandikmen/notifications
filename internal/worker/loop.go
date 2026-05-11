package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/tarkandikmen/notifications/internal/ratelimit"
	"github.com/tarkandikmen/notifications/internal/store"
)

// redisDownBackoff is the wait the worker sits on after a rate-limit
// Acquire returns ratelimit.ErrRedisDown per docs/phases/03-resilience.md
// §2.4 step 5 ("sleep 1 s"). The wait is cancellable so a graceful
// shutdown during a Redis outage isn't blocked behind the timer.
const redisDownBackoff = 1 * time.Second

// Phase 2 worker payload schema versions. Bumping either is a breaking
// change and must be coordinated with docs/design/04-kafka.md §7.
const (
	sendPayloadVersion  = 1
	eventPayloadVersion = 1
)

// previousStatusDispatched is the previous_status value the worker
// always emits — the worker only acts on rows the dispatcher set to
// DISPATCHED via T2 (docs/design/02-state-machine.md §State-driving
// components). Even in the superseded-attempt edge case where the
// attempt-guarded UPDATE matches zero rows, the worker's view at
// poll time was DISPATCHED.
const previousStatusDispatched = "DISPATCHED"

// occurredAtFormat is the RFC 3339 + millisecond layout from
// docs/design/04-kafka.md §Conventions
// ("2026-05-10T11:13:00.123Z"). Always called against a UTC time so
// the trailing zone marker renders as "Z".
const occurredAtFormat = "2006-01-02T15:04:05.000Z07:00"

// Consumer is the slim subset of *kgo.Client that Loop needs. Defining
// it as an interface lets cmd.go own the kgo lifecycle while keeping
// Loop independently testable: the integration test in loop_test.go
// drives Loop against a real *kgo.Client wired to a Kafka
// testcontainer; future unit tests can supply a fake without booting
// any container.
type Consumer interface {
	PollFetches(ctx context.Context) kgo.Fetches
	CommitRecords(ctx context.Context, rs ...*kgo.Record) error
}

// Sender is the slim subset of *Provider that Loop needs. Same
// rationale as Consumer — production wiring uses *Provider against the
// real webhook URL; tests substitute httptest-backed fakes when they
// want finer control than a live HTTP call.
type Sender interface {
	Send(ctx context.Context, recipient, channel, content string) ProviderResult
}

// Limiter is the slim subset of *ratelimit.Bucket that Loop needs.
// Defining it as an interface lets cmd.go inject the real bucket while
// loop_test.go drives the loop with a no-op fixture (when the test
// does not exercise rate-limit semantics) or a fake that returns
// ratelimit.ErrRedisDown (when the test exercises the redis-down branch).
//
// Acquire's contract matches docs/phases/03-resilience.md §1:
//
//   - nil on success (a token was deducted; caller proceeds to the
//     provider call).
//   - ratelimit.ErrRedisDown on a Redis call failure (caller pauses
//     processing per ARCHITECTURE_v3.md §6.6 — Kafka redelivers).
//   - ctx.Err() on cancellation (graceful shutdown; caller returns
//     without committing the Kafka offset).
type Limiter interface {
	Acquire(ctx context.Context, channel string) error
}

// Deps is the worker loop's per-process dependency bundle. The shape
// mirrors internal/dispatcher and internal/relay's Deps for
// consistency: storage + injected externals + logger + injectable
// clock.
//
// Channel labels every log line and is the per-channel key the rate
// limiter scopes against (final Redis key shape "rate:<channel>" per
// ARCHITECTURE_v3.md §6.6). Phase 2 shipped only `sms`; Phase 3 wires
// the Limiter and keeps the channel default; Phase 3 Chunk 7 widens
// cmd.go to set the field per --channel value.
//
// Limiter is required — applyDefaults panics when it is nil. Production
// wiring always provides one (cmd.go builds a *ratelimit.Bucket from
// the worker's *redis.Client). Tests inject a fake.
type Deps struct {
	Store    *store.Store
	Consumer Consumer
	Provider Sender
	Limiter  Limiter
	Logger   *slog.Logger
	Channel  string
	Clock    func() time.Time
}

// sendPayload is the JSON shape the worker consumes from
// `send.<channel>` per docs/design/04-kafka.md §1. The fields and
// nullability map 1:1 onto the spec; unknown fields are tolerated by
// json.Unmarshal (Phase 2 doesn't strict-mode the decode — additive
// schema evolution per docs/design/04-kafka.md §7 must continue to
// roll forward).
type sendPayload struct {
	Version      int             `json:"version"`
	ID           string          `json:"id"`
	Attempt      int             `json:"attempt"`
	Channel      string          `json:"channel"`
	Recipient    string          `json:"recipient"`
	Content      *string         `json:"content"`
	Template     *string         `json:"template"`
	TemplateData json.RawMessage `json:"template_data"`
	Priority     int             `json:"priority"`
}

// eventPayload is the JSON shape the worker emits to
// `events.notification` per docs/design/04-kafka.md §2.
//
// classification is `string` (not `*string`) because the worker is
// the only emitter for which classification is guaranteed non-null
// (docs/design/04-kafka.md §2 row "classification | string \| null |
// Set when the worker emits (T4–T8); null for T3 and T10."). The
// reaper / api emitters use their own payload constructors.
//
// failure_reason / batch_id stay `*string` so Phase 2 always renders
// them as JSON null (the spec example shows `"failure_reason": null`
// on success / transient outcomes, and Phase 2 single-creates have no
// batch_id).
type eventPayload struct {
	Version        int     `json:"version"`
	ID             string  `json:"id"`
	BatchID        *string `json:"batch_id"`
	Channel        string  `json:"channel"`
	Attempt        int     `json:"attempt"`
	PreviousStatus string  `json:"previous_status"`
	CurrentStatus  string  `json:"current_status"`
	Classification string  `json:"classification"`
	FailureReason  *string `json:"failure_reason"`
	OccurredAt     string  `json:"occurred_at"`
}

// Loop drives the consume → provider → RecordOutcome cycle until ctx
// is cancelled. Returns nil on graceful shutdown; never returns an
// error in Phase 2 — every per-record failure is either committed and
// skipped (decode failures, the Phase 3 DLQ source) or left
// uncommitted for Kafka to redeliver (RecordOutcome failures).
//
// The loop name avoids colliding with the package's cobra-bound Run
// from cmd.go — same convention as internal/dispatcher/loop.go and
// internal/relay/loop.go (the spec writes "loop.Run(ctx, deps)" in
// docs/phases/02-walking-skeleton.md §Repo layout, but loop.go and
// cmd.go share a package).
//
// docs/phases/02-walking-skeleton.md §9.
func Loop(ctx context.Context, deps Deps) error {
	deps = applyDefaults(deps)

	deps.Logger.Info("loop started",
		"mode", "worker",
		"channel", deps.Channel,
	)

	for {
		if ctx.Err() != nil {
			deps.Logger.Info("loop stopped",
				"mode", "worker",
				"channel", deps.Channel,
			)
			return nil
		}

		fetches := deps.Consumer.PollFetches(ctx)

		// Surface every per-(topic, partition) error franz-go reports.
		// A context-canceled error means the parent ctx was cancelled
		// during the poll — that's a graceful shutdown, not a fault.
		// Other errors (broker unreachable, coordinator unavailable,
		// per-partition fetch error) get a warn log and the loop
		// continues; Phase 3 routes the consistent-broker-down case
		// through metrics.
		for _, fe := range fetches.Errors() {
			if errors.Is(fe.Err, context.Canceled) || errors.Is(fe.Err, context.DeadlineExceeded) {
				deps.Logger.Info("loop stopped",
					"mode", "worker",
					"channel", deps.Channel,
				)
				return nil
			}
			deps.Logger.Warn("worker fetch error",
				"topic", fe.Topic,
				"partition", fe.Partition,
				"err", fe.Err,
			)
		}

		fetches.EachRecord(func(rec *kgo.Record) {
			handleRecord(ctx, deps, rec)
		})
	}
}

// handleRecord processes one Kafka record per the Phase 3 pipeline
// from docs/phases/03-resilience.md §2.4. The locked step order is:
//
//  1. Decode + schema validation (decode_failed / schema_mismatch /
//     missing_field / panic). Failures route to T8
//     (RecordUnprocessable) and commit the offset; the message lands
//     in send.<channel>.dlq + (when targeted) on events.notification.
//  2. Layer 1: state guard (CheckStateGuard). Skip outcomes ack + return.
//  3. Layer 2: separate-tx INSERT (BeginAttempt). Conflict acks + returns.
//  4. Rate-limit wait (deps.Limiter.Acquire). ErrRedisDown pauses without
//     committing; ctx.Err() returns without committing.
//  5. Provider call (deps.Provider.Send).
//  6. Classify + Tx B (deps.Store.RecordOutcome). Tx B failure leaves
//     the offset uncommitted — Layer 2 catches the redelivery.
//  7. Commit Kafka offset.
func handleRecord(ctx context.Context, deps Deps, rec *kgo.Record) {
	msg, errCode, errDetails, _ := decodeAndValidate(rec.Value)
	if errCode != "" {
		// T8 path: route the corrupt message to send.<channel>.dlq
		// (and, when the payload identified a target row, transition
		// it to FAILED + emit events.notification) per
		// docs/design/06-idempotency.md §T8 +
		// docs/phases/03-resilience.md §4.
		handleUnprocessable(ctx, deps, rec, msg, errCode, errDetails)
		return
	}

	// decodeAndValidate guarantees msg, msg.ID parses, msg.Attempt > 0,
	// msg.Recipient != "", and msg.Content != nil from here on. The
	// uuid.Parse below cannot fail (it would have surfaced as
	// schema_mismatch above), but the explicit branch keeps the worker
	// from silently producing a corrupt downstream INSERT if a future
	// refactor changes decodeAndValidate's invariants.
	notificationID, err := uuid.Parse(msg.ID)
	if err != nil {
		deps.Logger.Error("worker: id passed decodeAndValidate but failed re-parse (programmer bug)",
			"id", msg.ID,
			"err", err,
		)
		commitRecord(ctx, deps, rec)
		return
	}

	// Layer 1: state guard. The read is non-transactional; Layer 2 +
	// Layer 3 are the actual race-safety mechanisms. A Postgres call
	// failure here surfaces as an error and leaves the offset
	// uncommitted (Kafka redelivers).
	guard, err := CheckStateGuard(ctx, deps.Store, notificationID, msg.Attempt)
	if err != nil {
		deps.Logger.Error("worker: state guard read failed; will be redelivered",
			"id", msg.ID,
			"attempt", msg.Attempt,
			"err", err,
		)
		return
	}
	if guard != GuardProceed {
		deps.Logger.Debug("worker: state guard short-circuited record",
			"id", msg.ID,
			"attempt", msg.Attempt,
			"outcome", guard.String(),
		)
		commitRecord(ctx, deps, rec)
		return
	}

	// Layer 2: separate-tx INSERT. started_at uses the worker's clock
	// (deps.Clock) per docs/design/06-idempotency.md §Layer 2 +
	// docs/phases/03-resilience.md §2.2.
	startedAt := deps.Clock()
	started, err := deps.Store.BeginAttempt(ctx, notificationID, msg.Attempt, startedAt)
	if err != nil {
		deps.Logger.Error("worker: begin attempt failed; will be redelivered",
			"id", msg.ID,
			"attempt", msg.Attempt,
			"err", err,
		)
		return
	}
	if !started {
		deps.Logger.Debug("worker: layer 2 conflict; another worker started this attempt",
			"id", msg.ID,
			"attempt", msg.Attempt,
		)
		commitRecord(ctx, deps, rec)
		return
	}

	// Rate-limit wait. Sits between Layer 2 and the provider call per
	// docs/design/06-idempotency.md §Worker-layer + §4.3 of
	// ARCHITECTURE_v3.md ("directly before the provider call"). Tokens
	// are scarce; we burn them only on calls actually committed.
	if err := deps.Limiter.Acquire(ctx, deps.Channel); err != nil {
		if errors.Is(err, ratelimit.ErrRedisDown) {
			deps.Logger.Warn("worker: rate-limit acquire failed; pausing record (Kafka redelivers)",
				"id", msg.ID,
				"attempt", msg.Attempt,
				"err", err,
			)
			// Cancellable wait so a graceful shutdown during a Redis
			// outage isn't blocked behind the full timer.
			select {
			case <-ctx.Done():
			case <-time.After(redisDownBackoff):
			}
			return
		}
		// Graceful shutdown (ctx.Canceled / DeadlineExceeded) or any
		// other unexpected error. Leave the offset uncommitted; the
		// next worker run will re-poll and Layer 2 will short-circuit
		// the duplicate.
		deps.Logger.Info("worker: rate-limit acquire returned; not committing",
			"id", msg.ID,
			"attempt", msg.Attempt,
			"err", err,
		)
		return
	}

	result := deps.Provider.Send(ctx, msg.Recipient, msg.Channel, *msg.Content)
	finishedAt := deps.Clock()

	outcome := Classify(result, msg.Attempt, finishedAt)

	eventPayloadJSON, err := buildEventPayload(notificationID, *msg, outcome, finishedAt)
	if err != nil {
		// json.Marshal of a fixed-shape struct cannot fail in normal
		// flow, but the explicit branch keeps the worker from
		// silently producing a corrupt outbox row if it ever does.
		// Treat as RecordOutcome failure (no commit) — Kafka redelivers
		// and Layer 2 catches the duplicate.
		deps.Logger.Error("worker: marshal event payload failed; will be redelivered",
			"id", msg.ID,
			"attempt", msg.Attempt,
			"err", err,
		)
		return
	}

	in := store.OutcomeInput{
		NotificationID: notificationID,
		Attempt:        msg.Attempt,
		// StartedAt is ignored by the Phase 3 RecordOutcome
		// implementation (Layer 2 set started_at in its own tx). The
		// field stays in OutcomeInput for binary compatibility per
		// docs/phases/03-resilience.md §2.3; passing it here keeps
		// the call site self-documenting if a Phase 4+ test reads it.
		StartedAt:        startedAt,
		FinishedAt:       finishedAt,
		Classification:   outcome.Classification,
		ResponseJSON:     validResponseJSON(outcome.ResponseBody),
		ErrorMessage:     outcome.ErrorMessage,
		NewStatus:        outcome.NewStatus,
		NewEligibleAt:    outcome.NewEligibleAt,
		NewFailureReason: outcome.FailureReason,
		EventPayload:     eventPayloadJSON,
	}

	if err := deps.Store.RecordOutcome(ctx, in); err != nil {
		deps.Logger.Error("worker: record outcome failed; will be redelivered",
			"id", msg.ID,
			"attempt", msg.Attempt,
			"err", err,
		)
		return
	}

	deps.Logger.Debug("worker: outcome recorded",
		"id", msg.ID,
		"attempt", msg.Attempt,
		"classification", outcome.Classification,
		"new_status", outcome.NewStatus,
	)

	commitRecord(ctx, deps, rec)
}

// commitRecord acks one record's offset and logs on failure. A commit
// failure here doesn't change the row's authoritative state (Tx B
// already committed) — Kafka simply redelivers; Phase 3's Layer 2
// `ON CONFLICT DO NOTHING` on delivery_attempts catches the duplicate
// (and Phase 3 Chunk 4's RecordUnprocessable wraps its own INSERT in
// `ON CONFLICT DO NOTHING` so a redelivery on the T8 path is harmless
// per docs/phases/03-resilience.md §Chunk 4 notes).
func commitRecord(ctx context.Context, deps Deps, rec *kgo.Record) {
	if err := deps.Consumer.CommitRecords(ctx, rec); err != nil {
		deps.Logger.Error("worker: commit kafka offset failed",
			"topic", rec.Topic,
			"partition", rec.Partition,
			"offset", rec.Offset,
			"err", err,
		)
	}
}

// handleUnprocessable runs the T8 transaction for a corrupt Kafka
// record per docs/design/06-idempotency.md §T8 +
// docs/phases/03-resilience.md §4. The record's payload is either
// undecodable (msg == nil) or decoded but invalid (msg != nil); either
// way the disposition is the same: build the
// store.UnprocessableInput, run RecordUnprocessable, commit the
// offset.
//
// On a Postgres failure inside RecordUnprocessable the offset is left
// uncommitted (Kafka redelivers; the same T8 path runs on retry; the
// targeted-branch INSERT is idempotent via ON CONFLICT DO NOTHING per
// the store's RecordUnprocessable docstring). On a BuildUnprocessable
// failure (json.Marshal of a fixed-shape struct, which can't fail in
// normal flow) we log + commit + return — the message is
// unrecoverable as far as the worker can tell, and replaying it would
// loop forever.
func handleUnprocessable(ctx context.Context, deps Deps, rec *kgo.Record, msg *sendPayload, errCode, errDetails string) {
	in, err := BuildUnprocessable(rec, msg, errCode, errDetails, deps.Channel, deps.Clock())
	if err != nil {
		deps.Logger.Error("worker: build unprocessable input failed; committing offset to prevent redelivery loop",
			"topic", rec.Topic,
			"partition", rec.Partition,
			"offset", rec.Offset,
			"err_code", errCode,
			"err_details", errDetails,
			"err", err,
		)
		commitRecord(ctx, deps, rec)
		return
	}

	if err := deps.Store.RecordUnprocessable(ctx, in); err != nil {
		deps.Logger.Error("worker: record unprocessable failed; will be redelivered",
			"topic", rec.Topic,
			"partition", rec.Partition,
			"offset", rec.Offset,
			"err_code", errCode,
			"err_details", errDetails,
			"err", err,
		)
		return
	}

	// Targeted vs no-target log differentiation makes the operator's
	// "what was this corrupt message" investigation start with the
	// right narrowing.
	if in.NotificationID != nil && in.Attempt != nil {
		deps.Logger.Warn("worker: unprocessable message routed to DLQ (targeted)",
			"id", in.NotificationID.String(),
			"attempt", *in.Attempt,
			"err_code", errCode,
			"err_details", errDetails,
			"channel", deps.Channel,
		)
	} else {
		deps.Logger.Warn("worker: unprocessable message routed to DLQ (no target)",
			"topic", rec.Topic,
			"partition", rec.Partition,
			"offset", rec.Offset,
			"err_code", errCode,
			"err_details", errDetails,
			"channel", deps.Channel,
		)
	}

	commitRecord(ctx, deps, rec)
}

// buildEventPayload constructs the events.notification body per
// docs/design/04-kafka.md §2. The Phase 2 worker always sets
// previous_status to DISPATCHED (the only state from which it acts)
// and classification to "success" or "transient" (the Phase 2
// taxonomy from §10).
func buildEventPayload(id uuid.UUID, msg sendPayload, outcome Outcome, occurredAt time.Time) (json.RawMessage, error) {
	payload := eventPayload{
		Version:        eventPayloadVersion,
		ID:             id.String(),
		BatchID:        nil, // Phase 2 single-create only; Phase 4 ships batch
		Channel:        msg.Channel,
		Attempt:        msg.Attempt,
		PreviousStatus: previousStatusDispatched,
		CurrentStatus:  outcome.NewStatus,
		Classification: outcome.Classification,
		FailureReason:  outcome.FailureReason,
		OccurredAt:     occurredAt.UTC().Format(occurredAtFormat),
	}
	return json.Marshal(payload)
}

// validResponseJSON returns the bytes only when they parse as JSON.
// delivery_attempts.response is a JSONB column
// (docs/design/01-schema.md §2); pgx rejects non-JSON bytes against
// JSONB. The assessment provider (webhook.site) always returns JSON,
// so the happy path passes through. A defective provider that returns
// HTML / plain text drops the body to NULL rather than failing the
// outcome transaction — the classification + status values are still
// recorded.
func validResponseJSON(b []byte) json.RawMessage {
	if len(b) == 0 || !json.Valid(b) {
		return nil
	}
	return json.RawMessage(b)
}

// applyDefaults fills in zero-valued Deps fields with the locked
// defaults. Same shape as internal/dispatcher and internal/relay.
//
// Panics when Deps.Limiter is nil per docs/phases/03-resilience.md
// §Chunk 2: production wiring (cmd.go) always provides a
// *ratelimit.Bucket and a missing limiter is a programmer bug, not a
// recoverable misconfiguration. Tests that exercise Loop must inject
// a limiter (typically a no-op fixture or an ErrRedisDown fake).
//
// Phase 3 Chunk 7 also requires Channel to be set explicitly: the
// Phase 2 fallback to "sms" was a single-channel-only convenience
// that, with three channels in flight, would silently route an
// email or push worker through the wrong rate-limit key + log
// labels if the cmd.go wiring forgot to set it. Production wiring
// (cmd.go runForChannel) always sets Channel; tests that exercise
// Loop must too.
func applyDefaults(d Deps) Deps {
	if d.Limiter == nil {
		panic("worker.Loop: Deps.Limiter must not be nil; production wiring always provides one")
	}
	if d.Channel == "" {
		panic("worker.Loop: Deps.Channel must be set; production wiring always sets it (cmd.go runForChannel)")
	}
	if d.Logger == nil {
		d.Logger = slog.Default()
	}
	if d.Clock == nil {
		d.Clock = time.Now
	}
	return d
}
