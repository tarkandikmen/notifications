package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/tarkandikmen/notifications/internal/store"
)

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

// Deps is the worker loop's per-process dependency bundle. The shape
// mirrors internal/dispatcher and internal/relay's Deps for
// consistency: storage + injected externals + logger + injectable
// clock.
//
// Channel labels every log line and lets future Phase 3 metrics tag by
// origin without parsing the consumer group name. Phase 2 ships only
// `sms`; cmd.go fills the field from the --channel flag.
type Deps struct {
	Store    *store.Store
	Consumer Consumer
	Provider Sender
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

// handleRecord processes one Kafka record per docs/phases/02-walking-skeleton.md
// §9 step 2. Decode failures and unsupported versions log + commit +
// skip (Phase 3 routes them to the DLQ via T8). Provider success / fail
// flows through Classify and store.RecordOutcome. RecordOutcome
// failure leaves the offset uncommitted — Kafka redelivers, the
// store's ON CONFLICT DO NOTHING on delivery_attempts plus the
// attempt-guarded UPDATE keep the redelivery harmless (per
// docs/design/06-idempotency.md §Layer 3 + Phase 2's reduced posture
// in §9 of the walking-skeleton doc).
func handleRecord(ctx context.Context, deps Deps, rec *kgo.Record) {
	var msg sendPayload
	if err := json.Unmarshal(rec.Value, &msg); err != nil {
		deps.Logger.Warn("worker: decode send payload failed; skipping (Phase 3 DLQs this)",
			"topic", rec.Topic,
			"partition", rec.Partition,
			"offset", rec.Offset,
			"err", err,
		)
		commitRecord(ctx, deps, rec)
		return
	}

	if msg.Version != sendPayloadVersion {
		deps.Logger.Warn("worker: unsupported send payload version; skipping",
			"version", msg.Version,
			"expected", sendPayloadVersion,
			"topic", rec.Topic,
			"partition", rec.Partition,
			"offset", rec.Offset,
		)
		commitRecord(ctx, deps, rec)
		return
	}

	notificationID, err := uuid.Parse(msg.ID)
	if err != nil {
		deps.Logger.Warn("worker: invalid notification id in send payload; skipping",
			"id", msg.ID,
			"err", err,
		)
		commitRecord(ctx, deps, rec)
		return
	}

	if msg.Content == nil {
		// Phase 2 requires content via api validation
		// (docs/phases/02-walking-skeleton.md §5) so a null content
		// indicates either a stale Phase 6 message (templates land
		// later) or a corrupt payload. Same disposition as the decode
		// failures above.
		deps.Logger.Warn("worker: send payload missing content; skipping",
			"id", msg.ID,
		)
		commitRecord(ctx, deps, rec)
		return
	}

	startedAt := deps.Clock()
	result := deps.Provider.Send(ctx, msg.Recipient, msg.Channel, *msg.Content)
	finishedAt := deps.Clock()

	outcome := Classify(result, msg.Attempt, finishedAt)

	eventPayloadJSON, err := buildEventPayload(notificationID, msg, outcome, finishedAt)
	if err != nil {
		// json.Marshal of a fixed-shape struct cannot fail in normal
		// flow, but the explicit branch keeps the worker from
		// silently producing a corrupt outbox row if it ever does.
		// Treat as RecordOutcome failure (no commit) — Kafka redelivers.
		deps.Logger.Error("worker: marshal event payload failed; will be redelivered",
			"id", msg.ID,
			"attempt", msg.Attempt,
			"err", err,
		)
		return
	}

	in := store.OutcomeInput{
		NotificationID:   notificationID,
		Attempt:          msg.Attempt,
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
// already committed) — Kafka simply redelivers; Phase 2's
// ON CONFLICT DO NOTHING on delivery_attempts catches the duplicate
// (with the gap noted in docs/phases/02-walking-skeleton.md §9
// "Known terminal-state gap").
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

// applyDefaults fills in zero-valued Deps fields with the locked Phase
// 2 defaults. Same shape as internal/dispatcher and internal/relay.
func applyDefaults(d Deps) Deps {
	if d.Logger == nil {
		d.Logger = slog.Default()
	}
	if d.Clock == nil {
		d.Clock = time.Now
	}
	if d.Channel == "" {
		d.Channel = "sms"
	}
	return d
}
