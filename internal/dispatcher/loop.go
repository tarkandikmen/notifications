package dispatcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/tarkandikmen/notifications/internal/store"
)

// Phase 2 / Phase 3 dispatcher tunables. Values inlined from
// docs/design/07-constants.md §A (poll interval, batch size) and §C
// (dispatcher_lag_threshold) and §H (kafka_admin_lag_query_timeout);
// named constants live here so the loop's call sites read declaratively.
// Tests override via Deps fields.
const (
	defaultPollInterval = 100 * time.Millisecond
	defaultBatchSize    = 200

	// defaultLagThreshold is dispatcher_lag_threshold from
	// docs/design/07-constants.md §C. When the worker consumer group's
	// max-across-partitions lag on send.<channel> exceeds this value,
	// the dispatcher's runOnce skips the claim (per
	// docs/design/02-state-machine.md §Transitions row T2 + Phase 3
	// docs/phases/03-resilience.md §7).
	defaultLagThreshold = int64(1000)

	// defaultLagTimeout is kafka_admin_lag_query_timeout from
	// docs/design/07-constants.md §H. Caps any single lag-query
	// admin call so a hung Kafka coordinator can't lock up the
	// dispatcher's tick — the timeout firing routes through the
	// fail-open branch in runOnce.
	defaultLagTimeout = 5 * time.Second

	// sendPayloadVersion locks the payload schema version per
	// docs/design/04-kafka.md §1. Bumping this is a breaking change.
	sendPayloadVersion = 1
)

// defaultChannels is the Phase 3 channel set per
// docs/phases/03-resilience.md §7 + §Chunk 7. The dispatcher fans
// claim ticks across all three channels; each channel's send.<channel>
// topic is fed by the per-channel worker pool (one worker binary per
// --channel value, sharing the same dispatcher).
//
// Returned by value (slice literal) on every call so callers can't
// mutate the package-level state.
func defaultChannels() []string { return []string{"sms", "email", "push"} }

// LagQuery is the slim interface runOnce uses to check Kafka
// consumer-group lag before claiming. Defined here (rather than
// imported from internal/kafkaadmin) so the loop is independently
// testable with a fake — Chunk 5's loop_test.go injects a fakeLag
// that returns programmed (int64, error) pairs.
//
// *kafkaadmin.LagClient satisfies it for production; cmd.go wires the
// real client into Deps.Lag.
type LagQuery interface {
	MaxLag(ctx context.Context, group, topic string) (int64, error)
}

// Deps is the dispatcher loop's per-process dependency bundle. The api
// package's analogous Deps lives in internal/api/handlers.go; the shape
// is intentionally similar (storage + logger + injectable knobs).
//
// The loop holds *store.Store directly rather than an interface because
// Phase 2's only loop-level test is the integration test in loop_test.go —
// it always runs against a real Postgres testcontainer. There is no
// corresponding handler-style fake to satisfy.
//
// Phase 3 Chunk 5 adds Lag / LagTimeout / LagThreshold so the per-tick
// claim can fail-open when Kafka consumer-group lag exceeds the
// dispatcher_lag_threshold per docs/design/02-state-machine.md
// §Transitions row T2 + §Lag-query failure semantics.
type Deps struct {
	Store        *store.Store
	Logger       *slog.Logger
	PollInterval time.Duration
	BatchSize    int
	Channels     []string

	// Lag is the consumer-group lag oracle. Required; applyDefaults
	// panics when Lag == nil so production wiring (cmd.go) and tests
	// that exercise runOnce must inject one. The interface keeps the
	// loop independently testable; *kafkaadmin.LagClient satisfies it
	// for production.
	Lag LagQuery

	// LagTimeout caps the lag-query admin call. Zero-valued at
	// construction; applyDefaults fills in defaultLagTimeout
	// (kafka_admin_lag_query_timeout from
	// docs/design/07-constants.md §H).
	LagTimeout time.Duration

	// LagThreshold is the per-channel claim-skip threshold. Zero-valued
	// at construction; applyDefaults fills in defaultLagThreshold
	// (dispatcher_lag_threshold from
	// docs/design/07-constants.md §C). Surfaced on Deps (rather than
	// inlined) so the integration test can drive the threshold-crossing
	// branch deterministically.
	LagThreshold int64
}

// Loop drives the claim-and-publish cycle until ctx is cancelled. Returns
// nil on graceful shutdown; never returns an error in Phase 2 — per-tick
// failures are logged at warn and the next tick retries (the rolled-back
// claim leaves the rows PENDING).
//
// The loop name avoids colliding with the package's cobra-bound Run from
// cmd.go. The spec writes "loop.Run(ctx, deps)" in
// docs/phases/02-walking-skeleton.md §Repo layout, but loop.go and cmd.go
// share a package; renaming the loop entry to Loop preserves the cobra
// convention without splitting the package.
//
// docs/phases/02-walking-skeleton.md §7.
func Loop(ctx context.Context, deps Deps) error {
	deps = applyDefaults(deps)

	deps.Logger.Info("loop started",
		"mode", "dispatcher",
		"poll_interval", deps.PollInterval,
		"batch_size", deps.BatchSize,
		"channels", deps.Channels,
	)

	ticker := time.NewTicker(deps.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			deps.Logger.Info("loop stopped", "mode", "dispatcher")
			return nil
		case <-ticker.C:
			for _, ch := range deps.Channels {
				if err := runOnce(ctx, deps, ch); err != nil {
					if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
						return nil
					}
					deps.Logger.Warn("dispatcher tick failed",
						"channel", ch,
						"err", err,
					)
				}
			}
		}
	}
}

// runOnce performs a single dispatcher tick for one channel: check
// Kafka consumer-group lag, open a tx, claim up to deps.BatchSize
// PENDING rows, write one outbox row per claimed notification, commit.
// On any error along the way the deferred rollback fires, leaving the
// rows PENDING for the next tick to retry.
//
// Lag-check disposition (docs/design/02-state-machine.md §Lag-query
// failure semantics row T2 + docs/phases/03-resilience.md §7):
//
//   - lag > deps.LagThreshold → skip this tick (the worker is falling
//     behind; claiming more would just deepen the backlog). Returns
//     nil so the loop's per-tick error logging stays quiet — a paused
//     dispatcher under sustained load shouldn't spam warn logs every
//     poll interval.
//   - lag query errors → fail-open: log at warn and continue with the
//     claim. The api keeps accepting requests; pausing the dispatcher
//     just delays the inevitable, and the outbox absorbs the backlog
//     while the relay can't publish (per ARCHITECTURE_v3.md §6.9
//     row "dispatcher" under Kafka outage).
//
// Exposed (lowercase) at the package level so loop_test.go can drive a
// single, deterministic tick rather than racing the time.Ticker.
func runOnce(ctx context.Context, deps Deps, channel string) error {
	if skip := lagCheckSkip(ctx, deps, channel); skip {
		return nil
	}

	tx, err := deps.Store.Pool().Begin(ctx)
	if err != nil {
		return fmt.Errorf("dispatcher: begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := deps.Store.ClaimDispatchable(ctx, tx, channel, deps.BatchSize)
	if err != nil {
		return fmt.Errorf("dispatcher: claim dispatchable: %w", err)
	}
	if len(rows) == 0 {
		// Empty claim: rollback (the deferred call) is fine; the tx
		// performed no writes. Returning nil avoids a needless commit.
		return nil
	}

	for i := range rows {
		row := &rows[i]
		payload, err := buildSendPayload(row)
		if err != nil {
			return fmt.Errorf("dispatcher: build payload for %s: %w", row.ID, err)
		}
		idStr := row.ID.String()
		if err := deps.Store.InsertOutboxRow(ctx, tx, store.OutboxRow{
			Topic:        "send." + row.Channel,
			PartitionKey: &idStr,
			Payload:      payload,
		}); err != nil {
			return fmt.Errorf("dispatcher: insert outbox row for %s: %w", row.ID, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("dispatcher: commit: %w", err)
	}

	deps.Logger.Debug("dispatcher tick committed",
		"channel", channel,
		"rows", len(rows),
	)
	return nil
}

// sendPayload mirrors the JSON shape locked in docs/design/04-kafka.md §1.
// Order of fields here matches the doc's example for readability; the
// JSON encoder serializes by struct order so the produced bytes match the
// documented layout 1:1.
//
// Phase 2 always populates Content (api validation rejects the empty
// string) and never populates Template / TemplateData (api validation
// rejects either). The struct keeps the nullable shape so the produced
// JSON renders `template: null, template_data: null` exactly as the doc
// shows.
type sendPayload struct {
	Version      int             `json:"version"`
	ID           string          `json:"id"`
	Attempt      int             `json:"attempt"`
	Channel      string          `json:"channel"`
	Recipient    string          `json:"recipient"`
	Content      *string         `json:"content"`
	Template     *string         `json:"template"`
	TemplateData json.RawMessage `json:"template_data"`
	Priority     int16           `json:"priority"`
}

func buildSendPayload(n *store.Notification) (json.RawMessage, error) {
	p := sendPayload{
		Version:      sendPayloadVersion,
		ID:           n.ID.String(),
		Attempt:      n.Attempt,
		Channel:      n.Channel,
		Recipient:    n.Recipient,
		Content:      n.Content,
		Template:     n.Template,
		TemplateData: n.TemplateData,
		Priority:     n.Priority,
	}
	return json.Marshal(p)
}

// lagCheckSkip returns true when the dispatcher's per-tick claim must
// be skipped because consumer-group lag exceeds the configured
// threshold. Returns false to continue with the claim, including on a
// lag-query error (fail-open per
// docs/design/02-state-machine.md §Lag-query failure semantics row T2).
//
// The lag query wraps deps.LagTimeout around the parent ctx — the
// resulting deadline is the earlier of the two, so a graceful-shutdown
// cancellation on the parent still propagates into the admin call.
func lagCheckSkip(ctx context.Context, deps Deps, channel string) bool {
	group := "worker." + channel
	topic := "send." + channel

	lagCtx, cancel := context.WithTimeout(ctx, deps.LagTimeout)
	defer cancel()

	lag, err := deps.Lag.MaxLag(lagCtx, group, topic)
	if err != nil {
		deps.Logger.Warn("dispatcher: lag query failed; failing open and continuing tick",
			"channel", channel,
			"group", group,
			"topic", topic,
			"threshold", deps.LagThreshold,
			"err", err,
		)
		return false
	}
	if lag > deps.LagThreshold {
		deps.Logger.Info("dispatcher: lag above threshold; skipping tick",
			"channel", channel,
			"group", group,
			"topic", topic,
			"lag", lag,
			"threshold", deps.LagThreshold,
		)
		return true
	}
	return false
}

// applyDefaults fills in zero-valued Deps fields with the locked Phase 2 /
// Phase 3 defaults so callers (cmd.go in production, loop_test.go in
// tests) only need to set what they're customizing.
//
// Lag is required: a nil Lag panics here so production wiring (cmd.go)
// and tests that exercise runOnce must inject one. The interface keeps
// the loop independently testable; an alternative (treat nil as
// "lag check disabled") would silently regress the §7 behavior under a
// future cmd.go that forgets to wire the admin client, and the spec
// explicitly forbids that disposition.
func applyDefaults(d Deps) Deps {
	if d.PollInterval <= 0 {
		d.PollInterval = defaultPollInterval
	}
	if d.BatchSize <= 0 {
		d.BatchSize = defaultBatchSize
	}
	if len(d.Channels) == 0 {
		d.Channels = defaultChannels()
	}
	if d.Logger == nil {
		d.Logger = slog.Default()
	}
	if d.LagTimeout <= 0 {
		d.LagTimeout = defaultLagTimeout
	}
	if d.LagThreshold <= 0 {
		d.LagThreshold = defaultLagThreshold
	}
	if d.Lag == nil {
		panic("dispatcher: Deps.Lag is required (kafkaadmin.LagClient or fake)")
	}
	return d
}
