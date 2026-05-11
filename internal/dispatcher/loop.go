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

// Phase 2 dispatcher tunables. Values inlined from
// docs/design/07-constants.md §A; named constants live here so the loop's
// call sites read declaratively. Tests override via Deps fields.
const (
	defaultPollInterval = 100 * time.Millisecond
	defaultBatchSize    = 200

	// sendPayloadVersion locks the payload schema version per
	// docs/design/04-kafka.md §1. Bumping this is a breaking change.
	sendPayloadVersion = 1
)

// defaultChannels is the Phase 2 channel set per
// docs/phases/02-walking-skeleton.md §7 ("Phase 3 expands to the full set").
// Returned by value (slice literal) on every call so callers can't mutate
// the package-level state.
func defaultChannels() []string { return []string{"sms"} }

// Deps is the dispatcher loop's per-process dependency bundle. The api
// package's analogous Deps lives in internal/api/handlers.go; the shape
// is intentionally similar (storage + logger + injectable knobs).
//
// The loop holds *store.Store directly rather than an interface because
// Phase 2's only loop-level test is the integration test in loop_test.go —
// it always runs against a real Postgres testcontainer. There is no
// corresponding handler-style fake to satisfy.
type Deps struct {
	Store        *store.Store
	Logger       *slog.Logger
	PollInterval time.Duration
	BatchSize    int
	Channels     []string
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

// runOnce performs a single dispatcher tick for one channel: open a tx,
// claim up to deps.BatchSize PENDING rows, write one outbox row per
// claimed notification, commit. On any error along the way the deferred
// rollback fires, leaving the rows PENDING for the next tick to retry.
//
// Exposed (lowercase) at the package level so loop_test.go can drive a
// single, deterministic tick rather than racing the time.Ticker.
func runOnce(ctx context.Context, deps Deps, channel string) error {
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

// applyDefaults fills in zero-valued Deps fields with the locked Phase 2
// defaults so callers (cmd.go in production, loop_test.go in tests) only
// need to set what they're customizing.
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
	return d
}
