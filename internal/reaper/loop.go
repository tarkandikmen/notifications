package reaper

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/tarkandikmen/notifications/internal/store"
)

// Phase 2 reaper tunables. Values inlined from
// docs/design/07-constants.md §A (reaper_interval), §B
// (reaper_stuck_threshold), §D (max_attempts); named constants live here
// so the loop's call sites read declaratively. Tests override via Deps
// fields.
const (
	defaultInterval       = 60 * time.Second
	defaultStuckThreshold = 120 * time.Second
	defaultMaxAttempts    = 7
)

// Deps is the reaper loop's per-process dependency bundle. The shape
// mirrors internal/dispatcher/loop.go's Deps for consistency: storage +
// logger + injectable knobs.
//
// The loop holds *store.Store directly rather than wrapping it — Phase
// 2's only loop-level test (loop_test.go) is the integration test that
// always runs against a real Postgres testcontainer, so there is no
// handler-style fake to satisfy.
type Deps struct {
	Store          *store.Store
	Logger         *slog.Logger
	Interval       time.Duration
	StuckThreshold time.Duration
	MaxAttempts    int
}

// Loop drives the stuck-row recovery cycle until ctx is cancelled.
// Returns nil on graceful shutdown; never returns an error in Phase 2 —
// per-tick failures are logged at warn and the next tick retries.
//
// The loop name avoids colliding with the package's cobra-bound Run
// from cmd.go. The spec writes "loop.Run(ctx, deps)" in
// docs/phases/02-walking-skeleton.md §Repo layout, but loop.go and
// cmd.go share a package; renaming the loop entry to Loop preserves the
// cobra convention without splitting the package. Same shape as
// internal/dispatcher/loop.go and internal/relay/loop.go.
//
// docs/phases/02-walking-skeleton.md §11.
func Loop(ctx context.Context, deps Deps) error {
	deps = applyDefaults(deps)

	deps.Logger.Info("loop started",
		"mode", "reaper",
		"interval", deps.Interval,
		"stuck_threshold", deps.StuckThreshold,
		"max_attempts", deps.MaxAttempts,
	)

	ticker := time.NewTicker(deps.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			deps.Logger.Info("loop stopped", "mode", "reaper")
			return nil
		case <-ticker.C:
			if err := runOnce(ctx, deps); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return nil
				}
				deps.Logger.Warn("reaper tick failed", "err", err)
			}
		}
	}
}

// runOnce performs a single reaper cycle:
//
//   - T9 (DISPATCHED → PENDING) for stuck rows below max_attempts. No
//     events.notification emission (docs/design/04-kafka.md §2 row T9).
//   - T10 (DISPATCHED → FAILED) for stuck rows at max_attempts, with one
//     events.notification outbox row per affected notification.
//
// Both happen in a single store.ReapStuck transaction; on any error
// the deferred rollback inside ReapStuck fires and the rows stay
// DISPATCHED for the next cycle to retry.
//
// Phase 2 does **not** consult Kafka consumer-group lag here; Phase 3
// wraps the call with the lag-aware skip from
// docs/design/02-state-machine.md §Reaper cycle skip.
//
// Exposed (lowercase) at the package level so loop_test.go can drive a
// single, deterministic cycle rather than racing the time.Ticker — same
// pattern as internal/dispatcher/loop.go and internal/relay/loop.go.
//
// docs/phases/02-walking-skeleton.md §11.
func runOnce(ctx context.Context, deps Deps) error {
	reset, failed, err := deps.Store.ReapStuck(ctx, deps.MaxAttempts, deps.StuckThreshold)
	if err != nil {
		return fmt.Errorf("reaper: reap stuck: %w", err)
	}

	if reset > 0 || failed > 0 {
		deps.Logger.Info("reaper cycle committed",
			"reset", reset,
			"failed", failed,
		)
	} else {
		deps.Logger.Debug("reaper cycle committed",
			"reset", reset,
			"failed", failed,
		)
	}
	return nil
}

// applyDefaults fills in zero-valued Deps fields with the locked Phase 2
// defaults so callers (cmd.go in production, loop_test.go in tests) only
// need to set what they're customizing. Same shape as
// internal/dispatcher and internal/relay.
func applyDefaults(d Deps) Deps {
	if d.Interval <= 0 {
		d.Interval = defaultInterval
	}
	if d.StuckThreshold <= 0 {
		d.StuckThreshold = defaultStuckThreshold
	}
	if d.MaxAttempts <= 0 {
		d.MaxAttempts = defaultMaxAttempts
	}
	if d.Logger == nil {
		d.Logger = slog.Default()
	}
	return d
}
