package reaper

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/tarkandikmen/notifications/internal/metrics"
	"github.com/tarkandikmen/notifications/internal/observability"
	"github.com/tarkandikmen/notifications/internal/store"
	"github.com/tarkandikmen/notifications/internal/worker"
)

// Phase 2 / Phase 3 reaper tunables. Values inlined from
// docs/design/07-constants.md §A (reaper_interval), §B
// (reaper_stuck_threshold), §C (reaper_lag_threshold), §D
// (max_attempts, reaper_backoff_cap), §H (kafka_admin_lag_query_timeout);
// named constants live here so the loop's call sites read declaratively.
// Tests override via Deps fields.
const (
	defaultInterval       = 60 * time.Second
	defaultStuckThreshold = 120 * time.Second
	defaultMaxAttempts    = 7

	// defaultReaperBackoffCap is reaper_backoff_cap from
	// docs/design/07-constants.md §D ("reaper_backoff_cap = 8"). The
	// cap limits the SQL-side deterministic backoff exponent inside
	// store.ReapStuck; the post-pass jitter step further constrains
	// the result via worker.ReaperBackoff which uses the same cap, so
	// the eventual eligible_at lands within the documented
	// `[deterministic_capped/2, deterministic_capped]` range from
	// docs/design/05-retry.md §3.
	defaultReaperBackoffCap = 8

	// defaultLagThreshold is reaper_lag_threshold from
	// docs/design/07-constants.md §C. When the worker consumer group's
	// max-across-partitions lag on send.<channel> exceeds this value,
	// the reaper's runOnce skips the cycle (per
	// docs/design/02-state-machine.md §Reaper cycle skip + Phase 3
	// docs/phases/03-resilience.md §8).
	defaultLagThreshold = int64(1000)

	// defaultLagTimeout is kafka_admin_lag_query_timeout from
	// docs/design/07-constants.md §H. Caps any single lag-query
	// admin call so a hung Kafka coordinator can't lock up the
	// reaper's tick — the timeout firing routes through the
	// fail-closed branch in runOnce.
	defaultLagTimeout = 5 * time.Second
)

// defaultChannels is the channel set the reaper iterates for the
// per-channel lag check. Hardcoded to the full Phase 3 set
// {sms, email, push} because the reaper's cycle is global (the
// stuck-row sweep is channel-agnostic) and a stalled pipeline on any
// one channel must pause recovery for the whole cycle per
// docs/phases/03-resilience.md §8.
//
// Returned by value (slice literal) on every call so callers can't
// mutate the package-level state.
func defaultChannels() []string { return []string{"sms", "email", "push"} }

// LagQuery is the slim interface runOnce uses to check Kafka
// consumer-group lag before running ReapStuck. Defined here (rather
// than imported from internal/kafkaadmin) so the loop is independently
// testable with a fake — loop_test.go injects a fakeLag that returns
// programmed (int64, error) pairs per channel.
//
// *kafkaadmin.LagClient satisfies it for production; cmd.go wires the
// real client into Deps.Lag.
type LagQuery interface {
	MaxLag(ctx context.Context, group, topic string) (int64, error)
}

// Deps is the reaper loop's per-process dependency bundle. The shape
// mirrors internal/dispatcher/loop.go's Deps for consistency: storage +
// logger + injectable knobs, plus the Phase 3 lag-aware fields.
//
// The loop holds *store.Store directly rather than wrapping it — Phase
// 2's only loop-level test (loop_test.go) is the integration test that
// always runs against a real Postgres testcontainer, so there is no
// handler-style fake to satisfy.
//
// Phase 3 Chunk 6 adds Lag / LagTimeout / LagThreshold / Channels /
// ReaperBackoffCap so the per-cycle reap can fail-closed when Kafka
// consumer-group lag exceeds the reaper_lag_threshold per
// docs/design/02-state-machine.md §Reaper cycle skip + §Lag-query
// failure semantics, and so the post-pass equal-jitter UPDATE has the
// same cap the SQL used.
type Deps struct {
	Store  *store.Store
	Logger *slog.Logger

	// ApplyResetEligibleAt runs the post–T9 jitter UPDATE (same
	// contract as store.Store.ApplyResetEligibleAt). Nil means
	// Store.ApplyResetEligibleAt is used. Tests may inject a stub
	// that returns an error to exercise reaper_post_pass_jitter_failures_total.
	ApplyResetEligibleAt func(ctx context.Context, ids []uuid.UUID, eligibleAt []time.Time) error

	Interval       time.Duration
	StuckThreshold time.Duration
	MaxAttempts    int

	// ReaperBackoffCap is the exponent ceiling for the SQL-side
	// deterministic backoff in store.ReapStuck and for the Go-side
	// equal-jitter recompute in worker.ReaperBackoff. Zero-valued at
	// construction; applyDefaults fills in defaultReaperBackoffCap
	// (reaper_backoff_cap from docs/design/07-constants.md §D).
	ReaperBackoffCap int

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

	// LagThreshold is the per-channel cycle-skip threshold. Zero-valued
	// at construction; applyDefaults fills in defaultLagThreshold
	// (reaper_lag_threshold from docs/design/07-constants.md §C).
	// Surfaced on Deps (rather than inlined) so the integration test
	// can drive the threshold-crossing branch deterministically.
	LagThreshold int64

	// Channels is the channel set the lag check iterates. Zero-valued
	// at construction; applyDefaults fills in defaultChannels (the
	// full Phase 3 set {sms, email, push}). Tests can narrow this to
	// a single channel for cleaner assertions; production always uses
	// the full set.
	Channels []string

	// Now returns the wall-clock time the reaper uses when computing
	// post-pass jitter eligible_at values. Zero-valued at construction;
	// applyDefaults fills in time.Now. Tests can pin this to a known
	// value for deterministic assertions on the jittered eligible_at
	// range without having to also pin the reaper's tick rate.
	Now func() time.Time

	// Tracer is the OpenTelemetry tracer used to open the per-cycle
	// reaper.cycle span. Required; applyDefaults panics when nil
	// to mirror the Phase 3 lag-client convention. Production
	// (cmd.go) injects otel.Tracer(serviceName) backed by the global
	// tracer provider; tests inject a noop tracer or an in-memory
	// tracetest provider.
	//
	// docs/phases/05-observability.md §7.
	Tracer trace.Tracer
}

// Loop drives the stuck-row recovery cycle until ctx is cancelled.
// Returns nil on graceful shutdown; never returns an error in Phase 2 /
// Phase 3 — per-tick failures are logged at warn and the next tick
// retries.
//
// The loop name avoids colliding with the package's cobra-bound Run
// from cmd.go. The spec writes "loop.Run(ctx, deps)" in
// docs/phases/02-walking-skeleton.md §Repo layout, but loop.go and
// cmd.go share a package; renaming the loop entry to Loop preserves the
// cobra convention without splitting the package. Same shape as
// internal/dispatcher/loop.go and internal/relay/loop.go.
//
// docs/phases/02-walking-skeleton.md §11 + docs/phases/03-resilience.md §6 + §8.
func Loop(ctx context.Context, deps Deps) error {
	deps = applyDefaults(deps)

	deps.Logger.Info("loop started",
		"mode", "reaper",
		"interval", deps.Interval,
		"stuck_threshold", deps.StuckThreshold,
		"max_attempts", deps.MaxAttempts,
		"reaper_backoff_cap", deps.ReaperBackoffCap,
		"channels", deps.Channels,
		"lag_threshold", deps.LagThreshold,
		"lag_timeout", deps.LagTimeout,
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
//   - Lag check per channel: skip the cycle (fail-closed) on any
//     channel whose worker.<channel> consumer-group lag exceeds
//     reaper_lag_threshold OR whose lag query errors. Conservative —
//     a single overloaded channel pauses recovery for every channel.
//     Per docs/design/02-state-machine.md §Reaper cycle skip +
//     §Lag-query failure semantics rows T9 / T10 +
//     docs/phases/03-resilience.md §8.
//   - T9 (DISPATCHED → PENDING) for stuck rows below max_attempts. No
//     events.notification emission (docs/design/04-kafka.md §2 row T9).
//     The SQL stamps a deterministic eligible_at; the loop then runs
//     the post-pass equal-jitter UPDATE per
//     docs/phases/03-resilience.md §6.
//   - T10 (DISPATCHED → FAILED) for stuck rows at max_attempts, with one
//     events.notification outbox row per affected notification.
//
// T9 + T10 happen in a single store.ReapStuck transaction; on any error
// the deferred rollback inside ReapStuck fires and the rows stay
// DISPATCHED for the next cycle to retry. The post-pass UPDATE runs
// outside that transaction (one extra round trip) so the SQL layer
// stays integer-arithmetic-only — equal jitter is computed in Go to
// avoid coupling the test surface to Postgres's PRNG. Per
// docs/phases/03-resilience.md §6.
//
// Phase 5 layers per-cycle observability:
//   - One reaper.cycle span per call, attributed with reset / failed
//     counts + outcome (docs/phases/05-observability.md §7).
//   - reaper_cycles_total{outcome} counter on every branch
//     (ran, lag_skip, lag_query_error).
//   - reaper_rows_reset_total / reaper_rows_terminal_failed_total
//     counters Add'd by the per-cycle reset / failed counts.
//   - reaper_cycle_duration_seconds histogram on every branch.
//   - reaper_post_pass_jitter_failures_total counter incremented on
//     the existing log-warn-and-continue branch.
//
// The reaper does NOT stamp an "error" outcome on the cycle counter
// (the spec locks the three outcomes {ran, lag_skip, lag_query_error}
// in docs/phases/05-observability.md §1.1). When ReapStuck or any
// inner SQL call errors, the runOnce return path surfaces the error
// to Loop's per-tick log-warn — but the cycle counter increment for
// "ran" still fires under the deferred outcome-stamp, since the cycle
// was attempted (lag check passed). Operators see the SQL error in
// the warn log, not the metric vocabulary.
//
// Exposed (lowercase) at the package level so loop_test.go can drive a
// single, deterministic cycle rather than racing the time.Ticker — same
// pattern as internal/dispatcher/loop.go and internal/relay/loop.go.
//
// docs/phases/02-walking-skeleton.md §11 + docs/phases/03-resilience.md §6 + §8.
func runOnce(ctx context.Context, deps Deps) error {
	start := time.Now()
	ctx, span := deps.Tracer.Start(ctx, "reaper.cycle")
	outcome := "ran" // overwritten by lag-check skip branches; default for the proceed path
	defer func() {
		span.SetAttributes(attribute.String("outcome", outcome))
		metrics.ReaperCycles.WithLabelValues(outcome).Inc()
		metrics.ReaperCycleDuration.Observe(time.Since(start).Seconds())
		span.End()
	}()

	skipOutcome, skip := lagCheckSkip(ctx, deps)
	if skip {
		outcome = skipOutcome
		return nil
	}

	traceHeaders, terr := observability.TraceHeadersFromContext(ctx)
	if terr != nil {
		deps.Logger.Warn("reaper: trace headers from context failed", "err", terr)
	}
	reset, failed, err := deps.Store.ReapStuck(ctx, deps.MaxAttempts, deps.StuckThreshold, deps.ReaperBackoffCap, traceHeaders)
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("reaper: reap stuck: %w", err)
	}

	span.SetAttributes(
		attribute.Int("reset_rows", len(reset)),
		attribute.Int("failed_rows", failed),
	)
	for _, r := range reset {
		_, rowSpan := deps.Tracer.Start(ctx, "reaper.row",
			trace.WithAttributes(
				attribute.String("notification.id", r.ID.String()),
				attribute.Int("notification.attempt", r.Attempt),
			),
		)
		rowSpan.End()
	}
	metrics.ReaperRowsReset.Add(float64(len(reset)))
	metrics.ReaperRowsTerminalFailed.Add(float64(failed))

	if err := applyJitterPostPass(ctx, deps, reset); err != nil {
		// The reset itself committed; the post-pass jitter is best-effort
		// in the sense that a deterministic eligible_at (already stamped
		// by the SQL) is a safe fallback — the next tick will see the
		// rows as PENDING with the deterministic value, and the
		// dispatcher will claim them correctly. Log at warn so a
		// persistent failure surfaces without throwing away the reset
		// work. Increment the dedicated counter so operators can alert
		// on the rate of post-pass failures.
		metrics.ReaperPostPassJitterFailures.Inc()
		span.RecordError(err)
		deps.Logger.Warn("reaper: post-pass jitter failed; deterministic eligible_at remains in place",
			"reset_count", len(reset),
			"err", err,
		)
	}

	if len(reset) > 0 || failed > 0 {
		deps.Logger.Info("reaper cycle committed",
			"reset", len(reset),
			"failed", failed,
		)
	} else {
		deps.Logger.Debug("reaper cycle committed",
			"reset", len(reset),
			"failed", failed,
		)
	}
	return nil
}

// lagCheckSkip returns the (outcome, skip) pair for the per-cycle
// lag check. Three branches:
//
//   - ("", false): every channel's lag query succeeded and lag is at
//     or below the threshold; the caller proceeds with the reap and
//     stamps "ran" on the cycle counter.
//   - ("lag_skip", true): some channel reported lag > threshold; the
//     caller stamps "lag_skip" and returns nil.
//   - ("lag_query_error", true): some channel's lag query errored;
//     the caller fails closed (per docs/design/02-state-machine.md
//     §Lag-query failure semantics rows T9 / T10) and stamps
//     "lag_query_error" on the cycle counter.
//
// The check iterates every channel in deps.Channels in order; the
// first channel that crosses the threshold or errors short-circuits
// the rest (subsequent channels' lag is irrelevant once we've decided
// to skip). The lag query wraps deps.LagTimeout around the parent ctx
// so a hung admin call surfaces as ctx.DeadlineExceeded — the
// fail-closed disposition still skips the cycle, and the next tick
// retries.
//
// Each successful query is also published to the
// kafka_consumer_lag{group,topic} gauge via metrics.PublishLagSample,
// matching the dispatcher's per-tick publishing. The reaper writes
// to the same gauge series as the dispatcher; Prometheus
// distinguishes the producers via the per-binary `instance` label
// added at scrape time.
func lagCheckSkip(ctx context.Context, deps Deps) (string, bool) {
	for _, channel := range deps.Channels {
		group := "worker." + channel
		topic := "send." + channel

		lagCtx, cancel := context.WithTimeout(ctx, deps.LagTimeout)
		lag, err := deps.Lag.MaxLag(lagCtx, group, topic)
		cancel()
		metrics.PublishLagSample(group, topic, lag, err)

		if err != nil {
			// Fail-closed per docs/design/02-state-machine.md §Lag-query
			// failure semantics rows T9 / T10. Logged at info (not warn)
			// because the disposition is documented and benign — stuck
			// rows stay DISPATCHED through the outage and recover when
			// the broker returns.
			deps.Logger.Info("reaper: lag query failed; skipping cycle (fail-closed)",
				"channel", channel,
				"group", group,
				"topic", topic,
				"threshold", deps.LagThreshold,
				"err", err,
			)
			return "lag_query_error", true
		}
		if lag > deps.LagThreshold {
			deps.Logger.Info("reaper: lag above threshold; skipping cycle",
				"channel", channel,
				"group", group,
				"topic", topic,
				"lag", lag,
				"threshold", deps.LagThreshold,
			)
			return "lag_skip", true
		}
	}
	return "", false
}

// applyJitterPostPass computes worker.ReaperBackoff(attempt) for each
// row the reset SQL just touched and runs the batched UPDATE that
// overwrites eligible_at per row. The UPDATE is guarded by
// status='PENDING' inside store.ApplyResetEligibleAt to avoid stomping
// on a row the dispatcher claimed in the microsecond gap between the
// reset commit and this call.
//
// A zero-length reset slice is a no-op (no SQL fires); the helper
// returns nil so the caller can invoke it unconditionally. Per
// docs/phases/03-resilience.md §6.
func applyJitterPostPass(ctx context.Context, deps Deps, reset []store.ResetReturn) error {
	if len(reset) == 0 {
		return nil
	}
	now := deps.Now()
	ids := make([]uuid.UUID, len(reset))
	eligibleAt := make([]time.Time, len(reset))
	for i, r := range reset {
		ids[i] = r.ID
		eligibleAt[i] = now.Add(worker.ReaperBackoff(r.Attempt))
	}
	fn := deps.ApplyResetEligibleAt
	if fn == nil {
		fn = deps.Store.ApplyResetEligibleAt
	}
	return fn(ctx, ids, eligibleAt)
}

// applyDefaults fills in zero-valued Deps fields with the locked Phase 2 /
// Phase 3 defaults so callers (cmd.go in production, loop_test.go in
// tests) only need to set what they're customizing. Same shape as
// internal/dispatcher and internal/relay.
//
// Lag and Tracer are required: nil values for either panic here so
// production wiring (cmd.go) and tests that exercise runOnce must
// inject both. The Lag interface keeps the loop independently
// testable; the Tracer is similarly satisfied by trace.Tracer
// implementations from go.opentelemetry.io/otel/trace/noop or an
// in-memory tracetest provider. An alternative (treat nil as "no
// spans" / "no lag check") would silently regress the §7 / §8
// behavior under a future cmd.go that forgets to wire them.
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
	if d.ReaperBackoffCap <= 0 {
		d.ReaperBackoffCap = defaultReaperBackoffCap
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
	if len(d.Channels) == 0 {
		d.Channels = defaultChannels()
	}
	if d.Now == nil {
		d.Now = func() time.Time { return time.Now().UTC() }
	}
	if d.Lag == nil {
		panic("reaper: Deps.Lag is required (kafkaadmin.LagClient or fake)")
	}
	if d.Tracer == nil {
		panic("reaper: Deps.Tracer is required (otel.Tracer or noop)")
	}
	return d
}
