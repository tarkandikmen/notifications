// Package kafkaadmin wraps franz-go's kadm client with a narrow helper —
// MaxLag — that the dispatcher's circuit breaker and the reaper's cycle
// skip both depend on per docs/design/02-state-machine.md §Transitions
// (rows T2, T9, T10) and §Lag-query failure semantics.
//
// The wrapper is intentionally minimal: one struct, one read method.
// Owning the Kafka admin lifecycle here keeps the dispatcher and reaper
// packages from each having to know about *kgo.Client construction, and
// keeps the lag query's shape — group + topic in, single int64 out —
// uniform across the two call sites.
package kafkaadmin

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// defaultLagQueryTimeout is the per-call ceiling on a MaxLag invocation,
// inlined from docs/design/07-constants.md §H
// (kafka_admin_lag_query_timeout = 5 s). Documented here so a future
// caller that forgets to wrap ctx with its own deadline still gets a
// bounded request; the dispatcher and reaper also wrap ctx at the call
// site (per docs/phases/03-resilience.md §7 / §8), and the earlier of
// the two deadlines wins.
const defaultLagQueryTimeout = 5 * time.Second

// LagClient queries Kafka admin for consumer-group lag.
//
// Construction owns a *kgo.Client that the kadm.Client wraps; Close
// releases both. The raw client is kept on the struct (rather than
// discarded after wrapping) because kadm.NewClient does not take
// ownership — *kgo.Client.Close is the only way to release the
// underlying connections and goroutines.
type LagClient struct {
	raw     *kgo.Client
	client  *kadm.Client
	timeout time.Duration
}

// New constructs a LagClient against the given brokers. The underlying
// kgo.Client is built with kgo.SeedBrokers only — no producer / consumer
// options — because this client is admin-only.
//
// Caller is expected to wrap ctx with a deadline per
// docs/phases/03-resilience.md §7 (dispatcher) and §8 (reaper) before
// calling MaxLag; the struct's timeout field is informational and
// documents the constant from docs/design/07-constants.md §H
// (kafka_admin_lag_query_timeout).
func New(brokers []string) (*LagClient, error) {
	if len(brokers) == 0 {
		return nil, errors.New("kafkaadmin: no brokers configured")
	}

	raw, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, fmt.Errorf("kafkaadmin: build kgo client: %w", err)
	}

	return &LagClient{
		raw:     raw,
		client:  kadm.NewClient(raw),
		timeout: defaultLagQueryTimeout,
	}, nil
}

// Close releases the underlying Kafka client. Safe to call on a nil
// receiver so deferred cleanup in cmd.go is unconditional.
func (l *LagClient) Close() {
	if l == nil || l.raw == nil {
		return
	}
	l.raw.Close()
}

// Timeout returns the per-call timeout this LagClient was configured
// with. Callers (dispatcher + reaper cmd.go) use it to set their
// Deps.LagTimeout so the constant from docs/design/07-constants.md §H
// lives in exactly one place.
func (l *LagClient) Timeout() time.Duration {
	if l == nil {
		return defaultLagQueryTimeout
	}
	return l.timeout
}

// MaxLag returns max(end_offset - committed_offset) across every
// partition of topic for group.
//
// Lag semantics:
//
//   - A partition with no committed offset is treated as committed = 0
//     (a new consumer would start at the log's start offset under the
//     locked auto.offset.reset=earliest from docs/design/04-kafka.md §6).
//     The dispatcher's fail-open posture means a fresh group with a
//     fully-stocked topic correctly reports the full backlog as lag.
//   - A group / topic combination with no end-offset entries (topic
//     missing, no partitions reported) returns 0 — the topic is either
//     unborn or empty, and there is no lag to circuit-break on.
//   - Per-partition errors (UnknownTopicOrPartition, etc.) on either
//     the fetch-offsets or list-end-offsets calls surface as
//     (-1, error). Callers translate per their fail-open / fail-closed
//     policy from docs/design/02-state-machine.md §Lag-query failure
//     semantics.
//
// Two admin round trips per call (FetchOffsetsForTopics + ListEndOffsets)
// rather than the convenience kadm.Client.Lag, because kadm's Lag walks
// group members to build its result and returns an empty map for a
// group with no current members — which masks the backlog on a freshly
// committed group whose worker has not yet rejoined. The two-call
// shape sidesteps that and is straightforward to reason about.
func (l *LagClient) MaxLag(ctx context.Context, group, topic string) (int64, error) {
	if l == nil || l.client == nil {
		return -1, errors.New("kafkaadmin: lag client not initialized")
	}

	fetched, err := l.client.FetchOffsetsForTopics(ctx, group, topic)
	if err != nil {
		// A group that has never had any member or commit surfaces as
		// one of the "no coordinator yet" / "group unknown" errors. Per
		// docs/phases/03-resilience.md §7 the empty-group disposition
		// is "no committed offsets → treat as committed = 0," not
		// "return an error." Falling through with an empty fetched
		// map produces exactly that: every partition's Lookup misses
		// and defaults to committed = 0, so lag = end_offset.
		if !isUninitializedGroupErr(err) {
			return -1, fmt.Errorf("kafkaadmin: fetch offsets: %w", err)
		}
		fetched = kadm.OffsetResponses{}
	}

	endOffsets, err := l.client.ListEndOffsets(ctx, topic)
	if err != nil {
		return -1, fmt.Errorf("kafkaadmin: list end offsets: %w", err)
	}

	partEnds, ok := endOffsets[topic]
	if !ok || len(partEnds) == 0 {
		return 0, nil
	}

	var maxLag int64
	for partition, listed := range partEnds {
		// kadm.Client.ListEndOffsets surfaces a non-existent topic as a
		// single partition=-1 entry with UnknownTopicOrPartition. Skip
		// that sentinel; it is the "topic absent" answer documented on
		// kadm.Client.ListEndOffsets, not a real partition.
		if partition < 0 {
			continue
		}
		if listed.Err != nil {
			return -1, fmt.Errorf("kafkaadmin: end offset for %s/%d: %w", topic, partition, listed.Err)
		}

		var committed int64
		if got, hit := fetched.Lookup(topic, partition); hit {
			if got.Err != nil {
				return -1, fmt.Errorf("kafkaadmin: committed offset for %s/%d: %w", topic, partition, got.Err)
			}
			// FetchOffsetsForTopics returns At = -1 for partitions that
			// have no commit yet. Treat that as "consumer starts at the
			// log start offset"; the per-partition default of 0 below
			// catches it.
			if got.At >= 0 {
				committed = got.At
			}
		}

		lag := listed.Offset - committed
		// Defensive: a commit that overran the log end (broker offsets
		// reset, segment deletion) produces a negative delta. Round to
		// zero rather than reporting a misleading negative number.
		if lag < 0 {
			lag = 0
		}
		if lag > maxLag {
			maxLag = lag
		}
	}

	return maxLag, nil
}

// isUninitializedGroupErr identifies the family of broker responses
// that mean "the consumer group has never been registered with this
// cluster" — there is no coordinator elected, no metadata stored, no
// commits to fetch. Per docs/phases/03-resilience.md §7 this is the
// "empty group → no committed offsets → committed = 0" disposition;
// the caller falls through with an empty fetched map rather than
// returning an error.
//
// We treat all four codes as the same disposition because the broker's
// internal state machine moves through them as a group spins up (load
// in progress → not coordinator → coordinator not available → group
// id not found), and the difference is invisible to the dispatcher /
// reaper. A genuine outage where a real coordinator is unreachable
// produces a network-layer error rather than these protocol codes; it
// continues to surface as a wrapped error, triggering the dispatcher's
// fail-open / reaper's fail-closed branches per
// docs/design/02-state-machine.md §Lag-query failure semantics.
func isUninitializedGroupErr(err error) bool {
	return errors.Is(err, kerr.CoordinatorNotAvailable) ||
		errors.Is(err, kerr.CoordinatorLoadInProgress) ||
		errors.Is(err, kerr.NotCoordinator) ||
		errors.Is(err, kerr.GroupIDNotFound)
}
