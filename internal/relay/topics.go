package relay

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Phase 3 Kafka topology, locked by docs/design/04-kafka.md §Topic catalog
// and docs/design/07-constants.md §F. Phase 2 shipped only the SMS pipeline
// plus the shared events.notification topic; Phase 3 Chunk 4 adds the per-
// channel send.<channel>.dlq triplet, and Chunk 7 widens the send.<channel>
// fan-out to email + push.
const (
	topicSendSMS            = "send.sms"
	topicSendEmail          = "send.email"
	topicSendPush           = "send.push"
	topicEventsNotification = "events.notification"
	topicSendSMSDLQ         = "send.sms.dlq"
	topicSendEmailDLQ       = "send.email.dlq"
	topicSendPushDLQ        = "send.push.dlq"

	// sendPartitions / eventsPartitions are inlined from
	// docs/design/07-constants.md §F (send_partitions, events_partitions).
	// Both pinned at 20 to give later phases headroom for worker-pool
	// scaling without a re-partition; the per-channel rate cap bounds
	// throughput regardless.
	sendPartitions   = int32(20)
	eventsPartitions = int32(20)

	// dlqPartitions is the per-channel DLQ partition count from
	// docs/design/07-constants.md §F (dlq_partitions = 1). Low-volume by
	// definition (only unprocessable messages reach the DLQ per
	// docs/design/04-kafka.md §3); single-partition keeps replay tooling's
	// ordering trivial. Also matters operationally: docs/design/04-kafka.md
	// §3 ("Key when notification_id is null") specifies that the
	// no-target T8 path produces records with no Kafka key, and
	// dlq_partitions=1 makes the partition assignment deterministic.
	dlqPartitions = int32(1)

	// kafkaReplicationFactorDev is the docker-compose dev cluster's RF
	// from docs/design/07-constants.md §F (kafka_replication_factor_dev).
	// Production targets RF=3; phase 2 / 3 deploy against a single broker.
	kafkaReplicationFactorDev = int16(1)

	// kafkaDLQRetention is the per-DLQ retention period from
	// docs/design/07-constants.md §F (kafka_dlq_retention = 30 days).
	// Longer than the main pipeline (kafka_main_retention = 7 days, the
	// broker default the main topics inherit) so corrupt messages stay
	// available for human investigation while normal traffic ages out.
	kafkaDLQRetention = 30 * 24 * time.Hour
)

// desiredTopics is the canonical (topic → partition count) set
// Bootstrap creates on first run. Order of iteration doesn't matter —
// per-topic CreateTopic calls run independently. Renamed from Phase 2's
// phase2Topics to track Phase 3's expanded set; the name no longer ties
// to a specific phase.
var desiredTopics = map[string]int32{
	topicSendSMS:            sendPartitions,
	topicSendEmail:          sendPartitions,
	topicSendPush:           sendPartitions,
	topicEventsNotification: eventsPartitions,
	topicSendSMSDLQ:         dlqPartitions,
	topicSendEmailDLQ:       dlqPartitions,
	topicSendPushDLQ:        dlqPartitions,
}

// dlqTopics is the set of topics that take the per-topic
// `retention.ms` override during Bootstrap. The main topics (send.<channel>,
// events.notification) inherit the broker default (Kafka's 7-day default,
// matching docs/design/07-constants.md §F kafka_main_retention) so no
// override is needed there.
var dlqTopics = map[string]struct{}{
	topicSendSMSDLQ:   {},
	topicSendEmailDLQ: {},
	topicSendPushDLQ:  {},
}

// Bootstrap creates the desired Kafka topic set on the broker and treats
// TOPIC_ALREADY_EXISTS as success so the call is idempotent across relay
// restarts. Other per-topic errors are reported back to the caller, which
// fails the relay startup so a misconfigured cluster is loud rather than
// silently broken.
//
// Bootstrap is structured in three phases that together absorb the
// startup-time races we observed under heavy parallel testcontainer
// load (and that real production clusters can hit on first deploy or
// during a rolling broker restart):
//
//  1. Wait until the broker actually answers an admin call. confluent-
//     local (and Kafka clusters in general) accepts TCP connections
//     before the broker is fully ready to serve API requests; a fresh
//     kgo client's first ApiVersions handshake against a not-yet-ready
//     broker can fail with "broker closed the connection immediately."
//     waitForBrokerReady polls a cheap ListTopics call until it
//     succeeds, eating that brief window deterministically.
//
//  2. Issue CreateTopic per topic. Same per-topic error handling as
//     before; TOPIC_ALREADY_EXISTS still routes to the success branch.
//
//  3. After each successful CreateTopic (or already-exists), wait until
//     ListTopics reports the topic with at least the requested
//     partition count. CreateTopic returning success only proves the
//     controller accepted the create — it does not guarantee that
//     every broker's metadata cache has the topic, nor (crucially)
//     that a fresh producer's first metadata fetch will see it.
//     Without this step a producer that calls ProduceSync immediately
//     after Bootstrap returns can get UNKNOWN_TOPIC_OR_PARTITION on
//     the first record per topic until franz-go's UnknownTopicRetries
//     budget triggers a metadata refresh — which on a slow / contended
//     broker can run out before the metadata propagates.
//
// docs/phases/02-walking-skeleton.md §8 + docs/phases/03-resilience.md §9.
func Bootstrap(ctx context.Context, brokers []string, logger *slog.Logger) error {
	if len(brokers) == 0 {
		return errors.New("relay bootstrap: no kafka brokers configured")
	}
	if logger == nil {
		logger = slog.Default()
	}

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return fmt.Errorf("relay bootstrap: build admin client: %w", err)
	}
	defer cl.Close()

	adm := kadm.NewClient(cl)

	// Phase 1: gate every subsequent admin call behind proof that the
	// broker is responsive. Bounded by bootstrapBrokerReadyTimeout so a
	// truly broken cluster fails loud instead of hanging.
	readyCtx, readyCancel := context.WithTimeout(ctx, bootstrapBrokerReadyTimeout)
	if err := waitForBrokerReady(readyCtx, adm); err != nil {
		readyCancel()
		return fmt.Errorf("relay bootstrap: broker not ready: %w", err)
	}
	readyCancel()

	// Phase 2 + 3: one round trip per topic keeps each create
	// independent — a bad partition count or config on one topic never
	// blocks another from being created. The per-topic loop is small
	// (seven topics) so the extra latency from the visibility check is
	// negligible vs. the simpler error-handling shape.
	for topic, partitions := range desiredTopics {
		configs := topicConfigs(topic)
		resp, err := adm.CreateTopic(ctx, partitions, kafkaReplicationFactorDev, configs, topic)
		if err != nil {
			if isTopicAlreadyExists(err) {
				logger.Debug("relay bootstrap: topic already exists",
					"topic", topic,
				)
			} else {
				return fmt.Errorf("relay bootstrap: create %q: %w", topic, err)
			}
		} else {
			switch {
			case resp.Err == nil:
				logger.Info("relay bootstrap: topic created",
					"topic", topic,
					"partitions", partitions,
					"replication_factor", kafkaReplicationFactorDev,
				)
			case isTopicAlreadyExists(resp.Err):
				logger.Debug("relay bootstrap: topic already exists",
					"topic", topic,
				)
			default:
				return fmt.Errorf("relay bootstrap: create %q: %w", topic, resp.Err)
			}
		}

		// Visibility check fires whether we just created the topic or
		// observed it as already-existing. The already-exists branch
		// covers a relay restart against a half-bootstrapped broker
		// (e.g., Kafka was restarted mid-create), so we want the same
		// "metadata is queryable" guarantee.
		visibleCtx, visibleCancel := context.WithTimeout(ctx, bootstrapTopicVisibleTimeout)
		if err := waitForTopicVisible(visibleCtx, adm, topic, partitions); err != nil {
			visibleCancel()
			return fmt.Errorf("relay bootstrap: verify %q visible: %w", topic, err)
		}
		visibleCancel()
	}

	return nil
}

// isTopicAlreadyExists treats kerr.TopicAlreadyExists and opaque broker
// errors whose text still carries TOPIC_ALREADY_EXISTS (apache/kafka via
// kadm sometimes fails errors.Is against kerr after kafka-bootstrap already
// created the topic).
func isTopicAlreadyExists(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, kerr.TopicAlreadyExists) {
		return true
	}
	return strings.Contains(err.Error(), "TOPIC_ALREADY_EXISTS")
}

// Bootstrap timing constants. Generous enough to absorb slow Docker
// startup + heavily-loaded brokers without hanging forever on a real
// failure. The poll backoff doubles from initial to max so healthy
// brokers see sub-second total wait while the cap (1 s) keeps the
// cancellation latency bounded.
const (
	bootstrapBrokerReadyTimeout  = 30 * time.Second
	bootstrapTopicVisibleTimeout = 30 * time.Second
	bootstrapPollInitialBackoff  = 50 * time.Millisecond
	bootstrapPollMaxBackoff      = 1 * time.Second
)

// waitForBrokerReady polls ListTopics until it returns without error or
// ctx expires. ListTopics is one of the cheapest admin-API round trips
// and serves as a proxy for "the broker is answering ApiVersions and
// admin requests." Used by Bootstrap (Phase 1) to absorb the brief
// post-listen window where confluent-local accepts TCP but isn't yet
// serving the API.
func waitForBrokerReady(ctx context.Context, adm *kadm.Client) error {
	return retryUntilSuccess(ctx, func(ctx context.Context) error {
		_, err := adm.ListTopics(ctx)
		return err
	})
}

// waitForTopicVisible polls ListTopics(ctx, topic) until kadm reports
// the topic with at least the requested partition count, or ctx
// expires. Verifies partition count (not just topic existence) so a
// stale topic from a prior run with the wrong shape would surface as a
// Bootstrap failure rather than silently leaving an under-partitioned
// cluster in place. d.Err is also surfaced — an UnknownTopicOrPartition
// per-topic error during metadata propagation is exactly what this
// helper exists to retry past.
func waitForTopicVisible(ctx context.Context, adm *kadm.Client, topic string, partitions int32) error {
	return retryUntilSuccess(ctx, func(ctx context.Context) error {
		details, err := adm.ListTopics(ctx, topic)
		if err != nil {
			return err
		}
		d, ok := details[topic]
		if !ok {
			return fmt.Errorf("topic %q not yet in metadata response", topic)
		}
		if d.Err != nil {
			return fmt.Errorf("topic %q metadata error: %w", topic, d.Err)
		}
		if int32(len(d.Partitions)) < partitions {
			return fmt.Errorf("topic %q has %d partition(s); want >= %d (metadata still propagating)",
				topic, len(d.Partitions), partitions)
		}
		return nil
	})
}

// retryUntilSuccess invokes fn repeatedly with exponential backoff
// (capped at bootstrapPollMaxBackoff) until fn returns nil or ctx
// expires. On expiry the most recent fn error is wrapped into ctx.Err()
// so the caller's diagnostic preserves both the timeout signal and the
// underlying cause (e.g., "deadline exceeded (last error: topic foo
// metadata error: UNKNOWN_TOPIC_OR_PARTITION)").
//
// Shared between waitForBrokerReady and waitForTopicVisible so the
// retry shape (initial backoff, cap, ctx semantics) lives in one place.
func retryUntilSuccess(ctx context.Context, fn func(context.Context) error) error {
	backoff := bootstrapPollInitialBackoff
	var lastErr error
	for {
		if err := fn(ctx); err == nil {
			return nil
		} else {
			lastErr = err
		}
		select {
		case <-ctx.Done():
			if lastErr != nil {
				return fmt.Errorf("%w (last error: %v)", ctx.Err(), lastErr)
			}
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > bootstrapPollMaxBackoff {
			backoff = bootstrapPollMaxBackoff
		}
	}
}

// topicConfigs returns the per-topic configuration map for topic. The
// DLQ triplet stamps `retention.ms = kafkaDLQRetention` so corrupt
// messages live longer than normal pipeline traffic per
// docs/design/07-constants.md §F. Main topics return nil — they inherit
// the broker default.
//
// kadm's CreateTopic accepts map[string]*string for configs; the *string
// value is the documented carrier for "this is a string config" vs. nil
// for "this config has no value." The retention.ms wire shape uses the
// integer-as-string milliseconds value the broker expects.
func topicConfigs(topic string) map[string]*string {
	if _, ok := dlqTopics[topic]; !ok {
		return nil
	}
	retentionMs := strconv.FormatInt(kafkaDLQRetention.Milliseconds(), 10)
	return map[string]*string{
		"retention.ms": &retentionMs,
	}
}
