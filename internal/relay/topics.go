package relay

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Phase 2 Kafka topology, locked by docs/design/04-kafka.md §Topic catalog
// and docs/design/07-constants.md §F. Phase 3 adds send.email, send.push,
// and the per-channel send.<channel>.dlq triplet; phase 2 ships only the
// SMS pipeline plus the shared events.notification topic.
const (
	topicSendSMS            = "send.sms"
	topicEventsNotification = "events.notification"

	// sendPartitions / eventsPartitions are inlined from
	// docs/design/07-constants.md §F (send_partitions, events_partitions).
	// Both pinned at 20 to give later phases headroom for worker-pool
	// scaling without a re-partition; the per-channel rate cap bounds
	// throughput regardless.
	sendPartitions   = int32(20)
	eventsPartitions = int32(20)

	// kafkaReplicationFactorDev is the docker-compose dev cluster's RF
	// from docs/design/07-constants.md §F (kafka_replication_factor_dev).
	// Production targets RF=3; phase 2 deploys against a single broker.
	kafkaReplicationFactorDev = int16(1)
)

// phase2Topics is the canonical (topic → partition count) set Bootstrap
// creates on first run. Order of iteration doesn't matter — kadm's
// CreateTopics is a single round trip for the whole list.
var phase2Topics = map[string]int32{
	topicSendSMS:            sendPartitions,
	topicEventsNotification: eventsPartitions,
}

// Bootstrap creates the phase 2 Kafka topic set on the broker and treats
// TOPIC_ALREADY_EXISTS as success so the call is idempotent across relay
// restarts. Other per-topic errors are reported back to the caller, which
// fails the relay startup so a misconfigured cluster is loud rather than
// silently broken.
//
// docs/phases/02-walking-skeleton.md §8.
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

	// One round trip per topic keeps each create independent — a bad
	// partition count on one topic never blocks another from being
	// created. The per-topic loop is small enough (two topics) that the
	// extra latency is negligible vs. the simpler error-handling shape.
	for topic, partitions := range phase2Topics {
		resp, err := adm.CreateTopic(ctx, partitions, kafkaReplicationFactorDev, nil, topic)
		if err != nil {
			return fmt.Errorf("relay bootstrap: create %q: %w", topic, err)
		}
		if resp.Err != nil {
			if errors.Is(resp.Err, kerr.TopicAlreadyExists) {
				logger.Debug("relay bootstrap: topic already exists",
					"topic", topic,
				)
				continue
			}
			return fmt.Errorf("relay bootstrap: create %q: %w", topic, resp.Err)
		}
		logger.Info("relay bootstrap: topic created",
			"topic", topic,
			"partitions", partitions,
			"replication_factor", kafkaReplicationFactorDev,
		)
	}

	return nil
}
