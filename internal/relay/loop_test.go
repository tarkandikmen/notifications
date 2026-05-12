package relay

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/tarkandikmen/notifications/internal/metrics"
	"github.com/tarkandikmen/notifications/internal/observability"
	"github.com/tarkandikmen/notifications/internal/store"
	"github.com/tarkandikmen/notifications/internal/testsupport"
)

// newTestEnv boots the Postgres + Kafka testcontainers, creates the
// phase 2 topics via Bootstrap, builds a real franz-go producer with
// the locked phase 2 producer settings, and returns Deps shaped for
// deterministic single-tick tests. The producer's lifecycle is
// registered as a t.Cleanup so callers don't have to remember to close.
func newTestEnv(t *testing.T) (Deps, *store.Store, []string) {
	t.Helper()
	pool, _ := testsupport.StartPostgres(t)
	brokers := testsupport.StartKafka(t)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	require.NoError(t, Bootstrap(context.Background(), brokers, logger),
		"bootstrap topics on the testcontainer broker")

	client, err := kgo.NewClient(producerOpts(brokers)...)
	require.NoError(t, err, "build producer")
	t.Cleanup(client.Close)

	st := store.New(pool)
	deps := Deps{
		Store:        st,
		Producer:     client,
		Logger:       logger,
		PollInterval: 25 * time.Millisecond,
		BatchSize:    500,
		// Phase 5: a noop tracer satisfies Deps.Tracer for unit tests
		// so the per-tick relay.tick span is opened (and ended)
		// without any exporter wiring. Tests that need to assert on
		// span shape build an in-memory tracetest provider in-line.
		Tracer: noop.NewTracerProvider().Tracer("test"),
	}
	return deps, st, brokers
}

// insertOutboxRow persists one outbox row directly via the store. The
// topic name doubles as the test fixture's "what should land on Kafka."
func insertOutboxRow(t *testing.T, st *store.Store, topic, partitionKey string, payload []byte) {
	t.Helper()
	pk := partitionKey
	require.NoError(t, st.InsertOutboxRow(context.Background(), st.Pool(), store.OutboxRow{
		Topic:        topic,
		PartitionKey: &pk,
		Payload:      payload,
	}))
}

// fetchOnePublishedAt returns the published_at timestamp for the given
// outbox row id. Used to verify the relay's publish-then-mark ordering
// committed the timestamp atomically with the publish.
func fetchOnePublishedAt(t *testing.T, st *store.Store) (id int64, publishedAt *time.Time) {
	t.Helper()
	require.NoError(t, st.Pool().QueryRow(context.Background(),
		`SELECT id, published_at FROM outbox ORDER BY id ASC LIMIT 1`,
	).Scan(&id, &publishedAt))
	return id, publishedAt
}

// drainOneRecord opens a fresh kgo consumer on the given topic, polls
// for one record (or the deadline expires), and returns the record. The
// consumer is closed before the function returns so the test isn't
// holding broker resources after the assertion runs.
//
// AtStart on ConsumeResetOffset matches the worker's phase 2 consumer
// config from docs/design/04-kafka.md §6 (`auto.offset.reset = earliest`),
// so this assertion exercises the same code path the SMS worker will
// use in chunk 5.
func drainOneRecord(t *testing.T, brokers []string, topic string, timeout time.Duration) *kgo.Record {
	t.Helper()

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	require.NoError(t, err, "build consumer")
	defer consumer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for {
		fetches := consumer.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			t.Fatalf("kafka poll error: %v", err)
		}
		records := fetches.Records()
		if len(records) > 0 {
			return records[0]
		}
		if ctx.Err() != nil {
			t.Fatalf("no record arrived on %s within %s", topic, timeout)
		}
	}
}

// TestRunOnce_PublishesAndMarksPublished is the primary test required
// by docs/phases/02-walking-skeleton.md §Chunk 4: one unpublished
// outbox row → one tick → message on Kafka, outbox row marked
// published_at. Asserts the wire shape (topic, key, value bytes) so a
// regression in payload encoding doesn't slip through.
func TestRunOnce_PublishesAndMarksPublished(t *testing.T) {
	deps, st, brokers := newTestEnv(t)

	payload := []byte(`{"version":1,"id":"01927000-0000-7000-8000-000000000001","attempt":1,"channel":"sms","recipient":"+905551234567","content":"hello","template":null,"template_data":null,"priority":1}`)
	const partitionKey = "01927000-0000-7000-8000-000000000001"
	insertOutboxRow(t, st, "send.sms", partitionKey, payload)

	require.NoError(t, runOnce(context.Background(), deps))

	// Outbox row is now stamped as published.
	id, publishedAt := fetchOnePublishedAt(t, st)
	assert.Greater(t, id, int64(0), "outbox row id is positive")
	require.NotNil(t, publishedAt, "published_at must be non-null after the relay tick")
	assert.WithinDuration(t, time.Now().UTC(), *publishedAt, 10*time.Second,
		"published_at is set near now()")

	// The exact same payload + key landed on the right topic.
	rec := drainOneRecord(t, brokers, "send.sms", 15*time.Second)
	assert.Equal(t, "send.sms", rec.Topic)
	assert.Equal(t, partitionKey, string(rec.Key))
	assert.JSONEq(t, string(payload), string(rec.Value),
		"published value must equal the outbox payload")
	assert.Empty(t, rec.Headers,
		"phase 2 leaves outbox headers null; published record carries no Kafka headers")
}

// TestRunOnce_ForwardsHeadersToKafka locks Chunk 6: non-null outbox
// headers become Kafka record headers verbatim.
func TestRunOnce_ForwardsHeadersToKafka(t *testing.T) {
	deps, st, brokers := newTestEnv(t)
	payload := []byte(`{"version":1}`)
	const partitionKey = "01927000-0000-7000-8000-000000000099"
	headers := json.RawMessage(`{"traceparent":"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}`)
	require.NoError(t, st.InsertOutboxRow(context.Background(), st.Pool(), store.OutboxRow{
		Topic:        "send.sms",
		PartitionKey: strPtr(partitionKey),
		Headers:      headers,
		Payload:      payload,
	}))

	require.NoError(t, runOnce(context.Background(), deps))

	rec := drainOneRecord(t, brokers, "send.sms", 15*time.Second)
	var tp string
	for _, h := range rec.Headers {
		if h.Key == "traceparent" {
			tp = string(h.Value)
		}
	}
	assert.Equal(t, "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", tp)
}

func strPtr(s string) *string { return &s }

// TestRunOnce_NoUnpublishedRows_NoOp covers the early-return branch
// when ClaimUnpublishedOutbox returns zero rows. The deferred rollback
// must leave the broker untouched and the function must return nil.
func TestRunOnce_NoUnpublishedRows_NoOp(t *testing.T) {
	deps, _, _ := newTestEnv(t)

	require.NoError(t, runOnce(context.Background(), deps))
}

// TestRunOnce_BatchPublishesEveryRow exercises the for-each-row build
// loop: every claimed outbox row gets its own Kafka record with its own
// key. Two rows on the same topic should produce two records.
func TestRunOnce_BatchPublishesEveryRow(t *testing.T) {
	deps, st, brokers := newTestEnv(t)

	insertOutboxRow(t, st, "send.sms", "key-a", []byte(`{"version":1,"id":"a"}`))
	insertOutboxRow(t, st, "send.sms", "key-b", []byte(`{"version":1,"id":"b"}`))

	require.NoError(t, runOnce(context.Background(), deps))

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics("send.sms"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	require.NoError(t, err)
	defer consumer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	gotKeys := map[string]string{}
	for len(gotKeys) < 2 {
		fetches := consumer.PollFetches(ctx)
		if err := fetches.Err(); err != nil {
			t.Fatalf("kafka poll error: %v", err)
		}
		for _, r := range fetches.Records() {
			gotKeys[string(r.Key)] = string(r.Value)
		}
		if ctx.Err() != nil && len(gotKeys) < 2 {
			t.Fatalf("expected 2 records, got %d: %v", len(gotKeys), gotKeys)
		}
	}

	assert.Contains(t, gotKeys, "key-a")
	assert.Contains(t, gotKeys, "key-b")
	assert.JSONEq(t, `{"version":1,"id":"a"}`, gotKeys["key-a"])
	assert.JSONEq(t, `{"version":1,"id":"b"}`, gotKeys["key-b"])

	// Both rows are now marked published.
	var unpublished int
	require.NoError(t, st.Pool().QueryRow(context.Background(),
		`SELECT count(*) FROM outbox WHERE published_at IS NULL`,
	).Scan(&unpublished))
	assert.Equal(t, 0, unpublished)
}

// TestRunOnce_ProduceErrorRollsBack covers the failure branch from
// docs/phases/02-walking-skeleton.md §8: a producer-side error must
// abort the loop body so the deferred rollback fires, leaving rows
// published_at IS NULL for the next tick to retry.
//
// Drives the loop with a stub Producer that returns an error from
// ProduceSync — no Kafka container needed for this branch.
func TestRunOnce_ProduceErrorRollsBack(t *testing.T) {
	pool, _ := testsupport.StartPostgres(t)
	st := store.New(pool)

	insertOutboxRow(t, st, "send.sms", "rollback-key", []byte(`{"version":1}`))

	stub := &errProducer{err: assert.AnError}
	deps := Deps{
		Store:        st,
		Producer:     stub,
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		PollInterval: 25 * time.Millisecond,
		BatchSize:    500,
		Tracer:       noop.NewTracerProvider().Tracer("test"),
	}

	err := runOnce(context.Background(), deps)
	require.Error(t, err)
	assert.ErrorContains(t, err, "produce sync")

	var unpublished int
	require.NoError(t, st.Pool().QueryRow(context.Background(),
		`SELECT count(*) FROM outbox WHERE published_at IS NULL`,
	).Scan(&unpublished))
	assert.Equal(t, 1, unpublished, "row stays unpublished after a publish failure")
}

// TestLoop_StopsOnContextCancel proves the Loop entrypoint observes
// ctx and returns nil on cancellation. Mirrors the dispatcher loop's
// equivalent test (internal/dispatcher/loop_test.go) for behavioral
// consistency across the two outbox-driven loops.
func TestLoop_StopsOnContextCancel(t *testing.T) {
	deps, st, brokers := newTestEnv(t)
	deps.PollInterval = 5 * time.Millisecond

	insertOutboxRow(t, st, "send.sms", "loop-cancel-key",
		[]byte(`{"version":1,"id":"01927000-0000-7000-8000-000000000099"}`))

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)
	done := make(chan error, 1)
	go func() {
		defer wg.Done()
		done <- Loop(ctx, deps)
	}()

	// Wait until the row is marked published (the loop has run at least
	// one tick), then cancel.
	require.Eventually(t, func() bool {
		var n int
		err := st.Pool().QueryRow(context.Background(),
			`SELECT count(*) FROM outbox WHERE published_at IS NOT NULL`,
		).Scan(&n)
		return err == nil && n == 1
	}, 15*time.Second, 25*time.Millisecond, "loop must publish the row within budget")

	cancel()
	select {
	case err := <-done:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Loop did not return within 5 s after ctx cancel")
	}

	// Drain the consumer to confirm the message reached Kafka.
	rec := drainOneRecord(t, brokers, "send.sms", 10*time.Second)
	assert.Equal(t, "loop-cancel-key", string(rec.Key))
	wg.Wait()
}

// TestApplyDefaults exercises the zero-value field substitution. Pure
// unit test (no testcontainer) — runs on every `go test ./...` so the
// defaults are pinned even when integration tests are disabled.
func TestApplyDefaults(t *testing.T) {
	tr := noop.NewTracerProvider().Tracer("test")
	d := applyDefaults(Deps{Tracer: tr})
	assert.Equal(t, defaultPollInterval, d.PollInterval)
	assert.Equal(t, defaultBatchSize, d.BatchSize)
	assert.NotNil(t, d.Logger)
	assert.NotNil(t, d.Tracer)

	custom := applyDefaults(Deps{
		PollInterval: 7 * time.Second,
		BatchSize:    13,
		Tracer:       tr,
	})
	assert.Equal(t, 7*time.Second, custom.PollInterval)
	assert.Equal(t, 13, custom.BatchSize)
}

// TestApplyDefaults_PanicsOnNilTracer locks the documented behavior
// that applyDefaults panics when Deps.Tracer is nil. Same shape as
// internal/dispatcher/loop_test.go's TestApplyDefaults_PanicsOnNilTracer.
func TestApplyDefaults_PanicsOnNilTracer(t *testing.T) {
	assert.PanicsWithValue(t,
		"relay: Deps.Tracer is required (otel.Tracer or noop)",
		func() { _ = applyDefaults(Deps{}) },
	)
}

// TestKafkaHeadersFromOutboxHeaders_NullAndPopulated locks the headers JSONB → []kgo.RecordHeader
// translation. Phase 2 always passes nil; Phase 5 populates W3C
// Trace Context headers, and this test catches a regression in the
// decoder that would silently drop them.
func TestKafkaHeadersFromOutboxHeaders_NullAndPopulated(t *testing.T) {
	assert.Nil(t, observability.KafkaHeadersFromOutboxHeaders(nil), "null headers → empty slice")
	assert.Nil(t, observability.KafkaHeadersFromOutboxHeaders(json.RawMessage(`{}`)), "empty object → empty slice")
	assert.Nil(t, observability.KafkaHeadersFromOutboxHeaders(json.RawMessage(`not json`)), "malformed JSON is non-fatal")

	got := observability.KafkaHeadersFromOutboxHeaders(json.RawMessage(`{"traceparent":"00-abc-def-01"}`))
	require.Len(t, got, 1)
	assert.Equal(t, "traceparent", got[0].Key)
	assert.Equal(t, "00-abc-def-01", string(got[0].Value))
}

// TestKeyFrom_NilAndSet locks the partition_key → []byte conversion.
func TestKeyFrom_NilAndSet(t *testing.T) {
	assert.Nil(t, keyFrom(nil))
	s := "abc"
	assert.Equal(t, []byte("abc"), keyFrom(&s))
}

// errProducer is a stub Producer implementation that always returns the
// configured error from ProduceSync. Used by TestRunOnce_ProduceErrorRollsBack
// to exercise the publish-failure branch without a Kafka container.
type errProducer struct {
	err error
}

func (e *errProducer) ProduceSync(_ context.Context, rs ...*kgo.Record) kgo.ProduceResults {
	results := make(kgo.ProduceResults, 0, len(rs))
	for _, r := range rs {
		results = append(results, kgo.ProduceResult{
			Record: r,
			Err:    e.err,
		})
	}
	return results
}

// counterValue extracts the current value of a Prometheus counter
// child for testing, mirroring the helper in
// internal/dispatcher/loop_test.go.
func counterValue(t *testing.T, c interface {
	Write(*dto.Metric) error
}) float64 {
	t.Helper()
	var m dto.Metric
	require.NoError(t, c.Write(&m))
	require.NotNil(t, m.Counter, "counter metric must carry a Counter payload")
	require.NotNil(t, m.Counter.Value)
	return *m.Counter.Value
}

// TestRunOnce_StampsTickCounter_PerOutcome locks Phase 5 §1.1's
// relay_ticks_total counter shape: every runOnce branch must stamp
// exactly one outcome on the counter. Three outcomes
// {published, empty, error} together exhaust the runOnce return
// paths.
//
// The "published" sub-test reuses the Kafka testcontainer fixture;
// "empty" and "error" use only Postgres so they're cheaper.
func TestRunOnce_StampsTickCounter_PerOutcome(t *testing.T) {
	t.Run("published", func(t *testing.T) {
		deps, st, _ := newTestEnv(t)
		before := counterValue(t, metrics.RelayTicks.WithLabelValues("published"))

		insertOutboxRow(t, st, "send.sms", "tick-counter-published", []byte(`{"version":1,"id":"a"}`))
		require.NoError(t, runOnce(context.Background(), deps))

		after := counterValue(t, metrics.RelayTicks.WithLabelValues("published"))
		assert.Equal(t, float64(1), after-before, "published branch must increment relay_ticks_total{outcome=published}")
	})

	t.Run("empty", func(t *testing.T) {
		deps, _, _ := newTestEnv(t)
		before := counterValue(t, metrics.RelayTicks.WithLabelValues("empty"))

		require.NoError(t, runOnce(context.Background(), deps))

		after := counterValue(t, metrics.RelayTicks.WithLabelValues("empty"))
		assert.Equal(t, float64(1), after-before, "empty branch must increment relay_ticks_total{outcome=empty}")
	})

	t.Run("error", func(t *testing.T) {
		pool, _ := testsupport.StartPostgres(t)
		st := store.New(pool)
		insertOutboxRow(t, st, "send.sms", "tick-counter-err", []byte(`{"version":1}`))

		stub := &errProducer{err: assert.AnError}
		deps := Deps{
			Store:        st,
			Producer:     stub,
			Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
			PollInterval: 25 * time.Millisecond,
			BatchSize:    500,
			Tracer:       noop.NewTracerProvider().Tracer("test"),
		}

		before := counterValue(t, metrics.RelayTicks.WithLabelValues("error"))

		err := runOnce(context.Background(), deps)
		require.Error(t, err)

		after := counterValue(t, metrics.RelayTicks.WithLabelValues("error"))
		assert.Equal(t, float64(1), after-before, "error branch must increment relay_ticks_total{outcome=error}")
	})
}

// TestRunOnce_PublishedRecordsCounter_PerTopic asserts the
// relay_published_records_total{topic} counter increments by the
// per-topic count from the batch (not the batch total). A two-topic
// batch increments both counters by the right delta.
func TestRunOnce_PublishedRecordsCounter_PerTopic(t *testing.T) {
	deps, st, _ := newTestEnv(t)

	beforeSMS := counterValue(t, metrics.RelayPublishedRecords.WithLabelValues("send.sms"))
	beforeEmail := counterValue(t, metrics.RelayPublishedRecords.WithLabelValues("send.email"))

	insertOutboxRow(t, st, "send.sms", "rec-counter-sms-1", []byte(`{"version":1,"id":"a"}`))
	insertOutboxRow(t, st, "send.sms", "rec-counter-sms-2", []byte(`{"version":1,"id":"b"}`))
	insertOutboxRow(t, st, "send.email", "rec-counter-email", []byte(`{"version":1,"id":"c"}`))

	require.NoError(t, runOnce(context.Background(), deps))

	afterSMS := counterValue(t, metrics.RelayPublishedRecords.WithLabelValues("send.sms"))
	afterEmail := counterValue(t, metrics.RelayPublishedRecords.WithLabelValues("send.email"))
	assert.Equal(t, float64(2), afterSMS-beforeSMS, "two SMS records → +2 on send.sms counter")
	assert.Equal(t, float64(1), afterEmail-beforeEmail, "one email record → +1 on send.email counter")
}

// TestRunOnce_OpensTickSpan asserts the relay.tick span name +
// outcome attribute per docs/phases/05-observability.md §7. Uses a
// tracetest in-memory exporter so the span shape is inspectable
// without a real OTLP pipeline.
func TestRunOnce_OpensTickSpan(t *testing.T) {
	deps, st, _ := newTestEnv(t)

	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	deps.Tracer = tp.Tracer("relay")

	insertOutboxRow(t, st, "send.sms", "span-key", []byte(`{"version":1,"id":"a"}`))
	require.NoError(t, runOnce(context.Background(), deps))

	require.NoError(t, tp.ForceFlush(context.Background()))
	spans := exp.GetSpans()
	require.GreaterOrEqual(t, len(spans), 1, "at least relay.tick span")

	var tick *tracetest.SpanStub
	for i := range spans {
		if spans[i].Name == "relay.tick" {
			tick = &spans[i]
			break
		}
	}
	require.NotNil(t, tick, "relay.tick span missing")

	attrs := attrMap(tick.Attributes)
	assert.Equal(t, "published", attrs["outcome"])
	assert.EqualValues(t, 1, attrs["rows"])

	var rows int
	for i := range spans {
		if spans[i].Name == "relay.row" {
			rows++
		}
	}
	assert.Equal(t, 1, rows, "one claimed outbox row → one relay.row span")
}

// attrMap flattens a slice of trace.KeyValue attributes into a
// map[string]any keyed on the attribute name. Mirrors
// internal/dispatcher/loop_test.go's helper.
func attrMap(kvs []attribute.KeyValue) map[string]any {
	out := make(map[string]any, len(kvs))
	for _, kv := range kvs {
		switch kv.Value.Type() {
		case attribute.STRING:
			out[string(kv.Key)] = kv.Value.AsString()
		case attribute.INT64:
			out[string(kv.Key)] = kv.Value.AsInt64()
		case attribute.BOOL:
			out[string(kv.Key)] = kv.Value.AsBool()
		case attribute.FLOAT64:
			out[string(kv.Key)] = kv.Value.AsFloat64()
		default:
			out[string(kv.Key)] = kv.Value.Emit()
		}
	}
	return out
}
