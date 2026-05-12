package itest

// Full-stack list end-to-end test.
//
// Boots Postgres + Kafka testcontainers, stands up the api + dispatcher
// + relay + reaper plus one worker goroutine per channel (sms, email,
// push), then drives:
//
//  1. 5 SMS + 3 email + 2 push single-create POSTs (no batches; the
//     list endpoint must work against rows that have null batch_id).
//  2. Polls each row to DELIVERED so the status= filter exercise has
//     a stable post-condition.
//  3. Exercises GET /v1/notifications across the filter set and
//     pagination boundary cases:
//
//       - channel=sms&limit=3&offset=0 → 3 SMS rows, has_more=true.
//       - channel=email                → 3 email rows, has_more=false.
//       - status=DELIVERED&priority=normal → all 10 rows (none set
//         explicit priority, so all default to priority=normal).
//       - created_after=<too-early>    → all 10 rows.
//       - created_after=<future>       → empty list.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/tarkandikmen/notifications/internal/api"
	"github.com/tarkandikmen/notifications/internal/dispatcher"
	"github.com/tarkandikmen/notifications/internal/kafkaadmin"
	"github.com/tarkandikmen/notifications/internal/reaper"
	"github.com/tarkandikmen/notifications/internal/relay"
	"github.com/tarkandikmen/notifications/internal/store"
	"github.com/tarkandikmen/notifications/internal/testsupport"
	"github.com/tarkandikmen/notifications/internal/worker"
)

// TestList_FiltersAndPagination is the list acceptance test. The
// 5+3+2 channel split lets the per-channel + pagination assertions
// all land against the same dataset without re-posting per scenario;
// the pre-poll-to-DELIVERED step makes the status= filter exercise
// deterministic regardless of how fast the pipelines run.
func TestList_FiltersAndPagination(t *testing.T) {
	pool, _ := testsupport.StartPostgres(t)
	brokers := testsupport.StartKafka(t)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	testsupport.BootstrapWithRetry(t, func() error {
		return relay.Bootstrap(context.Background(), brokers, logger)
	})

	hits := make(map[string]*atomic.Int32, 3)
	for _, ch := range []string{"sms", "email", "push"} {
		hits[ch] = &atomic.Int32{}
	}
	var hitsMu sync.Mutex

	webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("webhook: read body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var req struct {
			Channel string `json:"channel"`
		}
		if err := json.Unmarshal(body, &req); err != nil {
			t.Errorf("webhook: decode body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		hitsMu.Lock()
		counter, ok := hits[req.Channel]
		hitsMu.Unlock()
		if !ok {
			t.Errorf("webhook: unexpected channel %q", req.Channel)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		counter.Add(1)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"messageId":"list-itest-1","status":"accepted"}`))
	}))
	t.Cleanup(webhook.Close)

	st := store.New(pool)

	producer, err := kgo.NewClient(producerOpts(brokers)...)
	require.NoError(t, err, "build relay producer")
	t.Cleanup(producer.Close)

	lagClient, err := kafkaadmin.New(brokers)
	require.NoError(t, err, "build kafkaadmin lag client")
	t.Cleanup(lagClient.Close)

	mux := http.NewServeMux()
	api.RegisterRoutes(mux, api.Deps{
		Store:    st,
		Registry: prometheus.NewRegistry(),
		Logger:   logger,
		Clock:    time.Now,
	})
	apiServer := httptest.NewServer(mux)
	t.Cleanup(apiServer.Close)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wg := &sync.WaitGroup{}
	loopErrs := make(chan error, 6)

	startLoop := func(name string, fn func() error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := fn(); err != nil {
				loopErrs <- fmt.Errorf("%s loop: %w", name, err)
			}
		}()
	}

	startLoop("dispatcher", func() error {
		return dispatcher.Loop(ctx, dispatcher.Deps{
			Store:        st,
			Logger:       logger,
			PollInterval: 25 * time.Millisecond,
			BatchSize:    200,
			Channels:     []string{"sms", "email", "push"},
			Lag:          lagClient,
			Tracer:       noop.NewTracerProvider().Tracer("dispatcher"),
		})
	})

	startLoop("relay", func() error {
		return relay.Loop(ctx, relay.Deps{
			Store:        st,
			Producer:     producer,
			Logger:       logger,
			PollInterval: 25 * time.Millisecond,
			BatchSize:    500,
			Tracer:       noop.NewTracerProvider().Tracer("relay"),
		})
	})

	for _, channel := range []string{"sms", "email", "push"} {
		ch := channel
		consumer, err := kgo.NewClient(consumerOptsForChannel(brokers, ch)...)
		require.NoError(t, err, "build worker consumer for channel %q", ch)
		t.Cleanup(consumer.Close)

		startLoop("worker."+ch, func() error {
			return worker.Loop(ctx, worker.Deps{
				Store:    st,
				Consumer: consumer,
				Provider: worker.NewProvider(webhook.URL),
				Limiter:  noOpLimiter{},
				Logger:   logger,
				Channel:  ch,
				Clock:    time.Now,
				Tracer:   noop.NewTracerProvider().Tracer("worker"),
			})
		})
	}

	startLoop("reaper", func() error {
		return reaper.Loop(ctx, reaper.Deps{
			Store:          st,
			Logger:         logger,
			Interval:       60 * time.Second,
			StuckThreshold: 120 * time.Second,
			MaxAttempts:    7,
			Channels:       []string{"sms", "email", "push"},
			Lag:            lagClient,
			Tracer:         noop.NewTracerProvider().Tracer("reaper"),
		})
	})

	// Capture a "before" timestamp to drive the created_after filter
	// later. Take it just before the first POST so every inserted row
	// satisfies created_at > beforePosts; the slop (-2 s) absorbs any
	// minor clock drift between this process and Postgres.
	beforePosts := time.Now().UTC().Add(-2 * time.Second)

	smsIDs := make([]uuid.UUID, 0, 5)
	for i := 0; i < 5; i++ {
		body := fmt.Sprintf(`{
			"channel": "sms",
			"recipient": "+90555%07d",
			"content": "list itest sms %d",
			"idempotency_key": "00000000-0000-4000-8000-%012d"
		}`, i, i, 0xb01+i)
		smsIDs = append(smsIDs, postNotification(t, apiServer.URL, body))
	}

	emailIDs := make([]uuid.UUID, 0, 3)
	for i := 0; i < 3; i++ {
		body := fmt.Sprintf(`{
			"channel": "email",
			"recipient": "list-user%d@example.com",
			"content": "list itest email %d",
			"idempotency_key": "00000000-0000-4000-8000-%012d"
		}`, i, i, 0xb10+i)
		emailIDs = append(emailIDs, postNotification(t, apiServer.URL, body))
	}

	pushIDs := make([]uuid.UUID, 0, 2)
	pushTokens := []string{
		"abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
		"fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210",
	}
	for i := 0; i < 2; i++ {
		body := fmt.Sprintf(`{
			"channel": "push",
			"recipient": "%s",
			"content": "list itest push %d",
			"idempotency_key": "00000000-0000-4000-8000-%012d"
		}`, pushTokens[i], i, 0xb20+i)
		pushIDs = append(pushIDs, postNotification(t, apiServer.URL, body))
	}

	// Capture an "after-posts" timestamp once every row is in. Used
	// to drive the created_after=future filter (rows posted before
	// this timestamp must be excluded).
	allPostedAt := time.Now().UTC()

	// Poll every row to DELIVERED so the status=DELIVERED filter
	// exercise below sees a stable dataset. Without this, the
	// status= filter assertion would race the dispatcher and could
	// observe a mix of PENDING / DISPATCHED / DELIVERED depending
	// on timing.
	allIDs := make([]uuid.UUID, 0, 10)
	allIDs = append(allIDs, smsIDs...)
	allIDs = append(allIDs, emailIDs...)
	allIDs = append(allIDs, pushIDs...)
	for _, id := range allIDs {
		awaitNotificationStatus(t, apiServer.URL, id, "DELIVERED", 30*time.Second)
	}

	// Webhook hit counts: one per row per channel. Catches a
	// pipeline regression where the list endpoint runs against
	// rows that never delivered.
	assert.Equal(t, int32(5), hits["sms"].Load(), "sms webhook hits = 5")
	assert.Equal(t, int32(3), hits["email"].Load(), "email webhook hits = 3")
	assert.Equal(t, int32(2), hits["push"].Load(), "push webhook hits = 2")

	// Scenario 1: channel=sms&limit=3&offset=0 → 3 SMS rows,
	// has_more=true (5 total SMS, limit=3 leaves 2 on the next
	// page).
	t.Run("channel_sms_limit_3_has_more_true", func(t *testing.T) {
		got := getList(t, apiServer.URL, url.Values{
			"channel": {"sms"},
			"limit":   {"3"},
			"offset":  {"0"},
		})
		assert.Equal(t, 0, got.Offset, "echoes the offset param")
		assert.Equal(t, 3, got.Limit, "echoes the limit param")
		assert.True(t, got.HasMore, "5 total SMS rows; limit=3 must report has_more=true")
		require.Len(t, got.Notifications, 3, "limit=3 caps the page at 3 rows")
		for _, n := range got.Notifications {
			assert.Equal(t, "sms", n.Channel, "every row matches the channel filter")
			assert.Equal(t, "DELIVERED", n.Status,
				"every SMS row in the test dataset is DELIVERED by this point")
			assert.Nil(t, n.Attempts,
				"list responses use the no-attempts representation")
		}
	})

	// Scenario 2: channel=email → 3 email rows, has_more=false. No
	// explicit limit, so the default (50) applies; with exactly 3
	// matching rows the LIMIT limit+1 trick must report has_more=false.
	t.Run("channel_email_has_more_false", func(t *testing.T) {
		got := getList(t, apiServer.URL, url.Values{
			"channel": {"email"},
		})
		assert.False(t, got.HasMore,
			"3 email rows fit within default limit; has_more must be false")
		require.Len(t, got.Notifications, 3,
			"every email row in the test dataset matches the channel filter")
		for _, n := range got.Notifications {
			assert.Equal(t, "email", n.Channel)
		}
	})

	// Scenario 3: status=DELIVERED&priority=normal → all 10 rows.
	// None of the test posts set an explicit priority, so every row
	// defaults to priority=normal.
	t.Run("status_delivered_priority_normal_matches_all_10", func(t *testing.T) {
		got := getList(t, apiServer.URL, url.Values{
			"status":   {"DELIVERED"},
			"priority": {"normal"},
		})
		// Filter to the rows this test posted — other tests in the
		// same process may have left rows in the DB (none expected
		// here since each integration test uses its own postgres
		// container, but defensive against testcontainer reuse).
		matched := filterByIDs(got.Notifications, allIDs)
		require.Len(t, matched, 10,
			"every row posted by this test must satisfy status=DELIVERED && priority=normal")
		for _, n := range matched {
			assert.Equal(t, "DELIVERED", n.Status)
		}
	})

	// Scenario 4: created_after=<beforePosts> → all 10 rows. The
	// beforePosts timestamp was captured before any POST, so every
	// inserted row's created_at is strictly after it.
	t.Run("created_after_too_early_matches_all_10", func(t *testing.T) {
		got := getList(t, apiServer.URL, url.Values{
			"created_after": {beforePosts.Format(time.RFC3339Nano)},
		})
		matched := filterByIDs(got.Notifications, allIDs)
		assert.Len(t, matched, 10,
			"created_after before any POST must include every row posted by this test")
	})

	// Scenario 5: created_after=<allPostedAt + 1h> → empty list (no
	// rows posted after that timestamp). The list endpoint is a
	// query, not a lookup; empty matches return 200 with
	// `notifications: []`.
	t.Run("created_after_future_empty_200", func(t *testing.T) {
		future := allPostedAt.Add(1 * time.Hour)
		got := getList(t, apiServer.URL, url.Values{
			"created_after": {future.Format(time.RFC3339Nano)},
		})
		matched := filterByIDs(got.Notifications, allIDs)
		assert.Empty(t, matched,
			"created_after in the future must exclude every row posted by this test")
		assert.False(t, got.HasMore,
			"empty result must report has_more=false")
	})

	// Scenario 6: offset boundary at the page size. With limit=2
	// and offset=4 against the 5 SMS rows, the response carries
	// exactly 1 row (the trailing SMS) and has_more=false.
	t.Run("channel_sms_offset_4_limit_2_one_row_no_more", func(t *testing.T) {
		got := getList(t, apiServer.URL, url.Values{
			"channel": {"sms"},
			"limit":   {"2"},
			"offset":  {"4"},
		})
		assert.Equal(t, 4, got.Offset)
		assert.Equal(t, 2, got.Limit)
		assert.False(t, got.HasMore,
			"offset=4 + limit=2 over 5 rows leaves no remainder")
		require.Len(t, got.Notifications, 1,
			"exactly one trailing SMS row must remain at offset=4 with 5 total rows")
		assert.Equal(t, "sms", got.Notifications[0].Channel)
	})

	cancel()
	if err := waitWithTimeout(wg, 10*time.Second); err != nil {
		t.Fatalf("loops did not shut down within 10s: %v", err)
	}
	close(loopErrs)
	for err := range loopErrs {
		t.Errorf("loop returned non-nil error: %v", err)
	}
}

// listResponse mirrors api.ListResponse. Notifications use the
// shared notificationGetResponse since the list shape is the same
// as the single-GET shape minus the nested attempts array — and the
// shared type's Attempts field is left nil by encoding/json when the
// wire payload has no `attempts` key.
type listResponse struct {
	Notifications []notificationGetResponse `json:"notifications"`
	Offset        int                       `json:"offset"`
	Limit         int                       `json:"limit"`
	HasMore       bool                      `json:"has_more"`
}

// getList GETs /v1/notifications with the given query params and
// returns the parsed 200 body. Any non-200 fails the test.
func getList(t *testing.T, baseURL string, params url.Values) listResponse {
	t.Helper()

	q := ""
	if encoded := params.Encode(); encoded != "" {
		q = "?" + encoded
	}
	resp, err := http.Get(baseURL + "/v1/notifications" + q)
	require.NoError(t, err, "GET /v1/notifications%s", q)
	defer func() { _ = resp.Body.Close() }()

	require.Equal(t, http.StatusOK, resp.StatusCode,
		"GET /v1/notifications%s must return 200", q)

	var got listResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got),
		"decode list response for %s", q)
	return got
}

// filterByIDs returns the subset of rows whose IDs appear in want.
// Used in the list test to ignore any rows that might exist in the
// database from prior runs (defensive — each integration test
// nominally runs against its own postgres container, but this keeps
// the assertion robust against testcontainer reuse).
func filterByIDs(rows []notificationGetResponse, want []uuid.UUID) []notificationGetResponse {
	wantSet := make(map[string]struct{}, len(want))
	for _, id := range want {
		wantSet[id.String()] = struct{}{}
	}
	out := make([]notificationGetResponse, 0, len(rows))
	for _, r := range rows {
		if _, ok := wantSet[r.ID]; ok {
			out = append(out, r)
		}
	}
	return out
}
