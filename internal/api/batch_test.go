package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tarkandikmen/notifications/internal/metrics"
	"github.com/tarkandikmen/notifications/internal/store"
)

// TestHandleBatchCreate_HappyPath posts 3 items and asserts:
//
//   - response is 201 with a UUIDv7 batch_id and 3 UUIDv7 ids in
//     request order;
//   - the store call received the same batch_id for the whole call;
//   - each store.Notification has the matching id, channel, recipient,
//     content, the shared batch_id, status=PENDING, and the right
//     priority (defaulting to normal when absent).
func TestHandleBatchCreate_HappyPath(t *testing.T) {
	fs := &fakeStore{}
	srv := newTestServer(t, fs)

	body := `{
		"notifications": [
			{"channel": "sms",   "recipient": "+905551234567", "content": "batch 1", "idempotency_key": "00000000-0000-4000-8000-000000000401"},
			{"channel": "email", "recipient": "u@example.com", "content": "batch 2", "idempotency_key": "00000000-0000-4000-8000-000000000402"},
			{"channel": "push",  "recipient": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", "content": "batch 3", "idempotency_key": "00000000-0000-4000-8000-000000000403"}
		]
	}`

	resp := postJSON(t, srv, "/v1/notifications/batch", body)
	defer resp.Body.Close()

	require.Equal(t, http.StatusCreated, resp.StatusCode)

	var got BatchCreateResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))

	require.NotEmpty(t, got.BatchID)
	batchID, err := uuid.Parse(got.BatchID)
	require.NoError(t, err)
	assert.Equal(t, uuid.Version(7), batchID.Version(), "batch_id is UUIDv7")

	require.Len(t, got.IDs, 3)
	for i, raw := range got.IDs {
		parsed, err := uuid.Parse(raw)
		require.NoError(t, err, "id[%d] must parse", i)
		assert.Equal(t, uuid.Version(7), parsed.Version(), "id[%d] is UUIDv7", i)
	}

	require.Equal(t, 1, fs.insertBatchCalled)
	assert.Equal(t, batchID, fs.insertBatchBatchID, "store received the same batch_id rendered to the wire")
	require.Len(t, fs.insertBatchArg, 3)

	for i, n := range fs.insertBatchArg {
		assert.Equal(t, got.IDs[i], n.ID.String(), "stored id matches response order")
		require.True(t, n.BatchID.Valid, "every row carries the batch_id")
		assert.Equal(t, batchID, n.BatchID.UUID)
		assert.Equal(t, statusPending, n.Status, "every row lands as PENDING")
		assert.Equal(t, 0, n.Attempt)
		assert.Equal(t, int16(1), n.Priority, "default priority is normal=1")
	}

	assert.Equal(t, "sms", fs.insertBatchArg[0].Channel)
	require.NotNil(t, fs.insertBatchArg[0].Content)
	assert.Equal(t, "batch 1", *fs.insertBatchArg[0].Content)

	assert.Equal(t, "email", fs.insertBatchArg[1].Channel)
	assert.Equal(t, "u@example.com", fs.insertBatchArg[1].Recipient)

	assert.Equal(t, "push", fs.insertBatchArg[2].Channel)
}

// TestHandleBatchCreate_OversizedBatch_413 builds a batch one larger
// than batchMax and asserts the response is 413 payload_too_large with
// no details[] (per docs/design/03-api.md §Error model). The store must
// not be called.
func TestHandleBatchCreate_OversizedBatch_413(t *testing.T) {
	fs := &fakeStore{}
	srv := newTestServer(t, fs)

	items := make([]string, 0, batchMax+1)
	for i := 0; i < batchMax+1; i++ {
		items = append(items, fmt.Sprintf(
			`{"channel":"sms","recipient":"+9055512%05d","content":"x","idempotency_key":"00000000-0000-4000-8000-%012d"}`,
			i+10000, i+1,
		))
	}
	body := `{"notifications":[` + strings.Join(items, ",") + `]}`

	resp := postJSON(t, srv, "/v1/notifications/batch", body)
	defer resp.Body.Close()

	require.Equal(t, http.StatusRequestEntityTooLarge, resp.StatusCode)
	env := decodeErrorEnvelope(t, resp.Body)
	assert.Equal(t, "payload_too_large", env.Error.Code)
	assert.Empty(t, env.Error.Details, "413 carries no details[]")

	assert.Zero(t, fs.insertBatchCalled, "store must not be called for an oversized batch")
}

// TestHandleBatchCreate_EmptyBatch_400 posts an empty notifications
// array and asserts a 400 with a single details[] entry against the
// "notifications" path (the contract requires at least one item).
func TestHandleBatchCreate_EmptyBatch_400(t *testing.T) {
	fs := &fakeStore{}
	srv := newTestServer(t, fs)

	resp := postJSON(t, srv, "/v1/notifications/batch", `{"notifications": []}`)
	defer resp.Body.Close()

	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	env := decodeErrorEnvelope(t, resp.Body)
	assert.Equal(t, "validation_failed", env.Error.Code)
	require.Len(t, env.Error.Details, 1)
	detail := asFieldIssue(t, env.Error.Details[0])
	assert.Equal(t, "notifications", detail.Path)
	assert.Contains(t, detail.Issue, "at least one")

	assert.Zero(t, fs.insertBatchCalled)
}

// TestHandleBatchCreate_IntraBatchDuplicate_400 posts two items sharing
// one idempotency_key. The validator must surface ONE duplicate issue
// against the second item's path (the first item's key is unique on
// its own row).
func TestHandleBatchCreate_IntraBatchDuplicate_400(t *testing.T) {
	fs := &fakeStore{}
	srv := newTestServer(t, fs)

	body := `{
		"notifications": [
			{"channel":"sms","recipient":"+905551234567","content":"x","idempotency_key":"00000000-0000-4000-8000-000000000501"},
			{"channel":"sms","recipient":"+905551234568","content":"y","idempotency_key":"00000000-0000-4000-8000-000000000501"}
		]
	}`

	resp := postJSON(t, srv, "/v1/notifications/batch", body)
	defer resp.Body.Close()

	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	env := decodeErrorEnvelope(t, resp.Body)
	assert.Equal(t, "validation_failed", env.Error.Code)

	found := false
	for _, raw := range env.Error.Details {
		fi := asFieldIssue(t, raw)
		if fi.Path == "notifications[1].idempotency_key" && strings.Contains(fi.Issue, "duplicate of notifications[0].idempotency_key") {
			found = true
		}
	}
	assert.True(t, found, "expected one duplicate issue on the second item's idempotency_key path")

	assert.Zero(t, fs.insertBatchCalled)
}

// TestHandleBatchCreate_PerItemValidationFailure_400 asserts a 400
// when one item fails a per-item rule (here an empty recipient on the
// second item). The details[] path must be prefixed
// "notifications[1].recipient".
func TestHandleBatchCreate_PerItemValidationFailure_400(t *testing.T) {
	fs := &fakeStore{}
	srv := newTestServer(t, fs)

	body := `{
		"notifications": [
			{"channel":"sms","recipient":"+905551234567","content":"x","idempotency_key":"00000000-0000-4000-8000-000000000601"},
			{"channel":"sms","recipient":"","content":"y","idempotency_key":"00000000-0000-4000-8000-000000000602"}
		]
	}`

	resp := postJSON(t, srv, "/v1/notifications/batch", body)
	defer resp.Body.Close()

	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	env := decodeErrorEnvelope(t, resp.Body)
	assert.Equal(t, "validation_failed", env.Error.Code)

	paths := make(map[string]bool, len(env.Error.Details))
	for _, raw := range env.Error.Details {
		paths[asFieldIssue(t, raw).Path] = true
	}
	assert.True(t, paths["notifications[1].recipient"], "expected prefixed per-item path on bad item")
	assert.False(t, paths["notifications[0].recipient"], "valid item's path must not appear")

	assert.Zero(t, fs.insertBatchCalled)
}

// TestHandleBatchCreate_IdempotencyConflict_409 wires the fake to
// return a BatchIdempotencyConflictError with two entries. The
// response must be 409 duplicate_idempotency_keys with one detail
// per conflict in the same order the store returned them.
func TestHandleBatchCreate_IdempotencyConflict_409(t *testing.T) {
	existingA := uuid.MustParse("11111111-1111-7111-8111-100000000001")
	existingB := uuid.MustParse("11111111-1111-7111-8111-100000000002")
	fs := &fakeStore{
		insertBatchErr: &store.BatchIdempotencyConflictError{
			Entries: []store.IdempotencyConflictEntry{
				{Key: "00000000-0000-4000-8000-000000000701", ExistingID: existingA, ExistingStatus: "PENDING"},
				{Key: "00000000-0000-4000-8000-000000000702", ExistingID: existingB, ExistingStatus: "DELIVERED"},
			},
		},
	}
	srv := newTestServer(t, fs)

	body := `{
		"notifications": [
			{"channel":"sms","recipient":"+905551234567","content":"x","idempotency_key":"00000000-0000-4000-8000-000000000701"},
			{"channel":"sms","recipient":"+905551234568","content":"y","idempotency_key":"00000000-0000-4000-8000-000000000702"}
		]
	}`

	resp := postJSON(t, srv, "/v1/notifications/batch", body)
	defer resp.Body.Close()

	require.Equal(t, http.StatusConflict, resp.StatusCode)
	env := decodeErrorEnvelope(t, resp.Body)
	assert.Equal(t, "duplicate_idempotency_keys", env.Error.Code)
	require.Len(t, env.Error.Details, 2)

	first := decodeConflictDetail(t, env.Error.Details[0])
	assert.Equal(t, "00000000-0000-4000-8000-000000000701", first.IdempotencyKey)
	assert.Equal(t, existingA.String(), first.ExistingID)
	assert.Equal(t, "PENDING", first.Status)

	second := decodeConflictDetail(t, env.Error.Details[1])
	assert.Equal(t, "00000000-0000-4000-8000-000000000702", second.IdempotencyKey)
	assert.Equal(t, existingB.String(), second.ExistingID)
	assert.Equal(t, "DELIVERED", second.Status)
}

// TestHandleBatchCreate_MalformedJSON_400 posts garbage and asserts
// the response mirrors handleCreate's malformed-JSON contract: 400
// validation_failed with one details[] entry against "body".
func TestHandleBatchCreate_MalformedJSON_400(t *testing.T) {
	fs := &fakeStore{}
	srv := newTestServer(t, fs)

	resp := postJSON(t, srv, "/v1/notifications/batch", `{not valid json`)
	defer resp.Body.Close()

	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	env := decodeErrorEnvelope(t, resp.Body)
	assert.Equal(t, "validation_failed", env.Error.Code)
	require.Len(t, env.Error.Details, 1)
	detail := asFieldIssue(t, env.Error.Details[0])
	assert.Equal(t, "body", detail.Path)
	assert.Equal(t, "malformed JSON", detail.Issue)

	assert.Zero(t, fs.insertBatchCalled, "store must not be called for malformed JSON")
}

// TestHandleBatchCreate_ScheduledAtSetsEligibleAt asserts the
// scheduled_at → eligible_at conversion runs per item, mirroring the
// single-create handler's behavior (handleCreate). When scheduled_at
// is set the row's eligible_at takes the same value; when absent it
// defaults to deps.Clock().
func TestHandleBatchCreate_ScheduledAtSetsEligibleAt(t *testing.T) {
	fs := &fakeStore{}
	srv := newTestServer(t, fs)

	body := `{
		"notifications": [
			{"channel":"sms","recipient":"+905551234567","content":"future","scheduled_at":"2026-05-11T13:00:00Z","idempotency_key":"00000000-0000-4000-8000-000000000801"},
			{"channel":"sms","recipient":"+905551234568","content":"now","idempotency_key":"00000000-0000-4000-8000-000000000802"}
		]
	}`

	resp := postJSON(t, srv, "/v1/notifications/batch", body)
	defer resp.Body.Close()

	require.Equal(t, http.StatusCreated, resp.StatusCode)
	require.Equal(t, 1, fs.insertBatchCalled)
	require.Len(t, fs.insertBatchArg, 2)

	require.NotNil(t, fs.insertBatchArg[0].ScheduledAt)
	assert.True(t, fs.insertBatchArg[0].EligibleAt.Equal(*fs.insertBatchArg[0].ScheduledAt),
		"eligible_at = scheduled_at when scheduled_at is set")

	assert.Nil(t, fs.insertBatchArg[1].ScheduledAt)
	assert.True(t, fs.insertBatchArg[1].EligibleAt.Equal(fixedNow),
		"eligible_at defaults to clock now when scheduled_at is absent")
}

// TestHandleBatchCreate_StoreErrorIsInternal asserts a non-conflict
// store error surfaces as 500 internal_error with no leaked details.
func TestHandleBatchCreate_StoreErrorIsInternal(t *testing.T) {
	fs := &fakeStore{insertBatchErr: errors.New("boom")}
	srv := newTestServer(t, fs)

	body := `{
		"notifications": [
			{"channel":"sms","recipient":"+905551234567","content":"x","idempotency_key":"00000000-0000-4000-8000-000000000901"}
		]
	}`

	resp := postJSON(t, srv, "/v1/notifications/batch", body)
	defer resp.Body.Close()

	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	env := decodeErrorEnvelope(t, resp.Body)
	assert.Equal(t, "internal_error", env.Error.Code)
}

// decodeConflictDetail treats one details[] entry as an
// IdempotencyConflictDetail. Same JSON-roundtrip pattern as
// asFieldIssue but typed differently.
func decodeConflictDetail(t *testing.T, raw any) IdempotencyConflictDetail {
	t.Helper()
	bs, err := json.Marshal(raw)
	require.NoError(t, err)
	var d IdempotencyConflictDetail
	require.NoError(t, json.Unmarshal(bs, &d))
	return d
}

// TestHandleBatchCreate_ObservesBatchSize asserts the
// api_batch_size_items histogram observation rises by 1 per
// successful batch-create request, with the observed value matching
// the input size. Per docs/phases/05-observability.md §1.1 the
// observation fires only after ValidateBatchCreate returns clean —
// oversized / malformed batches don't pollute the histogram.
//
// The endpoint label is the mux pattern ("POST
// /v1/notifications/batch") — same as the api_requests_total label
// for that route.
func TestHandleBatchCreate_ObservesBatchSize(t *testing.T) {
	const endpoint = "POST /v1/notifications/batch"
	const requestSize = 3
	before := histogramObservation(t, metrics.APIBatchSize.WithLabelValues(endpoint))

	fs := &fakeStore{}
	srv := newTestServer(t, fs)

	body := `{
		"notifications": [
			{"channel": "sms",   "recipient": "+905551234567", "content": "obs 1", "idempotency_key": "00000000-0000-4000-8000-000000000a01"},
			{"channel": "email", "recipient": "u@example.com", "content": "obs 2", "idempotency_key": "00000000-0000-4000-8000-000000000a02"},
			{"channel": "push",  "recipient": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef", "content": "obs 3", "idempotency_key": "00000000-0000-4000-8000-000000000a03"}
		]
	}`
	resp := postJSON(t, srv, "/v1/notifications/batch", body)
	resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	after := histogramObservation(t, metrics.APIBatchSize.WithLabelValues(endpoint))
	assert.Equal(t, before.SampleCount+1, after.SampleCount, "histogram sample count must rise by 1 per successful batch")
	assert.Equal(t, before.SampleSum+float64(requestSize), after.SampleSum, "histogram sample sum must rise by the request size")
}

// TestHandleBatchCreate_OversizedBatch_NoObservation asserts the
// histogram is NOT observed when ValidateBatchCreate rejects the
// batch as oversized — the spec locks "observe only after validation
// returns clean" so 413 / 400 paths must skip the observation.
func TestHandleBatchCreate_OversizedBatch_NoObservation(t *testing.T) {
	const endpoint = "POST /v1/notifications/batch"
	before := histogramObservation(t, metrics.APIBatchSize.WithLabelValues(endpoint))

	fs := &fakeStore{}
	srv := newTestServer(t, fs)

	items := make([]string, 0, batchMax+1)
	for i := 0; i < batchMax+1; i++ {
		items = append(items, fmt.Sprintf(
			`{"channel":"sms","recipient":"+9055512%05d","content":"x","idempotency_key":"00000000-0000-4000-8000-%012d"}`,
			i+10000, i+1,
		))
	}
	body := `{"notifications":[` + strings.Join(items, ",") + `]}`
	resp := postJSON(t, srv, "/v1/notifications/batch", body)
	resp.Body.Close()
	require.Equal(t, http.StatusRequestEntityTooLarge, resp.StatusCode)

	after := histogramObservation(t, metrics.APIBatchSize.WithLabelValues(endpoint))
	assert.Equal(t, before.SampleCount, after.SampleCount, "oversized batch must not observe the histogram")
}

// histogramSnapshot captures the SampleCount + SampleSum at a point
// in time so a subsequent observation can be asserted as a delta
// rather than depending on the absolute total (the registry is
// process-shared across tests).
type histogramSnapshot struct {
	SampleCount uint64
	SampleSum   float64
}

func histogramObservation(t *testing.T, h prometheus.Observer) histogramSnapshot {
	t.Helper()
	collector, ok := h.(prometheus.Metric)
	require.True(t, ok, "histogram Observer must satisfy prometheus.Metric")
	var m dto.Metric
	require.NoError(t, collector.Write(&m))
	require.NotNil(t, m.Histogram, "metric must carry a Histogram payload")
	require.NotNil(t, m.Histogram.SampleCount)
	require.NotNil(t, m.Histogram.SampleSum)
	return histogramSnapshot{
		SampleCount: *m.Histogram.SampleCount,
		SampleSum:   *m.Histogram.SampleSum,
	}
}
