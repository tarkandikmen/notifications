package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tarkandikmen/notifications/internal/metrics"
	"github.com/tarkandikmen/notifications/internal/store"
)

// fakeNotificationRow returns a populated store.Notification suitable
// for list / batch-get response assertions. Centralised so individual
// tests can vary one or two fields without re-listing the whole
// struct each time.
func fakeNotificationRow(t *testing.T, idStr, channel, recipient, content, key string) store.Notification {
	t.Helper()
	id := uuid.MustParse(idStr)
	created := time.Date(2026, 5, 11, 11, 30, 0, 0, time.UTC)
	updated := time.Date(2026, 5, 11, 11, 31, 0, 0, time.UTC)
	c := content
	return store.Notification{
		ID:             id,
		Channel:        channel,
		Recipient:      recipient,
		Priority:       1,
		Content:        &c,
		Status:         "PENDING",
		Attempt:        0,
		EligibleAt:     created,
		IdempotencyKey: key,
		CreatedAt:      created,
		UpdatedAt:      updated,
	}
}

// TestHandleList_HappyPath asserts a populated list response: every
// row appears in order, has_more reflects the store, and offset / limit
// are echoed verbatim.
func TestHandleList_HappyPath(t *testing.T) {
	rows := []store.Notification{
		fakeNotificationRow(t, "01890000-0000-7000-8000-000000000201", "sms", "+905551234567", "row 1", "00000000-0000-4000-8000-000000000201"),
		fakeNotificationRow(t, "01890000-0000-7000-8000-000000000202", "sms", "+905551234568", "row 2", "00000000-0000-4000-8000-000000000202"),
	}
	fs := &fakeStore{listRows: rows, listHasMore: false}
	srv := newTestServer(t, fs)

	resp, err := http.Get(srv.URL + "/v1/notifications")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var got ListResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	require.Len(t, got.Notifications, 2)
	assert.Equal(t, rows[0].ID.String(), got.Notifications[0].ID)
	assert.Equal(t, rows[1].ID.String(), got.Notifications[1].ID)
	assert.False(t, got.HasMore)
	assert.Equal(t, 0, got.Offset)
	assert.Equal(t, listDefaultLimit, got.Limit)
}

// TestHandleList_AttemptsOmitted insists list items render WITHOUT the
// nested attempts key (list, batch, and cancel responses do not
// include nested attempts).
func TestHandleList_AttemptsOmitted(t *testing.T) {
	rows := []store.Notification{
		fakeNotificationRow(t, "01890000-0000-7000-8000-000000000210", "sms", "+905551234567", "no attempts", "00000000-0000-4000-8000-000000000210"),
	}
	fs := &fakeStore{listRows: rows}
	srv := newTestServer(t, fs)

	resp, err := http.Get(srv.URL + "/v1/notifications")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(body, &raw))
	notifs, ok := raw["notifications"].([]any)
	require.True(t, ok)
	require.Len(t, notifs, 1)

	first, ok := notifs[0].(map[string]any)
	require.True(t, ok)
	_, present := first["attempts"]
	assert.False(t, present, "list items must not include the attempts key")
}

// TestHandleList_HasMoreTrue asserts the has_more field round-trips
// through the response when the store reports more rows beyond the
// page boundary.
func TestHandleList_HasMoreTrue(t *testing.T) {
	rows := []store.Notification{
		fakeNotificationRow(t, "01890000-0000-7000-8000-000000000220", "sms", "+905551234567", "row a", "00000000-0000-4000-8000-000000000220"),
	}
	fs := &fakeStore{listRows: rows, listHasMore: true}
	srv := newTestServer(t, fs)

	resp, err := http.Get(srv.URL + "/v1/notifications?limit=1")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var got ListResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	assert.True(t, got.HasMore)
	assert.Equal(t, 1, got.Limit)
}

// TestHandleList_EmptyResult_200_EmptyList insists an empty match
// returns 200 with notifications: [] (not 404 — list endpoints surface
// empty matches as success).
func TestHandleList_EmptyResult_200_EmptyList(t *testing.T) {
	fs := &fakeStore{listRows: nil, listHasMore: false}
	srv := newTestServer(t, fs)

	resp, err := http.Get(srv.URL + "/v1/notifications?status=DELIVERED")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(body, &raw))
	notifs, ok := raw["notifications"].([]any)
	require.True(t, ok, "notifications field must be a JSON array, not null")
	assert.Empty(t, notifs)
}

// TestHandleList_FilterCombination asserts the parsed filters match
// the query string: each query-param value flows into the store call's
// ListFilters argument.
func TestHandleList_FilterCombination(t *testing.T) {
	fs := &fakeStore{}
	srv := newTestServer(t, fs)

	q := "status=DISPATCHED&channel=email&priority=high&batch_id=11110000-0000-7000-8000-000000000222"
	resp, err := http.Get(srv.URL + "/v1/notifications?" + q)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	require.Equal(t, 1, fs.listCalled)
	require.NotNil(t, fs.listFilters.Status)
	assert.Equal(t, "DISPATCHED", *fs.listFilters.Status)
	require.NotNil(t, fs.listFilters.Channel)
	assert.Equal(t, "email", *fs.listFilters.Channel)
	require.NotNil(t, fs.listFilters.Priority)
	assert.Equal(t, int16(2), *fs.listFilters.Priority)
	require.NotNil(t, fs.listFilters.BatchID)
	assert.Equal(t, "11110000-0000-7000-8000-000000000222", fs.listFilters.BatchID.String())
}

// TestHandleList_DefaultsApplied verifies an empty query string lands
// at offset=0, limit=listDefaultLimit on the store call.
func TestHandleList_DefaultsApplied(t *testing.T) {
	fs := &fakeStore{}
	srv := newTestServer(t, fs)

	resp, err := http.Get(srv.URL + "/v1/notifications")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	require.Equal(t, 1, fs.listCalled)
	assert.Equal(t, 0, fs.listOffset)
	assert.Equal(t, listDefaultLimit, fs.listLimit)
}

// TestHandleList_OffsetLimitForwarded asserts a non-default offset /
// limit lands on the store call (catches a regression where the
// handler ignores the parsed values).
func TestHandleList_OffsetLimitForwarded(t *testing.T) {
	fs := &fakeStore{}
	srv := newTestServer(t, fs)

	resp, err := http.Get(srv.URL + "/v1/notifications?offset=20&limit=5")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	require.Equal(t, 1, fs.listCalled)
	assert.Equal(t, 20, fs.listOffset)
	assert.Equal(t, 5, fs.listLimit)
}

// TestHandleList_InvalidParams_400 asserts every invalid query param
// surfaces as a single 400 response with one details[] entry per
// failing rule. The store must NOT be called.
func TestHandleList_InvalidParams_400(t *testing.T) {
	fs := &fakeStore{}
	srv := newTestServer(t, fs)

	resp, err := http.Get(srv.URL + "/v1/notifications?offset=-1&limit=999&status=NOPE")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)

	env := decodeErrorEnvelope(t, resp.Body)
	assert.Equal(t, "validation_failed", env.Error.Code)

	paths := make(map[string]bool, len(env.Error.Details))
	for _, raw := range env.Error.Details {
		paths[asFieldIssue(t, raw).Path] = true
	}
	assert.True(t, paths["offset"])
	assert.True(t, paths["limit"])
	assert.True(t, paths["status"])

	assert.Zero(t, fs.listCalled, "store must not be called for an invalid request")
}

// TestHandleList_StoreError_500 asserts a store-layer error surfaces
// as a 500 internal_error with no leaked details.
func TestHandleList_StoreError_500(t *testing.T) {
	fs := &fakeStore{listErr: errors.New("boom")}
	srv := newTestServer(t, fs)

	resp, err := http.Get(srv.URL + "/v1/notifications")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	env := decodeErrorEnvelope(t, resp.Body)
	assert.Equal(t, "internal_error", env.Error.Code)
}

// TestHandleList_ObservesResultSize asserts the
// api_list_result_size_items histogram observation rises by 1 per
// list request, with the observed value matching the post-pagination
// row count returned by the store. The observation reflects the
// rendered result size, NOT the requested limit — so dashboards can
// graph "page-fill ratio" against has_more=true tails.
func TestHandleList_ObservesResultSize(t *testing.T) {
	const endpoint = "GET /v1/notifications"
	rows := []store.Notification{
		fakeNotificationRow(t, "01890000-0000-7000-8000-000000000a01", "sms", "+905551234567", "row 1", "00000000-0000-4000-8000-000000000a01"),
		fakeNotificationRow(t, "01890000-0000-7000-8000-000000000a02", "sms", "+905551234568", "row 2", "00000000-0000-4000-8000-000000000a02"),
	}
	fs := &fakeStore{listRows: rows, listHasMore: false}

	before := histogramObservation(t, metrics.APIListResultSize.WithLabelValues(endpoint))

	srv := newTestServer(t, fs)
	resp, err := http.Get(srv.URL + "/v1/notifications")
	require.NoError(t, err)
	_ = resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	after := histogramObservation(t, metrics.APIListResultSize.WithLabelValues(endpoint))
	assert.Equal(t, before.SampleCount+1, after.SampleCount, "histogram sample count must rise by 1 per list request")
	assert.Equal(t, before.SampleSum+float64(len(rows)), after.SampleSum, "histogram sample sum must rise by the rendered row count")
}

// TestHandleList_EmptyResult_ObservesZero asserts the histogram is
// observed with value 0 when the store returns no rows — the
// observation reflects the rendered result size and an empty match
// is still a successful query (200 with notifications: []), so
// dashboards see "zero-result page" as a measurable event.
func TestHandleList_EmptyResult_ObservesZero(t *testing.T) {
	const endpoint = "GET /v1/notifications"
	fs := &fakeStore{listRows: nil, listHasMore: false}

	before := histogramObservation(t, metrics.APIListResultSize.WithLabelValues(endpoint))

	srv := newTestServer(t, fs)
	resp, err := http.Get(srv.URL + "/v1/notifications?status=DELIVERED")
	require.NoError(t, err)
	_ = resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	after := histogramObservation(t, metrics.APIListResultSize.WithLabelValues(endpoint))
	assert.Equal(t, before.SampleCount+1, after.SampleCount, "even an empty match observes the histogram")
	assert.Equal(t, before.SampleSum, after.SampleSum, "empty match's value-zero observation leaves the sum unchanged")
}

// TestHandleGetBatch_HappyPath asserts a populated batch response: the
// requested batch_id round-trips, every row appears, and items have no
// nested attempts.
func TestHandleGetBatch_HappyPath(t *testing.T) {
	batchID := uuid.MustParse("11110000-0000-7000-8000-000000000300")
	rows := []store.Notification{
		fakeNotificationRow(t, "01890000-0000-7000-8000-000000000301", "sms", "+905551234567", "row 1", "00000000-0000-4000-8000-000000000301"),
		fakeNotificationRow(t, "01890000-0000-7000-8000-000000000302", "sms", "+905551234568", "row 2", "00000000-0000-4000-8000-000000000302"),
	}
	for i := range rows {
		rows[i].BatchID = uuid.NullUUID{UUID: batchID, Valid: true}
	}
	fs := &fakeStore{getBatchRows: rows}
	srv := newTestServer(t, fs)

	resp, err := http.Get(srv.URL + "/v1/batches/" + batchID.String())
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	var got BatchGetResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	assert.Equal(t, batchID.String(), got.BatchID)
	require.Len(t, got.Notifications, 2)
	for _, n := range got.Notifications {
		require.NotNil(t, n.BatchID)
		assert.Equal(t, batchID.String(), *n.BatchID)
	}

	require.Equal(t, 1, fs.getBatchCalled)
	assert.Equal(t, batchID, fs.getBatchArg)
}

// TestHandleGetBatch_AttemptsOmitted asserts batch items render WITHOUT
// the nested attempts key, mirroring the list-endpoint contract.
func TestHandleGetBatch_AttemptsOmitted(t *testing.T) {
	batchID := uuid.MustParse("11110000-0000-7000-8000-000000000310")
	rows := []store.Notification{
		fakeNotificationRow(t, "01890000-0000-7000-8000-000000000311", "sms", "+905551234567", "row", "00000000-0000-4000-8000-000000000311"),
	}
	rows[0].BatchID = uuid.NullUUID{UUID: batchID, Valid: true}
	fs := &fakeStore{getBatchRows: rows}
	srv := newTestServer(t, fs)

	resp, err := http.Get(srv.URL + "/v1/batches/" + batchID.String())
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(body, &raw))
	notifs, ok := raw["notifications"].([]any)
	require.True(t, ok)
	require.Len(t, notifs, 1)

	first, ok := notifs[0].(map[string]any)
	require.True(t, ok)
	_, present := first["attempts"]
	assert.False(t, present, "batch-get items must not include the attempts key")
}

// TestHandleGetBatch_NotFound_MissingBatch asserts a missing batch_id
// surfaces 404 not_found.
func TestHandleGetBatch_NotFound_MissingBatch(t *testing.T) {
	fs := &fakeStore{getBatchErr: store.ErrNotFound}
	srv := newTestServer(t, fs)

	resp, err := http.Get(srv.URL + "/v1/batches/11110000-0000-7000-8000-000000000999")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)

	env := decodeErrorEnvelope(t, resp.Body)
	assert.Equal(t, "not_found", env.Error.Code)
}

// TestHandleGetBatch_NotFound_MalformedID asserts a malformed UUID in
// the path surfaces 404 (mirrors handleGet's posture).
func TestHandleGetBatch_NotFound_MalformedID(t *testing.T) {
	fs := &fakeStore{}
	srv := newTestServer(t, fs)

	resp, err := http.Get(srv.URL + "/v1/batches/not-a-uuid")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)

	assert.Zero(t, fs.getBatchCalled, "store must not be called for a malformed path id")
}

// TestHandleGetBatch_StoreError_500 asserts a store-layer error other
// than ErrNotFound surfaces as 500 internal_error.
func TestHandleGetBatch_StoreError_500(t *testing.T) {
	fs := &fakeStore{getBatchErr: errors.New("boom")}
	srv := newTestServer(t, fs)

	resp, err := http.Get(srv.URL + "/v1/batches/11110000-0000-7000-8000-000000000888")
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	env := decodeErrorEnvelope(t, resp.Body)
	assert.Equal(t, "internal_error", env.Error.Code)
}
