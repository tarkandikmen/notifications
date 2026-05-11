package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tarkandikmen/notifications/internal/store"
)

// fakeStore is the in-memory Store the handler tests substitute for the
// real *store.Store per docs/phases/02-walking-skeleton.md §13. Each test
// wires its own per-call behavior via the function fields so the assertions
// stay near the test that needs them.
type fakeStore struct {
	insertCalled int
	insertArg    store.Notification
	insertErr    error

	getRow      store.Notification
	getAttempts []store.DeliveryAttempt
	getErr      error
}

func (f *fakeStore) InsertNotification(_ context.Context, n store.Notification) error {
	f.insertCalled++
	f.insertArg = n
	return f.insertErr
}

func (f *fakeStore) GetNotification(_ context.Context, _ uuid.UUID) (store.Notification, []store.DeliveryAttempt, error) {
	if f.getErr != nil {
		return store.Notification{}, nil, f.getErr
	}
	return f.getRow, f.getAttempts, nil
}

func newTestServer(t *testing.T, fs *fakeStore) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	RegisterRoutes(mux, Deps{
		Store:    fs,
		Registry: prometheus.NewRegistry(),
		Clock:    func() time.Time { return fixedNow },
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// TestHandleHealthz_ExactBody is the byte-match assertion that lived in
// internal/server/server_test.go through Phase 1; per
// docs/phases/02-walking-skeleton.md §6 it relocates here when handleHealthz
// moves into the api package. The body must be exactly `{"status":"ok"}`
// with no trailing newline so json.Encode is intentionally avoided.
func TestHandleHealthz_ExactBody(t *testing.T) {
	rr := httptest.NewRecorder()
	handleHealthz(rr, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	resp := rr.Result()
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "application/json", resp.Header.Get("Content-Type"))

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, `{"status":"ok"}`, string(body))
}

func TestHandleCreate_HappyPath(t *testing.T) {
	fs := &fakeStore{}
	srv := newTestServer(t, fs)

	body := `{
		"channel": "sms",
		"recipient": "+905551234567",
		"content": "phase 2 happy",
		"idempotency_key": "00000000-0000-4000-8000-000000000001"
	}`

	resp := postJSON(t, srv, "/v1/notifications", body)
	defer resp.Body.Close()

	require.Equal(t, http.StatusCreated, resp.StatusCode)

	var got CreateResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	require.NotEmpty(t, got.ID)

	parsed, err := uuid.Parse(got.ID)
	require.NoError(t, err)
	assert.Equal(t, uuid.Version(7), parsed.Version(), "api mints UUIDv7")

	require.Equal(t, 1, fs.insertCalled)
	stored := fs.insertArg
	assert.Equal(t, parsed, stored.ID)
	assert.Equal(t, "sms", stored.Channel)
	assert.Equal(t, "+905551234567", stored.Recipient)
	assert.Equal(t, int16(1), stored.Priority, "default priority is normal=1")
	require.NotNil(t, stored.Content)
	assert.Equal(t, "phase 2 happy", *stored.Content)
	assert.Equal(t, "PENDING", stored.Status)
	assert.Equal(t, 0, stored.Attempt)
	assert.True(t, stored.EligibleAt.Equal(fixedNow), "eligible_at defaults to clock now")
	assert.Nil(t, stored.ScheduledAt)
	assert.Equal(t, "00000000-0000-4000-8000-000000000001", stored.IdempotencyKey)
}

func TestHandleCreate_ScheduledAtSetsEligibleAt(t *testing.T) {
	fs := &fakeStore{}
	srv := newTestServer(t, fs)

	body := `{
		"channel": "sms",
		"recipient": "+905551234567",
		"content": "future",
		"scheduled_at": "2026-05-11T13:00:00Z",
		"idempotency_key": "00000000-0000-4000-8000-000000000002"
	}`

	resp := postJSON(t, srv, "/v1/notifications", body)
	defer resp.Body.Close()

	require.Equal(t, http.StatusCreated, resp.StatusCode)

	require.NotNil(t, fs.insertArg.ScheduledAt)
	want := time.Date(2026, 5, 11, 13, 0, 0, 0, time.UTC)
	assert.True(t, fs.insertArg.ScheduledAt.Equal(want))
	assert.True(t, fs.insertArg.EligibleAt.Equal(want), "eligible_at = scheduled_at when supplied")
}

func TestHandleCreate_PriorityHigh(t *testing.T) {
	fs := &fakeStore{}
	srv := newTestServer(t, fs)

	body := `{
		"channel": "sms",
		"recipient": "+905551234567",
		"content": "x",
		"priority": "high",
		"idempotency_key": "00000000-0000-4000-8000-000000000003"
	}`

	resp := postJSON(t, srv, "/v1/notifications", body)
	defer resp.Body.Close()

	require.Equal(t, http.StatusCreated, resp.StatusCode)
	assert.Equal(t, int16(2), fs.insertArg.Priority)
}

func TestHandleCreate_MalformedJSON(t *testing.T) {
	fs := &fakeStore{}
	srv := newTestServer(t, fs)

	resp := postJSON(t, srv, "/v1/notifications", `{not valid json`)
	defer resp.Body.Close()

	require.Equal(t, http.StatusBadRequest, resp.StatusCode)

	env := decodeErrorEnvelope(t, resp.Body)
	assert.Equal(t, "validation_failed", env.Error.Code)
	require.Len(t, env.Error.Details, 1)
	detail := asFieldIssue(t, env.Error.Details[0])
	assert.Equal(t, "body", detail.Path)
	assert.Equal(t, "malformed JSON", detail.Issue)
	assert.Zero(t, fs.insertCalled, "store must not be called for malformed JSON")
}

// TestHandleCreate_EmailChannel_201Lands is the Phase 3 Chunk 7
// counterpart of Phase 2's channel-restriction test. Per
// docs/phases/03-resilience.md §10 the channel restriction widens to
// {sms, email, push}; an email POST that satisfies the email
// recipient + content rules from docs/design/03-api.md §Validation
// rules now lands a 201 with a freshly minted UUIDv7.
func TestHandleCreate_EmailChannel_201Lands(t *testing.T) {
	fs := &fakeStore{}
	srv := newTestServer(t, fs)

	body := `{
		"channel": "email",
		"recipient": "u@example.com",
		"content": "phase 3 email",
		"idempotency_key": "00000000-0000-4000-8000-000000000010"
	}`

	resp := postJSON(t, srv, "/v1/notifications", body)
	defer resp.Body.Close()

	require.Equal(t, http.StatusCreated, resp.StatusCode)

	var got CreateResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	parsed, err := uuid.Parse(got.ID)
	require.NoError(t, err)
	assert.Equal(t, uuid.Version(7), parsed.Version(), "api mints UUIDv7")

	require.Equal(t, 1, fs.insertCalled)
	stored := fs.insertArg
	assert.Equal(t, "email", stored.Channel)
	assert.Equal(t, "u@example.com", stored.Recipient)
	require.NotNil(t, stored.Content)
	assert.Equal(t, "phase 3 email", *stored.Content)
}

// TestHandleCreate_PushChannel_201Lands mirrors the email test for
// the push channel: an opaque token of length recipientPushMin..max
// + content within content_push_max passes validation and lands a
// 201 per docs/design/03-api.md §Validation rules row `push`.
func TestHandleCreate_PushChannel_201Lands(t *testing.T) {
	fs := &fakeStore{}
	srv := newTestServer(t, fs)

	body := `{
		"channel": "push",
		"recipient": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		"content": "phase 3 push",
		"idempotency_key": "00000000-0000-4000-8000-000000000011"
	}`

	resp := postJSON(t, srv, "/v1/notifications", body)
	defer resp.Body.Close()

	require.Equal(t, http.StatusCreated, resp.StatusCode)

	var got CreateResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	parsed, err := uuid.Parse(got.ID)
	require.NoError(t, err)
	assert.Equal(t, uuid.Version(7), parsed.Version())

	require.Equal(t, 1, fs.insertCalled)
	stored := fs.insertArg
	assert.Equal(t, "push", stored.Channel)
	assert.Equal(t, 64, len(stored.Recipient), "push token preserved as-is")
}

// TestHandleCreate_UnknownChannel_400 keeps the negative coverage
// for an unknown channel value: only sms / email / push are accepted
// in Phase 3 per docs/design/01-schema.md §Domain values for
// notifications.channel.
func TestHandleCreate_UnknownChannel_400(t *testing.T) {
	fs := &fakeStore{}
	srv := newTestServer(t, fs)

	body := `{
		"channel": "fax",
		"recipient": "+905551234567",
		"content": "hello",
		"idempotency_key": "00000000-0000-4000-8000-000000000012"
	}`

	resp := postJSON(t, srv, "/v1/notifications", body)
	defer resp.Body.Close()

	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	env := decodeErrorEnvelope(t, resp.Body)
	assert.Equal(t, "validation_failed", env.Error.Code)
	require.NotEmpty(t, env.Error.Details)
	first := asFieldIssue(t, env.Error.Details[0])
	assert.Equal(t, "channel", first.Path)
	assert.Contains(t, first.Issue, "must be")
	assert.Zero(t, fs.insertCalled)
}

func TestHandleCreate_AllValidationsCollectedAtOnce(t *testing.T) {
	fs := &fakeStore{}
	srv := newTestServer(t, fs)

	// Empty body: every required rule fails. The handler must surface
	// channel + recipient + content + idempotency_key in one response.
	resp := postJSON(t, srv, "/v1/notifications", `{}`)
	defer resp.Body.Close()

	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	env := decodeErrorEnvelope(t, resp.Body)
	assert.Equal(t, "validation_failed", env.Error.Code)

	paths := make(map[string]bool, len(env.Error.Details))
	for _, raw := range env.Error.Details {
		paths[asFieldIssue(t, raw).Path] = true
	}
	assert.True(t, paths["channel"], "expected channel issue")
	assert.True(t, paths["recipient"], "expected recipient issue")
	assert.True(t, paths["content"], "expected content issue")
	assert.True(t, paths["idempotency_key"], "expected idempotency_key issue")
}

func TestHandleCreate_IdempotencyConflict(t *testing.T) {
	existingID := uuid.MustParse("11111111-1111-7111-8111-111111111111")
	fs := &fakeStore{
		insertErr: &store.IdempotencyConflictError{
			IdempotencyKey: "00000000-0000-4000-8000-000000000005",
			ExistingID:     existingID,
			ExistingStatus: "DELIVERED",
		},
	}
	srv := newTestServer(t, fs)

	body := `{
		"channel": "sms",
		"recipient": "+905551234567",
		"content": "duplicate",
		"idempotency_key": "00000000-0000-4000-8000-000000000005"
	}`

	resp := postJSON(t, srv, "/v1/notifications", body)
	defer resp.Body.Close()

	require.Equal(t, http.StatusConflict, resp.StatusCode)
	env := decodeErrorEnvelope(t, resp.Body)
	assert.Equal(t, "duplicate_idempotency_keys", env.Error.Code)
	require.Len(t, env.Error.Details, 1)

	raw, err := json.Marshal(env.Error.Details[0])
	require.NoError(t, err)
	var detail IdempotencyConflictDetail
	require.NoError(t, json.Unmarshal(raw, &detail))
	assert.Equal(t, "00000000-0000-4000-8000-000000000005", detail.IdempotencyKey)
	assert.Equal(t, existingID.String(), detail.ExistingID)
	assert.Equal(t, "DELIVERED", detail.Status)
}

func TestHandleCreate_StoreErrorIsInternal(t *testing.T) {
	fs := &fakeStore{insertErr: errors.New("boom")}
	srv := newTestServer(t, fs)

	body := `{
		"channel": "sms",
		"recipient": "+905551234567",
		"content": "x",
		"idempotency_key": "00000000-0000-4000-8000-000000000006"
	}`

	resp := postJSON(t, srv, "/v1/notifications", body)
	defer resp.Body.Close()

	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	env := decodeErrorEnvelope(t, resp.Body)
	assert.Equal(t, "internal_error", env.Error.Code)
}

func TestHandleGet_NotFound_MissingRow(t *testing.T) {
	fs := &fakeStore{getErr: store.ErrNotFound}
	srv := newTestServer(t, fs)

	resp, err := http.Get(srv.URL + "/v1/notifications/00000000-0000-4000-8000-000000000099")
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	env := decodeErrorEnvelope(t, resp.Body)
	assert.Equal(t, "not_found", env.Error.Code)
}

func TestHandleGet_NotFound_MalformedUUID(t *testing.T) {
	fs := &fakeStore{}
	srv := newTestServer(t, fs)

	resp, err := http.Get(srv.URL + "/v1/notifications/not-a-uuid")
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestHandleGet_HappyPath_OmitsNullableFields(t *testing.T) {
	id := uuid.MustParse("01890000-0000-7000-8000-000000000001")
	content := "hello"
	created := time.Date(2026, 5, 11, 11, 30, 0, 0, time.UTC)
	updated := time.Date(2026, 5, 11, 11, 31, 0, 0, time.UTC)
	eligible := time.Date(2026, 5, 11, 11, 30, 0, 0, time.UTC)

	fs := &fakeStore{
		getRow: store.Notification{
			ID:             id,
			Channel:        "sms",
			Recipient:      "+905551234567",
			Priority:       1,
			Content:        &content,
			Status:         "PENDING",
			Attempt:        0,
			EligibleAt:     eligible,
			IdempotencyKey: "00000000-0000-4000-8000-000000000010",
			CreatedAt:      created,
			UpdatedAt:      updated,
		},
		getAttempts: nil,
	}
	srv := newTestServer(t, fs)

	resp, err := http.Get(srv.URL + "/v1/notifications/" + id.String())
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	// Decode loosely to check both populated and omitted fields.
	var raw map[string]any
	require.NoError(t, json.Unmarshal(body, &raw))
	assert.Equal(t, id.String(), raw["id"])
	assert.Equal(t, "sms", raw["channel"])
	assert.Equal(t, "normal", raw["priority"])
	assert.Equal(t, "PENDING", raw["status"])
	assert.Equal(t, "hello", raw["content"])

	for _, omitted := range []string{"batch_id", "template", "template_data", "scheduled_at", "failure_reason"} {
		_, present := raw[omitted]
		assert.False(t, present, "expected %q to be omitted from the response", omitted)
	}

	// attempts: [] is always present (an empty array, not omitted).
	attempts, ok := raw["attempts"].([]any)
	require.True(t, ok)
	assert.Empty(t, attempts)
}

func TestHandleGet_HappyPath_PopulatedNullableFields(t *testing.T) {
	id := uuid.MustParse("01890000-0000-7000-8000-000000000002")
	batch := uuid.MustParse("11110000-0000-7000-8000-000000000020")
	content := "delivered"
	scheduled := time.Date(2026, 5, 11, 11, 30, 0, 0, time.UTC)
	failed := "permanent_error"
	created := time.Date(2026, 5, 11, 11, 0, 0, 0, time.UTC)
	updated := time.Date(2026, 5, 11, 11, 1, 0, 0, time.UTC)

	finished := time.Date(2026, 5, 11, 11, 0, 30, 0, time.UTC)
	cls := "success"
	attempts := []store.DeliveryAttempt{{
		NotificationID: id,
		Attempt:        1,
		StartedAt:      time.Date(2026, 5, 11, 11, 0, 0, 0, time.UTC),
		FinishedAt:     &finished,
		Classification: &cls,
		Response:       json.RawMessage(`{"status":"accepted"}`),
	}}

	fs := &fakeStore{
		getRow: store.Notification{
			ID:             id,
			BatchID:        uuid.NullUUID{UUID: batch, Valid: true},
			Channel:        "sms",
			Recipient:      "+905551234567",
			Priority:       2,
			Content:        &content,
			Status:         "DELIVERED",
			Attempt:        1,
			EligibleAt:     scheduled,
			ScheduledAt:    &scheduled,
			FailureReason:  &failed,
			IdempotencyKey: "00000000-0000-4000-8000-000000000020",
			CreatedAt:      created,
			UpdatedAt:      updated,
		},
		getAttempts: attempts,
	}
	srv := newTestServer(t, fs)

	resp, err := http.Get(srv.URL + "/v1/notifications/" + id.String())
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	var got map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))

	assert.Equal(t, batch.String(), got["batch_id"])
	assert.Equal(t, "DELIVERED", got["status"])
	assert.Equal(t, "high", got["priority"])
	assert.Equal(t, "permanent_error", got["failure_reason"])
	assert.Equal(t, "2026-05-11T11:30:00Z", got["scheduled_at"])

	rawAttempts, ok := got["attempts"].([]any)
	require.True(t, ok)
	require.Len(t, rawAttempts, 1)

	first, ok := rawAttempts[0].(map[string]any)
	require.True(t, ok)
	assert.EqualValues(t, 1, first["attempt"])
	assert.Equal(t, "success", first["classification"])
	assert.Equal(t, "2026-05-11T11:00:00Z", first["started_at"])
	assert.Equal(t, "2026-05-11T11:00:30Z", first["finished_at"])

	respBody, ok := first["response"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "accepted", respBody["status"])

	// Attempt-level nullables that were not set must be omitted.
	for _, omitted := range []string{"error_message"} {
		_, present := first[omitted]
		assert.False(t, present, "expected attempt field %q to be omitted", omitted)
	}
}

func TestRegisterRoutes_HealthzAndMetrics(t *testing.T) {
	fs := &fakeStore{}
	srv := newTestServer(t, fs)

	resp, err := http.Get(srv.URL + "/healthz")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	resp2, err := http.Get(srv.URL + "/metrics")
	require.NoError(t, err)
	defer resp2.Body.Close()
	assert.Equal(t, http.StatusOK, resp2.StatusCode)
}

// postJSON is a small test helper that POSTs the body and returns the
// response. The caller is responsible for closing resp.Body.
func postJSON(t *testing.T, srv *httptest.Server, path, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, srv.URL+path, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := srv.Client().Do(req)
	require.NoError(t, err)
	return resp
}

func decodeErrorEnvelope(t *testing.T, body io.Reader) ErrorEnvelope {
	t.Helper()
	var env ErrorEnvelope
	require.NoError(t, json.NewDecoder(body).Decode(&env))
	return env
}

// asFieldIssue treats a details[] entry as a FieldIssue. Marshaling
// through JSON keeps the test independent of how the handler stuffs
// concrete types into the []any details slice.
func asFieldIssue(t *testing.T, raw any) FieldIssue {
	t.Helper()
	bs, err := json.Marshal(raw)
	require.NoError(t, err)
	var fi FieldIssue
	require.NoError(t, json.Unmarshal(bs, &fi))
	return fi
}
