package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tarkandikmen/notifications/internal/store"
)

// cancelledRow returns a populated store.Notification shaped like the
// post-trigger row CancelNotification returns from T3 or T11: status
// is "CANCELLED" by the time the handler sees the row, updated_at
// reflects the trigger's refresh.
//
// The single helper is shared across the cancel handler tests so a
// regression in the wire shape (priority translation, ID stringification,
// time formatting) surfaces against every test, not just one.
func cancelledRow(t *testing.T, idStr string) store.Notification {
	t.Helper()
	id := uuid.MustParse(idStr)
	content := "cancel me"
	created := time.Date(2026, 5, 11, 11, 30, 0, 0, time.UTC)
	updated := time.Date(2026, 5, 11, 11, 31, 0, 0, time.UTC)
	return store.Notification{
		ID:             id,
		Channel:        "sms",
		Recipient:      "+905551234567",
		Priority:       1,
		Content:        &content,
		Status:         "CANCELLED",
		Attempt:        0,
		EligibleAt:     created,
		IdempotencyKey: "00000000-0000-4000-8000-000000000c01",
		CreatedAt:      created,
		UpdatedAt:      updated,
	}
}

// postNoBody is the cancel-handler counterpart of postJSON: the cancel
// endpoint accepts a POST with an empty body per
// docs/design/03-api.md §POST /v1/notifications/{id}/cancel.
func postNoBody(t *testing.T, url string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

// TestHandleCancel_PendingPath_200 wires the fake to return a row with
// Status=CANCELLED (the post-T3 shape) and asserts the response is
// 200 with the rendered notification body. The store is the source of
// truth for status transitions; the handler trusts the returned row.
func TestHandleCancel_PendingPath_200(t *testing.T) {
	row := cancelledRow(t, "01890000-0000-7000-8000-000000000c01")
	fs := &fakeStore{cancelRow: row}
	srv := newTestServer(t, fs)

	resp := postNoBody(t, srv.URL+"/v1/notifications/"+row.ID.String()+"/cancel")
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	var got NotificationResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	assert.Equal(t, row.ID.String(), got.ID)
	assert.Equal(t, "CANCELLED", got.Status)
	assert.Equal(t, "sms", got.Channel)
	assert.Equal(t, "normal", got.Priority)

	require.Equal(t, 1, fs.cancelCalled)
	assert.Equal(t, row.ID, fs.cancelArg)
}

// TestHandleCancel_DispatchedPath_200 mirrors the pending-path test
// against a notification that was DISPATCHED at cancel time. The wire
// shape is identical to T3 — Status reads as CANCELLED post-trigger;
// the client cannot tell T3 from T11 from the response shape (and is
// not meant to, per docs/design/03-api.md §POST /v1/notifications/{id}/cancel).
func TestHandleCancel_DispatchedPath_200(t *testing.T) {
	row := cancelledRow(t, "01890000-0000-7000-8000-000000000c02")
	row.Attempt = 1
	fs := &fakeStore{cancelRow: row}
	srv := newTestServer(t, fs)

	resp := postNoBody(t, srv.URL+"/v1/notifications/"+row.ID.String()+"/cancel")
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	var got NotificationResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	assert.Equal(t, "CANCELLED", got.Status)
	assert.Equal(t, 1, got.Attempt, "attempt reflects the in-flight attempt at T11 commit")
}

// TestHandleCancel_AlreadyCancelled_200_Idempotent insists a no-op
// cancel (row already CANCELLED before the call) surfaces the same 200
// wire shape as a transitioning cancel. The store reports no error;
// the handler reports no error.
func TestHandleCancel_AlreadyCancelled_200_Idempotent(t *testing.T) {
	row := cancelledRow(t, "01890000-0000-7000-8000-000000000c03")
	fs := &fakeStore{cancelRow: row}
	srv := newTestServer(t, fs)

	resp := postNoBody(t, srv.URL+"/v1/notifications/"+row.ID.String()+"/cancel")
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	var got NotificationResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&got))
	assert.Equal(t, "CANCELLED", got.Status)
}

// TestHandleCancel_AttemptsOmitted asserts the cancel response body
// omits the nested attempts key per docs/design/03-api.md §Notification
// representation ("List, batch, and cancel responses do not include
// nested attempts"). The single-GET handler is the only path that
// renders attempts.
func TestHandleCancel_AttemptsOmitted(t *testing.T) {
	row := cancelledRow(t, "01890000-0000-7000-8000-000000000c04")
	fs := &fakeStore{cancelRow: row}
	srv := newTestServer(t, fs)

	resp := postNoBody(t, srv.URL+"/v1/notifications/"+row.ID.String()+"/cancel")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(body, &raw))
	_, present := raw["attempts"]
	assert.False(t, present, "cancel response must not include the attempts key")
}

// TestHandleCancel_DeliveredTerminalState_409 wires the fake to return
// *store.TerminalStateError{CurrentStatus: "DELIVERED"}. The response
// must be 409 with one details[] entry carrying current_status =
// "DELIVERED" per docs/design/03-api.md §Error model.
func TestHandleCancel_DeliveredTerminalState_409(t *testing.T) {
	fs := &fakeStore{
		cancelErr: &store.TerminalStateError{CurrentStatus: "DELIVERED"},
	}
	srv := newTestServer(t, fs)

	id := "01890000-0000-7000-8000-000000000c05"
	resp := postNoBody(t, srv.URL+"/v1/notifications/"+id+"/cancel")
	defer resp.Body.Close()

	require.Equal(t, http.StatusConflict, resp.StatusCode)
	env := decodeErrorEnvelope(t, resp.Body)
	assert.Equal(t, "terminal_state", env.Error.Code)
	require.Len(t, env.Error.Details, 1)

	detail := decodeTerminalStateDetail(t, env.Error.Details[0])
	assert.Equal(t, "DELIVERED", detail.CurrentStatus)
}

// TestHandleCancel_FailedTerminalState_409 mirrors the delivered case
// with FAILED — the same 409 shape, different status string.
func TestHandleCancel_FailedTerminalState_409(t *testing.T) {
	fs := &fakeStore{
		cancelErr: &store.TerminalStateError{CurrentStatus: "FAILED"},
	}
	srv := newTestServer(t, fs)

	id := "01890000-0000-7000-8000-000000000c06"
	resp := postNoBody(t, srv.URL+"/v1/notifications/"+id+"/cancel")
	defer resp.Body.Close()

	require.Equal(t, http.StatusConflict, resp.StatusCode)
	env := decodeErrorEnvelope(t, resp.Body)
	assert.Equal(t, "terminal_state", env.Error.Code)
	require.Len(t, env.Error.Details, 1)

	detail := decodeTerminalStateDetail(t, env.Error.Details[0])
	assert.Equal(t, "FAILED", detail.CurrentStatus)
}

// TestHandleCancel_NotFound_MissingRow asserts a missing notification
// surfaces 404 not_found.
func TestHandleCancel_NotFound_MissingRow(t *testing.T) {
	fs := &fakeStore{cancelErr: store.ErrNotFound}
	srv := newTestServer(t, fs)

	resp := postNoBody(t, srv.URL+"/v1/notifications/01890000-0000-7000-8000-000000000c07/cancel")
	defer resp.Body.Close()

	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	env := decodeErrorEnvelope(t, resp.Body)
	assert.Equal(t, "not_found", env.Error.Code)
}

// TestHandleCancel_NotFound_MalformedID asserts a malformed UUID in
// the path surfaces 404 (mirrors handleGet's posture). The store must
// NOT be called — the handler short-circuits at the UUID parse step.
func TestHandleCancel_NotFound_MalformedID(t *testing.T) {
	fs := &fakeStore{}
	srv := newTestServer(t, fs)

	resp := postNoBody(t, srv.URL+"/v1/notifications/not-a-uuid/cancel")
	defer resp.Body.Close()

	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Zero(t, fs.cancelCalled, "store must not be called for a malformed path id")
}

// TestHandleCancel_StoreError_500 asserts a non-typed store error
// surfaces as 500 internal_error with no leaked details.
func TestHandleCancel_StoreError_500(t *testing.T) {
	fs := &fakeStore{cancelErr: errors.New("boom")}
	srv := newTestServer(t, fs)

	resp := postNoBody(t, srv.URL+"/v1/notifications/01890000-0000-7000-8000-000000000c08/cancel")
	defer resp.Body.Close()

	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	env := decodeErrorEnvelope(t, resp.Body)
	assert.Equal(t, "internal_error", env.Error.Code)
}

// TestHandleCancel_EmptyBodyAccepted insists the cancel endpoint
// accepts a POST with an empty body per docs/design/03-api.md
// §POST /v1/notifications/{id}/cancel ("Request body: empty"). A
// Content-Type-less POST with no body must succeed.
func TestHandleCancel_EmptyBodyAccepted(t *testing.T) {
	row := cancelledRow(t, "01890000-0000-7000-8000-000000000c09")
	fs := &fakeStore{cancelRow: row}
	srv := newTestServer(t, fs)

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/notifications/"+row.ID.String()+"/cancel", strings.NewReader(""))
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
}

// decodeTerminalStateDetail treats a details[] entry as a
// TerminalStateDetail. Same JSON-roundtrip pattern as asFieldIssue and
// decodeConflictDetail.
func decodeTerminalStateDetail(t *testing.T, raw any) TerminalStateDetail {
	t.Helper()
	bs, err := json.Marshal(raw)
	require.NoError(t, err)
	var d TerminalStateDetail
	require.NoError(t, json.Unmarshal(bs, &d))
	return d
}
