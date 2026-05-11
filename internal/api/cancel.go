package api

import (
	"errors"
	"net/http"

	"github.com/google/uuid"

	"github.com/tarkandikmen/notifications/internal/store"
)

// handleGetBatch implements GET /v1/batches/{id} per
// docs/design/03-api.md §GET /v1/batches/{id} and
// docs/phases/04-api-completeness.md §6.
//
// Behavior:
//
//  1. Parse {id} as a UUID. Malformed → 404 not_found (mirrors
//     handleGet's path-id parsing posture).
//  2. Call deps.Store.GetBatch. ErrNotFound → 404; other errors →
//     500 internal_error.
//  3. Render rows via renderNotificationWithoutAttempts (no nested
//     attempts per docs/design/03-api.md §Notification representation),
//     respond 200 with the BatchGetResponse envelope.
//
// The store enforces "no rows = ErrNotFound" so the handler never has
// to discriminate empty-match from missing-batch.
func handleGetBatch(deps Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		batchID, err := uuid.Parse(r.PathValue("id"))
		if err != nil {
			writeNotFound(w)
			return
		}

		rows, err := deps.Store.GetBatch(r.Context(), batchID)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				writeNotFound(w)
				return
			}
			deps.Logger.Error("api: get batch", "err", err, "batch_id", batchID)
			writeInternalError(w)
			return
		}

		out := BatchGetResponse{
			BatchID:       batchID.String(),
			Notifications: make([]NotificationResponse, 0, len(rows)),
		}
		for _, n := range rows {
			out.Notifications = append(out.Notifications, renderNotificationWithoutAttempts(n))
		}

		writeJSON(w, http.StatusOK, out)
	}
}

// handleCancel implements POST /v1/notifications/{id}/cancel per
// docs/design/03-api.md §POST /v1/notifications/{id}/cancel and
// docs/phases/04-api-completeness.md §7. Transitions T3 (PENDING →
// CANCELLED, emit events.notification) and T11 (DISPATCHED → CANCELLED,
// no emit) are owned by the store; the handler is a thin translator.
//
// Behavior:
//
//  1. Parse {id} as a UUID. Malformed → 404 not_found (mirrors
//     handleGet's path-id parsing posture).
//  2. Call deps.Store.CancelNotification:
//     - ErrNotFound → 404 not_found.
//     - *TerminalStateError (extracted via errors.As) → 409
//     terminal_state with one TerminalStateDetail in details[].
//     - other error → 500 internal_error.
//  3. On success (T3, T11, or idempotent CANCELLED no-op) → 200 OK
//     with renderNotificationWithoutAttempts(n). The wire shape is
//     identical for all three; the client cannot distinguish
//     "cancelled by this call" from "was already cancelled" by the
//     response shape alone, which is the intended idempotent contract
//     per docs/design/03-api.md.
//
// Best-effort concurrency note: a T11 cancel may be silently
// overwritten by a worker outcome (T4–T8) on the row's current
// attempt; the realized state surfaces via a subsequent GET. See
// docs/phases/04-api-completeness.md §7 Concurrency note and
// docs/design/02-state-machine.md §Cancellation race.
func handleCancel(deps Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := uuid.Parse(r.PathValue("id"))
		if err != nil {
			writeNotFound(w)
			return
		}

		n, err := deps.Store.CancelNotification(r.Context(), id)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				writeNotFound(w)
				return
			}
			var terr *store.TerminalStateError
			if errors.As(err, &terr) {
				writeTerminalState(w, terr.CurrentStatus)
				return
			}
			deps.Logger.Error("api: cancel notification", "err", err, "id", id)
			writeInternalError(w)
			return
		}

		writeJSON(w, http.StatusOK, renderNotificationWithoutAttempts(n))
	}
}

// writeTerminalState writes the canonical 409 terminal_state envelope
// per docs/design/03-api.md §Error model with one TerminalStateDetail
// carrying the row's observed terminal status.
func writeTerminalState(w http.ResponseWriter, currentStatus string) {
	writeJSON(w, http.StatusConflict, ErrorEnvelope{
		Error: ErrorBody{
			Code:    "terminal_state",
			Message: "notification is in a terminal state and cannot be cancelled",
			Details: []any{TerminalStateDetail{CurrentStatus: currentStatus}},
		},
	})
}
