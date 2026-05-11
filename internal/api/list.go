package api

import (
	"net/http"
)

// handleList implements GET /v1/notifications per
// docs/design/03-api.md §GET /v1/notifications and
// docs/phases/04-api-completeness.md §5.
//
// Behavior:
//
//  1. Parse query params via parseListRequest. On any failure surface
//     400 validation_failed with every issue at once (no
//     short-circuiting — same shape as the create handler).
//  2. Call deps.Store.ListNotifications with the parsed filters,
//     offset, and limit. Bubble store errors into 500 internal_error.
//  3. Render each row via renderNotificationWithoutAttempts and respond
//     200 with the ListResponse envelope. An empty match is a 200 with
//     `notifications: []` (the list endpoint is a query, not a lookup
//     — empty results are valid).
func handleList(deps Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		req, issues := parseListRequest(r)
		if len(issues) > 0 {
			writeValidationFailed(w, issues)
			return
		}

		rows, hasMore, err := deps.Store.ListNotifications(r.Context(), req.Filters, req.Offset, req.Limit)
		if err != nil {
			deps.Logger.Error("api: list notifications", "err", err)
			writeInternalError(w)
			return
		}

		out := ListResponse{
			Notifications: make([]NotificationResponse, 0, len(rows)),
			Offset:        req.Offset,
			Limit:         req.Limit,
			HasMore:       hasMore,
		}
		for _, n := range rows {
			out.Notifications = append(out.Notifications, renderNotificationWithoutAttempts(n))
		}

		writeJSON(w, http.StatusOK, out)
	}
}
