package api

import (
	"net/http"

	"github.com/tarkandikmen/notifications/internal/metrics"
)

// handleList implements GET /v1/notifications.
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

		// Observe the api_list_result_size_items histogram with the
		// post-pagination result size, NOT the requested limit. The
		// observation reflects what the handler actually rendered, so
		// dashboards can graph "page-fill ratio" against
		// has_more=true tails.
		metrics.APIListResultSize.WithLabelValues(r.Pattern).Observe(float64(len(rows)))

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
