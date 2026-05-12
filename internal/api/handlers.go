package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/tarkandikmen/notifications/internal/health"
	"github.com/tarkandikmen/notifications/internal/metrics"
	"github.com/tarkandikmen/notifications/internal/observability"
	"github.com/tarkandikmen/notifications/internal/store"
)

// Store is the storage surface the api package depends on. *store.Store
// satisfies it for production; handler tests substitute an in-memory
// fake. Defined as an interface here (rather than holding a concrete
// *store.Store or *pgxpool.Pool in Deps) so handler tests can wire a
// fake Store without touching real infrastructure.
type Store interface {
	InsertNotification(ctx context.Context, n store.Notification) error
	InsertBatch(ctx context.Context, ns []store.Notification, batchID uuid.UUID) error
	GetNotification(ctx context.Context, id uuid.UUID) (store.Notification, []store.DeliveryAttempt, error)
	ListNotifications(ctx context.Context, filters store.ListFilters, offset, limit int) ([]store.Notification, bool, error)
	GetBatch(ctx context.Context, batchID uuid.UUID) ([]store.Notification, error)
	CancelNotification(ctx context.Context, id uuid.UUID, traceHeaders json.RawMessage) (store.Notification, store.CancelTransition, error)
}

// Deps is the api package's per-process dependency bundle. cmd.go
// assembles it at startup and hands it to RegisterRoutes; tests build
// it manually with fakes.
//
// Healthz is an http.HandlerFunc owned by cmd.go and built from a
// multi-component health.Handler. A nil Healthz falls back to a
// byte-exact 200 handler so test fixtures that don't care about the
// probe (every fakeStore-only test) stay green.
type Deps struct {
	Store    Store
	Registry *prometheus.Registry
	Logger   *slog.Logger
	Clock    func() time.Time
	Healthz  http.HandlerFunc
}

// RegisterRoutes wires every endpoint in the api package onto mux.
//
// Each route is registered through metrics.Middleware so the
// api_requests_total + api_request_duration_seconds vectors observe
// every request reaching a registered route. Wrapping per-route (vs.
// wrapping the whole mux) keeps r.Pattern populated by the time the
// middleware reads it — wrapping the mux from the outside would land
// before the mux populates Pattern and surface every request with
// endpoint="".
func RegisterRoutes(mux *http.ServeMux, deps Deps) {
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	if deps.Clock == nil {
		deps.Clock = time.Now
	}
	if deps.Healthz == nil {
		// Fallback to a byte-exact 200 handler so test fixtures
		// that don't care about the probe (every fakeStore-only
		// test in handlers_test.go) stay green. Production cmd.go
		// always supplies a real handler built via
		// internal/health.Handler.
		deps.Healthz = health.Handler(nil)
	}

	mux.Handle("GET /healthz", metrics.Middleware(deps.Healthz))
	mux.Handle("GET /metrics", metrics.Middleware(promhttp.HandlerFor(deps.Registry, promhttp.HandlerOpts{})))
	mux.Handle("POST /v1/notifications", metrics.Middleware(handleCreate(deps)))
	mux.Handle("POST /v1/notifications/batch", metrics.Middleware(handleBatchCreate(deps)))
	mux.Handle("GET /v1/notifications", metrics.Middleware(handleList(deps)))
	mux.Handle("GET /v1/notifications/{id}", metrics.Middleware(handleGet(deps)))
	mux.Handle("POST /v1/notifications/{id}/cancel", metrics.Middleware(handleCancel(deps)))
	mux.Handle("GET /v1/batches/{id}", metrics.Middleware(handleGetBatch(deps)))
}

// handleCreate implements POST /v1/notifications.
func handleCreate(deps Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req CreateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeValidationFailed(w, []FieldIssue{{Path: "body", Issue: "malformed JSON"}})
			return
		}

		if issues := ValidateCreate(req, deps.Clock()); len(issues) > 0 {
			writeValidationFailed(w, issues)
			return
		}

		id, err := store.NewID()
		if err != nil {
			deps.Logger.Error("api: mint id", "err", err)
			writeInternalError(w)
			return
		}

		// Priority defaults to "normal" when absent; validation
		// already ensured a non-empty value is one of the three
		// accepted strings, so the ok=false branch below is
		// unreachable for validated input.
		priorityStr := req.Priority
		if priorityStr == "" {
			priorityStr = priorityNormal
		}
		priority, _ := priorityToInt(priorityStr)

		scheduledAt := parseValidatedScheduledAt(req.ScheduledAt)
		eligibleAt := deps.Clock()
		if scheduledAt != nil {
			eligibleAt = *scheduledAt
		}

		// Content is always populated here (validation rejects the
		// empty-string case); the *string field exists in store
		// to leave room for a future template-only path.
		content := req.Content
		n := store.Notification{
			ID:             id,
			Channel:        req.Channel,
			Recipient:      req.Recipient,
			Priority:       priority,
			Content:        &content,
			Status:         statusPending,
			Attempt:        0,
			EligibleAt:     eligibleAt,
			ScheduledAt:    scheduledAt,
			IdempotencyKey: req.IdempotencyKey,
		}

		if err := deps.Store.InsertNotification(r.Context(), n); err != nil {
			var conflict *store.IdempotencyConflictError
			if errors.As(err, &conflict) {
				observability.SetSpanNotificationID(r.Context(), conflict.ExistingID)
				writeJSON(w, http.StatusConflict, ErrorEnvelope{
					Error: ErrorBody{
						Code:    "duplicate_idempotency_keys",
						Message: "idempotency_key already used",
						Details: []any{IdempotencyConflictDetail{
							IdempotencyKey: conflict.IdempotencyKey,
							ExistingID:     conflict.ExistingID.String(),
							Status:         conflict.ExistingStatus,
						}},
					},
				})
				return
			}
			deps.Logger.Error("api: insert notification", "err", err, "idempotency_key", req.IdempotencyKey)
			writeInternalError(w)
			return
		}

		observability.SetSpanNotificationID(r.Context(), id)
		writeJSON(w, http.StatusCreated, CreateResponse{ID: id.String()})
	}
}

// handleGet implements GET /v1/notifications/{id}. Malformed UUIDs and
// missing rows both surface as 404 not_found.
func handleGet(deps Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		idStr := r.PathValue("id")
		id, err := uuid.Parse(idStr)
		if err != nil {
			writeNotFound(w)
			return
		}
		observability.SetSpanNotificationID(r.Context(), id)

		n, attempts, err := deps.Store.GetNotification(r.Context(), id)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				writeNotFound(w)
				return
			}
			deps.Logger.Error("api: get notification", "err", err, "id", id)
			writeInternalError(w)
			return
		}

		writeJSON(w, http.StatusOK, renderNotification(n, attempts))
	}
}

// renderNotification returns the single-GET response shape: every
// notification field plus the nested attempts: [...] array (rendered
// even when empty). Used only by handleGet.
//
// The helper always builds a non-nil slice via renderAttempts and
// stores &slice on the response so the wire format is always
// "attempts": [...].
func renderNotification(n store.Notification, attempts []store.DeliveryAttempt) NotificationResponse {
	out := renderNotificationWithoutAttempts(n)
	rendered := renderAttempts(attempts)
	out.Attempts = &rendered
	return out
}

// renderNotificationWithoutAttempts returns the list / batch-get /
// cancel response shape: every notification field, no nested attempts
// key.
//
// Leaves NotificationResponse.Attempts as nil so omitempty drops the
// field entirely from the wire format. The split between this helper
// and renderNotification keeps the field projection single-sourced so
// both code paths agree on every projected field.
func renderNotificationWithoutAttempts(n store.Notification) NotificationResponse {
	out := NotificationResponse{
		ID:             n.ID.String(),
		Channel:        n.Channel,
		Recipient:      n.Recipient,
		Priority:       priorityFromInt(n.Priority),
		Status:         n.Status,
		Attempt:        n.Attempt,
		EligibleAt:     formatTime(n.EligibleAt),
		IdempotencyKey: n.IdempotencyKey,
		CreatedAt:      formatTime(n.CreatedAt),
		UpdatedAt:      formatTime(n.UpdatedAt),
	}

	if n.BatchID.Valid {
		s := n.BatchID.UUID.String()
		out.BatchID = &s
	}
	if n.Content != nil {
		out.Content = n.Content
	}
	if n.Template != nil {
		out.Template = n.Template
	}
	if len(n.TemplateData) > 0 {
		out.TemplateData = n.TemplateData
	}
	if n.ScheduledAt != nil {
		s := formatTime(*n.ScheduledAt)
		out.ScheduledAt = &s
	}
	if n.FailureReason != nil {
		out.FailureReason = n.FailureReason
	}

	return out
}

func renderAttempts(attempts []store.DeliveryAttempt) []AttemptResponse {
	out := make([]AttemptResponse, 0, len(attempts))
	for _, a := range attempts {
		item := AttemptResponse{
			Attempt:   a.Attempt,
			StartedAt: formatTime(a.StartedAt),
		}
		if a.FinishedAt != nil {
			s := formatTime(*a.FinishedAt)
			item.FinishedAt = &s
		}
		if a.Classification != nil {
			item.Classification = a.Classification
		}
		if a.ErrorMessage != nil {
			item.ErrorMessage = a.ErrorMessage
		}
		if len(a.Response) > 0 {
			item.Response = a.Response
		}
		out = append(out, item)
	}
	return out
}

// parseValidatedScheduledAt assumes ValidateCreate has already accepted s;
// on unexpected parse failure it returns nil so the caller falls back to
// the absent-scheduled_at branch.
func parseValidatedScheduledAt(s string) *time.Time {
	if s == "" {
		return nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return nil
	}
	return &t
}

// formatTime renders a Postgres TIMESTAMPTZ value as RFC 3339 UTC. The
// nano-precision form keeps microsecond detail visible to API clients
// debugging timing issues without affecting correctness.
func formatTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}

func writeValidationFailed(w http.ResponseWriter, issues []FieldIssue) {
	details := make([]any, 0, len(issues))
	for _, issue := range issues {
		details = append(details, issue)
	}
	writeJSON(w, http.StatusBadRequest, ErrorEnvelope{
		Error: ErrorBody{
			Code:    "validation_failed",
			Message: "request validation failed",
			Details: details,
		},
	})
}

func writeNotFound(w http.ResponseWriter) {
	writeJSON(w, http.StatusNotFound, ErrorEnvelope{
		Error: ErrorBody{
			Code:    "not_found",
			Message: "notification not found",
		},
	})
}

func writeInternalError(w http.ResponseWriter) {
	writeJSON(w, http.StatusInternalServerError, ErrorEnvelope{
		Error: ErrorBody{
			Code:    "internal_error",
			Message: "internal error",
		},
	})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
