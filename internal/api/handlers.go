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

	"github.com/tarkandikmen/notifications/internal/store"
)

// Store is the storage surface the api package depends on. *store.Store
// satisfies it for production; handler tests substitute an in-memory
// fake. Defined as an interface here (rather than holding a concrete
// *store.Store or *pgxpool.Pool in Deps) so the test plan in
// docs/phases/02-walking-skeleton.md §13 ("fake Store interface
// satisfied in-memory") works without touching real infrastructure.
type Store interface {
	InsertNotification(ctx context.Context, n store.Notification) error
	GetNotification(ctx context.Context, id uuid.UUID) (store.Notification, []store.DeliveryAttempt, error)
}

// Deps is the api package's per-process dependency bundle. cmd.go
// assembles it at startup and hands it to RegisterRoutes; tests build
// it manually with fakes.
//
// docs/phases/02-walking-skeleton.md §6 locks the field set: storage,
// metrics registry, logger, and an injectable clock for tests that need
// to exercise scheduled_at boundaries deterministically.
type Deps struct {
	Store    Store
	Registry *prometheus.Registry
	Logger   *slog.Logger
	Clock    func() time.Time
}

// RegisterRoutes wires the Phase 2 endpoint set onto mux.
//
// docs/phases/02-walking-skeleton.md §6 lists the four routes; healthz
// is the verbatim Phase 1 handler that moved out of internal/server.
func RegisterRoutes(mux *http.ServeMux, deps Deps) {
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	if deps.Clock == nil {
		deps.Clock = time.Now
	}

	mux.HandleFunc("GET /healthz", handleHealthz)
	mux.Handle("GET /metrics", promhttp.HandlerFor(deps.Registry, promhttp.HandlerOpts{}))
	mux.Handle("POST /v1/notifications", handleCreate(deps))
	mux.Handle("GET /v1/notifications/{id}", handleGet(deps))
}

// handleHealthz is the Phase-1-locked exact-byte healthz handler from
// docs/design/03-api.md §`GET /healthz`. The body must NOT have a
// trailing newline so json.Encode is intentionally avoided.
func handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

// handleCreate implements POST /v1/notifications. The six steps below
// match docs/phases/02-walking-skeleton.md §3 1:1.
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

		// Phase 2 §3 step 4: priority defaults to "normal" when absent;
		// validation already ensured a non-empty value is one of the
		// three accepted strings, so the ok=false branch below is
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

		// Phase 2 always populates Content (validation rejects the
		// empty-string case); the *string field exists in store for
		// Phase 6's template path.
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

		writeJSON(w, http.StatusCreated, CreateResponse{ID: id.String()})
	}
}

// handleGet implements GET /v1/notifications/{id} per
// docs/phases/02-walking-skeleton.md §4. Malformed UUIDs and missing
// rows both surface as 404 not_found per step 1 / step 2.
func handleGet(deps Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		idStr := r.PathValue("id")
		id, err := uuid.Parse(idStr)
		if err != nil {
			writeNotFound(w)
			return
		}

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

// renderNotification turns the store representation into the JSON shape
// from docs/design/03-api.md §Notification representation. The six
// nullable fields documented there map to *string / json.RawMessage so
// `omitempty` drops them from the wire when null.
func renderNotification(n store.Notification, attempts []store.DeliveryAttempt) NotificationResponse {
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
		Attempts:       renderAttempts(attempts),
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
