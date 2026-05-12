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

	"github.com/tarkandikmen/notifications/internal/metrics"
	"github.com/tarkandikmen/notifications/internal/observability"
	"github.com/tarkandikmen/notifications/internal/store"
)

// Store is the storage surface the api package depends on. *store.Store
// satisfies it for production; handler tests substitute an in-memory
// fake. Defined as an interface here (rather than holding a concrete
// *store.Store or *pgxpool.Pool in Deps) so the test plan in
// docs/phases/02-walking-skeleton.md §13 ("fake Store interface
// satisfied in-memory") works without touching real infrastructure.
//
// Phase 4 Chunk 1 adds ListNotifications and GetBatch per
// docs/phases/04-api-completeness.md §1.2 + §1.3.
// Phase 4 Chunk 2 adds InsertBatch per
// docs/phases/04-api-completeness.md §1.1.
// Phase 4 Chunk 3 adds CancelNotification per
// docs/phases/04-api-completeness.md §1.4.
type Store interface {
	InsertNotification(ctx context.Context, n store.Notification) error
	InsertBatch(ctx context.Context, ns []store.Notification, batchID uuid.UUID) error
	GetNotification(ctx context.Context, id uuid.UUID) (store.Notification, []store.DeliveryAttempt, error)
	ListNotifications(ctx context.Context, filters store.ListFilters, offset, limit int) ([]store.Notification, bool, error)
	GetBatch(ctx context.Context, batchID uuid.UUID) ([]store.Notification, error)
	CancelNotification(ctx context.Context, id uuid.UUID, traceHeaders json.RawMessage) (store.Notification, store.CancelTransition, error)
}

// PingerFunc is the signature of a per-request liveness probe. The
// supplied pinger is called inside a 2 s context timeout in
// handleHealthz; any non-nil err produces a 503 with the failing
// component's name in the body per docs/design/03-api.md
// §`GET /healthz`.
//
// The 200 path returns the exact-byte body Phase 1's acceptance test 5
// asserts (`{"status":"ok"}` with no trailing newline); the 503 path
// returns the rich body locked in docs/design/03-api.md §`GET
// /healthz` (`{"status":"unhealthy","components":{"<name>":"<error>"}}`).
//
// Wired by api/cmd.go to pgxpool.Pool.Ping in production. Tests
// substitute a closure that returns a deterministic error to exercise
// the 503 path.
//
// docs/phases/05-observability.md §3 locks the surface; only Postgres
// is probed in Phase 5 (the api binary doesn't open Redis or Kafka
// clients — Phase 7 polish if k8s readiness probes need them).
type PingerFunc func(ctx context.Context) error

// Deps is the api package's per-process dependency bundle. cmd.go
// assembles it at startup and hands it to RegisterRoutes; tests build
// it manually with fakes.
//
// docs/phases/02-walking-skeleton.md §6 locks the original field set;
// docs/phases/05-observability.md §3 adds Pinger so handleHealthz can
// run the per-request Postgres ping that distinguishes "process up"
// (Phase 1 minimum) from "deps responding" (Phase 5 contract). A nil
// Pinger preserves the Phase 1 behavior for tests that don't care
// about the dep probe.
type Deps struct {
	Store    Store
	Registry *prometheus.Registry
	Logger   *slog.Logger
	Clock    func() time.Time
	Pinger   PingerFunc
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
//
// docs/phases/02-walking-skeleton.md §6 locked the original four routes;
// docs/phases/04-api-completeness.md adds the list (Chunk 1), batch-get
// (Chunk 1), batch-create (Chunk 2), and cancel (Chunk 3) endpoints;
// docs/phases/05-observability.md §1.3 wires the metrics middleware
// and §3 wires the rich healthz dep probe.
func RegisterRoutes(mux *http.ServeMux, deps Deps) {
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	if deps.Clock == nil {
		deps.Clock = time.Now
	}

	mux.Handle("GET /healthz", metrics.Middleware(handleHealthz(deps)))
	mux.Handle("GET /metrics", metrics.Middleware(promhttp.HandlerFor(deps.Registry, promhttp.HandlerOpts{})))
	mux.Handle("POST /v1/notifications", metrics.Middleware(handleCreate(deps)))
	mux.Handle("POST /v1/notifications/batch", metrics.Middleware(handleBatchCreate(deps)))
	mux.Handle("GET /v1/notifications", metrics.Middleware(handleList(deps)))
	mux.Handle("GET /v1/notifications/{id}", metrics.Middleware(handleGet(deps)))
	mux.Handle("POST /v1/notifications/{id}/cancel", metrics.Middleware(handleCancel(deps)))
	mux.Handle("GET /v1/batches/{id}", metrics.Middleware(handleGetBatch(deps)))
}

// handleHealthz implements the per-request dep-probe contract from
// docs/design/03-api.md §`GET /healthz`. When deps.Pinger is nil
// (default for unit tests not exercising the probe) the handler
// returns the Phase 1 byte-exact 200 body unconditionally, preserving
// the Phase 1 acceptance contract. When deps.Pinger is set (the
// production wiring is pgxpool.Pool.Ping) the handler runs it inside
// a 2 s context timeout per request:
//
//   - nil err: 200 with the byte-exact `{"status":"ok"}` body (no
//     trailing newline) so the Phase 1 acceptance test 5 stays green.
//   - non-nil err: 503 with the rich body
//     `{"status":"unhealthy","components":{"postgres":"<error>"}}`
//     (json.Encode is fine here — no exact-byte contract on 503).
//
// Phase 5 wires only the Postgres probe; deeper probes (Redis, Kafka)
// are Phase 7 polish per docs/phases/05-observability.md §Out of scope.
func handleHealthz(deps Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if deps.Pinger != nil {
			ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			defer cancel()
			if err := deps.Pinger(ctx); err != nil {
				writeUnhealthy(w, "postgres", err)
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}
}

// writeUnhealthy writes the 503 body shape locked by
// docs/design/03-api.md §`GET /healthz`. The component map carries
// one entry per failing dep; Phase 5 only probes Postgres so the map
// is always single-entry, but the shape generalizes for the
// Phase 7 polish that adds Redis / Kafka probes.
func writeUnhealthy(w http.ResponseWriter, component string, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusServiceUnavailable)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status": "unhealthy",
		"components": map[string]string{
			component: err.Error(),
		},
	})
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
// "attempts": [...] per docs/design/03-api.md §Nested attempts.
func renderNotification(n store.Notification, attempts []store.DeliveryAttempt) NotificationResponse {
	out := renderNotificationWithoutAttempts(n)
	rendered := renderAttempts(attempts)
	out.Attempts = &rendered
	return out
}

// renderNotificationWithoutAttempts returns the list / batch-get /
// cancel response shape per docs/design/03-api.md §Notification
// representation: every notification field, no nested attempts key.
//
// Leaves NotificationResponse.Attempts as nil so omitempty drops the
// field entirely from the wire format. The split between this helper
// and renderNotification keeps the field projection single-sourced;
// docs/phases/04-api-completeness.md §2 requires both code paths to
// agree on every projected field.
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
