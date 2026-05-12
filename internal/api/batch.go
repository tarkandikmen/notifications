package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/tarkandikmen/notifications/internal/metrics"
	"github.com/tarkandikmen/notifications/internal/observability"
	"github.com/tarkandikmen/notifications/internal/store"
)

// handleBatchCreate implements POST /v1/notifications/batch per
// docs/design/03-api.md §POST /v1/notifications/batch and
// docs/phases/04-api-completeness.md §4.
//
// Behavior:
//
//  1. Decode the JSON body. Malformed → 400 validation_failed with a
//     single details[] entry pointing at "body" (mirrors handleCreate).
//  2. ValidateBatchCreate. The oversize case surfaces as a single
//     "batch size N exceeded" issue against the "notifications" path,
//     which the handler maps to 413 payload_too_large (no details[],
//     per docs/design/03-api.md §Error model). Every other issue maps
//     to 400 validation_failed.
//  3. Mint one batch_id (UUIDv7) and one id per item; build the
//     store.Notification slice in request order.
//  4. Store.InsertBatch. On *store.BatchIdempotencyConflictError →
//     409 duplicate_idempotency_keys with one IdempotencyConflictDetail
//     per entry (preserves request order). On other error → 500.
//  5. On success → 201 Created with the BatchCreateResponse envelope.
func handleBatchCreate(deps Deps) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req BatchCreateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeValidationFailed(w, []FieldIssue{{Path: "body", Issue: "malformed JSON"}})
			return
		}

		if issues := ValidateBatchCreate(req, deps.Clock()); len(issues) > 0 {
			if isBatchOversize(issues) {
				writePayloadTooLarge(w)
				return
			}
			writeValidationFailed(w, issues)
			return
		}

		// docs/phases/05-observability.md §1.1 (api_batch_size_items
		// row): observe only after ValidateBatchCreate returns clean
		// so oversized / malformed batches don't pollute the
		// histogram. r.Pattern is populated by the api mux (Go 1.22+
		// ServeMux) — see metrics.Middleware comment for the wrapping
		// order rationale.
		metrics.APIBatchSize.WithLabelValues(r.Pattern).Observe(float64(len(req.Notifications)))

		batchID, err := store.NewID()
		if err != nil {
			deps.Logger.Error("api: mint batch id", "err", err)
			writeInternalError(w)
			return
		}

		ns := make([]store.Notification, 0, len(req.Notifications))
		ids := make([]string, 0, len(req.Notifications))
		for _, item := range req.Notifications {
			id, err := store.NewID()
			if err != nil {
				deps.Logger.Error("api: mint id", "err", err)
				writeInternalError(w)
				return
			}

			priorityStr := item.Priority
			if priorityStr == "" {
				priorityStr = priorityNormal
			}
			priority, _ := priorityToInt(priorityStr)

			scheduledAt := parseValidatedScheduledAt(item.ScheduledAt)
			eligibleAt := deps.Clock()
			if scheduledAt != nil {
				eligibleAt = *scheduledAt
			}

			content := item.Content
			ns = append(ns, store.Notification{
				ID:             id,
				BatchID:        uuid.NullUUID{UUID: batchID, Valid: true},
				Channel:        item.Channel,
				Recipient:      item.Recipient,
				Priority:       priority,
				Content:        &content,
				Status:         statusPending,
				Attempt:        0,
				EligibleAt:     eligibleAt,
				ScheduledAt:    scheduledAt,
				IdempotencyKey: item.IdempotencyKey,
			})
			ids = append(ids, id.String())
		}

		if err := deps.Store.InsertBatch(r.Context(), ns, batchID); err != nil {
			var conflict *store.BatchIdempotencyConflictError
			if errors.As(err, &conflict) {
				if sp := trace.SpanFromContext(r.Context()); sp.IsRecording() && len(conflict.Entries) > 0 {
					existing := make([]string, 0, len(conflict.Entries))
					for _, e := range conflict.Entries {
						existing = append(existing, e.ExistingID.String())
					}
					sp.SetAttributes(attribute.StringSlice("notification.ids", existing))
				}
				details := make([]any, 0, len(conflict.Entries))
				for _, e := range conflict.Entries {
					details = append(details, IdempotencyConflictDetail{
						IdempotencyKey: e.Key,
						ExistingID:     e.ExistingID.String(),
						Status:         e.ExistingStatus,
					})
				}
				writeJSON(w, http.StatusConflict, ErrorEnvelope{
					Error: ErrorBody{
						Code:    "duplicate_idempotency_keys",
						Message: "one or more idempotency_key values already used",
						Details: details,
					},
				})
				return
			}
			deps.Logger.Error("api: insert batch", "err", err, "batch_id", batchID)
			writeInternalError(w)
			return
		}

		observability.SetSpanBatchID(r.Context(), batchID)
		if sp := trace.SpanFromContext(r.Context()); sp.IsRecording() && len(ids) > 0 {
			const maxIDsOnSpan = 100
			if len(ids) <= maxIDsOnSpan {
				sp.SetAttributes(attribute.StringSlice("notification.ids", ids))
			} else {
				sp.SetAttributes(
					attribute.StringSlice("notification.ids", ids[:maxIDsOnSpan]),
					attribute.Int("notification.ids.more", len(ids)-maxIDsOnSpan),
				)
			}
		}
		writeJSON(w, http.StatusCreated, BatchCreateResponse{
			BatchID: batchID.String(),
			IDs:     ids,
		})
	}
}

// isBatchOversize returns true when ValidateBatchCreate surfaced only
// the "batch size N exceeded" issue. Per
// docs/phases/04-api-completeness.md §3.1 the validator short-circuits
// to a single issue on oversize, so checking len(issues) == 1 with the
// matching path + prefix is sufficient and not brittle.
func isBatchOversize(issues []FieldIssue) bool {
	return len(issues) == 1 &&
		issues[0].Path == "notifications" &&
		strings.HasPrefix(issues[0].Issue, "batch size ")
}

// writePayloadTooLarge writes the canonical 413 envelope per
// docs/design/03-api.md §Error model (no details[]).
func writePayloadTooLarge(w http.ResponseWriter) {
	writeJSON(w, http.StatusRequestEntityTooLarge, ErrorEnvelope{
		Error: ErrorBody{
			Code:    "payload_too_large",
			Message: fmt.Sprintf("batch exceeds maximum of %d items", batchMax),
		},
	})
}
