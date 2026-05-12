package api

import "encoding/json"

// CreateRequest is the JSON body of POST /v1/notifications.
//
// Fields are all string-typed (or json.RawMessage) so absent and empty
// collapse to the same shape; ValidateCreate enforces the per-field
// rules. Unknown fields are ignored.
type CreateRequest struct {
	Channel        string          `json:"channel"`
	Recipient      string          `json:"recipient"`
	Content        string          `json:"content"`
	Template       string          `json:"template"`
	TemplateData   json.RawMessage `json:"template_data"`
	Priority       string          `json:"priority"`
	ScheduledAt    string          `json:"scheduled_at"`
	IdempotencyKey string          `json:"idempotency_key"`
}

// CreateResponse is the 201 body of POST /v1/notifications.
type CreateResponse struct {
	ID string `json:"id"`
}

// BatchItem is one entry in BatchCreateRequest.Notifications. The field
// set mirrors CreateRequest exactly so the per-item validation logic
// is single-sourced via validateCreateItem (see internal/api/validation.go).
type BatchItem struct {
	Channel        string          `json:"channel"`
	Recipient      string          `json:"recipient"`
	Content        string          `json:"content"`
	Template       string          `json:"template"`
	TemplateData   json.RawMessage `json:"template_data"`
	Priority       string          `json:"priority"`
	ScheduledAt    string          `json:"scheduled_at"`
	IdempotencyKey string          `json:"idempotency_key"`
}

// BatchCreateRequest is the JSON body of POST /v1/notifications/batch.
// Up to batchMax items per request (rejected with 413 payload_too_large
// otherwise).
type BatchCreateRequest struct {
	Notifications []BatchItem `json:"notifications"`
}

// BatchCreateResponse is the 201 body of POST /v1/notifications/batch.
// ids is in request order — the api layer mints each item's UUIDv7
// while walking BatchCreateRequest.Notifications.
type BatchCreateResponse struct {
	BatchID string   `json:"batch_id"`
	IDs     []string `json:"ids"`
}

// FieldIssue is one entry in the validation_failed details[] array.
type FieldIssue struct {
	Path  string `json:"path"`
	Issue string `json:"issue"`
}

// IdempotencyConflictDetail is the single entry in the
// duplicate_idempotency_keys details[] array.
type IdempotencyConflictDetail struct {
	IdempotencyKey string `json:"idempotency_key"`
	ExistingID     string `json:"existing_id"`
	Status         string `json:"status"`
}

// TerminalStateDetail is the single entry in the terminal_state
// details[] array surfaced by POST /v1/notifications/{id}/cancel when
// the row is already DELIVERED or FAILED.
type TerminalStateDetail struct {
	CurrentStatus string `json:"current_status"`
}

// ErrorEnvelope is the canonical wrapper for every non-2xx response.
type ErrorEnvelope struct {
	Error ErrorBody `json:"error"`
}

// ErrorBody is the inner payload of ErrorEnvelope. Details may be nil
// (404, 500, 413) or a slice of typed entries (validation_failed,
// duplicate_idempotency_keys, terminal_state) — encoded as []any so
// each code can supply its own entry shape.
type ErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details []any  `json:"details,omitempty"`
}

// NotificationResponse is the JSON shape returned by GET, list,
// batch-get, and cancel endpoints. The six omitempty fields (batch_id,
// content, template, template_data, scheduled_at, failure_reason) are
// dropped from the JSON when null.
//
// Attempts uses *[]AttemptResponse so the field can be either:
//   - nil → dropped entirely from the wire by `omitempty` (used by list /
//     batch-get / cancel responses, which do not include nested
//     attempts).
//   - non-nil pointer to a (possibly empty) slice → encoded as `[]` or
//     `[{...}, ...]` (used by the single-GET response, which always
//     renders `attempts: []` for a brand-new row with zero attempts).
//
// A bare `[]AttemptResponse` with `omitempty` would not work because
// encoding/json drops both nil and empty slices on `omitempty`; the
// pointer wrapper restores the "empty array still renders" semantics
// the single-GET path needs.
type NotificationResponse struct {
	ID             string             `json:"id"`
	BatchID        *string            `json:"batch_id,omitempty"`
	Channel        string             `json:"channel"`
	Recipient      string             `json:"recipient"`
	Priority       string             `json:"priority"`
	Content        *string            `json:"content,omitempty"`
	Template       *string            `json:"template,omitempty"`
	TemplateData   json.RawMessage    `json:"template_data,omitempty"`
	Status         string             `json:"status"`
	Attempt        int                `json:"attempt"`
	EligibleAt     string             `json:"eligible_at"`
	ScheduledAt    *string            `json:"scheduled_at,omitempty"`
	FailureReason  *string            `json:"failure_reason,omitempty"`
	IdempotencyKey string             `json:"idempotency_key"`
	CreatedAt      string             `json:"created_at"`
	UpdatedAt      string             `json:"updated_at"`
	Attempts       *[]AttemptResponse `json:"attempts,omitempty"`
}

// ListResponse is the 200 body of GET /v1/notifications. List items use
// the notification representation without nested attempts; the handler
// builds them via renderNotificationWithoutAttempts.
type ListResponse struct {
	Notifications []NotificationResponse `json:"notifications"`
	Offset        int                    `json:"offset"`
	Limit         int                    `json:"limit"`
	HasMore       bool                   `json:"has_more"`
}

// BatchGetResponse is the 200 body of GET /v1/batches/{id}. Items use
// the no-attempts notification representation.
type BatchGetResponse struct {
	BatchID       string                 `json:"batch_id"`
	Notifications []NotificationResponse `json:"notifications"`
}

// AttemptResponse is one item in NotificationResponse.Attempts. The four
// omitempty fields (finished_at, classification, error_message, response)
// are dropped from the JSON when null.
type AttemptResponse struct {
	Attempt        int             `json:"attempt"`
	StartedAt      string          `json:"started_at"`
	FinishedAt     *string         `json:"finished_at,omitempty"`
	Classification *string         `json:"classification,omitempty"`
	ErrorMessage   *string         `json:"error_message,omitempty"`
	Response       json.RawMessage `json:"response,omitempty"`
}
