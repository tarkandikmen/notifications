package api

import "encoding/json"

// CreateRequest is the JSON body of POST /v1/notifications.
//
// Fields are all string-typed (or json.RawMessage) so absent and empty
// collapse to the same shape; ValidateCreate enforces the per-field rules
// from docs/phases/02-walking-skeleton.md §5. Unknown fields are ignored
// per docs/design/03-api.md §Conventions.
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

// FieldIssue is one entry in the validation_failed details[] array per
// docs/design/03-api.md §Error model.
type FieldIssue struct {
	Path  string `json:"path"`
	Issue string `json:"issue"`
}

// IdempotencyConflictDetail is the single entry in the
// duplicate_idempotency_keys details[] array per
// docs/phases/02-walking-skeleton.md §3 step 5 and
// docs/design/03-api.md §Error model.
type IdempotencyConflictDetail struct {
	IdempotencyKey string `json:"idempotency_key"`
	ExistingID     string `json:"existing_id"`
	Status         string `json:"status"`
}

// ErrorEnvelope is the canonical wrapper for every non-2xx response per
// docs/design/03-api.md §Error model.
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

// NotificationResponse is the GET /v1/notifications/{id} body shape per
// docs/design/03-api.md §Notification representation. The six omitempty
// fields (batch_id, content, template, template_data, scheduled_at,
// failure_reason) are dropped from the JSON when null per the same doc.
type NotificationResponse struct {
	ID             string            `json:"id"`
	BatchID        *string           `json:"batch_id,omitempty"`
	Channel        string            `json:"channel"`
	Recipient      string            `json:"recipient"`
	Priority       string            `json:"priority"`
	Content        *string           `json:"content,omitempty"`
	Template       *string           `json:"template,omitempty"`
	TemplateData   json.RawMessage   `json:"template_data,omitempty"`
	Status         string            `json:"status"`
	Attempt        int               `json:"attempt"`
	EligibleAt     string            `json:"eligible_at"`
	ScheduledAt    *string           `json:"scheduled_at,omitempty"`
	FailureReason  *string           `json:"failure_reason,omitempty"`
	IdempotencyKey string            `json:"idempotency_key"`
	CreatedAt      string            `json:"created_at"`
	UpdatedAt      string            `json:"updated_at"`
	Attempts       []AttemptResponse `json:"attempts"`
}

// AttemptResponse is one item in NotificationResponse.Attempts. The four
// omitempty fields (finished_at, classification, error_message, response)
// are dropped from the JSON when null per docs/design/03-api.md
// §Nested attempts.
type AttemptResponse struct {
	Attempt        int             `json:"attempt"`
	StartedAt      string          `json:"started_at"`
	FinishedAt     *string         `json:"finished_at,omitempty"`
	Classification *string         `json:"classification,omitempty"`
	ErrorMessage   *string         `json:"error_message,omitempty"`
	Response       json.RawMessage `json:"response,omitempty"`
}
