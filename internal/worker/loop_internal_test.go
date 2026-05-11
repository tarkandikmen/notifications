package worker

import (
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBuildEventPayload_MatchesKafkaSchema is a unit-style test (no
// testcontainers) that locks the events.notification JSON shape
// against docs/design/04-kafka.md §2. Catches regressions to the wire
// format without requiring a Postgres / Kafka container.
func TestBuildEventPayload_MatchesKafkaSchema(t *testing.T) {
	id := uuid.MustParse("01927000-0000-7000-8000-000000000001")
	occurredAt := time.Date(2026, 5, 11, 12, 0, 0, 123_000_000, time.UTC)

	cases := []struct {
		name    string
		outcome Outcome
		// asserted fields below
		wantStatus         string
		wantClassification string
		wantFailureReason  *string
	}{
		{
			name: "T4 success / DELIVERED",
			outcome: Outcome{
				Classification: classificationSuccess,
				NewStatus:      statusDelivered,
				NewEligibleAt:  occurredAt,
			},
			wantStatus:         "DELIVERED",
			wantClassification: "success",
		},
		{
			name: "T5 transient / PENDING (with response body)",
			outcome: Outcome{
				Classification: classificationTransient,
				NewStatus:      statusPending,
				NewEligibleAt:  occurredAt.Add(2 * time.Second),
				ResponseBody:   []byte(`{"error":"oops"}`),
			},
			wantStatus:         "PENDING",
			wantClassification: "transient",
		},
		{
			name: "T7 transient / FAILED (with failure_reason)",
			outcome: Outcome{
				Classification: classificationTransient,
				NewStatus:      statusFailed,
				NewEligibleAt:  occurredAt,
				FailureReason:  ptrOf(failureReasonMaxAttempts),
			},
			wantStatus:         "FAILED",
			wantClassification: "transient",
			wantFailureReason:  ptrOf(failureReasonMaxAttempts),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			msg := sendPayload{
				Channel: "sms",
				Attempt: 3,
			}
			got, err := buildEventPayload(id, msg, tc.outcome, occurredAt)
			require.NoError(t, err)

			var decoded map[string]any
			require.NoError(t, json.Unmarshal(got, &decoded))

			assert.Equal(t, float64(1), decoded["version"], "version locked at 1")
			assert.Equal(t, id.String(), decoded["id"])
			assert.Nil(t, decoded["batch_id"], "phase 2 single-create has null batch_id")
			assert.Equal(t, "sms", decoded["channel"])
			assert.Equal(t, float64(3), decoded["attempt"])
			assert.Equal(t, "DISPATCHED", decoded["previous_status"])
			assert.Equal(t, tc.wantStatus, decoded["current_status"])
			assert.Equal(t, tc.wantClassification, decoded["classification"])
			if tc.wantFailureReason == nil {
				assert.Nil(t, decoded["failure_reason"])
			} else {
				assert.Equal(t, *tc.wantFailureReason, decoded["failure_reason"])
			}
			assert.Equal(t, "2026-05-11T12:00:00.123Z", decoded["occurred_at"],
				"occurred_at must be RFC 3339 with millisecond precision per §Conventions")
		})
	}
}

// TestValidResponseJSON_KeepsValidDropsInvalid pins the JSONB-safety
// guard around delivery_attempts.response. Valid JSON passes through;
// non-JSON / empty bytes drop to nil so pgx never sends garbage to a
// JSONB column.
func TestValidResponseJSON_KeepsValidDropsInvalid(t *testing.T) {
	cases := []struct {
		name    string
		in      []byte
		wantNil bool
	}{
		{"valid object", []byte(`{"a":1}`), false},
		{"valid array", []byte(`[1,2,3]`), false},
		{"valid string", []byte(`"hello"`), false},
		{"valid number", []byte(`42`), false},
		{"valid null literal", []byte(`null`), false},
		{"empty", []byte{}, true},
		{"nil", nil, true},
		{"plain text", []byte(`hello`), true},
		{"truncated json", []byte(`{"a":`), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := validResponseJSON(tc.in)
			if tc.wantNil {
				assert.Nil(t, got)
			} else {
				assert.NotNil(t, got)
				assert.JSONEq(t, string(tc.in), string(got))
			}
		})
	}
}

// TestApplyDefaults exercises the zero-value field substitution.
// Same shape as internal/dispatcher and internal/relay's equivalent
// tests so the three loops' default machinery stays consistent.
func TestApplyDefaults(t *testing.T) {
	d := applyDefaults(Deps{})
	assert.NotNil(t, d.Logger)
	assert.NotNil(t, d.Clock)
	assert.Equal(t, "sms", d.Channel)

	custom := applyDefaults(Deps{
		Channel: "email",
		Logger:  slog.Default(),
	})
	assert.Equal(t, "email", custom.Channel)
}

// ptrOf is a tiny generic helper used by the table cases above so
// each row can declare an inline string pointer without taking
// addresses of locals.
func ptrOf[T any](v T) *T {
	return &v
}
