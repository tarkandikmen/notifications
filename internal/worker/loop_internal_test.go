package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"
)

// TestBuildEventPayload_MatchesKafkaSchema is a unit-style test (no
// testcontainers) that locks the events.notification JSON shape
// emitted by the worker. Catches regressions to the wire format
// without requiring a Postgres / Kafka container.
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
			assert.Nil(t, decoded["batch_id"], "single-create has null batch_id")
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
//
// applyDefaults panics when Deps.Limiter is nil, Deps.Channel is
// empty, or Deps.Tracer is nil. Each panic branch is covered by
// TestApplyDefaults_PanicsWhen*.
func TestApplyDefaults(t *testing.T) {
	tracer := noop.NewTracerProvider().Tracer("test")
	d := applyDefaults(Deps{Limiter: stubLimiter{}, Channel: "sms", Tracer: tracer})
	assert.NotNil(t, d.Logger)
	assert.NotNil(t, d.Clock)
	assert.Equal(t, "sms", d.Channel)
	assert.NotNil(t, d.Limiter)
	assert.NotNil(t, d.Tracer)

	for _, ch := range []string{"sms", "email", "push"} {
		custom := applyDefaults(Deps{
			Limiter: stubLimiter{},
			Channel: ch,
			Logger:  slog.Default(),
			Tracer:  tracer,
		})
		assert.Equal(t, ch, custom.Channel,
			"applyDefaults preserves the explicit channel value %q", ch)
	}
}

// TestApplyDefaults_PanicsWhenLimiterMissing pins the invariant that
// production wiring (cmd.go) always provides a *ratelimit.Bucket via
// Deps.Limiter, and Loop's applyDefaults treats a nil Limiter as a
// programmer bug rather than a recoverable misconfiguration. The
// panic surfaces immediately so tests + CI catch the bug at the first
// call rather than silently nil-defaulting to a different limiter.
func TestApplyDefaults_PanicsWhenLimiterMissing(t *testing.T) {
	tracer := noop.NewTracerProvider().Tracer("test")
	assert.Panics(t, func() {
		applyDefaults(Deps{Channel: "sms", Tracer: tracer})
	})
}

// TestApplyDefaults_PanicsWhenChannelMissing pins the invariant that
// production wiring (cmd.go runForChannel) always sets Deps.Channel
// from the --channel flag, and silently defaulting to "sms" would
// route an email or push worker through the wrong rate-limit key +
// log labels.
func TestApplyDefaults_PanicsWhenChannelMissing(t *testing.T) {
	tracer := noop.NewTracerProvider().Tracer("test")
	assert.Panics(t, func() {
		applyDefaults(Deps{Limiter: stubLimiter{}, Tracer: tracer})
	})
}

// TestApplyDefaults_PanicsWhenTracerMissing pins the invariant that
// production wiring (cmd.go runForChannel) always injects
// otel.Tracer(serviceName) into Deps.Tracer for the per-record
// worker.handleRecord span. Tests that exercise Loop must inject a
// tracer (typically a noop tracer or an in-memory tracetest
// provider).
func TestApplyDefaults_PanicsWhenTracerMissing(t *testing.T) {
	assert.Panics(t, func() {
		applyDefaults(Deps{Limiter: stubLimiter{}, Channel: "sms"})
	},
		"a missing tracer is a programmer bug — production wiring always provides one")
}

// stubLimiter is the unit-test fixture for applyDefaults. Cannot reuse
// noOpBucket from loop_test.go because that file uses external
// dependencies (ratelimit, testsupport) that don't compile against
// the unit test package's lighter import set; defining a tiny local
// fake keeps the loop_internal_test.go file self-contained.
type stubLimiter struct{}

func (stubLimiter) Acquire(_ context.Context, _ string) error { return nil }

// ptrOf is a tiny generic helper used by the table cases above so
// each row can declare an inline string pointer without taking
// addresses of locals.
func ptrOf[T any](v T) *T {
	return &v
}

// TestDecodeAndValidate exercises every branch of the helper that
// handleRecord calls before any state mutation.
//
// The msg-vs-nil contract: msg is nil ONLY when JSON failed to
// unmarshal (decode_failed) or a panic interrupted decoding. Schema +
// missing-field validation failures always return the parsed payload
// so the targeted T8 path (BuildUnprocessable + RecordUnprocessable)
// can fire when msg.ID is a valid UUID and msg.Attempt > 0.
func TestDecodeAndValidate(t *testing.T) {
	validJSON := []byte(`{
		"version":1,
		"id":"01927000-0000-7000-8000-000000000001",
		"attempt":3,
		"channel":"sms",
		"recipient":"+905551234567",
		"content":"hello"
	}`)

	cases := []struct {
		name           string
		input          []byte
		wantMsgNil     bool
		wantErrCode    string
		wantErrDetails string // substring match (full text varies by Go version for json errors)
	}{
		{
			name:        "happy path: every field present and valid",
			input:       validJSON,
			wantMsgNil:  false,
			wantErrCode: "",
		},
		{
			name:           "decode_failed: bytes are not JSON",
			input:          []byte(`not-valid-json{`),
			wantMsgNil:     true,
			wantErrCode:    "decode_failed",
			wantErrDetails: "invalid character",
		},
		{
			name:           "schema_mismatch: version != 1",
			input:          []byte(`{"version":2,"id":"01927000-0000-7000-8000-000000000001","attempt":1,"channel":"sms","recipient":"+1","content":"x"}`),
			wantMsgNil:     false,
			wantErrCode:    "schema_mismatch",
			wantErrDetails: "unsupported version 2",
		},
		{
			name:           "missing_field: id is empty string",
			input:          []byte(`{"version":1,"id":"","attempt":1,"channel":"sms","recipient":"+1","content":"x"}`),
			wantMsgNil:     false,
			wantErrCode:    "missing_field",
			wantErrDetails: "id is required",
		},
		{
			name:           "schema_mismatch: id is not a UUID",
			input:          []byte(`{"version":1,"id":"not-a-uuid","attempt":1,"channel":"sms","recipient":"+1","content":"x"}`),
			wantMsgNil:     false,
			wantErrCode:    "schema_mismatch",
			wantErrDetails: "invalid id",
		},
		{
			name:           "missing_field: attempt is zero",
			input:          []byte(`{"version":1,"id":"01927000-0000-7000-8000-000000000001","attempt":0,"channel":"sms","recipient":"+1","content":"x"}`),
			wantMsgNil:     false,
			wantErrCode:    "missing_field",
			wantErrDetails: "attempt must be > 0",
		},
		{
			name:           "missing_field: attempt is negative",
			input:          []byte(`{"version":1,"id":"01927000-0000-7000-8000-000000000001","attempt":-3,"channel":"sms","recipient":"+1","content":"x"}`),
			wantMsgNil:     false,
			wantErrCode:    "missing_field",
			wantErrDetails: "attempt must be > 0",
		},
		{
			name:           "missing_field: recipient is empty string",
			input:          []byte(`{"version":1,"id":"01927000-0000-7000-8000-000000000001","attempt":1,"channel":"sms","recipient":"","content":"x"}`),
			wantMsgNil:     false,
			wantErrCode:    "missing_field",
			wantErrDetails: "recipient is required",
		},
		{
			name:           "missing_field: content absent (null)",
			input:          []byte(`{"version":1,"id":"01927000-0000-7000-8000-000000000001","attempt":1,"channel":"sms","recipient":"+1","content":null}`),
			wantMsgNil:     false,
			wantErrCode:    "missing_field",
			wantErrDetails: "content is required",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			msg, errCode, errDetails, panicked := decodeAndValidate(tc.input)
			assert.False(t, panicked, "no test case here triggers the panic path")

			if tc.wantMsgNil {
				assert.Nil(t, msg,
					"msg=nil only on decode_failed / panic — JSON did not produce a struct")
			} else {
				assert.NotNil(t, msg,
					"validation failures preserve the parsed msg so the targeted T8 branch can fire when id+attempt are intact")
			}

			assert.Equal(t, tc.wantErrCode, errCode)
			if tc.wantErrDetails != "" {
				assert.Contains(t, errDetails, tc.wantErrDetails,
					"errDetails should describe the validation failure")
			}
			if tc.wantErrCode == "" {
				assert.Empty(t, errDetails, "happy path has no error details")
			}
		})
	}
}

// TestDecodeAndValidate_PanicRecovery pins the panic-recovery branch:
// a panic fired during decode (injected via the package-level
// SetDecodeAndValidatePanicHook seam) surfaces as
// (nil, "panic", "<%v of recovered>", true) without unwinding the
// goroutine. Without this protection the worker's franz-go partition
// handler would crash and head-of-line block the whole partition.
func TestDecodeAndValidate_PanicRecovery(t *testing.T) {
	prev := SetDecodeAndValidatePanicHook(func() {
		panic("deliberate test panic")
	})
	t.Cleanup(func() {
		SetDecodeAndValidatePanicHook(prev)
	})

	msg, errCode, errDetails, panicked := decodeAndValidate([]byte(`{"version":1}`))

	assert.True(t, panicked, "panic must surface as panicked=true")
	assert.Nil(t, msg, "panic must surface as msg=nil")
	assert.Equal(t, errCodePanic, errCode)
	assert.Contains(t, errDetails, "deliberate test panic",
		"errDetails must carry the recovered value's %%v rendering")
}

// TestSetDecodeAndValidatePanicHook_Restores pins the convention that
// SetDecodeAndValidatePanicHook returns the previous hook so callers
// can restore it via t.Cleanup. The package-level hook leaks across
// test functions otherwise, breaking test isolation.
func TestSetDecodeAndValidatePanicHook_Restores(t *testing.T) {
	prev := SetDecodeAndValidatePanicHook(func() { panic("first") })
	t.Cleanup(func() { SetDecodeAndValidatePanicHook(prev) })

	previous := SetDecodeAndValidatePanicHook(func() { panic("second") })
	assert.NotNil(t, previous,
		"SetDecodeAndValidatePanicHook must return the previous hook")
}
