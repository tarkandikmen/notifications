package worker

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/tarkandikmen/notifications/internal/store"
)

// fakeGuardReader stubs guardReader for the unit tests below. The
// fields drive ReadStateForGuard's return; the captured args field lets
// tests assert the function was invoked with the expected id.
//
// Phase 5 widened the return shape with createdAt; tests that don't
// care supply the zero time.Time and the worker handles it
// gracefully (the latency observation fires only on the GuardProceed
// branch when CreatedAt is non-zero).
type fakeGuardReader struct {
	status     string
	attempt    int
	createdAt  time.Time
	err        error
	calledWith uuid.UUID
}

func (f *fakeGuardReader) ReadStateForGuard(_ context.Context, id uuid.UUID) (string, int, time.Time, error) {
	f.calledWith = id
	return f.status, f.attempt, f.createdAt, f.err
}

// TestCheckStateGuard_BranchTable exercises every row in the
// docs/phases/03-resilience.md §2.1 decision table. Each case pairs a
// (status, attempt) Postgres-side reading with the message's attempt
// and asserts the resulting GuardOutcome.
func TestCheckStateGuard_BranchTable(t *testing.T) {
	id := uuid.MustParse("01927000-0000-7000-8000-000000000001")

	cases := []struct {
		name        string
		readerState fakeGuardReader
		msgAttempt  int
		want        GuardOutcome
	}{
		{
			name:        "DISPATCHED with matching attempt → Proceed",
			readerState: fakeGuardReader{status: "DISPATCHED", attempt: 3},
			msgAttempt:  3,
			want:        GuardProceed,
		},
		{
			name:        "DISPATCHED with mismatched attempt → SkipStale",
			readerState: fakeGuardReader{status: "DISPATCHED", attempt: 5},
			msgAttempt:  3,
			want:        GuardSkipStale,
		},
		{
			name:        "DISPATCHED at attempt 1, msg attempt 0 → SkipStale",
			readerState: fakeGuardReader{status: "DISPATCHED", attempt: 1},
			msgAttempt:  0,
			want:        GuardSkipStale,
		},
		{
			name:        "PENDING with matching attempt (reaper reset) → SkipStale",
			readerState: fakeGuardReader{status: "PENDING", attempt: 3},
			msgAttempt:  3,
			want:        GuardSkipStale,
		},
		{
			name:        "PENDING with future attempt (mismatched) → SkipStale",
			readerState: fakeGuardReader{status: "PENDING", attempt: 5},
			msgAttempt:  3,
			want:        GuardSkipStale,
		},
		{
			name:        "DELIVERED → SkipTerminal regardless of attempt",
			readerState: fakeGuardReader{status: "DELIVERED", attempt: 7},
			msgAttempt:  3,
			want:        GuardSkipTerminal,
		},
		{
			name:        "FAILED → SkipTerminal",
			readerState: fakeGuardReader{status: "FAILED", attempt: 7},
			msgAttempt:  7,
			want:        GuardSkipTerminal,
		},
		{
			name:        "CANCELLED → SkipTerminal",
			readerState: fakeGuardReader{status: "CANCELLED", attempt: 1},
			msgAttempt:  1,
			want:        GuardSkipTerminal,
		},
		{
			name:        "unknown status defensively SkipStale",
			readerState: fakeGuardReader{status: "WAT", attempt: 3},
			msgAttempt:  3,
			want:        GuardSkipStale,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reader := tc.readerState
			got, err := CheckStateGuard(context.Background(), &reader, id, tc.msgAttempt)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got.Outcome)
			assert.Equal(t, id, reader.calledWith,
				"CheckStateGuard must call ReadStateForGuard with the message's notification id")
		})
	}
}

// TestCheckStateGuard_ProceedCarriesCreatedAt locks the Phase 5
// widening: GuardProceed surfaces the row's created_at so the worker
// can observe notification_delivery_latency_seconds without a second
// round trip per docs/phases/05-observability.md §1.1 worker rows.
// Non-Proceed outcomes leave CreatedAt as the zero time.Time; the
// worker's latency observation guards on IsZero defensively.
func TestCheckStateGuard_ProceedCarriesCreatedAt(t *testing.T) {
	id := uuid.MustParse("01927000-0000-7000-8000-000000000005")
	createdAt := time.Date(2026, 5, 11, 12, 34, 56, 0, time.UTC)
	reader := &fakeGuardReader{
		status:    "DISPATCHED",
		attempt:   3,
		createdAt: createdAt,
	}

	got, err := CheckStateGuard(context.Background(), reader, id, 3)
	require.NoError(t, err)
	assert.Equal(t, GuardProceed, got.Outcome)
	assert.True(t, got.CreatedAt.Equal(createdAt),
		"GuardProceed must carry the row's created_at for the latency observation")
}

// TestCheckStateGuard_SkipOutcomesHaveZeroCreatedAt locks the
// invariant that non-Proceed outcomes leave CreatedAt as the zero
// time.Time. The worker's notification_delivery_latency_seconds
// observation is gated on the GuardProceed branch + IsZero check —
// a non-Proceed outcome with a populated CreatedAt would be a
// regression that pollutes the histogram with zero-bucket samples
// for skipped records.
func TestCheckStateGuard_SkipOutcomesHaveZeroCreatedAt(t *testing.T) {
	id := uuid.MustParse("01927000-0000-7000-8000-000000000006")
	createdAt := time.Date(2026, 5, 11, 12, 34, 56, 0, time.UTC)

	cases := []struct {
		name   string
		reader fakeGuardReader
	}{
		{"SkipStale", fakeGuardReader{status: "DISPATCHED", attempt: 5, createdAt: createdAt}},
		{"SkipTerminal", fakeGuardReader{status: "DELIVERED", attempt: 7, createdAt: createdAt}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reader := tc.reader
			got, err := CheckStateGuard(context.Background(), &reader, id, 3)
			require.NoError(t, err)
			assert.NotEqual(t, GuardProceed, got.Outcome)
			assert.True(t, got.CreatedAt.IsZero(),
				"non-Proceed outcomes must leave CreatedAt zero so the worker's latency observation is defensively skipped")
		})
	}
}

// TestCheckStateGuard_NotFound asserts that store.ErrNotFound surfaces
// as GuardSkipMissing without bubbling up as an error — the caller's
// branch table reads uniformly per the locked spec.
func TestCheckStateGuard_NotFound(t *testing.T) {
	id := uuid.MustParse("01927000-0000-7000-8000-000000000002")
	reader := &fakeGuardReader{err: store.ErrNotFound}

	got, err := CheckStateGuard(context.Background(), reader, id, 1)
	require.NoError(t, err, "ErrNotFound surfaces as GuardSkipMissing, not as an error")
	assert.Equal(t, GuardSkipMissing, got.Outcome)
}

// TestCheckStateGuard_StoreError verifies that real Postgres errors
// bubble up as errors (the worker leaves the offset uncommitted; Kafka
// redelivers). Sentinel errors that are not ErrNotFound are not
// flattened to a GuardOutcome.
func TestCheckStateGuard_StoreError(t *testing.T) {
	id := uuid.MustParse("01927000-0000-7000-8000-000000000003")
	dbErr := errors.New("connection refused")
	reader := &fakeGuardReader{err: dbErr}

	_, err := CheckStateGuard(context.Background(), reader, id, 1)
	require.Error(t, err)
	assert.ErrorIs(t, err, dbErr)
}

// TestCheckStateGuard_TerminalIgnoresAttempt locks the spec's "regardless
// of attempt" qualifier on the terminal-status row of the branch
// table — even when the message's attempt matches the row's recorded
// attempt, a terminal status still surfaces as GuardSkipTerminal.
func TestCheckStateGuard_TerminalIgnoresAttempt(t *testing.T) {
	id := uuid.MustParse("01927000-0000-7000-8000-000000000004")

	for _, terminal := range []string{"DELIVERED", "FAILED", "CANCELLED"} {
		t.Run(terminal, func(t *testing.T) {
			reader := &fakeGuardReader{status: terminal, attempt: 3}
			got, err := CheckStateGuard(context.Background(), reader, id, 3)
			require.NoError(t, err)
			assert.Equal(t, GuardSkipTerminal, got.Outcome,
				"matching attempt does not override terminal status")
		})
	}
}

// dlqFixedNow is the deterministic clock the BuildUnprocessable cases
// use so the dlqPayload's failed_at field has a known value. Named
// distinctly from classify_test.go's fixedNow to avoid the package-
// level collision (both tests share the worker package).
var dlqFixedNow = time.Date(2026, 5, 11, 12, 34, 56, 789_000_000, time.UTC)

// dlqFixedNowFormatted matches the occurredAtFormat layout the helper
// uses for failed_at — RFC 3339 with millisecond precision.
const dlqFixedNowFormatted = "2026-05-11T12:34:56.789Z"

// makeKafkaRecord is a tiny helper that constructs a *kgo.Record with
// the given topic / partition / offset / key / value. The four cases
// below all need a record to feed BuildUnprocessable; centralizing
// the construction keeps them readable.
func makeKafkaRecord(topic string, key, value []byte) *kgo.Record {
	return &kgo.Record{
		Topic:     topic,
		Partition: 0,
		Offset:    42,
		Key:       key,
		Value:     value,
	}
}

// assertOriginalMessageNull asserts that the unmarshaled
// dlq.OriginalMessage represents JSON null. The json.RawMessage
// quirk: a JSON value of `null` round-trips through
// (Marshal → Unmarshal) as `[]byte("null")`, NOT as a nil slice. So
// `assert.Nil(t, dlq.OriginalMessage)` would fail even though the
// wire payload was `"original_message": null`. This helper accepts
// either shape — nil OR the literal bytes `null` — both of which
// indicate that the producer set the field to JSON null per
// docs/design/04-kafka.md §3.
func assertOriginalMessageNull(t *testing.T, raw json.RawMessage, msgAndArgs ...interface{}) {
	t.Helper()
	if len(raw) == 0 {
		return
	}
	if string(raw) == "null" {
		return
	}
	assert.Failf(t, "expected JSON null", "got %q (%v)", string(raw), msgAndArgs)
}

// TestBuildUnprocessable_NoTarget_DecodeFailed asserts the no-target
// branch when the JSON payload was undecodable. NotificationID +
// Attempt are nil in the resulting UnprocessableInput; the DLQ
// payload uses original_message_raw (base64) and original_message is
// null. EventPayload is nil because statement 4 of T8 is skipped on
// the no-target branch.
func TestBuildUnprocessable_NoTarget_DecodeFailed(t *testing.T) {
	rec := makeKafkaRecord("send.sms", []byte("some-key"), []byte(`not-valid-json{`))

	in, err := BuildUnprocessable(rec, nil, "decode_failed", "invalid character", "sms", dlqFixedNow)
	require.NoError(t, err)

	assert.Nil(t, in.NotificationID, "no-target branch leaves NotificationID nil")
	assert.Nil(t, in.Attempt, "no-target branch leaves Attempt nil")
	assert.Equal(t, "sms", in.Channel)
	assert.Equal(t, "decode_failed", in.ErrorCode)
	assert.Equal(t, "invalid character", in.ErrorDetails)
	assert.True(t, in.StartedAt.Equal(dlqFixedNow))
	assert.Empty(t, in.EventPayload,
		"no-target branch must not populate EventPayload (T8 statement 4 is skipped)")

	var dlq dlqPayload
	require.NoError(t, json.Unmarshal(in.DLQPayload, &dlq))
	assert.Equal(t, dlqPayloadVersion, dlq.Version)
	assert.Nil(t, dlq.NotificationID, "no-target branch sets notification_id=null")
	assert.Equal(t, "sms", dlq.Channel)
	assert.Nil(t, dlq.Attempt, "no-target branch sets attempt=null")
	assertOriginalMessageNull(t, dlq.OriginalMessage,
		"decode_failed uses original_message_raw, not original_message")
	require.NotNil(t, dlq.OriginalMessageRaw)
	wantRaw := base64.StdEncoding.EncodeToString(rec.Value)
	assert.Equal(t, wantRaw, *dlq.OriginalMessageRaw)
	assert.Equal(t, "decode_failed", dlq.Error)
	require.NotNil(t, dlq.ErrorDetails)
	assert.Equal(t, "invalid character", *dlq.ErrorDetails)
	assert.Equal(t, dlqFixedNowFormatted, dlq.FailedAt)
}

// TestBuildUnprocessable_NoTarget_Panic asserts the no-target branch
// when decodeAndValidate's recover surfaced an errCode='panic' with
// msg=nil. The disposition is identical to decode_failed: base64 raw
// + null targets + skipped statement 4.
func TestBuildUnprocessable_NoTarget_Panic(t *testing.T) {
	rec := makeKafkaRecord("send.sms", []byte("k"), []byte(`{"version":1}`))

	in, err := BuildUnprocessable(rec, nil, "panic", "deliberate panic", "sms", dlqFixedNow)
	require.NoError(t, err)

	assert.Nil(t, in.NotificationID)
	assert.Nil(t, in.Attempt)
	assert.Empty(t, in.EventPayload)

	var dlq dlqPayload
	require.NoError(t, json.Unmarshal(in.DLQPayload, &dlq))
	assert.Equal(t, "panic", dlq.Error)
	require.NotNil(t, dlq.ErrorDetails)
	assert.Equal(t, "deliberate panic", *dlq.ErrorDetails)
	require.NotNil(t, dlq.OriginalMessageRaw,
		"panic-during-decode is treated like decode_failed for the DLQ payload")
	assertOriginalMessageNull(t, dlq.OriginalMessage,
		"panic-during-decode mirrors decode_failed: original_message is null")
	assert.Nil(t, dlq.NotificationID)
	assert.Nil(t, dlq.Attempt)
}

// TestBuildUnprocessable_Targeted_AllFieldsValid asserts the targeted
// branch: msg has a valid id and attempt > 0, so NotificationID +
// Attempt point at the parsed values. The DLQ payload uses
// original_message (the decoded JSON, rec.Value verbatim);
// original_message_raw is null. The events.notification payload is
// populated with the locked T8 discriminators (DISPATCHED → FAILED,
// classification=unprocessable, failure_reason=unprocessable_message).
func TestBuildUnprocessable_Targeted_AllFieldsValid(t *testing.T) {
	id := uuid.MustParse("01927000-0000-7000-8000-000000000050")
	content := "hello"
	msg := &sendPayload{
		Version:   1,
		ID:        id.String(),
		Attempt:   3,
		Channel:   "sms",
		Recipient: "+905551234567",
		Content:   &content,
	}
	rec := makeKafkaRecord("send.sms", []byte(id.String()), []byte(`{"version":1,"id":"01927000-0000-7000-8000-000000000050","attempt":3}`))

	in, err := BuildUnprocessable(rec, msg, "missing_field", "recipient is required", "sms", dlqFixedNow)
	require.NoError(t, err)

	require.NotNil(t, in.NotificationID, "targeted branch must populate NotificationID")
	assert.Equal(t, id, *in.NotificationID)
	require.NotNil(t, in.Attempt)
	assert.Equal(t, 3, *in.Attempt)
	assert.NotEmpty(t, in.EventPayload, "targeted branch must populate EventPayload")

	var dlq dlqPayload
	require.NoError(t, json.Unmarshal(in.DLQPayload, &dlq))
	require.NotNil(t, dlq.NotificationID, "targeted branch puts the id in the DLQ payload")
	assert.Equal(t, id.String(), *dlq.NotificationID)
	require.NotNil(t, dlq.Attempt)
	assert.Equal(t, 3, *dlq.Attempt)
	assert.JSONEq(t, string(rec.Value), string(dlq.OriginalMessage),
		"original_message is rec.Value verbatim on the targeted branch")
	assert.Nil(t, dlq.OriginalMessageRaw,
		"targeted branch must not double-store the payload as base64")
	assert.Equal(t, "missing_field", dlq.Error)

	// Verify the events payload shape matches docs/design/04-kafka.md §2
	// with the locked T8 discriminator values.
	var ev eventPayload
	require.NoError(t, json.Unmarshal(in.EventPayload, &ev))
	assert.Equal(t, eventPayloadVersion, ev.Version)
	assert.Equal(t, id.String(), ev.ID)
	assert.Nil(t, ev.BatchID)
	assert.Equal(t, "sms", ev.Channel,
		"event channel comes from the worker's deps, not msg.channel (locked authoritative source)")
	assert.Equal(t, 3, ev.Attempt)
	assert.Equal(t, "DISPATCHED", ev.PreviousStatus)
	assert.Equal(t, "FAILED", ev.CurrentStatus)
	assert.Equal(t, "unprocessable", ev.Classification)
	require.NotNil(t, ev.FailureReason)
	assert.Equal(t, "unprocessable_message", *ev.FailureReason)
	assert.Equal(t, dlqFixedNowFormatted, ev.OccurredAt)
}

// TestBuildUnprocessable_NoTarget_MsgWithBadID asserts the
// "msg != nil but msg.ID fails uuid.Parse" sub-branch from
// docs/phases/03-resilience.md §4 BuildUnprocessable notes. The DLQ
// payload uses original_message (decoded JSON), but
// NotificationID + Attempt stay nil because the layer-3 guard needs
// a real id. EventPayload stays empty.
func TestBuildUnprocessable_NoTarget_MsgWithBadID(t *testing.T) {
	content := "x"
	msg := &sendPayload{
		Version:   1,
		ID:        "not-a-uuid",
		Attempt:   3,
		Channel:   "sms",
		Recipient: "+905551234567",
		Content:   &content,
	}
	rec := makeKafkaRecord("send.sms", []byte("not-a-uuid"),
		[]byte(`{"version":1,"id":"not-a-uuid","attempt":3,"channel":"sms","recipient":"+1","content":"x"}`))

	in, err := BuildUnprocessable(rec, msg, "schema_mismatch", "invalid id", "sms", dlqFixedNow)
	require.NoError(t, err)

	assert.Nil(t, in.NotificationID,
		"msg.ID failing uuid.Parse falls back to no-target")
	assert.Nil(t, in.Attempt)
	assert.Empty(t, in.EventPayload)

	var dlq dlqPayload
	require.NoError(t, json.Unmarshal(in.DLQPayload, &dlq))
	assert.JSONEq(t, string(rec.Value), string(dlq.OriginalMessage),
		"original_message preserved for forensic value even on no-target branch")
	assert.Nil(t, dlq.OriginalMessageRaw)
	require.NotNil(t, dlq.NotificationID,
		"DLQ still surfaces the (malformed) id for forensic value via dlq.notification_id")
	assert.Equal(t, "not-a-uuid", *dlq.NotificationID)
	require.NotNil(t, dlq.Attempt)
	assert.Equal(t, 3, *dlq.Attempt)
}

// TestBuildUnprocessable_NoTarget_MsgWithZeroAttempt asserts the
// "msg != nil, valid id, msg.Attempt <= 0" sub-branch. The layer-3
// guard needs a real attempt (the UPDATE filters on `attempt = $2`),
// so we fall back to no-target. The DLQ payload still gets the id +
// attempt for forensic clarity.
func TestBuildUnprocessable_NoTarget_MsgWithZeroAttempt(t *testing.T) {
	id := uuid.MustParse("01927000-0000-7000-8000-000000000051")
	content := "x"
	msg := &sendPayload{
		Version:   1,
		ID:        id.String(),
		Attempt:   0, // bogus — the layer-3 guard needs > 0
		Channel:   "sms",
		Recipient: "+1",
		Content:   &content,
	}
	rec := makeKafkaRecord("send.sms", []byte(id.String()), []byte(`{"version":1}`))

	in, err := BuildUnprocessable(rec, msg, "missing_field", "attempt must be > 0", "sms", dlqFixedNow)
	require.NoError(t, err)

	assert.Nil(t, in.NotificationID,
		"msg.Attempt <= 0 falls back to no-target so the layer-3 guard isn't fed a bogus filter")
	assert.Nil(t, in.Attempt)
	assert.Empty(t, in.EventPayload)

	var dlq dlqPayload
	require.NoError(t, json.Unmarshal(in.DLQPayload, &dlq))
	require.NotNil(t, dlq.NotificationID,
		"DLQ surfaces the id for forensic value even on no-target")
	assert.Equal(t, id.String(), *dlq.NotificationID)
	assert.Nil(t, dlq.Attempt,
		"DLQ omits attempt when msg.Attempt is the zero value (no useful info to record)")
}

// TestBuildUnprocessable_ChannelOverridesMsgChannel locks the spec's
// "channel arg is authoritative" rule. The msg.Channel value is
// ignored — even when it disagrees with the worker's Deps.Channel,
// the resulting UnprocessableInput.Channel reflects the worker's
// channel (so the DLQ topic resolves to send.<deps_channel>.dlq).
func TestBuildUnprocessable_ChannelOverridesMsgChannel(t *testing.T) {
	id := uuid.MustParse("01927000-0000-7000-8000-000000000052")
	content := "x"
	msg := &sendPayload{
		Version:   1,
		ID:        id.String(),
		Attempt:   1,
		Channel:   "email", // disagrees with the worker's deps.Channel below
		Recipient: "u@example.com",
		Content:   &content,
	}
	rec := makeKafkaRecord("send.email", []byte(id.String()), []byte(`{"version":1}`))

	in, err := BuildUnprocessable(rec, msg, "missing_field", "x", "sms", dlqFixedNow)
	require.NoError(t, err)

	assert.Equal(t, "sms", in.Channel,
		"channel arg overrides msg.Channel; the DLQ topic resolves to send.sms.dlq")

	var dlq dlqPayload
	require.NoError(t, json.Unmarshal(in.DLQPayload, &dlq))
	assert.Equal(t, "sms", dlq.Channel,
		"DLQ payload's channel field reflects the authoritative channel arg")

	var ev eventPayload
	require.NoError(t, json.Unmarshal(in.EventPayload, &ev))
	assert.Equal(t, "sms", ev.Channel,
		"events.notification payload's channel field reflects the authoritative channel arg")
}

// TestBuildUnprocessable_EveryErrCode confirms every documented
// error code per docs/design/04-kafka.md §3 + docs/design/05-retry.md §2
// makes it through to the DLQ payload's error field unchanged. Acts
// as a smoke test against a future refactor that introduces a code
// translation step.
func TestBuildUnprocessable_EveryErrCode(t *testing.T) {
	rec := makeKafkaRecord("send.sms", nil, []byte(`x`))

	for _, code := range []string{"decode_failed", "schema_mismatch", "missing_field", "panic"} {
		t.Run(code, func(t *testing.T) {
			in, err := BuildUnprocessable(rec, nil, code, "details", "sms", dlqFixedNow)
			require.NoError(t, err)
			assert.Equal(t, code, in.ErrorCode)

			var dlq dlqPayload
			require.NoError(t, json.Unmarshal(in.DLQPayload, &dlq))
			assert.Equal(t, code, dlq.Error,
				"DLQ payload's error field carries the code verbatim")
		})
	}
}
