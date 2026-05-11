package api

import (
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fixedNow is the synthetic clock the validator tests use so
// scheduled_at boundaries are deterministic regardless of when the test
// runs.
var fixedNow = time.Date(2026, 5, 11, 12, 0, 0, 0, time.UTC)

func validRequest() CreateRequest {
	return CreateRequest{
		Channel:        "sms",
		Recipient:      "+905551234567",
		Content:        "phase 2 happy path",
		IdempotencyKey: "00000000-0000-4000-8000-000000000001",
	}
}

func TestValidateCreate_HappyPath(t *testing.T) {
	issues := ValidateCreate(validRequest(), fixedNow)
	assert.Empty(t, issues)
}

func TestValidateCreate_HappyPath_OptionalsSet(t *testing.T) {
	req := validRequest()
	req.Priority = "high"
	req.ScheduledAt = "2026-05-11T13:00:00Z"

	issues := ValidateCreate(req, fixedNow)
	assert.Empty(t, issues)
}

func TestValidateCreate_AllRulesRunNoShortCircuit(t *testing.T) {
	req := CreateRequest{}
	issues := ValidateCreate(req, fixedNow)

	paths := make(map[string]bool, len(issues))
	for _, issue := range issues {
		paths[issue.Path] = true
	}
	assert.True(t, paths["channel"], "expected channel issue")
	assert.True(t, paths["recipient"], "expected recipient issue")
	assert.True(t, paths["content"], "expected content issue")
	assert.True(t, paths["idempotency_key"], "expected idempotency_key issue")
}

func TestValidateCreate_Channel(t *testing.T) {
	cases := []struct {
		name      string
		channel   string
		recipient string
		content   string
		wantOK    bool
		wantWord  string
	}{
		{"empty channel", "", "+905551234567", "ok", false, "required"},
		{"sms accepted", "sms", "+905551234567", "ok", true, ""},
		{"email accepted in phase 3", "email", "u@example.com", "ok", true, ""},
		{"push accepted in phase 3", "push", strings.Repeat("a", recipientPushMin), "ok", true, ""},
		{"unknown rejected", "fax", "anything", "ok", false, "must be"},
		{"uppercase channel rejected", "SMS", "+905551234567", "ok", false, "must be"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := validRequest()
			req.Channel = tc.channel
			req.Recipient = tc.recipient
			req.Content = tc.content
			issues := ValidateCreate(req, fixedNow)
			if tc.wantOK {
				assert.NotContains(t, issuesPaths(issues), "channel")
			} else {
				require.NotEmpty(t, issues)
				assert.Equal(t, "channel", issues[0].Path)
				assert.Contains(t, issues[0].Issue, tc.wantWord)
			}
		})
	}
}

// TestValidateCreate_Recipient_Email exercises the per-channel email
// recipient validator added in Phase 3 Chunk 7. The regex is
// intentionally permissive (no full RFC 5322 enforcement) per
// docs/design/03-api.md §Validation rules row `email`.
func TestValidateCreate_Recipient_Email(t *testing.T) {
	cases := []struct {
		name      string
		recipient string
		wantOK    bool
	}{
		{"plain happy path", "u@example.com", true},
		{"subdomain", "alice@mail.example.co.uk", true},
		{"plus tag", "alice+tag@example.com", true},
		{"missing @", "no-at-sign", false},
		{"missing dot in domain", "u@example", false},
		{"missing local", "@example.com", false},
		{"missing domain", "u@", false},
		{"contains space", "u name@example.com", false},
		{"empty rejected via required check", "", false},
		{"too long over 254 chars", strings.Repeat("a", recipientEmailMax) + "@example.com", false},
		{"at limit (254 chars)", strings.Repeat("a", recipientEmailMax-len("@example.com")) + "@example.com", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := validRequest()
			req.Channel = "email"
			req.Recipient = tc.recipient
			req.Content = "hello"
			issues := ValidateCreate(req, fixedNow)
			if tc.wantOK {
				assert.NotContains(t, issuesPaths(issues), "recipient")
			} else {
				assert.Contains(t, issuesPaths(issues), "recipient")
			}
		})
	}
}

// TestValidateCreate_Recipient_Push exercises the per-channel push
// token validator. Push tokens are opaque per provider; the rule is
// length-only with the bounds from docs/design/07-constants.md §G.
func TestValidateCreate_Recipient_Push(t *testing.T) {
	cases := []struct {
		name      string
		recipient string
		wantOK    bool
	}{
		{"at min boundary (32 chars)", strings.Repeat("a", recipientPushMin), true},
		{"below min (31 chars)", strings.Repeat("a", recipientPushMin-1), false},
		{"typical APNs token (64 hex)", strings.Repeat("0123456789abcdef", 4), true},
		{"typical FCM token (~152 chars)", strings.Repeat("a", 152), true},
		{"at max boundary (4096 chars)", strings.Repeat("a", recipientPushMax), true},
		{"above max (4097 chars)", strings.Repeat("a", recipientPushMax+1), false},
		{"empty rejected via required check", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := validRequest()
			req.Channel = "push"
			req.Recipient = tc.recipient
			req.Content = "hello"
			issues := ValidateCreate(req, fixedNow)
			if tc.wantOK {
				assert.NotContains(t, issuesPaths(issues), "recipient")
			} else {
				assert.Contains(t, issuesPaths(issues), "recipient")
			}
		})
	}
}

func TestValidateCreate_Recipient_E164(t *testing.T) {
	cases := []struct {
		name      string
		recipient string
		wantOK    bool
	}{
		{"missing +", "905551234567", false},
		{"leading zero after +", "+05551234567", false},
		{"too short total digits=1", "+1", false},
		{"shortest valid total digits=2", "+12", true},
		{"longest valid total digits=15", "+123456789012345", true},
		{"too long total digits=16", "+1234567890123456", false},
		{"contains letters", "+90555ABCD123", false},
		{"contains spaces", "+90 555 123 4567", false},
		{"empty", "", false},
		{"plus only", "+", false},
		{"valid TR mobile", "+905551234567", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := validRequest()
			req.Recipient = tc.recipient
			issues := ValidateCreate(req, fixedNow)
			if tc.wantOK {
				for _, issue := range issues {
					assert.NotEqual(t, "recipient", issue.Path, "no recipient issue expected")
				}
			} else {
				require.NotEmpty(t, issues)
				found := false
				for _, issue := range issues {
					if issue.Path == "recipient" {
						found = true
					}
				}
				assert.True(t, found, "expected recipient issue")
			}
		})
	}
}

func TestValidateCreate_Content(t *testing.T) {
	t.Run("required", func(t *testing.T) {
		req := validRequest()
		req.Content = ""
		issues := ValidateCreate(req, fixedNow)
		assert.Contains(t, issuesPaths(issues), "content")
	})

	t.Run("at limit", func(t *testing.T) {
		req := validRequest()
		req.Content = strings.Repeat("a", contentSMSMax)
		issues := ValidateCreate(req, fixedNow)
		assert.NotContains(t, issuesPaths(issues), "content")
	})

	t.Run("over limit", func(t *testing.T) {
		req := validRequest()
		req.Content = strings.Repeat("a", contentSMSMax+1)
		issues := ValidateCreate(req, fixedNow)
		assert.Contains(t, issuesPaths(issues), "content")
	})

	t.Run("multibyte runes counted by rune not byte", func(t *testing.T) {
		req := validRequest()
		req.Content = strings.Repeat("😀", contentSMSMax)
		issues := ValidateCreate(req, fixedNow)
		assert.NotContains(t, issuesPaths(issues), "content")
	})
}

// TestValidateCreate_Content_PerChannelCaps locks the per-channel
// content cap boundaries from docs/design/07-constants.md §G:
// SMS = 1600, email = 100000, push = 4000. The boundary cases prove
// the cap is exclusive of the +1th rune.
func TestValidateCreate_Content_PerChannelCaps(t *testing.T) {
	cases := []struct {
		name      string
		channel   string
		recipient string
		length    int
		wantOK    bool
	}{
		{"sms at limit", "sms", "+905551234567", contentSMSMax, true},
		{"sms over limit", "sms", "+905551234567", contentSMSMax + 1, false},
		{"email at limit", "email", "u@example.com", contentEmailMax, true},
		{"email over limit", "email", "u@example.com", contentEmailMax + 1, false},
		{"push at limit", "push", strings.Repeat("a", recipientPushMin), contentPushMax, true},
		{"push over limit", "push", strings.Repeat("a", recipientPushMin), contentPushMax + 1, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := validRequest()
			req.Channel = tc.channel
			req.Recipient = tc.recipient
			req.Content = strings.Repeat("a", tc.length)
			issues := ValidateCreate(req, fixedNow)
			if tc.wantOK {
				assert.NotContains(t, issuesPaths(issues), "content",
					"%s @ %d chars must pass", tc.channel, tc.length)
			} else {
				assert.Contains(t, issuesPaths(issues), "content",
					"%s @ %d chars must fail", tc.channel, tc.length)
			}
		})
	}
}

func TestValidateCreate_TemplateFieldsRejected(t *testing.T) {
	t.Run("template present", func(t *testing.T) {
		req := validRequest()
		req.Template = "welcome"
		issues := ValidateCreate(req, fixedNow)
		assert.Contains(t, issuesPaths(issues), "template")
	})

	t.Run("template_data present", func(t *testing.T) {
		req := validRequest()
		req.TemplateData = []byte(`{"name":"Ada"}`)
		issues := ValidateCreate(req, fixedNow)
		assert.Contains(t, issuesPaths(issues), "template_data")
	})

	t.Run("both present", func(t *testing.T) {
		req := validRequest()
		req.Template = "welcome"
		req.TemplateData = []byte(`{"name":"Ada"}`)
		issues := ValidateCreate(req, fixedNow)
		paths := issuesPaths(issues)
		assert.Contains(t, paths, "template")
		assert.Contains(t, paths, "template_data")
	})
}

func TestValidateCreate_Priority(t *testing.T) {
	cases := []struct {
		value  string
		wantOK bool
	}{
		{"", true},
		{"low", true},
		{"normal", true},
		{"high", true},
		{"LOW", false},
		{"medium", false},
		{"0", false},
	}
	for _, tc := range cases {
		t.Run(tc.value, func(t *testing.T) {
			req := validRequest()
			req.Priority = tc.value
			issues := ValidateCreate(req, fixedNow)
			if tc.wantOK {
				assert.NotContains(t, issuesPaths(issues), "priority")
			} else {
				assert.Contains(t, issuesPaths(issues), "priority")
			}
		})
	}
}

func TestValidateCreate_ScheduledAt(t *testing.T) {
	cases := []struct {
		name     string
		value    string
		wantOK   bool
		wantWord string
	}{
		{"absent ok", "", true, ""},
		{"future ok", "2026-05-11T13:00:00Z", true, ""},
		{"future with offset ok", "2026-05-11T15:00:00+02:00", true, ""},
		{"past rejected", "2026-05-11T11:00:00Z", false, "future"},
		{"missing tz rejected", "2026-05-11T13:00:00", false, "RFC 3339"},
		{"plain date rejected", "2026-05-11", false, "RFC 3339"},
		{"garbage rejected", "tomorrow", false, "RFC 3339"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := validRequest()
			req.ScheduledAt = tc.value
			issues := ValidateCreate(req, fixedNow)
			if tc.wantOK {
				assert.NotContains(t, issuesPaths(issues), "scheduled_at")
			} else {
				require.NotEmpty(t, issues)
				found := false
				for _, issue := range issues {
					if issue.Path == "scheduled_at" {
						found = true
						assert.Contains(t, issue.Issue, tc.wantWord)
					}
				}
				assert.True(t, found, "expected scheduled_at issue")
			}
		})
	}
}

func TestValidateCreate_IdempotencyKey(t *testing.T) {
	t.Run("required", func(t *testing.T) {
		req := validRequest()
		req.IdempotencyKey = ""
		issues := ValidateCreate(req, fixedNow)
		assert.Contains(t, issuesPaths(issues), "idempotency_key")
	})

	t.Run("malformed", func(t *testing.T) {
		req := validRequest()
		req.IdempotencyKey = "not-a-uuid"
		issues := ValidateCreate(req, fixedNow)
		assert.Contains(t, issuesPaths(issues), "idempotency_key")
	})
}

func TestIsCanonicalUUIDv4Lower(t *testing.T) {
	cases := []struct {
		name string
		s    string
		want bool
	}{
		{"happy path", "00000000-0000-4000-8000-000000000001", true},
		{"variant 8", "11111111-2222-4333-8444-555555555555", true},
		{"variant 9", "11111111-2222-4333-9444-555555555555", true},
		{"variant a", "11111111-2222-4333-a444-555555555555", true},
		{"variant b", "11111111-2222-4333-b444-555555555555", true},
		{"variant c rejected", "11111111-2222-4333-c444-555555555555", false},
		{"variant 7 rejected", "11111111-2222-4333-7444-555555555555", false},
		{"version 1 rejected", "11111111-2222-1333-8444-555555555555", false},
		{"version 7 rejected", "11111111-2222-7333-8444-555555555555", false},
		{"uppercase rejected", "11111111-2222-4333-8444-555555555555"[:8] + "-AAAA-4333-8444-555555555555", false},
		{"compact form rejected", "11111111222243338444555555555555", false},
		{"missing hyphens rejected", "11111111 2222 4333 8444 555555555555", false},
		{"too short", "11111111-2222-4333-8444-55555555555", false},
		{"too long", "11111111-2222-4333-8444-5555555555555", false},
		{"empty", "", false},
		{"non-hex char", "11111111-2222-4333-8444-55555555555g", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, isCanonicalUUIDv4Lower(tc.s))
		})
	}
}

func TestPriorityToInt(t *testing.T) {
	got, ok := priorityToInt("low")
	require.True(t, ok)
	assert.Equal(t, int16(0), got)

	got, ok = priorityToInt("normal")
	require.True(t, ok)
	assert.Equal(t, int16(1), got)

	got, ok = priorityToInt("high")
	require.True(t, ok)
	assert.Equal(t, int16(2), got)

	_, ok = priorityToInt("")
	assert.False(t, ok)

	_, ok = priorityToInt("MEDIUM")
	assert.False(t, ok)
}

func TestPriorityFromInt(t *testing.T) {
	assert.Equal(t, "low", priorityFromInt(0))
	assert.Equal(t, "normal", priorityFromInt(1))
	assert.Equal(t, "high", priorityFromInt(2))
	assert.Equal(t, "normal", priorityFromInt(99), "unknown int collapses to normal default")
}

// issuesPaths is a small test helper for asserting which fields are
// represented in a slice of FieldIssues.
func issuesPaths(issues []FieldIssue) []string {
	out := make([]string, 0, len(issues))
	for _, issue := range issues {
		out = append(out, issue.Path)
	}
	return out
}

// TestParseListRequest_Defaults pins the empty-query disposition:
// offset=0, limit=listDefaultLimit, every filter pointer nil.
func TestParseListRequest_Defaults(t *testing.T) {
	req := httptest.NewRequest("GET", "/v1/notifications", nil)
	got, issues := parseListRequest(req)

	assert.Empty(t, issues)
	assert.Equal(t, 0, got.Offset)
	assert.Equal(t, listDefaultLimit, got.Limit)
	assert.Nil(t, got.Filters.Status)
	assert.Nil(t, got.Filters.Channel)
	assert.Nil(t, got.Filters.Priority)
	assert.Nil(t, got.Filters.BatchID)
	assert.Nil(t, got.Filters.CreatedAfter)
	assert.Nil(t, got.Filters.CreatedBefore)
}

func TestParseListRequest_OffsetLimit(t *testing.T) {
	cases := []struct {
		name       string
		query      string
		wantOffset int
		wantLimit  int
		wantPath   string
		wantWord   string
	}{
		{"valid offset and limit", "offset=10&limit=25", 10, 25, "", ""},
		{"limit at lower bound", "limit=1", 0, 1, "", ""},
		{"limit at upper bound", "limit=200", 0, 200, "", ""},
		{"limit zero rejected", "limit=0", 0, listDefaultLimit, "limit", "between"},
		{"limit over max rejected", "limit=201", 0, listDefaultLimit, "limit", "between"},
		{"limit non-integer rejected", "limit=abc", 0, listDefaultLimit, "limit", "integer"},
		{"offset negative rejected", "offset=-1", 0, listDefaultLimit, "offset", ">= 0"},
		{"offset non-integer rejected", "offset=xyz", 0, listDefaultLimit, "offset", "integer"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/v1/notifications?"+tc.query, nil)
			got, issues := parseListRequest(req)

			if tc.wantPath == "" {
				assert.Empty(t, issues)
				assert.Equal(t, tc.wantOffset, got.Offset)
				assert.Equal(t, tc.wantLimit, got.Limit)
			} else {
				require.Len(t, issues, 1)
				assert.Equal(t, tc.wantPath, issues[0].Path)
				assert.Contains(t, issues[0].Issue, tc.wantWord)
			}
		})
	}
}

func TestParseListRequest_StatusFilter(t *testing.T) {
	cases := []struct {
		name   string
		value  string
		wantOK bool
	}{
		{"PENDING accepted", "PENDING", true},
		{"DISPATCHED accepted", "DISPATCHED", true},
		{"DELIVERED accepted", "DELIVERED", true},
		{"FAILED accepted", "FAILED", true},
		{"CANCELLED accepted", "CANCELLED", true},
		{"lowercase rejected", "pending", false},
		{"unknown rejected", "QUEUED", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/v1/notifications?status="+tc.value, nil)
			got, issues := parseListRequest(req)

			if tc.wantOK {
				require.Empty(t, issues)
				require.NotNil(t, got.Filters.Status)
				assert.Equal(t, tc.value, *got.Filters.Status)
			} else {
				assert.Contains(t, issuesPaths(issues), "status")
				assert.Nil(t, got.Filters.Status)
			}
		})
	}
}

func TestParseListRequest_ChannelFilter(t *testing.T) {
	cases := []struct {
		name   string
		value  string
		wantOK bool
	}{
		{"sms accepted", "sms", true},
		{"email accepted", "email", true},
		{"push accepted", "push", true},
		{"uppercase rejected", "SMS", false},
		{"unknown rejected", "fax", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/v1/notifications?channel="+tc.value, nil)
			got, issues := parseListRequest(req)

			if tc.wantOK {
				require.Empty(t, issues)
				require.NotNil(t, got.Filters.Channel)
				assert.Equal(t, tc.value, *got.Filters.Channel)
			} else {
				assert.Contains(t, issuesPaths(issues), "channel")
				assert.Nil(t, got.Filters.Channel)
			}
		})
	}
}

func TestParseListRequest_PriorityFilter(t *testing.T) {
	cases := []struct {
		name      string
		value     string
		wantOK    bool
		wantValue int16
	}{
		{"low → 0", "low", true, 0},
		{"normal → 1", "normal", true, 1},
		{"high → 2", "high", true, 2},
		{"uppercase rejected", "HIGH", false, 0},
		{"numeric rejected", "1", false, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/v1/notifications?priority="+tc.value, nil)
			got, issues := parseListRequest(req)

			if tc.wantOK {
				require.Empty(t, issues)
				require.NotNil(t, got.Filters.Priority)
				assert.Equal(t, tc.wantValue, *got.Filters.Priority)
			} else {
				assert.Contains(t, issuesPaths(issues), "priority")
				assert.Nil(t, got.Filters.Priority)
			}
		})
	}
}

func TestParseListRequest_BatchIDFilter(t *testing.T) {
	t.Run("valid uuid accepted", func(t *testing.T) {
		id := "11110000-0000-7000-8000-000000000020"
		req := httptest.NewRequest("GET", "/v1/notifications?batch_id="+id, nil)
		got, issues := parseListRequest(req)

		require.Empty(t, issues)
		require.NotNil(t, got.Filters.BatchID)
		assert.Equal(t, uuid.MustParse(id), *got.Filters.BatchID)
	})

	t.Run("malformed rejected", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/v1/notifications?batch_id=not-a-uuid", nil)
		_, issues := parseListRequest(req)
		assert.Contains(t, issuesPaths(issues), "batch_id")
	})
}

func TestParseListRequest_CreatedFilters(t *testing.T) {
	t.Run("valid created_after accepted", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/v1/notifications?created_after=2026-05-11T00:00:00Z", nil)
		got, issues := parseListRequest(req)

		require.Empty(t, issues)
		require.NotNil(t, got.Filters.CreatedAfter)
	})

	t.Run("valid created_before accepted", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/v1/notifications?created_before=2026-05-12T00:00:00Z", nil)
		got, issues := parseListRequest(req)

		require.Empty(t, issues)
		require.NotNil(t, got.Filters.CreatedBefore)
	})

	t.Run("malformed rejected", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/v1/notifications?created_after=tomorrow", nil)
		_, issues := parseListRequest(req)
		assert.Contains(t, issuesPaths(issues), "created_after")
	})

	t.Run("missing tz rejected", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/v1/notifications?created_before=2026-05-11T00:00:00", nil)
		_, issues := parseListRequest(req)
		assert.Contains(t, issuesPaths(issues), "created_before")
	})

	t.Run("does NOT enforce after <= before", func(t *testing.T) {
		// created_after > created_before is a valid query that matches
		// nothing — the parser must not surface a cross-field issue.
		req := httptest.NewRequest("GET",
			"/v1/notifications?created_after=2026-06-01T00:00:00Z&created_before=2026-05-01T00:00:00Z", nil)
		_, issues := parseListRequest(req)
		assert.Empty(t, issues)
	})
}

func TestParseListRequest_AllFiltersAtOnce(t *testing.T) {
	q := "offset=5&limit=20" +
		"&status=DELIVERED" +
		"&channel=sms" +
		"&priority=high" +
		"&batch_id=11110000-0000-7000-8000-000000000030" +
		"&created_after=2026-05-01T00:00:00Z" +
		"&created_before=2026-05-12T00:00:00Z"

	req := httptest.NewRequest("GET", "/v1/notifications?"+q, nil)
	got, issues := parseListRequest(req)

	require.Empty(t, issues)
	assert.Equal(t, 5, got.Offset)
	assert.Equal(t, 20, got.Limit)
	require.NotNil(t, got.Filters.Status)
	assert.Equal(t, "DELIVERED", *got.Filters.Status)
	require.NotNil(t, got.Filters.Channel)
	assert.Equal(t, "sms", *got.Filters.Channel)
	require.NotNil(t, got.Filters.Priority)
	assert.Equal(t, int16(2), *got.Filters.Priority)
	require.NotNil(t, got.Filters.BatchID)
	assert.Equal(t, "11110000-0000-7000-8000-000000000030", got.Filters.BatchID.String())
	require.NotNil(t, got.Filters.CreatedAfter)
	require.NotNil(t, got.Filters.CreatedBefore)
}

// TestParseListRequest_AllInvalidParamsAtOnce verifies issues do NOT
// short-circuit: a single parse pass surfaces every bad param at once,
// matching the rest-of-validator posture from
// docs/design/03-api.md §Error model.
func TestParseListRequest_AllInvalidParamsAtOnce(t *testing.T) {
	q := "offset=-1&limit=0&status=garbage&channel=fax&priority=urgent&batch_id=nope&created_after=tomorrow&created_before=yesterday"
	req := httptest.NewRequest("GET", "/v1/notifications?"+q, nil)
	_, issues := parseListRequest(req)

	paths := issuesPaths(issues)
	for _, want := range []string{"offset", "limit", "status", "channel", "priority", "batch_id", "created_after", "created_before"} {
		assert.Contains(t, paths, want, "expected issue for path %q", want)
	}
}

// TestParseListRequest_UnknownParamsIgnored locks the behavior from
// docs/design/03-api.md §Conventions ("Unknown request body fields are
// ignored") applied symmetrically to the query string. A future
// query-param widening (e.g., template_id) doesn't break older clients.
func TestParseListRequest_UnknownParamsIgnored(t *testing.T) {
	req := httptest.NewRequest("GET", "/v1/notifications?garbage=ignored&also_garbage=42", nil)
	got, issues := parseListRequest(req)

	assert.Empty(t, issues)
	assert.Equal(t, 0, got.Offset)
	assert.Equal(t, listDefaultLimit, got.Limit)
}

// validBatchItem returns a fresh BatchItem that passes every
// validateCreateItem rule (mirrors validRequest at the BatchItem level).
// Tests can mutate one field at a time so the failure points are
// isolated.
func validBatchItem(key string) BatchItem {
	return BatchItem{
		Channel:        "sms",
		Recipient:      "+905551234567",
		Content:        "batch happy path",
		IdempotencyKey: key,
	}
}

// TestValidateBatchCreate_HappyPath asserts a fully valid batch
// surfaces zero issues.
func TestValidateBatchCreate_HappyPath(t *testing.T) {
	req := BatchCreateRequest{
		Notifications: []BatchItem{
			validBatchItem("00000000-0000-4000-8000-000000000a01"),
			validBatchItem("00000000-0000-4000-8000-000000000a02"),
			validBatchItem("00000000-0000-4000-8000-000000000a03"),
		},
	}
	issues := ValidateBatchCreate(req, fixedNow)
	assert.Empty(t, issues)
}

// TestValidateBatchCreate_EmptyBatch insists the empty-batch case
// surfaces exactly one issue against the "notifications" path. No
// per-item rules apply.
func TestValidateBatchCreate_EmptyBatch(t *testing.T) {
	issues := ValidateBatchCreate(BatchCreateRequest{Notifications: nil}, fixedNow)
	require.Len(t, issues, 1)
	assert.Equal(t, "notifications", issues[0].Path)
	assert.Contains(t, issues[0].Issue, "at least one")

	issues = ValidateBatchCreate(BatchCreateRequest{Notifications: []BatchItem{}}, fixedNow)
	require.Len(t, issues, 1)
	assert.Equal(t, "notifications", issues[0].Path)
}

// TestValidateBatchCreate_OversizedBatch_ShortCircuits asserts an
// oversize batch produces ONLY the "batch size exceeded" issue. The
// per-item walk is skipped — the handler routes the single short-
// circuit issue to 413 payload_too_large directly per
// docs/phases/04-api-completeness.md §3.1.
func TestValidateBatchCreate_OversizedBatch_ShortCircuits(t *testing.T) {
	items := make([]BatchItem, batchMax+1)
	for i := range items {
		items[i] = validBatchItem(fmt.Sprintf("00000000-0000-4000-8000-%012d", i+1))
	}
	// Break one item so we can prove the per-item walk did NOT run.
	items[0].Content = ""
	items[0].Channel = ""

	issues := ValidateBatchCreate(BatchCreateRequest{Notifications: items}, fixedNow)
	require.Len(t, issues, 1, "oversize batch must short-circuit to one issue")
	assert.Equal(t, "notifications", issues[0].Path)
	assert.Contains(t, issues[0].Issue, "batch size")
	assert.Contains(t, issues[0].Issue, fmt.Sprintf("%d", batchMax))
}

// TestValidateBatchCreate_AtBoundary asserts a batch of exactly
// batchMax items passes the size check (the cap is exclusive of the
// +1th item).
func TestValidateBatchCreate_AtBoundary(t *testing.T) {
	items := make([]BatchItem, batchMax)
	for i := range items {
		items[i] = validBatchItem(fmt.Sprintf("00000000-0000-4000-8000-%012d", i+1))
	}
	issues := ValidateBatchCreate(BatchCreateRequest{Notifications: items}, fixedNow)
	assert.Empty(t, issues, "exactly batchMax items must pass size check")
}

// TestValidateBatchCreate_PerItemPathsPrefixed asserts every per-item
// FieldIssue gets the "notifications[i]." prefix on its path. Verifies
// the validator surfaces non-zero, non-first item failures correctly.
func TestValidateBatchCreate_PerItemPathsPrefixed(t *testing.T) {
	good := validBatchItem("00000000-0000-4000-8000-000000000b01")

	// Item 2 has an empty channel + empty recipient + bad idempotency.
	bad := BatchItem{
		Channel:        "fax",
		Recipient:      "",
		Content:        "x",
		IdempotencyKey: "garbage",
	}

	// Item 3 has a non-RFC-3339 scheduled_at.
	scheduled := validBatchItem("00000000-0000-4000-8000-000000000b03")
	scheduled.ScheduledAt = "tomorrow"

	req := BatchCreateRequest{Notifications: []BatchItem{good, bad, scheduled}}
	issues := ValidateBatchCreate(req, fixedNow)

	paths := issuesPaths(issues)
	for _, path := range paths {
		assert.False(t, strings.HasPrefix(path, "notifications[0]."),
			"item 0 was valid; no notifications[0].* path expected, got %q", path)
	}
	assert.Contains(t, paths, "notifications[1].channel")
	assert.Contains(t, paths, "notifications[1].recipient")
	assert.Contains(t, paths, "notifications[1].idempotency_key")
	assert.Contains(t, paths, "notifications[2].scheduled_at")
}

// TestValidateBatchCreate_IntraBatchDuplicate asserts two items sharing
// one idempotency_key surface ONE duplicate issue against the second
// item's path. The first item's path is not flagged (its key, on its
// own, is acceptable).
func TestValidateBatchCreate_IntraBatchDuplicate(t *testing.T) {
	a := validBatchItem("00000000-0000-4000-8000-000000000c01")
	b := validBatchItem("00000000-0000-4000-8000-000000000c01")
	b.Recipient = "+905551234568"

	req := BatchCreateRequest{Notifications: []BatchItem{a, b}}
	issues := ValidateBatchCreate(req, fixedNow)

	duplicates := 0
	for _, issue := range issues {
		if strings.Contains(issue.Issue, "duplicate of notifications[") {
			duplicates++
			assert.Equal(t, "notifications[1].idempotency_key", issue.Path)
			assert.Contains(t, issue.Issue, "notifications[0].idempotency_key")
		}
	}
	assert.Equal(t, 1, duplicates, "exactly one duplicate issue surfaces")
}

// TestValidateBatchCreate_TripleDuplicate asserts three items sharing
// one key produce TWO duplicate issues (one per duplicate occurrence,
// not one per pair): notifications[1] and notifications[2] each get
// one issue pointing back at notifications[0].
func TestValidateBatchCreate_TripleDuplicate(t *testing.T) {
	key := "00000000-0000-4000-8000-000000000c10"
	a := validBatchItem(key)
	b := validBatchItem(key)
	b.Recipient = "+905551234568"
	c := validBatchItem(key)
	c.Recipient = "+905551234569"

	req := BatchCreateRequest{Notifications: []BatchItem{a, b, c}}
	issues := ValidateBatchCreate(req, fixedNow)

	dupPaths := map[string]bool{}
	for _, issue := range issues {
		if strings.Contains(issue.Issue, "duplicate of notifications[") {
			dupPaths[issue.Path] = true
		}
	}
	assert.True(t, dupPaths["notifications[1].idempotency_key"])
	assert.True(t, dupPaths["notifications[2].idempotency_key"])
	assert.Equal(t, 2, len(dupPaths), "exactly two duplicate issues")
}

// TestValidateBatchCreate_EmptyKeysNotDeduped asserts the duplicate
// check skips empty idempotency_key values: the per-item walk's
// "required" rule already flagged them, and treating empty keys as
// "duplicate of each other" would surface confusing extra issues.
func TestValidateBatchCreate_EmptyKeysNotDeduped(t *testing.T) {
	a := validBatchItem("")
	b := validBatchItem("")

	req := BatchCreateRequest{Notifications: []BatchItem{a, b}}
	issues := ValidateBatchCreate(req, fixedNow)

	for _, issue := range issues {
		assert.NotContains(t, issue.Issue, "duplicate of notifications[",
			"empty keys must not trigger duplicate-of- issues, got %v", issue)
	}

	// Sanity check: both items have an idempotency_key required issue.
	paths := issuesPaths(issues)
	assert.Contains(t, paths, "notifications[0].idempotency_key")
	assert.Contains(t, paths, "notifications[1].idempotency_key")
}

// TestValidateBatchCreate_MixedScenario combines per-item failures,
// a valid item, and an intra-batch duplicate. The response must
// include every relevant issue at once (no short-circuiting).
func TestValidateBatchCreate_MixedScenario(t *testing.T) {
	// Item 0: valid.
	valid := validBatchItem("00000000-0000-4000-8000-000000000d01")
	// Item 1: per-item failure (empty content).
	bad := validBatchItem("00000000-0000-4000-8000-000000000d02")
	bad.Content = ""
	// Item 2: duplicate of item 0's key.
	dup := validBatchItem("00000000-0000-4000-8000-000000000d01")
	dup.Recipient = "+905551234569"

	req := BatchCreateRequest{Notifications: []BatchItem{valid, bad, dup}}
	issues := ValidateBatchCreate(req, fixedNow)

	paths := issuesPaths(issues)
	assert.Contains(t, paths, "notifications[1].content", "per-item failure surfaces")
	assert.Contains(t, paths, "notifications[2].idempotency_key", "intra-batch duplicate surfaces")
	for _, path := range paths {
		assert.False(t, strings.HasPrefix(path, "notifications[0]."),
			"valid item's path must not appear, got %q", path)
	}
}

// TestValidateBatchCreate_NoShortCircuitOnFirstItemFail asserts the
// per-item walk continues past a failing item — item 0's failure does
// not suppress item 1's.
func TestValidateBatchCreate_NoShortCircuitOnFirstItemFail(t *testing.T) {
	a := BatchItem{Channel: "fax", Recipient: "x", Content: "x", IdempotencyKey: "00000000-0000-4000-8000-000000000e01"}
	b := BatchItem{Channel: "fax", Recipient: "y", Content: "y", IdempotencyKey: "00000000-0000-4000-8000-000000000e02"}

	req := BatchCreateRequest{Notifications: []BatchItem{a, b}}
	issues := ValidateBatchCreate(req, fixedNow)
	paths := issuesPaths(issues)
	assert.Contains(t, paths, "notifications[0].channel")
	assert.Contains(t, paths, "notifications[1].channel",
		"per-item walk must run for every item, not just the first")
}
