package api

import (
	"strings"
	"testing"
	"time"

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
