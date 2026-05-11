package api

import (
	"fmt"
	"regexp"
	"time"
)

// Validation rules — per-channel surface locked in
// docs/design/03-api.md §Validation rules and
// docs/phases/03-resilience.md §10. Phase 2 only handled SMS;
// Phase 3 Chunk 7 widens the channel restriction and adds the
// email + push recipient + content rules.
//
// Hand-written per docs/phases/00-phases.md §Library stack ("No
// validator library"). Regex is stdlib and counts as hand-written;
// this file holds every rule (no third-party schema layer).

const (
	// channelSMS / channelEmail / channelPush are the three channel
	// values the api accepts in Phase 3 per
	// docs/design/01-schema.md §Domain values for notifications.channel.
	channelSMS   = "sms"
	channelEmail = "email"
	channelPush  = "push"

	// content_<channel>_max constants from docs/design/07-constants.md §G.
	// SMS = 1600 (10 GSM-7 concatenated segments); email = 100000
	// (~100 KB plaintext body); push = 4000 (FCM ~4 KB / APNs 4–5 KB).
	contentSMSMax   = 1600
	contentEmailMax = 100000
	contentPushMax  = 4000

	// recipient_email_max / recipient_push_min / recipient_push_max
	// constants from docs/design/07-constants.md §G. Email cap is the
	// RFC 5321 §4.5.3.1.3 maximum. Push tokens are opaque per
	// provider; the bounds bracket Apple device tokens (64 hex chars),
	// FCM tokens (~152 chars typical), and VAPID web push (longer).
	recipientEmailMax = 254
	recipientPushMin  = 32
	recipientPushMax  = 4096

	priorityLow    = "low"
	priorityNormal = "normal"
	priorityHigh   = "high"

	// statusPending is the only status the api ever writes (T1) per
	// docs/design/02-state-machine.md §State-driving components.
	statusPending = "PENDING"
)

// e164Re matches E.164 phone numbers per docs/design/03-api.md
// §Validation rules: a leading +, a non-zero first digit, then 1–14 more
// digits (total 2–15 digits after the +).
var e164Re = regexp.MustCompile(`^\+[1-9]\d{1,14}$`)

// emailRe is the intentionally permissive email regex from
// docs/design/03-api.md §Validation rules row `email`: <local>@<domain>
// with at least one `.` in <domain>. Full RFC 5322 is explicitly NOT
// enforced; the rule's job is to reject obvious typos and route
// per-channel formatting downstream, not to validate deliverability.
var emailRe = regexp.MustCompile(`^[^@\s]+@[^@\s]+\.[^@\s]+$`)

// uuidV4Re enforces the canonical lowercase UUIDv4 string per
// docs/design/03-api.md §Validation rules and the inline expansion in
// docs/phases/02-walking-skeleton.md §5: 36 chars, hyphens at positions
// 8/13/18/23, hex lowercase, position 14 = '4' (the version), position 19
// in {8,9,a,b} (the RFC 4122 variant). Compact (32-hex) form and
// uppercase hex are rejected.
var uuidV4Re = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

// ValidateCreate runs every rule from docs/design/03-api.md §Validation
// rules and returns one FieldIssue per failing rule. Rules do NOT
// short-circuit — the caller's response surfaces every issue at once
// so a single round trip is enough for the client to fix everything.
//
// `now` is the server-side clock used for the scheduled_at >= now() check.
// The handler injects it via Deps.Clock so tests can pin time without
// monkey-patching.
//
// Phase 3 Chunk 7 widens the per-channel rules: the channel value
// determines which recipient regex + content cap applies.
func ValidateCreate(req CreateRequest, now time.Time) []FieldIssue {
	var issues []FieldIssue

	channelKnown := false
	switch req.Channel {
	case "":
		issues = append(issues, FieldIssue{Path: "channel", Issue: "required"})
	case channelSMS, channelEmail, channelPush:
		channelKnown = true
	default:
		issues = append(issues, FieldIssue{Path: "channel", Issue: `must be "sms", "email", or "push"`})
	}

	// Recipient + content rules read against the resolved channel only
	// when the channel is one of the three known values; otherwise the
	// per-channel format check would surface a misleading second issue
	// (e.g., "must match E.164" against an "email" recipient that the
	// channel rule already rejected). The "required" check still fires
	// regardless so an empty recipient + unknown channel surfaces both
	// problems on one round trip.
	if req.Recipient == "" {
		issues = append(issues, FieldIssue{Path: "recipient", Issue: "required"})
	} else if channelKnown {
		if issue := validateRecipient(req.Channel, req.Recipient); issue != nil {
			issues = append(issues, *issue)
		}
	}

	if req.Content == "" {
		issues = append(issues, FieldIssue{Path: "content", Issue: "required"})
	} else if channelKnown {
		if issue := validateContent(req.Channel, req.Content); issue != nil {
			issues = append(issues, *issue)
		}
	}

	// Phase 6 ships templates; Phase 3 still rejects either field.
	if req.Template != "" {
		issues = append(issues, FieldIssue{Path: "template", Issue: "templates are not supported in phase 3"})
	}
	if len(req.TemplateData) > 0 {
		issues = append(issues, FieldIssue{Path: "template_data", Issue: "templates are not supported in phase 3"})
	}

	if req.Priority != "" {
		if _, ok := priorityToInt(req.Priority); !ok {
			issues = append(issues, FieldIssue{Path: "priority", Issue: `must be "low", "normal", or "high"`})
		}
	}

	if req.ScheduledAt != "" {
		t, err := time.Parse(time.RFC3339, req.ScheduledAt)
		if err != nil {
			issues = append(issues, FieldIssue{Path: "scheduled_at", Issue: "must be RFC 3339 with timezone"})
		} else if t.Before(now) {
			issues = append(issues, FieldIssue{Path: "scheduled_at", Issue: "must be in the future"})
		}
	}

	if req.IdempotencyKey == "" {
		issues = append(issues, FieldIssue{Path: "idempotency_key", Issue: "required"})
	} else if !isCanonicalUUIDv4Lower(req.IdempotencyKey) {
		issues = append(issues, FieldIssue{Path: "idempotency_key", Issue: "must be a canonical lowercase UUIDv4"})
	}

	return issues
}

// validateRecipient enforces the per-channel recipient rules from
// docs/design/03-api.md §Validation rules row `recipient`. Returns
// nil on success.
func validateRecipient(channel, recipient string) *FieldIssue {
	switch channel {
	case channelSMS:
		if !e164Re.MatchString(recipient) {
			return &FieldIssue{Path: "recipient", Issue: "must match E.164 format (^\\+[1-9]\\d{1,14}$)"}
		}
	case channelEmail:
		if len([]rune(recipient)) > recipientEmailMax {
			return &FieldIssue{Path: "recipient", Issue: fmt.Sprintf("exceeds maximum length %d", recipientEmailMax)}
		}
		if !emailRe.MatchString(recipient) {
			return &FieldIssue{Path: "recipient", Issue: "must be a valid email address"}
		}
	case channelPush:
		n := len([]rune(recipient))
		if n < recipientPushMin || n > recipientPushMax {
			return &FieldIssue{Path: "recipient", Issue: fmt.Sprintf("length must be between %d and %d", recipientPushMin, recipientPushMax)}
		}
	}
	return nil
}

// validateContent enforces the per-channel content cap from
// docs/design/03-api.md §Validation rules row `content`. Returns nil
// on success. Length is measured in runes (not bytes) so emoji /
// multibyte characters count once each, matching the SMS-segment
// semantics that originally drove the cap.
func validateContent(channel, content string) *FieldIssue {
	var max int
	switch channel {
	case channelSMS:
		max = contentSMSMax
	case channelEmail:
		max = contentEmailMax
	case channelPush:
		max = contentPushMax
	default:
		return nil
	}
	if len([]rune(content)) > max {
		return &FieldIssue{Path: "content", Issue: fmt.Sprintf("exceeds maximum length %d", max)}
	}
	return nil
}

// isCanonicalUUIDv4Lower validates the idempotency_key format. Exposed at
// the package level so handler tests can reuse the same check.
func isCanonicalUUIDv4Lower(s string) bool {
	return uuidV4Re.MatchString(s)
}

// priorityToInt translates the string priority to its int16 storage value
// per docs/design/01-schema.md §1 (Domain values: 0=low, 1=normal, 2=high).
// Returns ok=false for unknown strings; callers may treat that as a
// validation failure or a programmer error depending on context.
func priorityToInt(s string) (int16, bool) {
	switch s {
	case priorityLow:
		return 0, true
	case priorityNormal:
		return 1, true
	case priorityHigh:
		return 2, true
	default:
		return 0, false
	}
}

// priorityFromInt is the reverse of priorityToInt for the GET response.
// Unknown int values render as "normal" (the storage default) rather than
// surfacing a panic — defective rows still render.
func priorityFromInt(v int16) string {
	switch v {
	case 0:
		return priorityLow
	case 2:
		return priorityHigh
	default:
		return priorityNormal
	}
}
