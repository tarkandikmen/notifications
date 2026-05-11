package api

import (
	"regexp"
	"time"
)

// Phase 2 validation rules — locked in docs/phases/02-walking-skeleton.md §5.
//
// Hand-written per docs/phases/00-phases.md §Library stack ("No validator
// library"). Regex is stdlib and counts as hand-written; this file holds
// every rule (no third-party schema layer).

const (
	// contentSMSMax is locked in docs/design/07-constants.md §G
	// (`content_sms_max`). Phase 2 only handles SMS so the per-channel
	// table collapses to one constant; Phase 3 widens.
	contentSMSMax = 1600

	// channelSMS is the only accepted channel value in Phase 2 per
	// docs/phases/02-walking-skeleton.md §3 ("Phase 2 channel restriction").
	channelSMS = "sms"

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

// uuidV4Re enforces the canonical lowercase UUIDv4 string per
// docs/design/03-api.md §Validation rules and the inline expansion in
// docs/phases/02-walking-skeleton.md §5: 36 chars, hyphens at positions
// 8/13/18/23, hex lowercase, position 14 = '4' (the version), position 19
// in {8,9,a,b} (the RFC 4122 variant). Compact (32-hex) form and
// uppercase hex are rejected.
var uuidV4Re = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

// ValidateCreate runs every rule in docs/phases/02-walking-skeleton.md §5
// and returns one FieldIssue per failing rule. Rules do NOT short-circuit
// — the caller's response surfaces every issue at once so a single round
// trip is enough for the client to fix everything.
//
// `now` is the server-side clock used for the scheduled_at >= now() check.
// The handler injects it via Deps.Clock so tests can pin time without
// monkey-patching.
func ValidateCreate(req CreateRequest, now time.Time) []FieldIssue {
	var issues []FieldIssue

	switch req.Channel {
	case "":
		issues = append(issues, FieldIssue{Path: "channel", Issue: "required"})
	case channelSMS:
		// ok
	default:
		issues = append(issues, FieldIssue{Path: "channel", Issue: `channel must be "sms" in phase 2`})
	}

	switch {
	case req.Recipient == "":
		issues = append(issues, FieldIssue{Path: "recipient", Issue: "required"})
	case !e164Re.MatchString(req.Recipient):
		issues = append(issues, FieldIssue{Path: "recipient", Issue: "must match E.164 format (^\\+[1-9]\\d{1,14}$)"})
	}

	if req.Content == "" {
		issues = append(issues, FieldIssue{Path: "content", Issue: "required"})
	} else if len([]rune(req.Content)) > contentSMSMax {
		issues = append(issues, FieldIssue{Path: "content", Issue: "exceeds maximum length 1600"})
	}

	// Phase 6 ships templates; Phase 2 rejects either field.
	if req.Template != "" {
		issues = append(issues, FieldIssue{Path: "template", Issue: "templates are not supported in phase 2"})
	}
	if len(req.TemplateData) > 0 {
		issues = append(issues, FieldIssue{Path: "template_data", Issue: "templates are not supported in phase 2"})
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
