package api

import (
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/tarkandikmen/notifications/internal/store"
)

// Validation rules — per-channel surface. The channel value selects
// the recipient regex and content-length cap that apply; the file
// covers SMS, email, and push.
//
// Rules are hand-written against stdlib regex; the file holds every
// rule and the package depends on no third-party schema library.

const (
	// channelSMS / channelEmail / channelPush are the three channel
	// values the api accepts.
	channelSMS   = "sms"
	channelEmail = "email"
	channelPush  = "push"

	// Per-channel content-length caps measured in runes.
	// SMS = 1600 (10 GSM-7 concatenated segments); email = 100000
	// (~100 KB plaintext body); push = 4000 (FCM ~4 KB / APNs 4–5 KB).
	contentSMSMax   = 1600
	contentEmailMax = 100000
	contentPushMax  = 4000

	// Per-channel recipient length bounds. Email cap is the
	// RFC 5321 §4.5.3.1.3 maximum. Push tokens are opaque per
	// provider; the bounds bracket Apple device tokens (64 hex chars),
	// FCM tokens (~152 chars typical), and VAPID web push (longer).
	recipientEmailMax = 254
	recipientPushMin  = 32
	recipientPushMax  = 4096

	priorityLow    = "low"
	priorityNormal = "normal"
	priorityHigh   = "high"

	// statusPending is the only status the api ever writes (T1).
	statusPending = "PENDING"

	// Pagination bounds for GET /v1/notifications: default limit 50,
	// maximum 200.
	listDefaultLimit = 50
	listMaxLimit     = 200

	// batchMax is the cap on POST /v1/notifications/batch.Notifications
	// length. Inlined here so the validator and handler can reference
	// one name; the OpenAPI spec mirrors the value.
	batchMax = 1000
)

// validStatuses is the set of values GET /v1/notifications accepts for
// the `status` query param.
var validStatuses = map[string]struct{}{
	"PENDING":    {},
	"DISPATCHED": {},
	"DELIVERED":  {},
	"FAILED":     {},
	"CANCELLED":  {},
}

// e164Re matches E.164 phone numbers: a leading +, a non-zero first
// digit, then 1–14 more digits (total 2–15 digits after the +).
var e164Re = regexp.MustCompile(`^\+[1-9]\d{1,14}$`)

// emailRe is the intentionally permissive email regex used for the
// email channel: <local>@<domain> with at least one `.` in <domain>.
// Full RFC 5322 is explicitly NOT enforced; the rule's job is to
// reject obvious typos and route per-channel formatting downstream,
// not to validate deliverability.
var emailRe = regexp.MustCompile(`^[^@\s]+@[^@\s]+\.[^@\s]+$`)

// uuidV4Re enforces the canonical lowercase UUIDv4 string: 36 chars,
// hyphens at positions 8/13/18/23, hex lowercase, position 14 = '4'
// (the version), position 19 in {8,9,a,b} (the RFC 4122 variant).
// Compact (32-hex) form and uppercase hex are rejected.
var uuidV4Re = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

// ValidateCreate runs every validation rule and returns one FieldIssue
// per failing rule. Rules do NOT short-circuit — the caller's response
// surfaces every issue at once so a single round trip is enough for
// the client to fix everything.
//
// `now` is the server-side clock used for the scheduled_at >= now() check.
// The handler injects it via Deps.Clock so tests can pin time without
// monkey-patching.
//
// The per-channel rules read the channel value to select the recipient
// regex and content cap that apply. The per-field rules are factored
// into validateCreateItem so the batch validator can rerun them with
// prefixed paths.
func ValidateCreate(req CreateRequest, now time.Time) []FieldIssue {
	return validateCreateItem(BatchItem(req), now)
}

// validateCreateItem runs the per-item rules shared by ValidateCreate
// (single create) and ValidateBatchCreate (batch create, with paths
// prefixed by notifications[i]. in the caller). Rules do NOT
// short-circuit so every issue surfaces in one round trip.
func validateCreateItem(item BatchItem, now time.Time) []FieldIssue {
	var issues []FieldIssue

	channelKnown := false
	switch item.Channel {
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
	if item.Recipient == "" {
		issues = append(issues, FieldIssue{Path: "recipient", Issue: "required"})
	} else if channelKnown {
		if issue := validateRecipient(item.Channel, item.Recipient); issue != nil {
			issues = append(issues, *issue)
		}
	}

	if item.Content == "" {
		issues = append(issues, FieldIssue{Path: "content", Issue: "required"})
	} else if channelKnown {
		if issue := validateContent(item.Channel, item.Content); issue != nil {
			issues = append(issues, *issue)
		}
	}

	if item.Template != "" {
		issues = append(issues, FieldIssue{Path: "template", Issue: "templates are not supported"})
	}
	if len(item.TemplateData) > 0 {
		issues = append(issues, FieldIssue{Path: "template_data", Issue: "templates are not supported"})
	}

	if item.Priority != "" {
		if _, ok := priorityToInt(item.Priority); !ok {
			issues = append(issues, FieldIssue{Path: "priority", Issue: `must be "low", "normal", or "high"`})
		}
	}

	if item.ScheduledAt != "" {
		t, err := time.Parse(time.RFC3339, item.ScheduledAt)
		if err != nil {
			issues = append(issues, FieldIssue{Path: "scheduled_at", Issue: "must be RFC 3339 with timezone"})
		} else if t.Before(now) {
			issues = append(issues, FieldIssue{Path: "scheduled_at", Issue: "must be in the future"})
		}
	}

	if item.IdempotencyKey == "" {
		issues = append(issues, FieldIssue{Path: "idempotency_key", Issue: "required"})
	} else if !isCanonicalUUIDv4Lower(item.IdempotencyKey) {
		issues = append(issues, FieldIssue{Path: "idempotency_key", Issue: "must be a canonical lowercase UUIDv4"})
	}

	return issues
}

// ValidateBatchCreate runs every per-item rule against every item in
// the batch, with paths rewritten to "notifications[i].<field>". It
// also enforces the batch-only rules:
//
//   - len(req.Notifications) >= 1 (empty batch is validation_failed,
//     not 201 — the contract requires at least one item).
//   - len(req.Notifications) <= batchMax (otherwise the api layer
//     returns 413 payload_too_large, NOT 400; the handler discriminates
//     by inspecting the issue path + text).
//   - All idempotency_key values pairwise distinct (intra-batch
//     duplicates are validation_failed).
//
// Returns one FieldIssue per failing rule. Rules do not short-circuit
// — every item's failures and the batch-level failures all surface in
// the same response so the client fixes everything in one round trip.
//
// The oversize case short-circuits: a >batchMax batch returns only the
// single "batch size <N> exceeded" issue so the handler can map it to
// 413 without surfacing per-item issues against a 50,000-item input
// (wasted work; the client must shrink before any other feedback is
// actionable).
func ValidateBatchCreate(req BatchCreateRequest, now time.Time) []FieldIssue {
	if len(req.Notifications) == 0 {
		return []FieldIssue{{Path: "notifications", Issue: "at least one item required"}}
	}
	if len(req.Notifications) > batchMax {
		return []FieldIssue{{
			Path:  "notifications",
			Issue: fmt.Sprintf("batch size %d exceeded", batchMax),
		}}
	}

	var issues []FieldIssue

	for i := range req.Notifications {
		for _, raw := range validateCreateItem(req.Notifications[i], now) {
			issues = append(issues, FieldIssue{
				Path:  fmt.Sprintf("notifications[%d].%s", i, raw.Path),
				Issue: raw.Issue,
			})
		}
	}

	seen := make(map[string]int, len(req.Notifications))
	for i, item := range req.Notifications {
		if item.IdempotencyKey == "" {
			continue
		}
		if first, ok := seen[item.IdempotencyKey]; ok {
			issues = append(issues, FieldIssue{
				Path:  fmt.Sprintf("notifications[%d].idempotency_key", i),
				Issue: fmt.Sprintf("duplicate of notifications[%d].idempotency_key", first),
			})
			continue
		}
		seen[item.IdempotencyKey] = i
	}

	return issues
}

// validateRecipient enforces the per-channel recipient rules. Returns
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

// validateContent enforces the per-channel content cap. Returns nil
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
// (0=low, 1=normal, 2=high). Returns ok=false for unknown strings;
// callers may treat that as a validation failure or a programmer error
// depending on context.
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

// ListRequest is the parsed view of GET /v1/notifications query
// parameters. Filters embed the store-layer ListFilters since the api
// layer's job here is purely translation: parse string → typed value,
// then hand off to the store query.
type ListRequest struct {
	Offset  int
	Limit   int
	Filters store.ListFilters
}

// parseListRequest reads query parameters off r and returns a populated
// ListRequest plus any FieldIssue list (empty on success). Defaults:
// offset=0, limit=listDefaultLimit. Bounds: offset >= 0,
// 1 <= limit <= listMaxLimit.
//
// Unknown query parameters are ignored, matching the unknown-body-field
// posture applied symmetrically to the query string.
//
// The function does not enforce `created_after <= created_before`; an
// empty range is a legitimate query that produces an empty list.
func parseListRequest(r *http.Request) (ListRequest, []FieldIssue) {
	out := ListRequest{
		Offset: 0,
		Limit:  listDefaultLimit,
	}
	var issues []FieldIssue
	q := r.URL.Query()

	if raw := q.Get("offset"); raw != "" {
		v, err := strconv.Atoi(raw)
		switch {
		case err != nil:
			issues = append(issues, FieldIssue{Path: "offset", Issue: "not an integer"})
		case v < 0:
			issues = append(issues, FieldIssue{Path: "offset", Issue: "must be >= 0"})
		default:
			out.Offset = v
		}
	}

	if raw := q.Get("limit"); raw != "" {
		v, err := strconv.Atoi(raw)
		switch {
		case err != nil:
			issues = append(issues, FieldIssue{Path: "limit", Issue: "not an integer"})
		case v < 1 || v > listMaxLimit:
			issues = append(issues, FieldIssue{
				Path:  "limit",
				Issue: fmt.Sprintf("must be between 1 and %d", listMaxLimit),
			})
		default:
			out.Limit = v
		}
	}

	if raw := q.Get("status"); raw != "" {
		if _, ok := validStatuses[raw]; !ok {
			issues = append(issues, FieldIssue{
				Path:  "status",
				Issue: `must be one of "PENDING", "DISPATCHED", "DELIVERED", "FAILED", "CANCELLED"`,
			})
		} else {
			s := raw
			out.Filters.Status = &s
		}
	}

	if raw := q.Get("channel"); raw != "" {
		switch raw {
		case channelSMS, channelEmail, channelPush:
			c := raw
			out.Filters.Channel = &c
		default:
			issues = append(issues, FieldIssue{Path: "channel", Issue: `must be "sms", "email", or "push"`})
		}
	}

	if raw := q.Get("priority"); raw != "" {
		if v, ok := priorityToInt(raw); ok {
			p := v
			out.Filters.Priority = &p
		} else {
			issues = append(issues, FieldIssue{Path: "priority", Issue: `must be "low", "normal", or "high"`})
		}
	}

	if raw := q.Get("batch_id"); raw != "" {
		id, err := uuid.Parse(raw)
		if err != nil {
			issues = append(issues, FieldIssue{Path: "batch_id", Issue: "must be a valid UUID"})
		} else {
			b := id
			out.Filters.BatchID = &b
		}
	}

	if raw := q.Get("created_after"); raw != "" {
		t, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			issues = append(issues, FieldIssue{Path: "created_after", Issue: "must be RFC 3339 with timezone"})
		} else {
			out.Filters.CreatedAfter = &t
		}
	}

	if raw := q.Get("created_before"); raw != "" {
		t, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			issues = append(issues, FieldIssue{Path: "created_before", Issue: "must be RFC 3339 with timezone"})
		} else {
			out.Filters.CreatedBefore = &t
		}
	}

	return out, issues
}
