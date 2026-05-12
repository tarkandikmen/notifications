package testsupport

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestBootstrapWithRetry_SucceedsOnFirstAttempt locks the happy
// path: a callback that returns nil immediately must not be invoked
// again.
func TestBootstrapWithRetry_SucceedsOnFirstAttempt(t *testing.T) {
	calls := 0
	BootstrapWithRetry(t, func() error {
		calls++
		return nil
	})
	assert.Equal(t, 1, calls, "no retries on success")
}

// TestBootstrapWithRetry_RetriesUntilSuccess is the central
// motivation for the helper: a transient transport-shaped error on
// the first attempt followed by success on the next must produce a
// passing test, not a flaky fail-then-pass artifact. Also verifies
// the documented inter-attempt backoff actually runs (regression
// guard against accidentally tightening the loop into a hot retry).
func TestBootstrapWithRetry_RetriesUntilSuccess(t *testing.T) {
	calls := 0
	start := time.Now()
	BootstrapWithRetry(t, func() error {
		calls++
		if calls < 2 {
			return errors.New("connection reset by peer (synthetic)")
		}
		return nil
	})
	assert.Equal(t, 2, calls, "must retry exactly until success, not further")
	assert.GreaterOrEqual(t, time.Since(start), kafkaBootstrapRetryDelay,
		"retry path must back off the documented amount between attempts")
}

// The exhaustion path (callback failing every attempt) is not
// unit-tested here because it would require changing
// BootstrapWithRetry's signature from *testing.T to
// require.TestingT — the test would need to inject a recording
// fixture rather than the real *testing.T (whose FailNow tears down
// the parent test). The helper's loop is 15 LOC of straight-line code
// over kafkaBootstrapMaxAttempts; the success-path tests above plus
// the integration tier exercise it sufficiently.
