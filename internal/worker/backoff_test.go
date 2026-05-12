package worker

import (
	"math"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestTransientBackoff_InRange asserts the equal-jitter contract: for
// every attempt, the result lies in [deterministic/2, deterministic].
// 100 iterations per attempt build confidence the lower edge is
// reachable; the bound itself proves the upper edge holds.
func TestTransientBackoff_InRange(t *testing.T) {
	for _, attempt := range []int{1, 2, 5, 7} {
		det := time.Duration(math.Pow(2, float64(attempt))) * time.Second
		floor := det / 2

		for i := 0; i < 100; i++ {
			got := TransientBackoff(attempt)
			assert.GreaterOrEqualf(t, got, floor,
				"attempt=%d iteration=%d: backoff %s below floor %s", attempt, i, got, floor)
			assert.LessOrEqualf(t, got, det,
				"attempt=%d iteration=%d: backoff %s above ceiling %s", attempt, i, got, det)
		}
	}
}

// TestTransientBackoff_ClampsBelowOne is the defensive clamp — a stray
// attempt=0 (shouldn't happen because the dispatcher always increments
// to >= 1 before the worker sees a row) shouldn't divide by zero or
// produce a sub-second backoff. attempt < 1 is treated as attempt=1,
// so the result range collapses to [1 s, 2 s].
func TestTransientBackoff_ClampsBelowOne(t *testing.T) {
	for _, attempt := range []int{0, -3} {
		for i := 0; i < 50; i++ {
			got := TransientBackoff(attempt)
			assert.GreaterOrEqualf(t, got, 1*time.Second,
				"attempt=%d iteration=%d: backoff %s below floor 1 s", attempt, i, got)
			assert.LessOrEqualf(t, got, 2*time.Second,
				"attempt=%d iteration=%d: backoff %s above ceiling 2 s", attempt, i, got)
		}
	}
}

// TestReaperBackoff_InRangeBelowCap asserts ReaperBackoff matches
// TransientBackoff's equal-jitter shape for attempts within
// reaper_backoff_cap = 8. Above the cap, the test below
// (TestReaperBackoff_CapsExponent) takes over.
func TestReaperBackoff_InRangeBelowCap(t *testing.T) {
	for _, attempt := range []int{1, 2, 5, 7, 8} {
		det := time.Duration(math.Pow(2, float64(attempt))) * time.Second
		floor := det / 2

		for i := 0; i < 100; i++ {
			got := ReaperBackoff(attempt)
			assert.GreaterOrEqualf(t, got, floor,
				"attempt=%d iteration=%d: backoff %s below floor %s", attempt, i, got, floor)
			assert.LessOrEqualf(t, got, det,
				"attempt=%d iteration=%d: backoff %s above ceiling %s", attempt, i, got, det)
		}
	}
}

// TestReaperBackoff_CapsExponent proves the reaper_backoff_cap = 8
// invariant: any attempt > 8 collapses to the same range as attempt = 8.
// Without the cap, attempt=20 would compute 2^20 seconds (~12 days);
// the cap pegs the worst case at 256 s deterministic, [128 s, 256 s]
// jittered.
func TestReaperBackoff_CapsExponent(t *testing.T) {
	capped := time.Duration(math.Pow(2, float64(reaperBackoffCap))) * time.Second
	floor := capped / 2

	for _, attempt := range []int{9, 12, 100} {
		for i := 0; i < 50; i++ {
			got := ReaperBackoff(attempt)
			assert.GreaterOrEqualf(t, got, floor,
				"attempt=%d iteration=%d: backoff %s below floor %s", attempt, i, got, floor)
			assert.LessOrEqualf(t, got, capped,
				"attempt=%d iteration=%d: backoff %s above ceiling %s — cap not respected",
				attempt, i, got, capped)
		}
	}
}

// TestSetRand_DeterministicSequence proves the test seam restores
// determinism: with the same seed, repeated TransientBackoff calls
// produce the same sequence of jitter draws. Locks the runtime
// invariant that production tests can pin a known seed for exact
// eligible_at assertions.
func TestSetRand_DeterministicSequence(t *testing.T) {
	runOnce := func() []time.Duration {
		prev := SetRand(rand.New(rand.NewPCG(42, 0)))
		defer SetRand(prev)
		out := make([]time.Duration, 5)
		for i := range out {
			out[i] = TransientBackoff(3)
		}
		return out
	}

	first := runOnce()
	second := runOnce()
	assert.Equal(t, first, second, "same seed → identical sequence")
}

// TestSetRand_RestoresPrevious proves SetRand's return value is the
// PRNG that was active at call time. The contract matters because tests
// must restore the production PRNG via t.Cleanup so a degenerate test
// PRNG does not leak into other tests — Go runs test functions
// sequentially within a package by default, and a leaked PRNG with a
// pinned seed would silently make every later test deterministic.
func TestSetRand_RestoresPrevious(t *testing.T) {
	pinned := rand.New(rand.NewPCG(7, 0))
	prev := SetRand(pinned)
	t.Cleanup(func() { SetRand(prev) })

	round := SetRand(prev)
	assert.Same(t, pinned, round, "SetRand returns the PRNG it just installed")

	got := TransientBackoff(3)
	assert.GreaterOrEqual(t, got, 4*time.Second,
		"after restoring the production PRNG, draws must stay within [det/2, det]")
	assert.LessOrEqual(t, got, 8*time.Second)
}
