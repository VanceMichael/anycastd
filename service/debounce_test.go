package service

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func report(rawUp, stableUp, announced bool, candidate CandidateDirection, streak uint32) StateReport {
	return StateReport{
		RawUp:     rawUp,
		StableUp:  stableUp,
		Announced: announced,
		Candidate: candidate,
		Streak:    streak,
	}
}

// A fresh debouncer starts from the determined initial state: stable
// down, nothing announced, no candidate, zero counters.
func TestDebouncerInitialState(t *testing.T) {
	r := require.New(t)

	d := newDebouncer(3, 2)
	r.Equal(report(false, false, false, CandidateNone, 0), d.snapshot(false))
	r.Equal(report(true, false, false, CandidateNone, 0), d.snapshot(true))
}

// With thresholds of 1 every opposing cycle fires immediately, matching
// the historical single-cycle behavior.
func TestDebouncerThresholdOneParity(t *testing.T) {
	r := require.New(t)
	d := newDebouncer(1, 1)

	r.Equal(actionAnnounce, d.observe(false))
	d.commit(false)
	r.Equal(report(true, true, true, CandidateNone, 0), d.snapshot(true))

	r.Equal(actionDenounce, d.observe(true))
	d.commit(true)
	r.Equal(report(false, false, false, CandidateNone, 0), d.snapshot(false))

	// Results agreeing with the stable state never produce an action.
	r.Equal(actionNone, d.observe(true))
}

// Consecutive failures must reach the failure threshold before a
// denounce; an opposite result resets the failure streak.
func TestDebouncerFailureThresholdWithBlipReset(t *testing.T) {
	r := require.New(t)
	d := newDebouncer(3, 3)

	// recover first so the service is stably up
	r.Equal(actionNone, d.observe(false))
	r.Equal(report(true, false, false, CandidateUp, 1), d.snapshot(true))
	r.Equal(actionNone, d.observe(false))
	r.Equal(report(true, false, false, CandidateUp, 2), d.snapshot(true))
	r.Equal(actionAnnounce, d.observe(false))
	d.commit(false)

	// two failures are not enough ...
	r.Equal(actionNone, d.observe(true))
	r.Equal(report(false, true, true, CandidateDown, 1), d.snapshot(false))
	r.Equal(actionNone, d.observe(true))
	r.Equal(report(false, true, true, CandidateDown, 2), d.snapshot(false))

	// ... a healthy cycle resets the failure streak ...
	r.Equal(actionNone, d.observe(false))
	r.Equal(report(true, true, true, CandidateNone, 0), d.snapshot(true))

	// ... so counting starts over and only the third failure fires
	r.Equal(actionNone, d.observe(true))
	r.Equal(actionNone, d.observe(true))
	r.Equal(actionDenounce, d.observe(true))
	d.commit(true)
	r.Equal(report(false, false, false, CandidateNone, 0), d.snapshot(false))
}

// Consecutive successes must reach the recovery threshold before an
// announce; a failing cycle resets the recovery streak.
func TestDebouncerRecoveryThresholdWithReset(t *testing.T) {
	r := require.New(t)
	d := newDebouncer(1, 2)

	// initially down, failing cycles agree with the stable state
	r.Equal(actionNone, d.observe(true))
	r.Equal(report(false, false, false, CandidateNone, 0), d.snapshot(false))

	// one success is not enough ...
	r.Equal(actionNone, d.observe(false))
	r.Equal(report(true, false, false, CandidateUp, 1), d.snapshot(true))

	// ... a failure resets the recovery streak ...
	r.Equal(actionNone, d.observe(true))
	r.Equal(report(false, false, false, CandidateNone, 0), d.snapshot(false))

	// ... only two consecutive successes fire the announce
	r.Equal(actionNone, d.observe(false))
	r.Equal(actionAnnounce, d.observe(false))
	d.commit(false)
	r.Equal(report(true, true, true, CandidateNone, 0), d.snapshot(true))
}

// A failed announce must not be committed: the candidate is retained,
// the action is retried on further healthy cycles, and the stable and
// announced layers keep reporting the old (down) state.
func TestDebouncerAnnounceFailureRetriesThenCommits(t *testing.T) {
	r := require.New(t)
	d := newDebouncer(1, 1)

	r.Equal(actionAnnounce, d.observe(false))
	// no commit: action failed, state stays down, candidate retained
	r.Equal(report(true, false, false, CandidateUp, 1), d.snapshot(true))

	r.Equal(actionAnnounce, d.observe(false))
	// still failing, streak keeps growing and the action is retried
	r.Equal(report(true, false, false, CandidateUp, 2), d.snapshot(true))

	// an opposite cycle abandons the pending recovery
	r.Equal(actionNone, d.observe(true))
	r.Equal(report(false, false, false, CandidateNone, 0), d.snapshot(false))

	// next healthy cycle starts a fresh attempt and finally commits
	r.Equal(actionAnnounce, d.observe(false))
	d.commit(false)
	r.Equal(report(true, true, true, CandidateNone, 0), d.snapshot(true))
}

// A failed denounce must not be committed either: the service stays
// stably up/announced while the down candidate is retried.
func TestDebouncerDenounceFailureRetriesThenCommits(t *testing.T) {
	r := require.New(t)
	d := newDebouncer(1, 1)

	r.Equal(actionAnnounce, d.observe(false))
	d.commit(false)

	r.Equal(actionDenounce, d.observe(true))
	// denounce failed: stable/announced stay up, candidate retained
	r.Equal(report(false, true, true, CandidateDown, 1), d.snapshot(false))
	r.Equal(actionDenounce, d.observe(true))
	r.Equal(report(false, true, true, CandidateDown, 2), d.snapshot(false))

	// a healthy cycle cancels the pending withdrawal, no divergence left
	r.Equal(actionNone, d.observe(false))
	r.Equal(report(true, true, true, CandidateNone, 0), d.snapshot(true))

	// next failure retries the denounce and commits on success
	r.Equal(actionDenounce, d.observe(true))
	d.commit(true)
	r.Equal(report(false, false, false, CandidateNone, 0), d.snapshot(false))
}

// Counters of independent debouncers must not influence each other.
func TestDebouncerInstancesIndependent(t *testing.T) {
	r := require.New(t)
	d1 := newDebouncer(2, 2)
	d2 := newDebouncer(2, 2)

	// d1 starts down; two consecutive ups are required to announce it
	r.Equal(actionNone, d1.observe(true))
	r.Equal(report(false, false, false, CandidateNone, 0), d1.snapshot(false))
	r.Equal(actionNone, d1.observe(false))
	r.Equal(actionAnnounce, d1.observe(false))
	d1.commit(false)

	// d1 then accumulates one failure while stable up (threshold 2) ...
	r.Equal(actionNone, d1.observe(true))
	r.Equal(report(false, true, true, CandidateDown, 1), d1.snapshot(false))

	// ... d2 remains untouched in its initial state no matter what d1 does
	r.Equal(report(false, false, false, CandidateNone, 0), d2.snapshot(false))

	r.Equal(actionNone, d2.observe(false))
	r.Equal(report(true, false, false, CandidateUp, 1), d2.snapshot(true))
	// d2 activity does not touch d1's pending failure streak
	r.Equal(report(false, true, true, CandidateDown, 1), d1.snapshot(false))
}
