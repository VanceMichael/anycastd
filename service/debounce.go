package service

// action is the announcement action to attempt after a check cycle.
type action int

const (
	actionNone action = iota
	actionAnnounce
	actionDenounce
)

// debouncer implements per-service flap damping. It keeps the four
// state layers apart:
//   - the raw per-cycle result is fed into observe;
//   - direction/streak track the candidate transition awaiting
//     confirmation by consecutive cycles;
//   - stableUp is the committed stable state, flipped only after the
//     corresponding Announce/Denounce succeeds;
//   - announced mirrors what the BGP RIB actually contains.
//
// A fresh debouncer starts from a determined initial state: nothing is
// announced, the service is treated as stable down and all counters are
// zero, so every (re)start recounts recovery from scratch.
type debouncer struct {
	stableUp  bool
	announced bool

	direction CandidateDirection
	streak    uint32

	failureThreshold  uint32
	recoveryThreshold uint32
}

func newDebouncer(failureThreshold, recoveryThreshold uint32) *debouncer {
	return &debouncer{
		failureThreshold:  failureThreshold,
		recoveryThreshold: recoveryThreshold,
	}
}

// observe consumes the raw strategy result (serviceDown == true means
// the check strategy considers the service down this cycle), updates the
// candidate transition counters and returns the action that should be
// attempted this cycle, if any.
//
// A result agreeing with the stable state clears any pending candidate
// and its streak. An opposing result grows the streak in the candidate
// direction; the action is returned only once the streak reaches the
// threshold configured for that transition. When a previous action has
// failed, the candidate is retained on purpose, so every further
// opposing cycle returns the same action and retries it instead of
// committing a state the RIB does not reflect.
func (d *debouncer) observe(serviceDown bool) action {
	desiredUp := !serviceDown
	if desiredUp == d.stableUp {
		d.direction = CandidateNone
		d.streak = 0
		return actionNone
	}

	direction := CandidateUp
	threshold := d.recoveryThreshold
	if serviceDown {
		direction = CandidateDown
		threshold = d.failureThreshold
	}

	if d.direction != direction {
		d.direction = direction
		d.streak = 0
	}
	d.streak++

	if d.streak < threshold {
		return actionNone
	}

	if desiredUp {
		return actionAnnounce
	}
	return actionDenounce
}

// commit records a successfully executed action as the new stable
// state. It must not be called when the action failed: in that case the
// candidate must be retained for a retry on the following cycle.
func (d *debouncer) commit(serviceDown bool) {
	d.stableUp = !serviceDown
	d.announced = d.stableUp
	d.direction = CandidateNone
	d.streak = 0
}

// snapshot returns the observable state layers after a cycle.
func (d *debouncer) snapshot(rawUp bool) StateReport {
	return StateReport{
		RawUp:     rawUp,
		StableUp:  d.stableUp,
		Announced: d.announced,
		Candidate: d.direction,
		Streak:    d.streak,
	}
}
