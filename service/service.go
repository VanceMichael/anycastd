package service

import (
	"context"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/runityru/anycastd/announcer"
	"github.com/runityru/anycastd/checkers"
)

type Service interface {
	Run(ctx context.Context) error
}

type Checker struct {
	Check checkers.Checker
	Group string
}

type service struct {
	name      string
	announcer announcer.Announcer
	checks    []Checker
	interval  time.Duration
	metrics   Metrics
	strategy  Strategy

	debouncer *debouncer
}

func New(
	name string,
	a announcer.Announcer,
	checks []Checker,
	interval time.Duration,
	metrics Metrics,
	strategy Strategy,
	failureThreshold uint32,
	recoveryThreshold uint32,
) Service {
	return &service{
		name:      name,
		announcer: a,
		checks:    checks,
		interval:  interval,
		metrics:   metrics,
		strategy:  strategy,
		debouncer: newDebouncer(failureThreshold, recoveryThreshold),
	}
}

func (s *service) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			// Do not perform any tail announce/denounce on shutdown:
			// a pending candidate must not trigger RIB changes after
			// the service has been canceled.
			return nil
		case <-time.After(s.interval):
			if err := s.run(ctx); err != nil {
				return errors.Wrap(err, "service error")
			}
		}
	}
}

func (s *service) run(ctx context.Context) error {
	checkResults := []CheckResult{}
	for _, check := range s.checks {
		result := true
		if err := s.metrics.MeasureCall(ctx, s.name, check.Check.Kind(), check.Check.Check); err != nil {
			log.Warnf("check failed: %s", err)
			result = false
		}
		checkResults = append(checkResults, CheckResult{check, result})
	}

	serviceDown, err := s.strategy(checkResults)
	if err != nil {
		return err
	}

	// The raw result updates the candidate counters. The stable state
	// (and the announced flag mirroring the RIB) is committed only once
	// the requested action actually succeeds; otherwise the candidate is
	// retained and the correct action is retried on the next cycle.
	switch s.debouncer.observe(serviceDown) {
	case actionDenounce:
		if err := s.announcer.Denounce(ctx); err != nil {
			log.Warnf("denounce failed: %s", err)
		} else {
			s.debouncer.commit(serviceDown)
		}
	case actionAnnounce:
		if err := s.announcer.Announce(ctx); err != nil {
			log.Warnf("announce failed: %s", err)
		} else {
			s.debouncer.commit(serviceDown)
		}
	}

	s.metrics.ObserveState(s.name, s.debouncer.snapshot(!serviceDown))

	return nil
}
