package service

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/runityru/anycastd/announcer"
	"github.com/runityru/anycastd/checkers"
)

type Service interface {
	Run(ctx context.Context) error
}

// Checker binds a runtime checker to its stable per-service identity
// (Name) and optional strategy group. Name is the explicit check name, or
// the kind itself for legacy unnamed single-instance checks; it is used as
// the check label of service-level metrics and in log fields.
type Checker struct {
	Name  string
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

	announced *atomic.Bool
}

func New(
	name string,
	a announcer.Announcer,
	checks []Checker,
	interval time.Duration,
	metrics Metrics,
	strategy Strategy,
) Service {
	return &service{
		name:      name,
		announcer: a,
		checks:    checks,
		interval:  interval,
		metrics:   metrics,
		strategy:  strategy,
		announced: &atomic.Bool{},
	}
}

func (s *service) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
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
		if err := s.metrics.MeasureCall(ctx, s.name, check.Name, check.Check.Check); err != nil {
			log.WithFields(log.Fields{
				"service": s.name,
				"check":   check.Name,
				"kind":    check.Check.Kind(),
			}).Warnf("check failed: %s", err)
			result = false
		}
		checkResults = append(checkResults, CheckResult{checker: check, result: result})
	}

	serviceDown, err := s.strategy(checkResults)
	if err != nil {
		return err
	}

	if serviceDown {
		s.metrics.ServiceDown(s.name)
	} else {
		s.metrics.ServiceUp(s.name)
	}

	if serviceDown {
		if s.announced.Load() {
			if err := s.announcer.Denounce(ctx); err != nil {
				log.Warnf("denounce failed: %s", err)
			}
			s.announced.Store(false)
		}
	} else {
		if !s.announced.Load() {
			if err := s.announcer.Announce(ctx); err != nil {
				log.Warnf("announce failed: %s", err)
			}
		}

		s.announced.Store(true)
	}

	return nil
}
