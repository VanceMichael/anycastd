package service

import (
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/runityru/anycastd/announcer"
	"github.com/runityru/anycastd/checkers"
)

func init() {
	log.SetLevel(log.TraceLevel)
}

func (s *serviceTestSuite) TestRunPass() {
	s.announcerM.On("Announce").Return(nil).Once()

	s.checkM.On("Kind").Return("test_check").Once()
	s.checkM.On("Check").Return(nil).Once()

	s.metricsM.On("ObserveState", "test_service",
		report(true, true, true, CandidateNone, 0),
	).Return().Once()
	s.metricsM.On("MeasureCall", "test_service", "test_check").Return().Once()

	strategy, _ := GetStrategyNoOptions("")

	svc := New("test_service", s.announcerM, []Checker{{s.checkM, ""}}, 1*time.Second, s.metricsM, strategy, 1, 1).(*service)

	err := svc.run(s.ctx)
	s.Require().NoError(err)
}

func (s *serviceTestSuite) TestRunFail() {
	s.checkM.On("Kind").Return("test_check").Once()
	s.checkM.On("Check").Return(errors.New("error")).Once()

	s.metricsM.On("ObserveState", "test_service",
		report(false, false, false, CandidateNone, 0),
	).Return().Once()
	s.metricsM.On("MeasureCall", "test_service", "test_check").Return().Once()

	strategy, _ := GetStrategyNoOptions("")
	svc := New("test_service", s.announcerM, []Checker{{s.checkM, ""}}, 1*time.Second, s.metricsM, strategy, 1, 1).(*service)

	err := svc.run(s.ctx)
	s.Require().NoError(err)
}

func (s *serviceTestSuite) TestRunPassThenFailThenPass() {
	aCall1 := s.announcerM.On("Announce").Return(nil).Once()
	aCall2 := s.announcerM.On("Denounce").Return(nil).NotBefore(aCall1).Once()
	s.announcerM.On("Announce").Return(nil).NotBefore(aCall2).Once()

	s.checkM.On("Kind").Return("test_check").Times(3)
	cCall1 := s.checkM.On("Check").Return(nil).Once()
	cCall2 := s.checkM.On("Check").Return(errors.New("error")).NotBefore(cCall1).Once()
	s.checkM.On("Check").Return(nil).NotBefore(cCall2).Once()

	s.metricsM.On("MeasureCall", "test_service", "test_check").Return().Times(3)
	s.metricsM.On("ObserveState", "test_service",
		report(true, true, true, CandidateNone, 0),
	).Return().Times(2)
	s.metricsM.On("ObserveState", "test_service",
		report(false, false, false, CandidateNone, 0),
	).Return().Once()

	strategy, _ := GetStrategyNoOptions("")
	svc := New("test_service", s.announcerM, []Checker{{s.checkM, ""}}, 1*time.Second, s.metricsM, strategy, 1, 1).(*service)

	for i := 0; i < 3; i++ {
		err := svc.run(s.ctx)
		s.Require().NoError(err)
	}
}

// With failure/recovery thresholds of 3 a short flap must not withdraw
// the prefix: only the third consecutive failure denounces, and a
// healthy blip resets the accumulated failure streak.
func (s *serviceTestSuite) TestRunFailureThresholdDelaysDenounce() {
	s.announcerM.On("Announce").Return(nil).Once()
	s.announcerM.On("Denounce").Return(nil).Once()

	s.checkM.On("Kind").Return("test_check").Times(9)

	// cycles: up up up(announce) down down up(blip) down down down(denounce)
	calls := []*mock.Call{
		s.checkM.On("Check").Return(nil).Once(),
	}
	for _, result := range []error{nil, nil, errors.New("error"), errors.New("error"), nil, errors.New("error"), errors.New("error"), errors.New("error")} {
		calls = append(calls, s.checkM.On("Check").Return(result).NotBefore(calls[len(calls)-1]).Once())
	}

	s.metricsM.On("MeasureCall", "test_service", "test_check").Return().Times(9)
	s.metricsM.On("ObserveState", "test_service",
		report(true, false, false, CandidateUp, 1),
	).Return().Once()
	s.metricsM.On("ObserveState", "test_service",
		report(true, false, false, CandidateUp, 2),
	).Return().Once()
	s.metricsM.On("ObserveState", "test_service",
		report(true, true, true, CandidateNone, 0),
	).Return().Times(2)
	s.metricsM.On("ObserveState", "test_service",
		report(false, true, true, CandidateDown, 1),
	).Return().Times(2)
	s.metricsM.On("ObserveState", "test_service",
		report(false, true, true, CandidateDown, 2),
	).Return().Times(2)
	s.metricsM.On("ObserveState", "test_service",
		report(false, false, false, CandidateNone, 0),
	).Return().Once()

	strategy, _ := GetStrategyNoOptions("")
	svc := New("test_service", s.announcerM, []Checker{{s.checkM, ""}}, 1*time.Second, s.metricsM, strategy, 3, 3).(*service)

	for i := 0; i < 9; i++ {
		err := svc.run(s.ctx)
		s.Require().NoError(err)
	}
}

// With a recovery threshold of 2 an isolated success must not announce
// a persistently failing service; only the second consecutive success
// does.
func (s *serviceTestSuite) TestRunRecoveryThresholdDelaysAnnounce() {
	s.announcerM.On("Announce").Return(nil).Once()

	s.checkM.On("Kind").Return("test_check").Times(5)

	// cycles: down up down up up(announce)
	calls := []*mock.Call{
		s.checkM.On("Check").Return(errors.New("error")).Once(),
	}
	for _, result := range []error{nil, errors.New("error"), nil, nil} {
		calls = append(calls, s.checkM.On("Check").Return(result).NotBefore(calls[len(calls)-1]).Once())
	}

	s.metricsM.On("MeasureCall", "test_service", "test_check").Return().Times(5)
	s.metricsM.On("ObserveState", "test_service",
		report(false, false, false, CandidateNone, 0),
	).Return().Times(2)
	s.metricsM.On("ObserveState", "test_service",
		report(true, false, false, CandidateUp, 1),
	).Return().Times(2)
	s.metricsM.On("ObserveState", "test_service",
		report(true, true, true, CandidateNone, 0),
	).Return().Once()

	strategy, _ := GetStrategyNoOptions("")
	svc := New("test_service", s.announcerM, []Checker{{s.checkM, ""}}, 1*time.Second, s.metricsM, strategy, 1, 2).(*service)

	for i := 0; i < 5; i++ {
		err := svc.run(s.ctx)
		s.Require().NoError(err)
	}
}

// A failed announce must not be committed: the next healthy cycles
// retry it, a failure drops the pending recovery, and a later success
// announces exactly once.
func (s *serviceTestSuite) TestRunAnnounceFailureIsRetried() {
	aCall1 := s.announcerM.On("Announce").Return(errors.New("rib error")).Once()
	aCall2 := s.announcerM.On("Announce").Return(errors.New("rib error")).NotBefore(aCall1).Once()
	s.announcerM.On("Announce").Return(nil).NotBefore(aCall2).Once()

	s.checkM.On("Kind").Return("test_check").Times(4)

	// cycles: up(fail) up(fail) down(reset) up(announce)
	calls := []*mock.Call{
		s.checkM.On("Check").Return(nil).Once(),
	}
	for _, result := range []error{nil, errors.New("error"), nil} {
		calls = append(calls, s.checkM.On("Check").Return(result).NotBefore(calls[len(calls)-1]).Once())
	}

	s.metricsM.On("MeasureCall", "test_service", "test_check").Return().Times(4)
	s.metricsM.On("ObserveState", "test_service",
		report(true, false, false, CandidateUp, 1),
	).Return().Once()
	s.metricsM.On("ObserveState", "test_service",
		report(true, false, false, CandidateUp, 2),
	).Return().Once()
	s.metricsM.On("ObserveState", "test_service",
		report(false, false, false, CandidateNone, 0),
	).Return().Once()
	s.metricsM.On("ObserveState", "test_service",
		report(true, true, true, CandidateNone, 0),
	).Return().Once()

	strategy, _ := GetStrategyNoOptions("")
	svc := New("test_service", s.announcerM, []Checker{{s.checkM, ""}}, 1*time.Second, s.metricsM, strategy, 1, 1).(*service)

	for i := 0; i < 4; i++ {
		err := svc.run(s.ctx)
		s.Require().NoError(err)
	}
}

// A failed denounce keeps the service announced/stably up while the
// withdrawal candidate is retried; a healthy blip cancels it and the
// next failure retries the withdrawal successfully.
func (s *serviceTestSuite) TestRunDenounceFailureIsRetried() {
	aCall1 := s.announcerM.On("Announce").Return(nil).Once()
	dCall1 := s.announcerM.On("Denounce").Return(errors.New("rib error")).NotBefore(aCall1).Once()
	dCall2 := s.announcerM.On("Denounce").Return(errors.New("rib error")).NotBefore(dCall1).Once()
	s.announcerM.On("Denounce").Return(nil).NotBefore(dCall2).Once()

	s.checkM.On("Kind").Return("test_check").Times(5)

	// cycles: up(announce) down(fail) down(fail) up(blip) down(denounce)
	calls := []*mock.Call{
		s.checkM.On("Check").Return(nil).Once(),
	}
	for _, result := range []error{errors.New("error"), errors.New("error"), nil, errors.New("error")} {
		calls = append(calls, s.checkM.On("Check").Return(result).NotBefore(calls[len(calls)-1]).Once())
	}

	s.metricsM.On("MeasureCall", "test_service", "test_check").Return().Times(5)
	s.metricsM.On("ObserveState", "test_service",
		report(true, true, true, CandidateNone, 0),
	).Return().Times(2)
	s.metricsM.On("ObserveState", "test_service",
		report(false, true, true, CandidateDown, 1),
	).Return().Once()
	s.metricsM.On("ObserveState", "test_service",
		report(false, true, true, CandidateDown, 2),
	).Return().Once()
	s.metricsM.On("ObserveState", "test_service",
		report(false, false, false, CandidateNone, 0),
	).Return().Once()

	strategy, _ := GetStrategyNoOptions("")
	svc := New("test_service", s.announcerM, []Checker{{s.checkM, ""}}, 1*time.Second, s.metricsM, strategy, 1, 1).(*service)

	for i := 0; i < 5; i++ {
		err := svc.run(s.ctx)
		s.Require().NoError(err)
	}
}

// A canceled service must not perform any tail announce or denounce
// because of a residual candidate streak.
func (s *serviceTestSuite) TestRunCanceledDoesNotAnnounce() {
	ctx, cancel := context.WithCancel(s.ctx)
	cancel()

	strategy, _ := GetStrategyNoOptions("")
	// Long interval guarantees the cancellation branch is the only
	// ready one on the first select, so no check cycle can sneak in.
	svc := New("test_service", s.announcerM, []Checker{{s.checkM, ""}}, 1*time.Hour, s.metricsM, strategy, 3, 3).(*service)

	err := svc.Run(ctx)
	s.Require().NoError(err)
}

// Definitions ...
type serviceTestSuite struct {
	suite.Suite

	ctx        context.Context
	announcerM *announcer.Mock
	checkM     *checkers.Mock
	metricsM   *MetricsMock
}

func (s *serviceTestSuite) SetupTest() {
	s.ctx = context.Background()

	s.announcerM = announcer.NewMock()
	s.checkM = checkers.NewMock()
	s.metricsM = NewMetricsMock()
}

func (s *serviceTestSuite) TearDownTest() {
	s.announcerM.AssertExpectations(s.T())
	s.checkM.AssertExpectations(s.T())
	s.metricsM.AssertExpectations(s.T())
}

func TestServiceTestSuite(t *testing.T) {
	suite.Run(t, &serviceTestSuite{})
}
