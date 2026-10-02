package service

import (
	"context"
	"testing"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"

	"github.com/runityru/anycastd/announcer"
	"github.com/runityru/anycastd/checkers"
)

func init() {
	log.SetLevel(log.TraceLevel)
}

func (s *serviceTestSuite) TestRunPass() {
	s.announcerM.On("Announce").Return(nil).Once()

	s.checkM.On("Check").Return(nil).Once()

	s.metricsM.On("ServiceUp", "test_service").Return().Once()
	s.metricsM.On("MeasureCall", "test_service", "test_check").Return().Once()

	strategy, _ := GetStrategyNoOptions("")

	svc := New("test_service", s.announcerM, []Checker{{
		Name:  "test_check",
		Check: s.checkM,
	}}, 1*time.Second, s.metricsM, strategy).(*service)

	err := svc.run(s.ctx)
	s.Require().NoError(err)
}

func (s *serviceTestSuite) TestRunFail() {
	s.checkM.On("Kind").Return("test_check").Once()
	s.checkM.On("Check").Return(errors.New("error")).Once()

	s.metricsM.On("ServiceDown", "test_service").Return().Once()
	s.metricsM.On("MeasureCall", "test_service", "test_check").Return().Once()

	strategy, _ := GetStrategyNoOptions("")
	svc := New("test_service", s.announcerM, []Checker{{
		Name:  "test_check",
		Check: s.checkM,
	}}, 1*time.Second, s.metricsM, strategy).(*service)

	err := svc.run(s.ctx)
	s.Require().NoError(err)
}

func (s *serviceTestSuite) TestRunPassThenFailThenPass() {
	aCall1 := s.announcerM.On("Announce").Return(nil).Once()
	aCall2 := s.announcerM.On("Denounce").Return(nil).NotBefore(aCall1).Once()
	s.announcerM.On("Announce").Return(nil).NotBefore(aCall2).Once()

	s.checkM.On("Kind").Return("test_check").Times(1)
	cCall1 := s.checkM.On("Check").Return(nil).Once()
	cCall2 := s.checkM.On("Check").Return(errors.New("error")).NotBefore(cCall1).Once()
	s.checkM.On("Check").Return(nil).NotBefore(cCall2).Once()

	mCall1 := s.metricsM.On("MeasureCall", "test_service", "test_check").Return().Once()
	mCall2 := s.metricsM.On("ServiceUp", "test_service").Return().NotBefore(mCall1).Once()
	mCall3 := s.metricsM.On("MeasureCall", "test_service", "test_check").Return().NotBefore(mCall2).Once()
	mCall4 := s.metricsM.On("ServiceDown", "test_service").Return().NotBefore(mCall3).Once()
	mCall5 := s.metricsM.On("MeasureCall", "test_service", "test_check").Return().NotBefore(mCall4).Once()
	s.metricsM.On("ServiceUp", "test_service").Return().NotBefore(mCall5).Once()

	strategy, _ := GetStrategyNoOptions("")
	svc := New("test_service", s.announcerM, []Checker{{
		Name:  "test_check",
		Check: s.checkM,
	}}, 1*time.Second, s.metricsM, strategy).(*service)

	for i := 0; i < 3; i++ {
		err := svc.run(s.ctx)
		s.Require().NoError(err)
	}
}

// TestRunSiblingInstancesIndependent wires two checks of the same kind
// with distinct names (mimicking e.g. two icmp_ping instances with
// different parameters). When only one instance fails, its failure,
// MeasureCall label and result must not affect the sibling; with the "all"
// strategy the service stays up and the healthy instance's result is
// evaluated independently.
func (s *serviceTestSuite) TestRunSiblingInstancesIndependent() {
	checkAM := checkers.NewMock()
	checkBM := checkers.NewMock()

	s.announcerM.On("Announce").Return(nil).Once()

	checkAM.On("Kind").Return("icmp_ping").Once()
	checkAM.On("Check").Return(errors.New("first instance failed")).Once()
	checkBM.On("Check").Return(nil).Once()

	s.metricsM.On("MeasureCall", "test_service", "ping-a").Return().Once()
	s.metricsM.On("MeasureCall", "test_service", "ping-b").Return().Once()
	s.metricsM.On("ServiceUp", "test_service").Return().Once()

	strategy, err := GetStrategyNoOptions("all")
	s.Require().NoError(err)

	svc := New("test_service", s.announcerM, []Checker{
		{Name: "ping-a", Check: checkAM, Group: "g1"},
		{Name: "ping-b", Check: checkBM, Group: "g1"},
	}, 1*time.Second, s.metricsM, strategy).(*service)

	s.Require().NoError(svc.run(s.ctx))

	checkAM.AssertExpectations(s.T())
	checkBM.AssertExpectations(s.T())
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
