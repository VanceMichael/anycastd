package service

import (
	"context"

	"github.com/stretchr/testify/mock"
)

var _ Metrics = (*MetricsMock)(nil)

type MetricsMock struct {
	mock.Mock
}

func NewMetricsMock() *MetricsMock {
	return &MetricsMock{}
}

func (m *MetricsMock) ObserveState(service string, state StateReport) {
	m.Called(service, state)
}

func (m *MetricsMock) MeasureCall(ctx context.Context, service, check string, fn func(ctx context.Context) error) error {
	m.Called(service, check)
	return fn(ctx)
}
