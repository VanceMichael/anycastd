package service

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type Metrics interface {
	ServiceUp(service string)
	ServiceDown(service string)
	DeleteService(service string)
	Unregister()

	MeasureCall(ctx context.Context, service, check string, fn func(ctx context.Context) error) error
}

type metrics struct {
	appUpGauge           *prometheus.GaugeVec
	upGauge              *prometheus.GaugeVec
	checkDurationSeconds *prometheus.GaugeVec
}

func NewMetrics(appVersion string) (Metrics, error) {
	appUpGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "anycastd",
			Name:      "up",
			Help:      "Application liveness status (must always be 1)",
		},
		[]string{"version"},
	)

	upGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "anycastd",
			Name:      "service_up",
			Help:      "Service liveness status based on checks",
		},
		[]string{"service"},
	)

	checkDurationSeconds := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "anycastd",
			Name:      "check_duration_seconds",
			Help:      "Duration of check execution in seconds",
		},
		[]string{"service", "check"},
	)

	m := &metrics{
		appUpGauge:           appUpGauge,
		upGauge:              upGauge,
		checkDurationSeconds: checkDurationSeconds,
	}

	for _, c := range m.collectors() {
		if err := prometheus.Register(c); err != nil {
			return nil, err
		}
	}

	appUpGauge.WithLabelValues(appVersion).Set(1)

	return m, nil
}

func (m *metrics) collectors() []prometheus.Collector {
	return []prometheus.Collector{m.appUpGauge, m.upGauge, m.checkDurationSeconds}
}

func (m *metrics) Unregister() {
	for _, c := range m.collectors() {
		prometheus.Unregister(c)
	}
}

func (m *metrics) ServiceUp(service string) {
	m.upGauge.WithLabelValues(service).Set(1.0)
}

func (m *metrics) ServiceDown(service string) {
	m.upGauge.WithLabelValues(service).Set(0.0)
}

// DeleteService removes the label series owned by a service that is no
// longer part of the active configuration generation, so stale services
// do not linger in the /metrics output.
func (m *metrics) DeleteService(service string) {
	m.upGauge.DeleteLabelValues(service)
	m.checkDurationSeconds.DeletePartialMatch(prometheus.Labels{"service": service})
}

func (m *metrics) MeasureCall(ctx context.Context, service, check string, fn func(ctx context.Context) error) error {
	start := time.Now()

	err := fn(ctx)

	m.checkDurationSeconds.WithLabelValues(service, check).Set(time.Since(start).Seconds())

	return err
}
