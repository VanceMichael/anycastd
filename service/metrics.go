package service

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type Metrics interface {
	ServiceUp(service string)
	ServiceDown(service string)

	MeasureCall(ctx context.Context, service, check string, fn func(ctx context.Context) error) error
}

type metrics struct {
	appUpGauge           *prometheus.GaugeVec
	upGauge              *prometheus.GaugeVec
	checkDurationSeconds *prometheus.GaugeVec
}

func newMetrics() *metrics {
	return &metrics{
		appUpGauge: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "anycastd",
				Name:      "up",
				Help:      "Application liveness status (must always be 1)",
			},
			[]string{"version"},
		),
		upGauge: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "anycastd",
				Name:      "service_up",
				Help:      "Service liveness status based on checks",
			},
			[]string{"service"},
		),
		checkDurationSeconds: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "anycastd",
				Name:      "check_duration_seconds",
				Help:      "Duration of check execution in seconds",
			},
			[]string{"service", "check"},
		),
	}
}

func NewMetrics(appVersion string) (Metrics, error) {
	m := newMetrics()

	for _, c := range []prometheus.Collector{m.appUpGauge, m.upGauge, m.checkDurationSeconds} {
		if err := prometheus.Register(c); err != nil {
			return nil, err
		}
	}

	m.appUpGauge.WithLabelValues(appVersion).Set(1)

	return m, nil
}

func (m *metrics) ServiceUp(service string) {
	m.upGauge.WithLabelValues(service).Set(1.0)
}

func (m *metrics) ServiceDown(service string) {
	m.upGauge.WithLabelValues(service).Set(0.0)
}

func (m *metrics) MeasureCall(ctx context.Context, service, check string, fn func(ctx context.Context) error) error {
	start := time.Now()

	err := fn(ctx)

	m.checkDurationSeconds.WithLabelValues(service, check).Set(time.Since(start).Seconds())

	return err
}
