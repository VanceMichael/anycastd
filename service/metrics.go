package service

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// CandidateDirection is the direction of a state transition awaiting
// confirmation by consecutive check cycles.
type CandidateDirection int

const (
	// CandidateNone means no transition is pending: the raw result of
	// the current cycle agrees with the committed stable state.
	CandidateNone CandidateDirection = 0
	// CandidateDown means the service is stable up, but keeps failing
	// and may be denounced once the failure threshold is reached.
	CandidateDown CandidateDirection = 1
	// CandidateUp means the service is stable down, but keeps passing
	// and may be announced once the recovery threshold is reached.
	CandidateUp CandidateDirection = 2
)

// StateReport carries the four independently observable state layers
// of a service after a check cycle:
//   - RawUp: checker/strategy result of the current cycle;
//   - StableUp: the committed stable state after debounce thresholds;
//   - Announced: whether the prefixes are actually present in the BGP
//     RIB (flips only on successful Announce/Denounce);
//   - Candidate/Streak: the pending transition direction and how many
//     consecutive cycles confirm it.
type StateReport struct {
	RawUp     bool
	StableUp  bool
	Announced bool

	Candidate CandidateDirection
	Streak    uint32
}

type Metrics interface {
	ObserveState(service string, state StateReport)

	MeasureCall(ctx context.Context, service, check string, fn func(ctx context.Context) error) error
}

type metrics struct {
	appUpGauge *prometheus.GaugeVec

	// Raw per-cycle result reported by checkers and strategy.
	upGauge *prometheus.GaugeVec
	// Committed stable state after debounce thresholds.
	stableUpGauge *prometheus.GaugeVec
	// What is actually announced in the BGP RIB.
	announcedGauge *prometheus.GaugeVec
	// Pending transition direction and consecutive-cycle progress.
	candidateDirectionGauge *prometheus.GaugeVec
	candidateProgressGauge  *prometheus.GaugeVec

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
			Help:      "Raw service health reported by checkers and strategy for the current check cycle (1=up, 0=down)",
		},
		[]string{"service"},
	)

	stableUpGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "anycastd",
			Name:      "service_stable_up",
			Help:      "Committed stable service state after debounce thresholds (1=up, 0=down)",
		},
		[]string{"service"},
	)

	announcedGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "anycastd",
			Name:      "service_announced",
			Help:      "Whether the service prefixes are actually announced in the BGP RIB (1=announced, 0=withdrawn)",
		},
		[]string{"service"},
	)

	candidateDirectionGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "anycastd",
			Name:      "service_candidate_direction",
			Help:      "Pending stable-state transition: 0=none, 1=transition to down awaiting confirmation, 2=transition to up awaiting confirmation",
		},
		[]string{"service"},
	)

	candidateProgressGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "anycastd",
			Name:      "service_candidate_progress",
			Help:      "Number of consecutive check cycles confirming the pending transition direction",
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

	for _, m := range []prometheus.Collector{
		appUpGauge,
		upGauge,
		stableUpGauge,
		announcedGauge,
		candidateDirectionGauge,
		candidateProgressGauge,
		checkDurationSeconds,
	} {
		if err := prometheus.Register(m); err != nil {
			return nil, err
		}
	}

	appUpGauge.WithLabelValues(appVersion).Set(1)

	return &metrics{
		appUpGauge:               appUpGauge,
		upGauge:                  upGauge,
		stableUpGauge:            stableUpGauge,
		announcedGauge:           announcedGauge,
		candidateDirectionGauge:  candidateDirectionGauge,
		candidateProgressGauge:   candidateProgressGauge,
		checkDurationSeconds:     checkDurationSeconds,
	}, nil
}

func boolToFloat(v bool) float64 {
	if v {
		return 1
	}
	return 0
}

func (m *metrics) ObserveState(service string, state StateReport) {
	m.upGauge.WithLabelValues(service).Set(boolToFloat(state.RawUp))
	m.stableUpGauge.WithLabelValues(service).Set(boolToFloat(state.StableUp))
	m.announcedGauge.WithLabelValues(service).Set(boolToFloat(state.Announced))
	m.candidateDirectionGauge.WithLabelValues(service).Set(float64(state.Candidate))
	m.candidateProgressGauge.WithLabelValues(service).Set(float64(state.Streak))
}

func (m *metrics) MeasureCall(ctx context.Context, service, check string, fn func(ctx context.Context) error) error {
	start := time.Now()

	err := fn(ctx)

	m.checkDurationSeconds.WithLabelValues(service, check).Set(time.Since(start).Seconds())

	return err
}
