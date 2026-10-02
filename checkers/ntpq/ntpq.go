package ntpq

import (
	"context"
	"encoding/json"
	"time"

	"github.com/beevik/ntp"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/runityru/anycastd/checkers"
)

var (
	_ checkers.Checker = (*ntpq)(nil)

	ntpOffset = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "anycastd",
			Name:      "check_last_ntp_offset_ms",
			Help:      "The estimated offset of the local system clock relative to the server's clock",
		},
		[]string{"check", "host"},
	)

	ntpRtt = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "anycastd",
			Name:      "check_last_ntp_rtt_ms",
			Help:      "An estimate of the round-trip-time delay between the client and the server",
		},
		[]string{"check", "host"},
	)

	ntpPacketsSent = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "anycastd",
			Name:      "check_ntp_packets_sent_total",
			Help:      "Total amount of ntp packets sent",
		},
		[]string{"check", "host"},
	)

	ErrOffset = errors.New("Offset is too big")
)

type ntpq struct {
	name            string
	server          string
	srcAddr         string
	tries           uint8
	offsetThreshold time.Duration
	interval        time.Duration
	timeout         time.Duration

	queryFn func(string, ntp.QueryOptions) (*ntp.Response, error)
}

const checkName = "ntpq"

func init() {
	checkers.MustRegister(checkName, NewFromSpec)

	prometheus.MustRegister(ntpOffset)
	prometheus.MustRegister(ntpPacketsSent)
	prometheus.MustRegister(ntpRtt)
}

func New(s spec) (checkers.Checker, error) {
	return NewNamed(checkName, s)
}

// NewNamed builds an NTP checker with an explicit instance identity used in
// logs and the checker-owned Prometheus label sets.
func NewNamed(name string, s spec) (checkers.Checker, error) {
	if err := s.Validate(); err != nil {
		return nil, err
	}

	return &ntpq{
		name:            name,
		server:          s.Server,
		srcAddr:         s.SrcAddr,
		tries:           s.Tries,
		offsetThreshold: s.OffsetThreshold.TimeDuration(),
		interval:        s.Interval.TimeDuration(),
		timeout:         s.Timeout.TimeDuration(),
		queryFn:         ntp.QueryWithOptions,
	}, nil
}

func NewFromSpec(name string, in json.RawMessage) (checkers.Checker, error) {
	s := spec{}
	if err := json.Unmarshal(in, &s); err != nil {
		return nil, err
	}

	return NewNamed(name, s)
}

func (h *ntpq) Kind() string {
	return checkName
}

func (d *ntpq) Check(ctx context.Context) error {
	var lastErr error
	for i := 0; i < int(d.tries); i++ {
		log.WithFields(log.Fields{
			"check":   d.name,
			"attempt": i + 1,
		}).Tracef("running check")

		if err := d.check(ctx); err != nil {
			lastErr = err
			log.WithFields(log.Fields{
				"check":   d.name,
				"attempt": i + 1,
			}).Infof("error received: %s", err)
		} else {
			return nil
		}

		time.Sleep(d.interval)
	}

	if lastErr != nil {
		return errors.Errorf(
			"check failed: %d tries with %s interval; last error: `%s`",
			d.tries, d.interval, lastErr.Error(),
		)
	}
	return nil
}

func (d *ntpq) check(_ context.Context) error {
	// defaut timeout is 5s
	options := ntp.QueryOptions{LocalAddress: d.srcAddr, Timeout: d.timeout}
	response, err := d.queryFn(d.server, options)
	if err != nil {
		return err
	}

	log.WithFields(log.Fields{
		"check": d.name,
	}).Tracef("Offset: %d, RTT: %d, RefID: %d", response.ClockOffset.Milliseconds(), response.RTT.Milliseconds(), response.ReferenceID)
	// since beevik/ntp doesn't do retries by itself we increment just by 1
	ntpPacketsSent.WithLabelValues(d.name, d.server).Add(float64(1))
	ntpOffset.WithLabelValues(d.name, d.server).Set(float64(response.ClockOffset.Milliseconds()))
	ntpRtt.WithLabelValues(d.name, d.server).Set(float64(response.RTT.Milliseconds()))

	if response.ClockOffset.Abs() > d.offsetThreshold {
		return ErrOffset
	}

	return nil

}
