package icmp_ping

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	th "github.com/teran/go-time"
)

func TestSpec(t *testing.T) {
	r := require.New(t)

	data, err := os.ReadFile("testdata/spec.json")
	r.NoError(err)

	c, err := NewFromSpec(checkName, json.RawMessage(data))
	r.NoError(err)

	p := c.(*icmp_ping)
	r.Equal(checkName, p.name)
	r.Equal("127.0.0.33", p.host)
	r.Equal(uint8(5), p.tries)
	r.Equal(100*time.Millisecond, p.interval)
	r.Equal(5*time.Second, p.timeout)
}

func TestCheck(t *testing.T) {
	r := require.New(t)

	c, err := newWithPinger(
		checkName,
		spec{
			Static:   Static{Host: "127.0.0.1"},
			Tries:    10,
			Interval: th.Duration(10 * time.Second),
			Timeout:  th.Duration(30 * time.Second),
		},
		func(host string, tries uint8, interval, timeout time.Duration) (*pingStats, error) {
			r.Equal("127.0.0.1", host)
			r.Equal(uint8(10), tries)
			r.Equal(10*time.Second, interval)
			r.Equal(30*time.Second, timeout)

			return &pingStats{}, nil
		},
	)
	r.NoError(err)

	err = c.Check(context.Background())
	r.NoError(err)
}

// TestCheckMetricsAreIsolatedByName verifies that two instances of the same
// kind probing the same host but built with different names write to
// distinct Prometheus series: a later execution of one instance must never
// overwrite the other instance's gauge values.
func TestCheckMetricsAreIsolatedByName(t *testing.T) {
	r := require.New(t)

	baseSpec := func() spec {
		return spec{
			Static:   Static{Host: "127.0.0.1"},
			Tries:    1,
			Interval: th.Duration(10 * time.Millisecond),
			Timeout:  th.Duration(30 * time.Second),
		}
	}

	first, err := newWithPinger("first-ping", baseSpec(),
		func(host string, tries uint8, interval, timeout time.Duration) (*pingStats, error) {
			return &pingStats{
				PacketsSent: 1,
				PacketsRecv: 1,
				MinRTT:      1 * time.Millisecond,
				MaxRTT:      10 * time.Millisecond,
				AvgRTT:      5 * time.Millisecond,
			}, nil
		},
	)
	r.NoError(err)

	second, err := newWithPinger("second-ping", baseSpec(),
		func(host string, tries uint8, interval, timeout time.Duration) (*pingStats, error) {
			return &pingStats{
				PacketsSent: 1,
				PacketsRecv: 1,
				MinRTT:      2 * time.Millisecond,
				MaxRTT:      20 * time.Millisecond,
				AvgRTT:      15 * time.Millisecond,
			}, nil
		},
	)
	r.NoError(err)

	r.NoError(first.Check(context.Background()))
	r.NoError(second.Check(context.Background()))
	// Running the second instance again must not touch the first one.
	r.NoError(second.Check(context.Background()))

	r.Equal(0.01, testutil.ToFloat64(maxRttSeconds.WithLabelValues("first-ping", "127.0.0.1")))
	r.Equal(0.02, testutil.ToFloat64(maxRttSeconds.WithLabelValues("second-ping", "127.0.0.1")))
	r.Equal(0.005, testutil.ToFloat64(avgRttSeconds.WithLabelValues("first-ping", "127.0.0.1")))
	r.Equal(0.015, testutil.ToFloat64(avgRttSeconds.WithLabelValues("second-ping", "127.0.0.1")))

	// Counters must likewise accumulate per instance rather than on a
	// shared kind-labelled series.
	r.Equal(1.0, testutil.ToFloat64(packetsSent.WithLabelValues("first-ping", "127.0.0.1")))
	r.Equal(2.0, testutil.ToFloat64(packetsSent.WithLabelValues("second-ping", "127.0.0.1")))

	// Legacy unnamed instance keeps the kind itself as the check label.
	legacy, err := New(baseSpec())
	r.NoError(err)
	r.Equal(checkName, legacy.(*icmp_ping).name)
}
